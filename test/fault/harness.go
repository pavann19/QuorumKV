// Package fault is QuorumKV's M2 exit criterion: a fault-injection harness
// that partitions links, kills/restarts nodes, and delays traffic against
// a real running cluster, recording every client operation's start time,
// end time, and result, then checking the recorded history against a real
// linearizability checker (Porcupine) -- not just asserting the cluster
// "seems fine."
//
// Every inter-node Raft RPC is routed through a per-directed-edge
// faultproxy.DirectedEdge (see internal/faultproxy and docs/DECISIONS.md
// for why: no Docker/iptables available, and per-node-perspective peer
// addressing gives independent control over each direction of each link
// without needing source-port fingerprinting).
package fault

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pavann19/quorumkv/internal/faultproxy"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

var binPath string

// Build compiles cmd/quorumkv-node once per test binary run. Safe to call
// from any package (e.g. test/fault's own tests, or bench, which sits at a
// different directory depth) since the source path is resolved relative to
// this file's own location via runtime.Caller, not the caller's working
// directory.
func Build() error {
	if binPath != "" {
		return nil
	}
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("faultharness: could not determine source location")
	}
	// thisFile is .../quorumkv/test/fault/harness.go; cmd/quorumkv-node is
	// two levels up from test/fault.
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	srcDir := filepath.Join(repoRoot, "cmd", "quorumkv-node")

	dir, err := os.MkdirTemp("", "quorumkv-node-bin-*")
	if err != nil {
		return err
	}
	p := filepath.Join(dir, "quorumkv-node.exe")
	cmd := exec.Command("go", "build", "-o", p, srcDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("building quorumkv-node: %w\n%s", err, out)
	}
	binPath = p
	return nil
}

// Node is one cluster member: a real quorumkv-node subprocess plus the
// directed edges other nodes use to reach it (edgesIn) and it uses to
// reach others (edgesOut).
type Node struct {
	ID       string
	RealAddr string
	cmd      *exec.Cmd
}

// Cluster is a running, real, multi-process QuorumKV cluster with a
// faultproxy edge on every directed pair of nodes.
type Cluster struct {
	Nodes []*Node
	// Edges[from][to] is the proxy carrying traffic FROM node "from" TO
	// node "to" -- i.e. it's part of "from"'s outbound view of "to".
	Edges map[string]map[string]*faultproxy.DirectedEdge
	// DataRoot is the shared temp directory Start laid every node's own
	// data directory under (DataRoot/<id>) -- exposed so a scenario can
	// restart a node against the exact same data directory it started
	// with, replaying its real WAL/Raft log rather than starting fresh.
	DataRoot string

	t *testing.T
}

// Start launches n real quorumkv-node processes and n*(n-1) directed edge
// proxies between them. Each node is bootstrapped with its OWN local view
// of every peer's address (see docs/DECISIONS.md), so cutting the edge
// from node A to node B affects only A's ability to reach B, not anyone
// else's.
func Start(t *testing.T, n int, basePort int) *Cluster {
	t.Helper()
	if err := Build(); err != nil {
		t.Fatal(err)
	}

	ids := make([]string, n)
	realAddrs := make(map[string]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("node%d", i+1)
		realAddrs[ids[i]] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}

	edges := make(map[string]map[string]*faultproxy.DirectedEdge, n)
	for _, from := range ids {
		edges[from] = make(map[string]*faultproxy.DirectedEdge, n-1)
		for _, to := range ids {
			if from == to {
				continue
			}
			edge, err := faultproxy.NewDirectedEdge("127.0.0.1:0", realAddrs[to])
			if err != nil {
				t.Fatalf("starting edge %s->%s: %v", from, to, err)
			}
			edges[from][to] = edge
		}
	}
	t.Cleanup(func() {
		for _, m := range edges {
			for _, e := range m {
				e.Close()
			}
		}
	})

	dataRoot := t.TempDir()
	nodes := make([]*Node, n)
	for i, id := range ids {
		peerSpec := ""
		for _, other := range ids {
			if peerSpec != "" {
				peerSpec += ","
			}
			if other == id {
				peerSpec += other + "=" + realAddrs[other]
			} else {
				peerSpec += other + "=" + edges[id][other].ListenAddr
			}
		}
		dataDir := filepath.Join(dataRoot, id)
		cmd := exec.Command(binPath, "-id="+id, "-peers="+peerSpec, "-data-dir="+dataDir)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("starting %s: %v", id, err)
		}
		nodes[i] = &Node{ID: id, RealAddr: realAddrs[id], cmd: cmd}
	}

	c := &Cluster{Nodes: nodes, Edges: edges, DataRoot: dataRoot, t: t}
	t.Cleanup(func() {
		for _, n := range c.Nodes {
			if n.cmd.Process != nil && n.cmd.ProcessState == nil {
				_ = n.cmd.Process.Kill()
				_ = n.cmd.Wait()
			}
		}
	})
	return c
}

// Kill abruptly terminates a node's process (leaving its edges intact --
// a dead process behind a live proxy simply won't accept new connections,
// which is what actually happens).
func (c *Cluster) Kill(id string) {
	for _, n := range c.Nodes {
		if n.ID == id && n.cmd.Process != nil && n.cmd.ProcessState == nil {
			_ = n.cmd.Process.Kill()
			_ = n.cmd.Wait()
			return
		}
	}
}

// Restart starts a fresh process for a previously-killed node, reusing its
// existing data directory (so it replays its own Raft log/WAL, exactly as
// a real restarted node would).
func (c *Cluster) Restart(id string, dataDir string, peerSpec string) {
	c.t.Helper()
	for _, n := range c.Nodes {
		if n.ID != id {
			continue
		}
		cmd := exec.Command(binPath, "-id="+id, "-peers="+peerSpec, "-data-dir="+dataDir)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			c.t.Fatalf("restarting %s: %v", id, err)
		}
		n.cmd = cmd
	}
}

// Partition fully cuts node id off from every other node, in both
// directions -- the standard "leader/minority partition" fault.
func (c *Cluster) Partition(id string) {
	for from, m := range c.Edges {
		for to, e := range m {
			if from == id || to == id {
				e.Cut()
			}
		}
	}
}

// Heal restores every edge to/from id.
func (c *Cluster) Heal(id string) {
	for from, m := range c.Edges {
		for to, e := range m {
			if from == id || to == id {
				e.Restore()
			}
		}
	}
}

// SetDelay adds latency to every edge in the cluster.
func (c *Cluster) SetDelay(d time.Duration) {
	for _, m := range c.Edges {
		for _, e := range m {
			e.SetDelay(d)
		}
	}
}

// Client dials node id's real (client-facing) address directly -- test
// clients are external users, not cluster members, so they are never
// routed through the inter-node faultproxy edges.
func (c *Cluster) Client(t *testing.T, id string) pb.KVClient {
	t.Helper()
	for _, n := range c.Nodes {
		if n.ID == id {
			conn, err := grpc.NewClient(n.RealAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatalf("dialing %s: %v", id, err)
			}
			t.Cleanup(func() { conn.Close() })
			return pb.NewKVClient(conn)
		}
	}
	t.Fatalf("no such node %q", id)
	return nil
}

// FindLeader polls every node's Put RPC (only the leader accepts writes)
// until exactly one accepts, restricted to the given candidate node IDs
// (e.g. only the majority side of a partition).
func (c *Cluster) FindLeader(t *testing.T, candidates []string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range candidates {
			client := c.Client(t, id)
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err := client.Put(ctx, &pb.PutRequest{Key: []byte("__leader_probe__"), Value: []byte("x")})
			cancel()
			if err == nil {
				return id
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("no leader elected among %v within %s", candidates, timeout)
	return ""
}

// AllIDs returns every node ID in the cluster.
func (c *Cluster) AllIDs() []string {
	ids := make([]string, len(c.Nodes))
	for i, n := range c.Nodes {
		ids[i] = n.ID
	}
	return ids
}

// --- Workload + history recording ---

// Op is one recorded client operation, in the shape written to the
// committed history JSON files (test/fault/results/*.json).
type Op struct {
	ClientID int    `json:"client_id"`
	Kind     string `json:"kind"` // "put" or "get"
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	CallNS   int64  `json:"call_ns"`
	ReturnNS int64  `json:"return_ns"`
	Found    bool   `json:"found,omitempty"`
	GotValue string `json:"got_value,omitempty"`
	Err      string `json:"err,omitempty"`
}

// retryingCall issues fn against a rotating set of node clients, retrying
// on any error (an unreachable/non-leader node) until it succeeds or
// overallDeadline passes. This is what makes the workload realistic: a
// real client facing a partition or a leader crash retries against a
// different node, exactly as internal/raftnode.KVServer's "not leader"
// error is meant to prompt.
func retryingCall(clients []pb.KVClient, overallDeadline time.Time, fn func(pb.KVClient) error) error {
	var lastErr error
	i := 0
	for time.Now().Before(overallDeadline) {
		err := fn(clients[i%len(clients)])
		if err == nil {
			return nil
		}
		lastErr = err
		i++
		time.Sleep(50 * time.Millisecond)
	}
	return lastErr
}

// RunWorkload runs numClients concurrent "clients," each issuing opsPer
// operations (randomly Put or Get, over the given keys) against the
// cluster, retrying through partitions/leader changes, and returns the
// full recorded history in call order (safe for Porcupine, which only
// needs Call/Return timestamps, not slice order).
func RunWorkload(cluster *Cluster, candidateIDs []string, numClients, opsPer int, keys []string, perOpTimeout time.Duration) []Op {
	clients := make([]pb.KVClient, len(candidateIDs))
	for i, id := range candidateIDs {
		clients[i] = cluster.Client(cluster.t, id)
	}

	var mu sync.Mutex
	var history []Op
	var counter int64

	var wg sync.WaitGroup
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			for i := 0; i < opsPer; i++ {
				n := atomic.AddInt64(&counter, 1)
				key := keys[int(n)%len(keys)]
				isPut := n%2 == 0

				op := Op{ClientID: clientID, Key: key}
				op.CallNS = time.Now().UnixNano()
				deadline := time.Now().Add(perOpTimeout)

				if isPut {
					value := fmt.Sprintf("v%d", n)
					op.Kind = "put"
					op.Value = value
					err := retryingCall(clients, deadline, func(cl pb.KVClient) error {
						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						defer cancel()
						_, err := cl.Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte(value)})
						return err
					})
					if err != nil {
						op.Err = err.Error()
					}
				} else {
					op.Kind = "get"
					err := retryingCall(clients, deadline, func(cl pb.KVClient) error {
						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						defer cancel()
						resp, err := cl.Get(ctx, &pb.GetRequest{Key: []byte(key)})
						if err != nil {
							return err
						}
						op.Found = resp.Found
						op.GotValue = string(resp.Value)
						return nil
					})
					if err != nil {
						op.Err = err.Error()
					}
				}

				op.ReturnNS = time.Now().UnixNano()
				mu.Lock()
				history = append(history, op)
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()
	return history
}

// WriteHistory commits the recorded history to test/fault/results/<name>.json
// -- a real run's raw output, per the same "committed run output, not a
// hand-typed number" discipline used elsewhere in this portfolio.
func WriteHistory(name string, history []Op, linearizable bool) error {
	if err := os.MkdirAll("results", 0o755); err != nil {
		return err
	}
	out := struct {
		Scenario     string    `json:"scenario"`
		Timestamp    time.Time `json:"timestamp"`
		Linearizable bool      `json:"linearizable"`
		OpCount      int       `json:"op_count"`
		Ops          []Op      `json:"ops"`
	}{
		Scenario:     name,
		Timestamp:    time.Now().UTC(),
		Linearizable: linearizable,
		OpCount:      len(history),
		Ops:          history,
	}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join("results", name+".json"), data, 0o644)
}
