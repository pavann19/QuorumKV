// Package cluster is M1's exit criterion: "a real 3-node cluster ...
// kill the leader, assert a new leader is elected and the cluster keeps
// accepting writes." Each node runs as a real, separate OS process (the
// same pattern already proven in test/crashrecovery), not an in-process
// goroutine cluster -- Raft doesn't care whether peers are processes or
// containers, and this needed no Docker (see docs/DECISIONS.md for why
// Docker Compose, which the original build plan named, was swapped for
// this instead).
package cluster

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "quorumkv-node-bin-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	binPath = filepath.Join(dir, "quorumkv-node.exe")
	build := exec.Command("go", "build", "-o", binPath, "../../cmd/quorumkv-node")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building quorumkv-node: %v\n%s\n", err, out)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

type clusterNode struct {
	id   string
	addr string
	cmd  *exec.Cmd
}

func startCluster(t *testing.T, n int, basePort int) []*clusterNode {
	t.Helper()

	peerSpec := ""
	nodes := make([]*clusterNode, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node%d", i+1)
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		nodes[i] = &clusterNode{id: id, addr: addr}
		if i > 0 {
			peerSpec += ","
		}
		peerSpec += id + "=" + addr
	}

	dataRoot := t.TempDir()
	for _, node := range nodes {
		dataDir := filepath.Join(dataRoot, node.id)
		cmd := exec.Command(binPath, "-id="+node.id, "-peers="+peerSpec, "-data-dir="+dataDir)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("starting %s: %v", node.id, err)
		}
		node.cmd = cmd
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			if node.cmd.Process != nil {
				_ = node.cmd.Process.Kill()
				_ = node.cmd.Wait()
			}
		}
	})

	return nodes
}

func dial(t *testing.T, addr string) pb.KVClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewKVClient(conn)
}

// findLeader polls every node's Put RPC (a cheap, real leadership probe:
// only the leader accepts writes) until exactly one accepts, or the
// timeout elapses.
func findLeader(t *testing.T, nodes []*clusterNode, timeout time.Duration) (*clusterNode, pb.KVClient) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.cmd.ProcessState != nil {
				continue // already exited (e.g. the one we killed)
			}
			client := dial(t, node.addr)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := client.Put(ctx, &pb.PutRequest{Key: []byte("__leader_probe__"), Value: []byte("x")})
			cancel()
			if err == nil {
				return node, client
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil, nil
}

func TestCluster_LeaderElectionAndReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process cluster test in -short mode")
	}

	nodes := startCluster(t, 3, 17001)
	leaderNode, leaderClient := findLeader(t, nodes, 20*time.Second)
	t.Logf("elected leader: %s", leaderNode.id)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := leaderClient.Put(ctx, &pb.PutRequest{Key: []byte("k1"), Value: []byte("v1")}); err != nil {
		t.Fatalf("Put on leader failed: %v", err)
	}

	// Give replication a moment, then confirm a Get on the leader sees it
	// (Get is leader-only per internal/raftnode.KVServer, so this also
	// exercises that path, not just Put).
	time.Sleep(500 * time.Millisecond)
	getCtx, getCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer getCancel()
	resp, err := leaderClient.Get(getCtx, &pb.GetRequest{Key: []byte("k1")})
	if err != nil {
		t.Fatalf("Get on leader failed: %v", err)
	}
	if !resp.Found || string(resp.Value) != "v1" {
		t.Fatalf("expected k1=v1 on leader, got found=%v value=%q", resp.Found, resp.Value)
	}
}

func TestCluster_KillLeader_NewLeaderElectedAndClusterStaysWritable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-process cluster test in -short mode")
	}

	nodes := startCluster(t, 3, 17011)
	firstLeader, firstClient := findLeader(t, nodes, 20*time.Second)
	t.Logf("initial leader: %s", firstLeader.id)

	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := firstClient.Put(putCtx, &pb.PutRequest{Key: []byte("before-kill"), Value: []byte("v1")}); err != nil {
		t.Fatalf("Put before kill failed: %v", err)
	}
	putCancel()

	t.Logf("killing leader %s (pid %d)", firstLeader.id, firstLeader.cmd.Process.Pid)
	if err := firstLeader.cmd.Process.Kill(); err != nil {
		t.Fatalf("killing leader: %v", err)
	}
	_ = firstLeader.cmd.Wait()

	remaining := make([]*clusterNode, 0, len(nodes)-1)
	for _, n := range nodes {
		if n.id != firstLeader.id {
			remaining = append(remaining, n)
		}
	}

	newLeader, newClient := findLeader(t, remaining, 30*time.Second)
	if newLeader.id == firstLeader.id {
		t.Fatal("expected a different node to become leader after the kill")
	}
	t.Logf("new leader after kill: %s", newLeader.id)

	writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer writeCancel()
	if _, err := newClient.Put(writeCtx, &pb.PutRequest{Key: []byte("after-kill"), Value: []byte("v2")}); err != nil {
		t.Fatalf("expected the cluster to keep accepting writes after the leader was killed, got: %v", err)
	}

	getCtx, getCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer getCancel()
	resp, err := newClient.Get(getCtx, &pb.GetRequest{Key: []byte("before-kill")})
	if err != nil {
		t.Fatalf("Get for pre-kill write on new leader failed: %v", err)
	}
	if !resp.Found || string(resp.Value) != "v1" {
		t.Fatalf("expected the write committed before the kill to have replicated, got found=%v value=%q", resp.Found, resp.Value)
	}
}
