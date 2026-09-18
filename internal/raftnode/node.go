package raftnode

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pavann19/quorumkv/internal/store"
)

// Peer is one member of the static cluster configuration every node
// bootstraps with. Per hashicorp/raft's own documented pattern for a
// fixed-membership cluster, every node calls BootstrapCluster with an
// identical peer list at startup; it is a no-op on any node that already
// has log entries, so it's safe to call unconditionally on every restart.
type Peer struct {
	ID   string
	Addr string // host:port, also used as the gRPC listen address
}

// Node bundles a running raft.Raft instance with everything needed to
// register it (and the gRPC-based Raft transport) onto a *grpc.Server, and
// the Store the FSM applies committed entries to.
type Node struct {
	Raft      *raft.Raft
	Store     *store.Store
	transport *transport.Manager
}

// New bootstraps a Raft node: opens (or replays) its local store, opens
// its Raft log/stable/snapshot stores under dataDir, wires up the gRPC
// transport for the given self peer, and bootstraps the cluster with
// peers (a no-op if this node's log is already non-empty, e.g. on
// restart). It does not start serving gRPC -- call Register on the
// returned Node's transport via RegisterOn before starting the server.
func New(self Peer, peers []Peer, dataDir string) (*Node, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("raftnode: creating data dir: %w", err)
	}

	st, err := store.Open(filepath.Join(dataDir, "store.wal"))
	if err != nil {
		return nil, fmt.Errorf("raftnode: opening store: %w", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("raftnode: opening raft log store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raftnode: opening snapshot store: %w", err)
	}

	// No TLS at this milestone -- see docs/DECISIONS.md ("Raft transport has
	// no TLS yet").
	tm := transport.New(raft.ServerAddress(self.Addr), []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())})

	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(self.ID)

	fsm := &FSM{Store: st}
	r, err := raft.NewRaft(cfg, fsm, logStore, logStore, snapshotStore, tm.Transport())
	if err != nil {
		return nil, fmt.Errorf("raftnode: starting raft: %w", err)
	}

	servers := make([]raft.Server, len(peers))
	for i, p := range peers {
		servers[i] = raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Addr)}
	}
	// Idempotent: only takes effect if this node's log is empty. Safe to
	// call on every startup, including after a restart with existing state.
	r.BootstrapCluster(raft.Configuration{Servers: servers})

	return &Node{Raft: r, Store: st, transport: tm}, nil
}

// RegisterOn attaches the Raft gRPC transport's service handlers to s. The
// caller is responsible for also registering the KV service (internal/kv)
// on the same *grpc.Server and starting it.
func (n *Node) RegisterOn(s *grpc.Server) {
	n.transport.Register(s)
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	return n.Raft.State() == raft.Leader
}

// LeaderAddr returns the current leader's address as this node's Raft
// instance understands it (which may be empty during an election).
func (n *Node) LeaderAddr() string {
	addr, _ := n.Raft.LeaderWithID()
	return string(addr)
}

// Apply submits cmd to the Raft log and waits for it to be committed and
// applied (via FSM.Apply) on this node, or for timeout/an error. It must
// only be called on the leader; a non-leader returns raft.ErrNotLeader.
func (n *Node) apply(cmd command, timeout time.Duration) error {
	data, err := encodeCommand(cmd)
	if err != nil {
		return err
	}
	future := n.Raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return err
	}
	if fsmErr, ok := future.Response().(error); ok && fsmErr != nil {
		return fsmErr
	}
	return nil
}

// Put replicates a Put through Raft.
func (n *Node) Put(key, value []byte, timeout time.Duration) error {
	return n.apply(command{Op: opPut, Key: key, Value: value}, timeout)
}

// Delete replicates a Delete through Raft.
func (n *Node) Delete(key []byte, timeout time.Duration) error {
	return n.apply(command{Op: opDelete, Key: key}, timeout)
}

// Get reads directly from this node's local store -- a leader-only-read
// policy is enforced by the caller (internal/raftnode.KVServer), not here;
// Get itself is just a local lookup regardless of leadership, since a
// stale-follower-read mode (explicitly opt-in, per the build plan) may
// want to call it on a follower too.
func (n *Node) Get(key []byte) ([]byte, bool) {
	return n.Store.Get(key)
}

// Shutdown stops the Raft instance and closes the local store.
func (n *Node) Shutdown() error {
	if err := n.Raft.Shutdown().Error(); err != nil {
		return fmt.Errorf("raftnode: shutting down raft: %w", err)
	}
	return n.Store.Close()
}
