package raftnode

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pavann19/quorumkv/internal/diskhealth"
	"github.com/pavann19/quorumkv/internal/memberlock"
	"github.com/pavann19/quorumkv/internal/requestlimits"
	"github.com/pavann19/quorumkv/internal/store"
	"github.com/pavann19/quorumkv/internal/wal"
)

// Peer identifies a voting member in the operator-supplied configuration.
type Peer struct {
	ID   string
	Addr string // host:port, also used as the gRPC listen address
}

// Node bundles a running raft.Raft instance with everything needed to
// register it (and the gRPC-based Raft transport) onto a *grpc.Server, and
// the Store the FSM applies committed entries to.
type Node struct {
	Raft                  *raft.Raft
	fsm                   *FSM
	readLogs              raft.LogStore
	readMode              string
	readFence             readFenceCache
	readPhases            [4]readPhaseCounters
	Store                 *store.Store
	transport             *transport.Manager
	logStore              *raftboltdb.BoltStore
	directoryLock         *memberlock.Lock
	kvOnce                sync.Once
	kvServer              *KVServer
	dataDir               string
	diskPolicy            diskhealth.Policy
	requestLimits         requestlimits.Limits
	snapshotFormatVersion uint32
	walFormatVersion      uint32
	commandFormatVersion  uint32
	raftSettings          RaftSettings
	clusterID             string
	diskProbe             func(string) (diskhealth.Space, error)
}

// New opens a plaintext lab node. Empty state requires explicit bootstrap options.
func New(self Peer, peers []Peer, dataDir string, options ...StartOptions) (*Node, error) {
	return newNodeTransport(self, peers, dataDir, nil, insecure.NewCredentials(), options...)
}

// NewAuthenticated uses the same deterministic storage/FSM with an authenticated transport.
func NewAuthenticated(self Peer, peers []Peer, dataDir string, creds credentials.TransportCredentials, options ...StartOptions) (*Node, error) {
	if creds == nil {
		return nil, errors.New("transport credentials required")
	}
	return newNodeTransport(self, peers, dataDir, nil, creds, options...)
}

func newNode(self Peer, peers []Peer, dataDir string, cfg *raft.Config) (_ *Node, resultErr error) {
	return newNodeTransport(self, peers, dataDir, cfg, insecure.NewCredentials(), StartOptions{DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}, Bootstrap: true, BootstrapExpect: len(peers), ClusterID: "internal-test-cluster"})
}

func newNodeTransport(self Peer, peers []Peer, dataDir string, cfg *raft.Config, creds credentials.TransportCredentials, options ...StartOptions) (_ *Node, resultErr error) {
	start, err := validateStart(self, peers, options)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		cfg = start.RaftSettings.config(raft.ServerID(self.ID))
	} else {
		cloned := *cfg
		cfg = &cloned
		cfg.LocalID = raft.ServerID(self.ID)
		cfg.MaxAppendEntries = start.RaftSettings.MaxAppendEntries
	}
	if err := settingsFromRaft(cfg).Validate(); err != nil {
		return nil, err
	}
	if err := raft.ValidateConfig(cfg); err != nil {
		return nil, err
	}
	if err := wal.MakeDurableDir(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("raftnode: creating data dir: %w", err)
	}
	directoryLock, err := memberlock.Acquire(dataDir)
	if err != nil {
		return nil, fmt.Errorf("raftnode: member ownership: %w", err)
	}
	defer func() {
		if resultErr != nil {
			directoryLock.Close()
		}
	}()
	if err := checkFatal(dataDir, start.ClearFatal); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(filepath.Join(dataDir, migrationIncomplete)); !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("raftnode: incomplete migration copy; preserve directory and follow conversion runbook")
	}
	// Validate only after acquiring ownership, before opening any persistent store.
	if err := validateManifest(dataDir, self, peers, start); err != nil {
		return nil, err
	}

	walFormat := wal.Format{Version: start.WALFormatVersion}
	if walFormat.Version == 1 {
		walFormat.ClusterID = start.ClusterID
	}
	st, err := store.OpenWithWALFormat(filepath.Join(dataDir, "store.wal"), start.StoreLimits, walFormat)
	if err != nil {
		if errors.Is(err, wal.ErrCorrupt) {
			markerErr := writeFatal(dataDir, "wal_corruption", 0)
			return nil, errors.Join(ErrFatalState, err, markerErr)
		}
		return nil, fmt.Errorf("raftnode: opening store: %w", err)
	}
	defer func() {
		if resultErr != nil {
			st.Close()
		}
	}()

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("raftnode: opening raft log store: %w", err)
	}
	defer func() {
		if resultErr != nil {
			logStore.Close()
		}
	}()

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raftnode: opening snapshot store: %w", err)
	}

	hasState, err := raft.HasExistingState(logStore, logStore, snapshotStore)
	if err != nil {
		return nil, fmt.Errorf("raftnode: inspect persisted state: %w", err)
	}
	if !hasState && !start.Bootstrap {
		return nil, errors.New("raftnode: empty Raft state; explicit -bootstrap required for a new cluster")
	}
	if err := persistManifest(dataDir, self, peers, start, hasState); err != nil {
		return nil, err
	}

	// Inspect persisted membership without replaying snapshots into the live FSM,
	// starting background work or opening network connections. Clone the config:
	// raft.GetConfiguration mutates its private skipStartup flag.
	if hasState {
		inspection := raft.DefaultConfig()
		if cfg != nil {
			*inspection = *cfg
		}
		inspection.LocalID = raft.ServerID(self.ID)
		inspection.NoSnapshotRestoreOnStart = true
		_, probeTransport := raft.NewInmemTransport(raft.ServerAddress(self.Addr))
		actual, inspectErr := inspectConfiguration(inspection, logStore, logStore, snapshotStore, probeTransport)
		probeTransport.Close()
		if inspectErr != nil {
			return nil, fmt.Errorf("raftnode: inspect membership: %w", inspectErr)
		}
		if !sameConfiguration(actual, peers) {
			return nil, errors.New("raftnode: configured peers differ from persisted Raft membership")
		}
	}

	// The store WAL is an independently verified durability copy, not a replay
	// cursor. Rebuild the FSM from Raft snapshot + committed log, starting empty,
	// so historical capacity decisions see their original state rather than the
	// latest preloaded WAL state. Existing corrupt WALs were refused above.
	if !hasState && st.Usage().Entries != 0 {
		return nil, errors.New("raftnode: nonempty store without Raft state; explicit recovery required")
	}
	if err := st.Restore(nil); err != nil {
		return nil, fmt.Errorf("raftnode: initialize Raft replay: %w", err)
	}

	tm := transport.New(raft.ServerAddress(self.Addr), []grpc.DialOption{grpc.WithTransportCredentials(creds), grpc.WithChainUnaryInterceptor(peerUnaryClient(start.ClusterID, start.SnapshotFormatVersion, start.CommandFormatVersion)), grpc.WithChainStreamInterceptor(peerStreamClient(start.ClusterID, start.SnapshotFormatVersion, start.CommandFormatVersion)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(requestlimits.MaxMessageBytes), grpc.MaxCallSendMsgSize(requestlimits.MaxMessageBytes))})
	defer func() {
		if resultErr != nil {
			tm.Close()
		}
	}()

	// cfg was cloned, bounded and validated before any member storage was opened.

	fsm := &FSM{Store: st, clusterID: start.ClusterID, snapshotFormatVersion: start.SnapshotFormatVersion, commandFormatVersion: start.CommandFormatVersion, fatal: func(reason string, index uint64) { fatalStop(dataDir, reason, index) }}
	r, err := raft.NewRaft(cfg, fsm, logStore, logStore, snapshotStore, tm.Transport())
	if err != nil {
		return nil, fmt.Errorf("raftnode: starting raft: %w", err)
	}

	servers := make([]raft.Server, len(peers))
	for i, p := range peers {
		servers[i] = raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Addr)}
	}
	if !hasState {
		if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
			r.Shutdown().Error()
			return nil, fmt.Errorf("raftnode: bootstrap: %w", err)
		}
	} else {
		actual := r.GetConfiguration()
		if err := actual.Error(); err != nil {
			r.Shutdown().Error()
			return nil, err
		}
		if !sameConfiguration(actual.Configuration(), peers) {
			r.Shutdown().Error()
			return nil, errors.New("raftnode: configured peers differ from persisted Raft membership")
		}
	}

	return &Node{Raft: r, fsm: fsm, readLogs: logStore, readMode: start.ReadMode, Store: st, transport: tm, logStore: logStore, directoryLock: directoryLock, dataDir: dataDir, diskPolicy: start.DiskPolicy, diskProbe: diskhealth.Probe, requestLimits: start.RequestLimits, snapshotFormatVersion: start.SnapshotFormatVersion, walFormatVersion: start.WALFormatVersion, commandFormatVersion: start.CommandFormatVersion, raftSettings: settingsFromRaft(cfg), clusterID: start.ClusterID}, nil
}

// RegisterOn attaches the Raft gRPC transport's service handlers to s. The
// caller is responsible for also registering the KV service (internal/kv)
// on the same *grpc.Server and starting it.
func (n *Node) RegisterOn(s *grpc.Server) {
	n.transport.Register(peerRegistrar{registrar: s, clusterID: n.clusterID, format: n.snapshotFormatVersion, commandFormat: n.commandFormatVersion})
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
// applied (via FSM.Apply) on this node, or an error. The timeout bounds
// enqueueing, not commitment. It must only be called on the leader;
// a non-leader returns raft.ErrNotLeader.
func (n *Node) apply(cmd command, timeout time.Duration) error {
	cmd.Limits = n.Store.Limits()
	data, err := encodeCommandFormat(cmd, n.clusterID, n.commandFormatVersion)
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
	if err := n.admitDisk(); err != nil {
		return err
	}
	release, err := n.Store.ReservePut(key, value)
	if err != nil {
		return err
	}
	defer release()
	return n.apply(command{Op: opPut, Key: key, Value: value}, timeout)
}

// Delete replicates a Delete through Raft.
func (n *Node) Delete(key []byte, timeout time.Duration) error {
	if err := n.admitDisk(); err != nil {
		return err
	}
	return n.apply(command{Op: opDelete, Key: key}, timeout)
}

// Get reads directly from this node's local store -- a leader-only-read
// barrier is enforced by the caller (internal/raftnode.KVServer), not here;
// Get itself is just a local lookup regardless of leadership, since a
// stale-follower-read mode (explicitly opt-in, per the build plan) may
// want to call it on a follower too.
func (n *Node) Get(key []byte) ([]byte, bool) {
	return n.Store.Get(key)
}

// Shutdown stops the Raft instance and closes the local store.
func (n *Node) Shutdown() error {
	err := n.Raft.Shutdown().Error()
	if n.transport != nil {
		err = errors.Join(err, n.transport.Close())
	}
	err = errors.Join(err, n.Store.Close())
	if n.logStore != nil {
		err = errors.Join(err, n.logStore.Close())
	}
	if n.directoryLock != nil {
		err = errors.Join(err, n.directoryLock.Close())
	}
	return err
}

func (n *Node) RequestLimits() requestlimits.Limits {
	if n.requestLimits == (requestlimits.Limits{}) {
		return requestlimits.Defaults()
	}
	return n.requestLimits
}

// ClusterID is the operator-assigned identity used by this member transport.
func (n *Node) ClusterID() string { return n.clusterID }

func (n *Node) SnapshotFormatVersion() uint32 { return n.snapshotFormatVersion }

func (n *Node) WALFormatVersion() uint32 { return n.walFormatVersion }

func (n *Node) CommandFormatVersion() uint32 { return n.commandFormatVersion }
func (n *Node) PeerProtocolVersion() string  { return peerProtocolFor(n.commandFormatVersion) }
