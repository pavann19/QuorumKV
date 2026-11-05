package raftnode

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pavann19/quorumkv/internal/store"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

type gatedReadFSM struct {
	*FSM
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *gatedReadFSM) Apply(log *raft.Log) any {
	cmd, err := decodeCommand(log.Data)
	if err == nil && string(cmd.Key) == "blocked" {
		f.once.Do(func() { close(f.entered); <-f.release })
	}
	return f.FSM.Apply(log)
}

// Preserve the deliberately blocked Apply oracle when the embedded FSM also
// implements the optional batching interface.
func (f *gatedReadFSM) ApplyBatch(logs []*raft.Log) []any {
	responses := make([]any, len(logs))
	for i, log := range logs {
		if log.Type == raft.LogCommand {
			responses[i] = f.Apply(log)
		}
	}
	return responses
}

func readFixture(t *testing.T, mode string, gate bool) (*Node, *gatedReadFSM) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "store.wal"))
	if err != nil {
		t.Fatal(err)
	}
	fsm := &FSM{Store: st}
	gated := &gatedReadFSM{FSM: fsm, entered: make(chan struct{}), release: make(chan struct{})}
	var actual raft.FSM = fsm
	if gate {
		actual = gated
	}
	addr, tr := raft.NewInmemTransport("")
	cfg := raft.DefaultConfig()
	cfg.LocalID = "read-fixture"
	cfg.LogOutput = io.Discard
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 25 * time.Millisecond
	logs := raft.NewInmemStore()
	r, err := raft.NewRaft(cfg, actual, logs, logs, raft.NewInmemSnapshotStore(), tr)
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{Raft: r, Store: st, fsm: fsm, readLogs: logs, readMode: mode}
	t.Cleanup(func() { r.Shutdown().Error(); st.Close(); tr.Close() })
	if err := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{ID: cfg.LocalID, Address: addr, Suffrage: raft.Voter}}}).Error(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n.IsLeader() && r.CommitIndex() >= 2 {
			return n, gated
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("fixture leader did not commit its current-term entry")
	return nil, nil
}
func readApply(t *testing.T, n *Node, key, value string) raft.ApplyFuture {
	t.Helper()
	data, err := encodeCommand(command{Op: opPut, Key: []byte(key), Value: []byte(value), Limits: n.Store.Limits()})
	if err != nil {
		t.Fatal(err)
	}
	return n.Raft.Apply(data, time.Second)
}
func TestQuorumReadsDoNotAppendLogs(t *testing.T) {
	for _, mode := range []string{"quorum", "barrier"} {
		t.Run(mode, func(t *testing.T) {
			n, _ := readFixture(t, mode, false)
			if err := readApply(t, n, "key", "ack").Error(); err != nil {
				t.Fatal(err)
			}
			before := n.Raft.LastIndex()
			beforeWAL := n.Store.WALStats()
			for i := 0; i < 32; i++ {
				result, err := NewKVServer(n).Get(context.Background(), &pb.GetRequest{Key: []byte("key")})
				if err != nil || !result.GetFound() || string(result.GetValue()) != "ack" {
					t.Fatal("readback", result, err)
				}
			}
			growth := n.Raft.LastIndex() - before
			if mode == "quorum" && growth != 0 {
				t.Fatalf("read-only load appended %d entries", growth)
			}
			if mode == "barrier" && growth != 32 {
				t.Fatalf("control expected 32 barriers, got %d", growth)
			}
			if n.Store.WALStats() != beforeWAL {
				t.Fatal("read changed store WAL")
			}
		})
	}
}
func TestQuorumReadWaitsForCompletedFSM(t *testing.T) {
	n, gate := readFixture(t, "quorum", true)
	released := false
	defer func() {
		if !released {
			close(gate.release)
		}
	}()
	future := readApply(t, n, "blocked", "new")
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("FSM was not blocked")
	}
	deadline := time.Now().Add(time.Second)
	for n.Raft.AppliedIndex() < future.Index() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n.Raft.AppliedIndex() < future.Index() {
		t.Fatal("fixture did not dispatch blocked apply")
	}
	if n.fsm.lastCommandIndex.Load() >= future.Index() {
		t.Fatal("blocked command reported complete")
	}
	before := n.Raft.LastIndex()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	_, err := NewKVServer(n).Get(ctx, &pb.GetRequest{Key: []byte("blocked")})
	cancel()
	if err == nil {
		t.Fatal("dispatched but unfinished command allowed stale read")
	}
	type result struct {
		response *pb.GetResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		r, e := NewKVServer(n).Get(context.Background(), &pb.GetRequest{Key: []byte("blocked")})
		done <- result{r, e}
	}()
	select {
	case <-done:
		t.Fatal("read escaped blocked apply")
	case <-time.After(20 * time.Millisecond):
	}
	close(gate.release)
	released = true
	if err := future.Error(); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-done:
		if r.err != nil || !r.response.GetFound() || string(r.response.GetValue()) != "new" {
			t.Fatal("completed readback", r)
		}
	case <-time.After(time.Second):
		t.Fatal("completed apply did not wake read")
	}
	if n.Raft.LastIndex() != before {
		t.Fatal("quorum wait appended a log")
	}
	phases := n.ReadPhaseStats()
	if phases[2].Count != 1 || phases[2].MaxNS < 20_000_000 {
		t.Fatal("phase diagnostics did not observe completed-application wait", phases[2])
	}
}
func TestQuorumReadAfterCompactedSnapshot(t *testing.T) {
	n, _ := readFixture(t, "quorum", false)
	if err := readApply(t, n, "key", "snapshot").Error(); err != nil {
		t.Fatal(err)
	}
	if err := n.Raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	// Simulate the adopted compactor removing the already-snapshotted prefix.
	if err := n.readLogs.DeleteRange(1, n.Raft.CommitIndex()); err != nil {
		t.Fatal(err)
	}
	response, err := NewKVServer(n).Get(context.Background(), &pb.GetRequest{Key: []byte("key")})
	if err != nil || string(response.GetValue()) != "snapshot" {
		t.Fatal(response, err)
	}
}
func TestCompletedCommandNotificationCancellationAndWake(t *testing.T) {
	f := &FSM{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if f.waitCommand(ctx, 0) == nil {
		t.Fatal("canceled wait succeeded")
	}
	done := make(chan error, 1)
	go func() { done <- f.waitCommand(context.Background(), 7) }()
	f.publishCommandIndex(6)
	select {
	case <-done:
		t.Fatal("wait escaped before its index")
	case <-time.After(10 * time.Millisecond):
	}
	f.publishCommandIndex(7)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("missed completion notification")
	}
}

type oldTermReadLogs struct{ raft.LogStore }

func (l oldTermReadLogs) GetLog(index uint64, log *raft.Log) error {
	if err := l.LogStore.GetLog(index, log); err != nil {
		return err
	}
	log.Term = 0
	return nil
}
func TestQuorumReadRequiresCurrentTermCommitment(t *testing.T) {
	n, _ := readFixture(t, "quorum", false)
	n.readLogs = oldTermReadLogs{n.readLogs}
	before := n.Raft.LastIndex()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, err := NewKVServer(n).Get(ctx, &pb.GetRequest{Key: []byte("key")}); err == nil {
		t.Fatal("leader confirmation alone admitted read without current-term commit")
	}
	if n.Raft.LastIndex() != before {
		t.Fatal("refused read appended log")
	}
}
func TestQuorumReadRejectsShutdownDuringApplyWait(t *testing.T) {
	n, gate := readFixture(t, "quorum", true)
	released := false
	defer func() {
		if !released {
			close(gate.release)
		}
	}()
	_ = readApply(t, n, "blocked", "new")
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("apply not gated")
	}
	done := make(chan error, 1)
	go func() {
		_, err := NewKVServer(n).Get(context.Background(), &pb.GetRequest{Key: []byte("blocked")})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatal("read escaped apply", err)
	case <-time.After(20 * time.Millisecond):
	}
	stopped := n.Raft.Shutdown()
	close(gate.release)
	released = true
	if err := stopped.Error(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("lost leader state returned a value")
		}
	case <-time.After(time.Second):
		t.Fatal("read did not terminate")
	}
}

type pendingTermReadLogs struct {
	raft.LogStore
	pending atomic.Bool
}

func (l *pendingTermReadLogs) GetLog(index uint64, log *raft.Log) error {
	if err := l.LogStore.GetLog(index, log); err != nil {
		return err
	}
	if l.pending.Load() {
		log.Term = 0
	}
	return nil
}
func TestQuorumReadWaitsForCurrentTermCommitment(t *testing.T) {
	n, _ := readFixture(t, "quorum", false)
	if err := readApply(t, n, "key", "ack").Error(); err != nil {
		t.Fatal(err)
	}
	logs := &pendingTermReadLogs{LogStore: n.readLogs}
	logs.pending.Store(true)
	n.readLogs = logs
	before := n.Raft.LastIndex()
	type result struct {
		response *pb.GetResponse
		err      error
	}
	done := make(chan result, 1)
	go func() {
		r, e := NewKVServer(n).Get(context.Background(), &pb.GetRequest{Key: []byte("key")})
		done <- result{r, e}
	}()
	select {
	case r := <-done:
		t.Fatal("read did not wait for term commitment", r)
	case <-time.After(20 * time.Millisecond):
	}
	logs.pending.Store(false)
	select {
	case r := <-done:
		if r.err != nil || string(r.response.GetValue()) != "ack" {
			t.Fatal("commit wait did not recover", r)
		}
	case <-time.After(time.Second):
		t.Fatal("commit wait did not terminate")
	}
	if n.Raft.LastIndex() != before {
		t.Fatal("initialization wait appended a log")
	}
}
