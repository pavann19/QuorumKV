package raftnode

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pavann19/quorumkv/internal/store"
	"github.com/pavann19/quorumkv/internal/wal"
)

func batchFSMFixture(t *testing.T, limits store.Limits, single bool) *FSM {
	t.Helper()
	s, err := store.OpenWithLimits(filepath.Join(t.TempDir(), "store.wal"), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return &FSM{Store: s, singleSync: single}
}
func TestBatchFSMMatchesSingleApplyCapacityAndConfigurationOrder(t *testing.T) {
	limits := store.Limits{MaxBytes: 6, MaxEntries: 2}
	commands := []command{{Op: opPut, Key: []byte("a"), Value: []byte("12345")}, {Op: opPut, Key: []byte("b"), Value: []byte("no")}, {Op: opPut, Key: []byte("a"), Value: []byte("x")}, {Op: opPut, Key: []byte("b"), Value: []byte("yes")}, {Op: opDelete, Key: []byte("a")}, {Op: opPut, Key: []byte("c"), Value: []byte("!")}}
	logs := []*raft.Log{{Index: 1, Type: raft.LogConfiguration}}
	for i, command := range commands {
		command.Limits = limits
		raw, err := encodeCommand(command)
		if err != nil {
			t.Fatal(err)
		}
		logs = append(logs, &raft.Log{Index: uint64(i + 2), Type: raft.LogCommand, Data: raw})
	}
	logs = append(logs, &raft.Log{Index: 8, Type: raft.LogConfiguration})
	batched, single := batchFSMFixture(t, limits, false), batchFSMFixture(t, limits, true)
	for _, fsm := range []*FSM{batched, single} {
		responses := fsm.ApplyBatch(logs)
		if len(responses) != len(logs) {
			t.Fatal("response length")
		}
		for i, result := range responses {
			if i == 2 {
				err, ok := result.(error)
				if !ok || !errors.Is(err, store.ErrCapacity) {
					t.Fatal("capacity response lost", result)
				}
			} else if result != nil {
				t.Fatal("response misordered", i, result)
			}
		}
		if fsm.lastCommandIndex.Load() != 7 {
			t.Fatal("configuration index reported as command completion")
		}
	}
	if !reflect.DeepEqual(batched.Store.Usage(), single.Store.Usage()) {
		t.Fatal("capacity accounting differs")
	}
	for _, fsm := range []*FSM{batched, single} {
		actual := map[string]string{}
		for _, entry := range fsm.Store.Dump() {
			actual[string(entry.Key)] = string(entry.Value)
		}
		if !reflect.DeepEqual(actual, map[string]string{"b": "yes", "c": "!"}) {
			t.Fatal("ordered batch diverged", actual)
		}
	}
	if batched.Store.WALStats().Syncs != 1 || single.Store.WALStats().Syncs != 5 {
		t.Fatal("fsync grouping not observed", batched.Store.WALStats(), single.Store.WALStats())
	}
}
func TestBatchFSMSplitsAtBoundAndRetainsEveryResponse(t *testing.T) {
	f := batchFSMFixture(t, store.DefaultLimits(), false)
	logs := []*raft.Log{}
	for i := 0; i < wal.MaxBatchRecords*2+3; i++ {
		raw, err := encodeCommand(command{Op: opPut, Key: []byte(fmt.Sprint(i)), Value: []byte("ack"), Limits: f.Store.Limits()})
		if err != nil {
			t.Fatal(err)
		}
		logs = append(logs, &raft.Log{Index: uint64(i + 1), Data: raw})
	}
	responses := f.ApplyBatch(logs)
	for _, response := range responses {
		if response != nil {
			t.Fatal(response)
		}
	}
	if len(responses) != len(logs) || f.Store.Usage().Entries != int64(len(logs)) || f.lastCommandIndex.Load() != uint64(len(logs)) || f.Store.WALStats().Syncs != 3 {
		t.Fatal("bounded split lost command/ack", f.Store.Usage(), f.Store.WALStats())
	}
}
func TestBatchFSMFatalPrevalidationAndStorageNeverReportSuccess(t *testing.T) {
	for _, badDecode := range []bool{false, true} {
		f := batchFSMFixture(t, store.DefaultLimits(), false)
		raw, err := encodeCommand(command{Op: opPut, Key: []byte("must-not-ack"), Value: []byte("value"), Limits: f.Store.Limits()})
		if err != nil {
			t.Fatal(err)
		}
		logs := []*raft.Log{{Index: 1, Data: raw}}
		if badDecode {
			logs = append(logs, &raft.Log{Index: 2, Data: []byte("invalid")})
		} else {
			f.Store.Close()
		}
		panicked := false
		func() { defer func() { panicked = recover() != nil }(); f.ApplyBatch(logs) }()
		if !panicked || f.lastCommandIndex.Load() != 0 || f.Store.Usage().Entries != 0 {
			t.Fatal("failed batch continued or published completion", badDecode)
		}
	}
}

type batchGateLogs struct {
	raft.LogStore
	once             sync.Once
	entered, release chan struct{}
}

func (s *batchGateLogs) StoreLogs(logs []*raft.Log) error {
	for _, log := range logs {
		if log.Type == raft.LogCommand {
			s.once.Do(func() { close(s.entered); <-s.release })
			break
		}
	}
	return s.LogStore.StoreLogs(logs)
}
func TestRealRaftBatchesQueuedCommandsBeforeAcknowledgement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	fsm := &FSM{Store: st}
	logs := raft.NewInmemStore()
	gated := &batchGateLogs{LogStore: logs, entered: make(chan struct{}), release: make(chan struct{})}
	address, transport := raft.NewInmemTransport("")
	cfg := raft.DefaultConfig()
	cfg.LocalID = "batch-fixture"
	cfg.MaxAppendEntries = 8
	cfg.BatchApplyCh = true
	cfg.LogOutput = io.Discard
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 25 * time.Millisecond
	r, err := raft.NewRaft(cfg, fsm, gated, logs, raft.NewInmemSnapshotStore(), transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Shutdown().Error(); st.Close(); transport.Close() })
	released := false
	defer func() {
		if !released {
			close(gated.release)
		}
	}()
	if err = r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{ID: cfg.LocalID, Address: address, Suffrage: raft.Voter}}}).Error(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for (r.State() != raft.Leader || r.CommitIndex() < 2) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if r.State() != raft.Leader || r.CommitIndex() < 2 {
		t.Fatal("no committed leader")
	}
	submit := func(i int) raft.ApplyFuture {
		raw, err := encodeCommand(command{Op: opPut, Key: []byte(fmt.Sprint(i)), Value: []byte("ack"), Limits: st.Limits()})
		if err != nil {
			t.Fatal(err)
		}
		return r.Apply(raw, time.Second)
	}
	futures := []raft.ApplyFuture{submit(0)}
	select {
	case <-gated.entered:
	case <-time.After(time.Second):
		t.Fatal("write not gated")
	}
	// Main Raft loop is blocked in the first log persistence. Exactly eight
	// subsequent commands fill its configured apply buffer before it can drain.
	for i := 1; i <= 8; i++ {
		futures = append(futures, submit(i))
	}
	if st.Usage().Entries != 0 {
		t.Fatal("uncommitted writes published")
	}
	close(gated.release)
	released = true
	for _, future := range futures {
		if err := future.Error(); err != nil || future.Response() != nil {
			t.Fatal("batch acknowledgement", err, future.Response())
		}
	}
	stats := st.WALStats()
	if stats.Appends != 9 || stats.Syncs != 2 || fsm.lastCommandIndex.Load() != futures[8].Index() {
		t.Fatal("real Raft did not share ordered syncs", stats, fsm.lastCommandIndex.Load())
	}
	for i := 0; i < 9; i++ {
		value, found := st.Get([]byte(fmt.Sprint(i)))
		if !found || string(value) != "ack" {
			t.Fatal("acknowledged value missing", i)
		}
	}
	r.Shutdown().Error()
	st.Close()
	recovered, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if recovered.Usage().Entries != 9 {
		t.Fatal("batch recovery lost acknowledgements")
	}
}

func TestBatchFSMSplitsAtByteBudget(t *testing.T) {
	f := batchFSMFixture(t, store.DefaultLimits(), false)
	value := make([]byte, 4<<20)
	logs := make([]*raft.Log, 8)
	for i := range logs {
		raw, err := encodeCommand(command{Op: opPut, Key: []byte{byte(i)}, Value: value, Limits: f.Store.Limits()})
		if err != nil {
			t.Fatal(err)
		}
		logs[i] = &raft.Log{Index: uint64(i + 1), Data: raw}
	}
	responses := f.ApplyBatch(logs)
	for _, response := range responses {
		if response != nil {
			t.Fatal(response)
		}
	}
	if f.Store.Usage().Entries != 8 || f.Store.WALStats().Syncs != 2 || f.Store.WALStats().Appends != 8 {
		t.Fatal("byte-bound split lost records", f.Store.Usage(), f.Store.WALStats())
	}
	for i := range logs {
		got, found := f.Store.Get([]byte{byte(i)})
		if !found || len(got) != len(value) {
			t.Fatal("large legacy-compatible record lost", i)
		}
	}
}
