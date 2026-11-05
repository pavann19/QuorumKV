// Package raftnode wires internal/store into hashicorp/raft: an FSM that
// applies committed log entries to the store, and a Node that bootstraps a
// raft.Raft instance over a gRPC transport (github.com/Jille/raft-grpc-transport).
// Per the build plan's own guidance, Raft consensus and the gRPC wire
// protocol are both adopted libraries, not reimplemented -- the engineering
// here is the adapter between them and internal/store, plus (in a later
// commit) the fault-injection harness this whole project exists to run.
package raftnode

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"

	"github.com/pavann19/quorumkv/internal/store"
	"github.com/pavann19/quorumkv/internal/wal"
)

// commandOp mirrors wal.OpType but is declared separately: the Raft log's
// on-the-wire command encoding and the local WAL's record encoding are
// different concerns (the Raft log is itself already a replicated,
// durable log -- see docs/DECISIONS.md for why internal/store's own WAL
// underneath it is still kept, not treated as redundant).
type commandOp byte

const (
	opPut commandOp = iota
	opDelete
)

type command struct {
	Op     commandOp
	Key    []byte
	Value  []byte
	Limits store.Limits
}

func encodeCommand(c command) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return nil, fmt.Errorf("raftnode: encoding command: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeCommand(data []byte) (command, error) {
	var c command
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&c); err != nil {
		return command{}, fmt.Errorf("raftnode: decoding command: %w", err)
	}
	return c, nil
}

// FSM applies committed Raft log entries to a local *store.Store.
type FSM struct {
	Store                 *store.Store
	singleSync            bool
	clusterID             string
	snapshotFormatVersion uint32
	commandFormatVersion  uint32
	lastCommandIndex      atomic.Uint64
	appliedMu             sync.Mutex
	appliedChanged        chan struct{}
	fatal                 func(string, uint64)
}

var _ raft.FSM = (*FSM)(nil)

// Apply is called once per committed log entry, in log order, on every
// node (leader and followers alike) -- this is what makes replication
// actually replicate state, not just the log.
func (f *FSM) committedRecord(log *raft.Log) wal.Record {
	cmd, err := decodeCommandFormat(log.Data, f.clusterID, f.commandFormatVersion)
	if err != nil {
		f.stop("committed_decode", log.Index)
	}
	limits := cmd.Limits
	if limits == (store.Limits{}) {
		limits = store.DefaultLimits()
	}
	if limits != f.Store.Limits() {
		f.stop("committed_limit_mismatch", log.Index)
	}
	record := wal.Record{Key: cmd.Key, Value: cmd.Value}
	switch cmd.Op {
	case opPut:
		record.Op = wal.OpPut
	case opDelete:
		record.Op = wal.OpDelete
		record.Value = nil
	default:
		f.stop("committed_unknown_operation", log.Index)
	}
	return record
}

func (f *FSM) Apply(log *raft.Log) any {
	record := f.committedRecord(log)
	results, err := f.Store.ApplyBatch([]wal.Record{record})
	if err != nil {
		f.stop("committed_storage", log.Index)
	}
	f.publishCommandIndex(log.Index)
	if errors.Is(results[0], store.ErrCapacity) {
		return store.ErrCapacity
	}
	return nil
}

var _ raft.BatchingFSM = (*FSM)(nil)

// ApplyBatch retains one response per library input, including configuration
// entries. Large library batches are split at explicit WAL byte/record bounds;
// all chunks finish before Raft can acknowledge any future in this call.
func (f *FSM) ApplyBatch(logs []*raft.Log) []any {
	responses := make([]any, len(logs))
	records := make([]wal.Record, 0, len(logs))
	positions := make([]int, 0, len(logs))
	for i, log := range logs {
		switch log.Type {
		case raft.LogConfiguration:
			continue
		case raft.LogCommand:
			if f.singleSync {
				responses[i] = f.Apply(log)
				continue
			}
			record := f.committedRecord(log)
			if err := wal.ValidateBatch([]wal.Record{record}); err != nil {
				f.stop("committed_storage", log.Index)
			}
			records = append(records, record)
			positions = append(positions, i)
		default:
			f.stop("committed_batch_type", log.Index)
		}
	}
	for start := 0; start < len(records); {
		end, size := start, 0
		for end < len(records) && end-start < wal.MaxBatchRecords {
			next := wal.EncodedRecordSize(records[end])
			if next > wal.MaxBatchBytes-size {
				break
			}
			size += next
			end++
		}
		results, err := f.Store.ApplyBatch(records[start:end])
		if err != nil {
			f.stop("committed_storage", logs[positions[start]].Index)
		}
		for i, result := range results {
			responses[positions[start+i]] = result
		}
		f.publishCommandIndex(logs[positions[end-1]].Index)
		start = end
	}
	return responses
}

// Snapshot and Restore exist to let Raft compact its log; this MVP-for-M1
// slice keeps them minimal (full key/value dump) rather than incremental,
// since log compaction tuning is not what M1's exit criterion asks for.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	entries, err := f.Store.CompactSnapshot()
	if err != nil {
		f.stop("snapshot_compaction_storage", 0)
		return nil, err
	}
	return &fsmSnapshot{data: entries, format: f.snapshotFormatVersion, clusterID: f.clusterID, limits: f.Store.Limits(), index: f.lastCommandIndex.Load()}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if f.snapshotFormatVersion > 1 {
		return ErrSnapshotFormat
	}
	var entries []store.Entry
	var index uint64
	if f.snapshotFormatVersion == 1 {
		var err error
		entries, index, err = decodeSnapshotV1(rc, f.clusterID, f.Store.Limits())
		if err != nil {
			return err
		}
	} else {
		if err := gob.NewDecoder(rc).Decode(&entries); err != nil {
			return fmt.Errorf("raftnode: decoding legacy snapshot: %w", err)
		}
	}

	if err := f.Store.Restore(entries); err != nil {
		f.stop("snapshot_restore_storage", 0)
		return err
	}
	f.publishCommandIndex(index)
	return nil
}

type fsmSnapshot struct {
	data      []store.Entry
	format    uint32
	clusterID string
	limits    store.Limits
	index     uint64
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	var err error
	if s.format > 1 {
		err = ErrSnapshotFormat
	} else if s.format == 1 {
		err = encodeSnapshotV1(sink, s.clusterID, s.limits, s.index, s.data)
	} else {
		err = gob.NewEncoder(sink).Encode(s.data)
	}
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("raftnode: persisting snapshot: %w", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
