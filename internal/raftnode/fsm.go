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
	"fmt"
	"io"

	"github.com/hashicorp/raft"

	"github.com/pavann19/quorumkv/internal/store"
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
	Op    commandOp
	Key   []byte
	Value []byte
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
	Store *store.Store
}

var _ raft.FSM = (*FSM)(nil)

// Apply is called once per committed log entry, in log order, on every
// node (leader and followers alike) -- this is what makes replication
// actually replicate state, not just the log.
func (f *FSM) Apply(log *raft.Log) any {
	cmd, err := decodeCommand(log.Data)
	if err != nil {
		// A decode failure here means a bug in this FSM's own encoding, not
		// bad input from a client (that's validated before Apply is ever
		// called) -- panicking is the standard hashicorp/raft convention
		// for FSM errors, since there's no well-defined way to "reject" an
		// already-committed log entry after the fact.
		panic(fmt.Sprintf("raftnode: corrupt committed log entry: %v", err))
	}
	switch cmd.Op {
	case opPut:
		return f.Store.Put(cmd.Key, cmd.Value)
	case opDelete:
		return f.Store.Delete(cmd.Key)
	default:
		panic(fmt.Sprintf("raftnode: unknown command op %d in committed log entry", cmd.Op))
	}
}

// Snapshot and Restore exist to let Raft compact its log; this MVP-for-M1
// slice keeps them minimal (full key/value dump) rather than incremental,
// since log compaction tuning is not what M1's exit criterion asks for.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{data: f.Store.Dump()}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var entries []store.Entry
	if err := gob.NewDecoder(rc).Decode(&entries); err != nil {
		return fmt.Errorf("raftnode: decoding snapshot: %w", err)
	}
	return f.Store.Restore(entries)
}

type fsmSnapshot struct {
	data []store.Entry
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := gob.NewEncoder(sink).Encode(s.data); err != nil {
		sink.Cancel()
		return fmt.Errorf("raftnode: persisting snapshot: %w", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
