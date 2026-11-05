// Package store is the single-node key-value store the MVP calls for: an
// in-memory map whose every mutation goes through the WAL first (see
// internal/wal), so the store is exactly as crash-durable as the WAL
// itself already proved to be.
package store

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/pavann19/quorumkv/internal/wal"
)

// Store is a durable, single-node key-value store. All methods are safe
// for concurrent use.
type Store struct {
	mu                sync.RWMutex
	writeMu           sync.Mutex
	writeErr          error
	publishingBytes   int64
	publishingEntries int64
	publishingKeys    map[string]struct{}
	replacing         bool
	wal               journal
	data              map[string][]byte
	limits            Limits
	usedBytes         int64
	reservedBytes     int64
	reservedEntries   int64
	pending           map[string]int
}

type journal interface {
	AppendBatch([]wal.Record) error
	Reset([]wal.Record) error
	Close() error
	Stats() wal.Stats
}

func (s *Store) WALStats() wal.Stats { return s.wal.Stats() }

// Open opens (creating if necessary) the WAL at path, replays it to
// rebuild in-memory state, and returns a Store ready to serve requests.
func Open(path string) (*Store, error) { return OpenWithLimits(path, DefaultLimits()) }

func OpenWithLimits(path string, limits Limits) (*Store, error) {
	return OpenWithWALFormat(path, limits, wal.Format{})
}

func OpenWithWALFormat(path string, limits Limits, format wal.Format) (*Store, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	s := &Store{data: map[string][]byte{}, limits: limits, pending: map[string]int{}}
	w, err := wal.OpenReplayFormat(path, format, func(r wal.Record) error {
		switch r.Op {
		case wal.OpPut:
			delta, entries := s.putDelta(r.Key, r.Value)
			if !s.fits(s.usedBytes+delta, int64(len(s.data))+entries) {
				return ErrCapacity
			}
			s.data[string(r.Key)] = r.Value
			s.usedBytes += delta
		case wal.OpDelete:
			if old, found := s.data[string(r.Key)]; found {
				s.usedBytes -= int64(len(r.Key)) + int64(len(old))
			}
			delete(s.data, string(r.Key))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store: streaming WAL recovery: %w", err)
	}
	s.wal = w
	return s, nil
}

// Close closes the underlying WAL file.
func (s *Store) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.wal.Close()
}

// Get returns the value for key and whether it exists.
func (s *Store) Get(key []byte) (value []byte, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[string(key)]
	return bytes.Clone(v), ok
}

// Put durably writes key=value: the WAL append (and its fsync) completes
// before the in-memory map is updated or Put returns, so a crash
// immediately after Put returns nil can never lose the write.
func (s *Store) Put(key, value []byte) error {
	results, err := s.ApplyBatch([]wal.Record{{Op: wal.OpPut, Key: key, Value: value}})
	if err != nil {
		return err
	}
	return results[0]
}

// Delete records a durable tombstone even when the key is absent.
func (s *Store) Delete(key []byte) error {
	results, err := s.ApplyBatch([]wal.Record{{Op: wal.OpDelete, Key: key}})
	if err != nil {
		return err
	}
	return results[0]
}

// Entry is one key/value pair, used only by Dump/Restore (Raft snapshots).
type Entry struct {
	Key   []byte
	Value []byte
}

// Dump returns every key/value pair currently in the store, for use as a
// Raft snapshot (internal/raftnode.FSM.Snapshot). It does not go through
// the WAL -- a snapshot is a point-in-time copy of already-durable state,
// not a new write.
func (s *Store) Dump() []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make([]Entry, 0, len(s.data))
	for k, v := range s.data {
		entries = append(entries, Entry{Key: []byte(k), Value: bytes.Clone(v)})
	}
	return entries
}

// Restore replaces the store's entire contents with entries -- used when
// Raft installs a snapshot (internal/raftnode.FSM.Restore), which replaces
// local state wholesale rather than applying it incrementally. It resets
// the WAL rather than appending: appending would leave stale pre-snapshot
// records behind that Replay would incorrectly resurrect on next restart.
func (s *Store) Restore(entries []Entry) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeErr != nil {
		return s.writeErr
	}

	records := make([]wal.Record, len(entries))
	data := make(map[string][]byte, len(entries))
	for i, e := range entries {
		records[i] = wal.Record{Op: wal.OpPut, Key: e.Key, Value: e.Value}
		data[string(e.Key)] = bytes.Clone(e.Value)
	}
	total := usage(data)
	if !s.fits(total, int64(len(data))) {
		return ErrCapacity
	}
	s.mu.Lock()
	s.publishingBytes = max(int64(0), total-s.usedBytes)
	s.publishingEntries = max(int64(0), int64(len(data))-int64(len(s.data)))
	s.replacing = true
	s.mu.Unlock()
	defer s.endPublication()
	if err := s.wal.Reset(records); err != nil {
		return fmt.Errorf("store: restore: %w", err)
	}
	s.mu.Lock()
	s.data = data
	s.usedBytes = total
	s.mu.Unlock()
	return nil
}

// CompactSnapshot captures a point-in-time keyspace and replaces the redundant
// mutation history with exactly that durable state. The store lock serializes
// reset with every mutation, so entries applied after the capture append to the
// replacement WAL rather than being lost. Raft snapshot persistence may fail
// independently: the compacted WAL still contains the complete local state.
func (s *Store) CompactSnapshot() ([]Entry, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeErr != nil {
		return nil, s.writeErr
	}
	s.mu.RLock()
	entries := make([]Entry, 0, len(s.data))
	records := make([]wal.Record, 0, len(s.data))
	for k, v := range s.data {
		key, value := []byte(k), bytes.Clone(v)
		entries = append(entries, Entry{Key: key, Value: value})
		records = append(records, wal.Record{Op: wal.OpPut, Key: key, Value: value})
	}
	s.mu.RUnlock()
	if err := s.wal.Reset(records); err != nil {
		return nil, fmt.Errorf("store: compact snapshot: %w", err)
	}
	return entries, nil
}
