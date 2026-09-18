// Package store is the single-node key-value store the MVP calls for: an
// in-memory map whose every mutation goes through the WAL first (see
// internal/wal), so the store is exactly as crash-durable as the WAL
// itself already proved to be.
package store

import (
	"fmt"
	"sync"

	"github.com/pavann19/quorumkv/internal/wal"
)

// Store is a durable, single-node key-value store. All methods are safe
// for concurrent use.
type Store struct {
	mu   sync.RWMutex
	wal  *wal.WAL
	data map[string][]byte
}

// Open opens (creating if necessary) the WAL at path, replays it to
// rebuild in-memory state, and returns a Store ready to serve requests.
func Open(path string) (*Store, error) {
	records, err := wal.Replay(path)
	if err != nil {
		return nil, fmt.Errorf("store: replaying WAL: %w", err)
	}

	data := make(map[string][]byte, len(records))
	for _, r := range records {
		switch r.Op {
		case wal.OpPut:
			data[string(r.Key)] = r.Value
		case wal.OpDelete:
			delete(data, string(r.Key))
		}
	}

	w, err := wal.Open(path)
	if err != nil {
		return nil, fmt.Errorf("store: opening WAL for writing: %w", err)
	}

	return &Store{wal: w, data: data}, nil
}

// Close closes the underlying WAL file.
func (s *Store) Close() error {
	return s.wal.Close()
}

// Get returns the value for key and whether it exists.
func (s *Store) Get(key []byte) (value []byte, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[string(key)]
	return v, ok
}

// Put durably writes key=value: the WAL append (and its fsync) completes
// before the in-memory map is updated or Put returns, so a crash
// immediately after Put returns nil can never lose the write.
func (s *Store) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.wal.Append(wal.Record{Op: wal.OpPut, Key: key, Value: value}); err != nil {
		return fmt.Errorf("store: put: %w", err)
	}
	s.data[string(key)] = value
	return nil
}

// Delete durably removes key, if present. Deleting an absent key is not an
// error -- it is still durably recorded as a tombstone, which matters once
// Raft log replication is in play (M1): a delete of a key a follower never
// had must still replay correctly.
func (s *Store) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.wal.Append(wal.Record{Op: wal.OpDelete, Key: key}); err != nil {
		return fmt.Errorf("store: delete: %w", err)
	}
	delete(s.data, string(key))
	return nil
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
		entries = append(entries, Entry{Key: []byte(k), Value: v})
	}
	return entries
}

// Restore replaces the store's entire contents with entries -- used when
// Raft installs a snapshot (internal/raftnode.FSM.Restore), which replaces
// local state wholesale rather than applying it incrementally. It resets
// the WAL rather than appending: appending would leave stale pre-snapshot
// records behind that Replay would incorrectly resurrect on next restart.
func (s *Store) Restore(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	records := make([]wal.Record, len(entries))
	data := make(map[string][]byte, len(entries))
	for i, e := range entries {
		records[i] = wal.Record{Op: wal.OpPut, Key: e.Key, Value: e.Value}
		data[string(e.Key)] = e.Value
	}
	if err := s.wal.Reset(records); err != nil {
		return fmt.Errorf("store: restore: %w", err)
	}
	s.data = data
	return nil
}
