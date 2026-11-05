package store

import (
	"bytes"
	"fmt"

	"github.com/pavann19/quorumkv/internal/wal"
)

type batchValue struct {
	value  []byte
	exists bool
}

// ApplyBatch makes each capacity decision in command order. Rejected commands
// are deterministic no-ops; accepted records share a sync. The error slice has
// one response per input. A batch-level error means none can be acknowledged,
// although the disk may contain an uncertain prefix. Caller buffers are copied.
func (s *Store) ApplyBatch(records []wal.Record) ([]error, error) {
	if err := wal.ValidateBatch(records); err != nil {
		return nil, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if s.writeErr != nil {
		return nil, fmt.Errorf("store: unavailable after write failure: %w", s.writeErr)
	}
	results := make([]error, len(records))
	accepted := make([]wal.Record, 0, len(records))
	overlay := make(map[string]batchValue, len(records))
	s.mu.Lock()
	used, entries := s.usedBytes, int64(len(s.data))
	for i, record := range records {
		key := string(record.Key)
		previous, changed := overlay[key]
		if !changed {
			previous.value, previous.exists = s.data[key]
		}
		nextUsed, nextEntries := used, entries
		switch record.Op {
		case wal.OpPut:
			delta := int64(len(record.Key)) + int64(len(record.Value))
			if previous.exists {
				delta -= int64(len(record.Key)) + int64(len(previous.value))
			} else {
				nextEntries++
			}
			if delta > 0 && delta > s.limits.MaxBytes-used {
				results[i] = ErrCapacity
				continue
			}
			nextUsed += delta
			if !s.fits(nextUsed, nextEntries) {
				results[i] = ErrCapacity
				continue
			}
			owned := wal.Record{Op: record.Op, Key: bytes.Clone(record.Key), Value: bytes.Clone(record.Value)}
			accepted = append(accepted, owned)
			overlay[key] = batchValue{value: owned.Value, exists: true}
		case wal.OpDelete:
			if previous.exists {
				nextUsed -= int64(len(record.Key)) + int64(len(previous.value))
				nextEntries--
			}
			accepted = append(accepted, wal.Record{Op: record.Op, Key: bytes.Clone(record.Key)})
			overlay[key] = batchValue{}
		}
		used, entries = nextUsed, nextEntries
	}
	// Reservations cannot borrow overwrite/deletion credit while publication is
	// pending. Existing readers can continue to observe the old durable map.
	s.publishingBytes = max(int64(0), used-s.usedBytes)
	s.publishingEntries = max(int64(0), entries-int64(len(s.data)))
	s.publishingKeys = make(map[string]struct{}, len(overlay))
	for key := range overlay {
		s.publishingKeys[key] = struct{}{}
	}
	s.mu.Unlock()
	defer s.endPublication()
	if err := s.wal.AppendBatch(accepted); err != nil {
		s.writeErr = err
		return nil, fmt.Errorf("store: append batch: %w", err)
	}
	s.mu.Lock()
	for key, value := range overlay {
		if value.exists {
			s.data[key] = value.value
		} else {
			delete(s.data, key)
		}
	}
	s.usedBytes = used
	s.mu.Unlock()
	return results, nil
}

func (s *Store) endPublication() {
	s.mu.Lock()
	s.publishingBytes, s.publishingEntries = 0, 0
	s.publishingKeys = nil
	s.replacing = false
	s.mu.Unlock()
}
