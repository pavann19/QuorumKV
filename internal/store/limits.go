package store

import (
	"errors"
	"sync"
)

var ErrCapacity = errors.New("store: keyspace capacity exceeded")

type Limits struct {
	MaxBytes   int64 `json:"max_bytes"`
	MaxEntries int64 `json:"max_entries"`
}

func DefaultLimits() Limits { return Limits{MaxBytes: 256 << 20, MaxEntries: 1_000_000} }
func (l Limits) Validate() error {
	if l.MaxBytes <= 0 || l.MaxEntries <= 0 {
		return errors.New("store: positive byte and entry limits required")
	}
	return nil
}

type Usage struct {
	Bytes           int64
	Entries         int64
	ReservedBytes   int64
	ReservedEntries int64
}

func (s *Store) Usage() Usage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Usage{s.usedBytes, int64(len(s.data)), s.reservedBytes + s.publishingBytes, s.reservedEntries + s.publishingEntries}
}
func (s *Store) Limits() Limits { return s.limits }

func usage(data map[string][]byte) int64 {
	var total int64
	for k, v := range data {
		total += int64(len(k)) + int64(len(v))
	}
	return total
}
func (s *Store) putDelta(key, value []byte) (int64, int64) {
	old, found := s.data[string(key)]
	if found {
		return int64(len(value)) - int64(len(old)), 0
	}
	return int64(len(key)) + int64(len(value)), 1
}
func (s *Store) fits(bytes, entries int64) bool {
	return bytes <= s.limits.MaxBytes && entries <= s.limits.MaxEntries
}

// ReservePut provides conservative pre-submission admission. Reservations stay
// live until the caller's Raft future resolves, including after RPC cancellation.
// Same-key concurrent updates reserve a full value rather than sharing credit.
// FSM enforcement remains authoritative across leadership/replay races.
func (s *Store) ReservePut(key, value []byte) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bytes, entries := s.putDelta(key, value)
	k := string(key)
	_, publishing := s.publishingKeys[k]
	if s.pending[k] > 0 || publishing || s.replacing {
		bytes = int64(len(key)) + int64(len(value))
		entries = 1
	}
	if bytes < 0 {
		bytes = 0
	}
	if !s.fits(s.usedBytes+s.publishingBytes+s.reservedBytes+bytes, int64(len(s.data))+s.publishingEntries+s.reservedEntries+entries) {
		return nil, ErrCapacity
	}
	s.reservedBytes += bytes
	s.reservedEntries += entries
	s.pending[k]++
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.reservedBytes -= bytes
			s.reservedEntries -= entries
			s.pending[k]--
			if s.pending[k] == 0 {
				delete(s.pending, k)
			}
		})
	}, nil
}
