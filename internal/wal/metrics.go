package wal

import "sync/atomic"

// Counters are process-local observations, never part of replay or correctness.
type Counters struct {
	Appends   atomic.Uint64
	Bytes     atomic.Uint64
	Syncs     atomic.Uint64
	Failures  atomic.Uint64
	SyncNanos atomic.Uint64
}
type Stats struct {
	Appends   uint64
	Bytes     uint64
	Syncs     uint64
	Failures  uint64
	SyncNanos uint64
}

func (w *WAL) Stats() Stats {
	return Stats{w.metrics.Appends.Load(), w.metrics.Bytes.Load(), w.metrics.Syncs.Load(), w.metrics.Failures.Load(), w.metrics.SyncNanos.Load()}
}
