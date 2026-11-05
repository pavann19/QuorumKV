package raftnode

import "sync/atomic"

var readPhaseBoundsNS = [12]uint64{250000, 500000, 1000000, 2000000, 4000000, 8000000, 16000000, 32000000, 64000000, 128000000, 256000000, ^uint64(0)}
var readPhaseNames = [4]string{"target", "quorum", "application", "lookup_and_term"}

type readPhaseCounters struct {
	count, total, maximum atomic.Uint64
	buckets               [12]atomic.Uint64
}
type ReadPhaseSnapshot struct {
	Phase         string     `json:"phase"`
	Count         uint64     `json:"count"`
	TotalNS       uint64     `json:"total_ns"`
	MaxNS         uint64     `json:"max_ns"`
	UpperBoundsNS [12]uint64 `json:"upper_bounds_ns"`
	Counts        [12]uint64 `json:"counts"`
}

func (n *Node) observeReadPhases(durations [4]uint64) {
	for phase, duration := range durations {
		counters := &n.readPhases[phase]
		counters.count.Add(1)
		counters.total.Add(duration)
		for maximum := counters.maximum.Load(); duration > maximum; maximum = counters.maximum.Load() {
			if counters.maximum.CompareAndSwap(maximum, duration) {
				break
			}
		}
		for bucket, upper := range readPhaseBoundsNS {
			if duration <= upper {
				counters.buckets[bucket].Add(1)
				break
			}
		}
	}
}

// ReadPhaseStats exposes fixed-cardinality successful-quorum-read timing only.
// Counters are atomic, process-local, payload-free and never part of a fence.
// Snapshots taken during activity are observations, not a consistent state oracle.
func (n *Node) ReadPhaseStats() [4]ReadPhaseSnapshot {
	var result [4]ReadPhaseSnapshot
	for phase := range result {
		counters := &n.readPhases[phase]
		result[phase] = ReadPhaseSnapshot{Phase: readPhaseNames[phase], Count: counters.count.Load(), TotalNS: counters.total.Load(), MaxNS: counters.maximum.Load(), UpperBoundsNS: readPhaseBoundsNS}
		for bucket := range readPhaseBoundsNS {
			result[phase].Counts[bucket] = counters.buckets[bucket].Load()
		}
	}
	return result
}
