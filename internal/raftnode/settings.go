package raftnode

import (
	"errors"
	"time"

	"github.com/hashicorp/raft"
)

// RaftSettings is the supported startup tuning surface. Batching stays at the
// byte-bounded transport ceiling, with actual wire-size splitting.
type RaftSettings struct {
	BatchApply        bool   `json:"batch_apply"`
	HeartbeatMS       int64  `json:"heartbeat_timeout_ms"`
	ElectionMS        int64  `json:"election_timeout_ms"`
	LeaderLeaseMS     int64  `json:"leader_lease_timeout_ms"`
	CommitMS          int64  `json:"commit_timeout_ms"`
	SnapshotMS        int64  `json:"snapshot_interval_ms"`
	SnapshotThreshold uint64 `json:"snapshot_threshold"`
	TrailingLogs      uint64 `json:"trailing_logs"`
	MaxAppendEntries  int    `json:"max_append_entries"`
}

func DefaultRaftSettings() RaftSettings {
	cfg := raft.DefaultConfig()
	// Larger groups are opt-in until the mixed-read latency gate passes.
	cfg.MaxAppendEntries = 1
	cfg.BatchApplyCh = true
	return settingsFromRaft(cfg)
}
func settingsFromRaft(cfg *raft.Config) RaftSettings {
	return RaftSettings{BatchApply: cfg.BatchApplyCh,
		HeartbeatMS: cfg.HeartbeatTimeout.Milliseconds(), ElectionMS: cfg.ElectionTimeout.Milliseconds(), LeaderLeaseMS: cfg.LeaderLeaseTimeout.Milliseconds(), CommitMS: cfg.CommitTimeout.Milliseconds(), SnapshotMS: cfg.SnapshotInterval.Milliseconds(), SnapshotThreshold: cfg.SnapshotThreshold, TrailingLogs: cfg.TrailingLogs, MaxAppendEntries: cfg.MaxAppendEntries,
	}
}
func (s RaftSettings) Validate() error {
	for _, ms := range []int64{s.HeartbeatMS, s.ElectionMS, s.LeaderLeaseMS, s.CommitMS} {
		if ms < 5 || ms > 300000 {
			return errors.New("raft timings must be 5..300000 ms")
		}
	}
	if s.SnapshotMS < 5 || s.SnapshotMS > 86400000 || s.SnapshotThreshold == 0 || s.SnapshotThreshold > 1<<30 || s.TrailingLogs > 1<<30 {
		return errors.New("invalid snapshot interval/threshold/log retention")
	}
	if s.LeaderLeaseMS > s.HeartbeatMS || s.ElectionMS < s.HeartbeatMS {
		return errors.New("Raft lease must not exceed heartbeat and election must not be shorter than heartbeat")
	}
	if s.MaxAppendEntries < 1 || s.MaxAppendEntries > MaxReplicationEntries {
		return errors.New("max_append_entries must be 1..8")
	}
	cfg := s.config(raft.ServerID("validation"))
	return raft.ValidateConfig(cfg)
}
func (s RaftSettings) config(id raft.ServerID) *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = id
	cfg.HeartbeatTimeout = time.Duration(s.HeartbeatMS) * time.Millisecond
	cfg.ElectionTimeout = time.Duration(s.ElectionMS) * time.Millisecond
	cfg.LeaderLeaseTimeout = time.Duration(s.LeaderLeaseMS) * time.Millisecond
	cfg.CommitTimeout = time.Duration(s.CommitMS) * time.Millisecond
	cfg.SnapshotInterval = time.Duration(s.SnapshotMS) * time.Millisecond
	cfg.SnapshotThreshold = s.SnapshotThreshold
	cfg.TrailingLogs = s.TrailingLogs
	cfg.MaxAppendEntries = s.MaxAppendEntries
	cfg.BatchApplyCh = s.BatchApply
	return cfg
}
func (n *Node) EffectiveRaftSettings() RaftSettings {
	if n.raftSettings == (RaftSettings{}) {
		return DefaultRaftSettings()
	}
	return n.raftSettings
}
