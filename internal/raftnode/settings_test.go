package raftnode

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pavann19/quorumkv/internal/diskhealth"
)

func TestRaftSettingsValidatedBeforeStorage(t *testing.T) {
	peer := Peer{ID: "one", Addr: "127.0.0.1:29982"}
	for _, change := range []func(*RaftSettings){func(s *RaftSettings) { s.LeaderLeaseMS = s.HeartbeatMS + 1 }, func(s *RaftSettings) { s.ElectionMS = s.HeartbeatMS - 1 }, func(s *RaftSettings) { s.CommitMS = 0 }, func(s *RaftSettings) { s.SnapshotThreshold = 0 }, func(s *RaftSettings) { s.MaxAppendEntries = MaxReplicationEntries + 1 }} {
		settings := DefaultRaftSettings()
		change(&settings)
		dir := filepath.Join(t.TempDir(), "must-not-exist")
		n, err := New(peer, []Peer{peer}, dir, StartOptions{ClusterID: "settings-test", Bootstrap: true, BootstrapExpect: 1, RaftSettings: settings})
		if err == nil {
			n.Shutdown()
			t.Fatal("invalid Raft settings accepted")
		}
		if _, err = os.Stat(dir); !os.IsNotExist(err) {
			t.Fatal("invalid settings opened member storage")
		}
	}
}

func TestCallerRaftConfigurationRemainsUnchanged(t *testing.T) {
	cfg := raft.DefaultConfig()
	cfg.SnapshotInterval = time.Hour
	cfg.TrailingLogs = 0
	before := *cfg
	peer := Peer{ID: "one", Addr: "127.0.0.1:29981"}
	n, err := newNode(peer, []Peer{peer}, t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()
	if cfg.LocalID != before.LocalID || cfg.MaxAppendEntries != before.MaxAppendEntries || cfg.NoSnapshotRestoreOnStart != before.NoSnapshotRestoreOnStart || cfg.SnapshotInterval != before.SnapshotInterval {
		t.Fatal("caller configuration was mutated")
	}
	actual := n.EffectiveRaftSettings()
	if actual.MaxAppendEntries != 1 || actual.SnapshotMS != time.Hour.Milliseconds() || actual.TrailingLogs != 0 {
		t.Fatalf("effective cloned config wrong %+v", actual)
	}
}

func TestConfiguredAutomaticSnapshotAndRestart(t *testing.T) {
	settings := DefaultRaftSettings()
	settings.HeartbeatMS = 200
	settings.ElectionMS = 300
	settings.LeaderLeaseMS = 100
	settings.CommitMS = 10
	settings.SnapshotMS = 20
	settings.SnapshotThreshold = 8
	settings.TrailingLogs = 0
	options := StartOptions{ClusterID: "snapshot-settings", Bootstrap: true, BootstrapExpect: 1, RaftSettings: settings, DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}}
	peer := Peer{ID: "one", Addr: "127.0.0.1:29980"}
	dir := t.TempDir()
	n, err := New(peer, []Peer{peer}, dir, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if n != nil {
			n.Shutdown()
		}
	}()
	waitLeader := func() {
		deadline := time.Now().Add(5 * time.Second)
		for !n.IsLeader() {
			if time.Now().After(deadline) {
				t.Fatal("configured election did not establish leader")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitLeader()
	for i := 0; i < 16; i++ {
		if err = n.Put([]byte(fmt.Sprint(i)), []byte("ack"), time.Second); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	var index uint64
	for {
		index, _ = strconv.ParseUint(n.Raft.Stats()["last_snapshot_index"], 10, 64)
		if index > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("configured automatic snapshot did not execute")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n.EffectiveRaftSettings() != settings {
		t.Fatal("effective settings do not match applied profile")
	}
	n.Shutdown()
	n = nil
	options.Bootstrap = false
	n, err = New(peer, []Peer{peer}, dir, options)
	if err != nil {
		t.Fatal(err)
	}
	waitLeader()
	if err = n.Raft.Barrier(time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		value, found := n.Get([]byte(fmt.Sprint(i)))
		if !found || string(value) != "ack" {
			t.Fatalf("configured snapshot/restart lost %d", i)
		}
	}
	t.Logf("configured_snapshot_index=%d exact_retained_keys=16", index)
}
