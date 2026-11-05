package raftnode

import (
	"bytes"
	"github.com/pavann19/quorumkv/internal/memberlock"
	"github.com/pavann19/quorumkv/internal/wal"
	"go.etcd.io/bbolt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/diskhealth"
)

func legacyFixture(t *testing.T, snapshot bool) (Peer, []Peer, string, StartOptions) {
	t.Helper()
	dir := t.TempDir()
	self := Peer{ID: "one", Addr: "127.0.0.1:29987"}
	peers := []Peer{self}
	options := StartOptions{ClusterID: "legacy-fixture", Bootstrap: true, BootstrapExpect: 1, DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}}
	n, err := New(self, peers, dir, options)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !n.IsLeader() {
		if time.Now().After(deadline) {
			n.Shutdown()
			t.Fatal("leader timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Encode genuine legacy commands: the added Limits field is zero/absent in
	// old gob representations. Bypass the new submission encoder intentionally.
	for _, c := range []command{{Op: opPut, Key: []byte("retained"), Value: []byte("ack")}, {Op: opPut, Key: []byte("deleted"), Value: []byte("old")}, {Op: opDelete, Key: []byte("deleted")}} {
		raw, _ := encodeCommand(c)
		future := n.Raft.Apply(raw, time.Second)
		if err := future.Error(); err != nil {
			n.Shutdown()
			t.Fatal(err)
		}
	}
	if snapshot {
		if err := n.Raft.Snapshot().Error(); err != nil {
			n.Shutdown()
			t.Fatal(err)
		}
	}
	n.Shutdown()
	if err = os.Remove(filepath.Join(dir, "cluster.json")); err != nil {
		t.Fatal(err)
	}
	options.Bootstrap = false
	options.BootstrapExpect = 0
	return self, peers, dir, options
}

func TestOfflineMigrationPreservesEvidenceAndRestarts(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "log", true: "snapshot"}[snapshot], func(t *testing.T) {
			self, peers, dir, options := legacyFixture(t, snapshot)
			before, err := migrationFiles(dir)
			if err != nil {
				t.Fatal(err)
			}
			report, err := LegacyIdentityMigration(self, peers, dir, options, "")
			if err != nil {
				t.Fatal(err)
			}
			if report.Status != "INSPECTED_NOT_APPLIED" || len(report.SourceSHA256) != 64 {
				t.Fatalf("bad inspection %+v", report)
			}
			if _, err = os.Stat(filepath.Join(dir, "cluster.json")); !os.IsNotExist(err) {
				t.Fatal("inspection wrote manifest")
			}
			after, _ := migrationFiles(dir)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("inspection changed existing evidence")
			}
			if _, err = LegacyIdentityMigration(self, peers, dir, options, "wrong"); err == nil {
				t.Fatal("unapproved migration accepted")
			}
			applied, err := LegacyIdentityMigration(self, peers, dir, options, report.SourceSHA256)
			if err != nil {
				t.Fatal(err)
			}
			if applied.Status != "APPLIED_IDENTITY_ONLY" {
				t.Fatal(applied.Status)
			}
			after, _ = migrationFiles(dir)
			var originalAfter []MigrationFile
			for _, f := range after {
				if f.Path != "cluster.json" {
					originalAfter = append(originalAfter, f)
				}
			}
			if !reflect.DeepEqual(before, originalAfter) {
				t.Fatal("migration rewrote data evidence")
			}
			if _, err = LegacyIdentityMigration(self, peers, dir, options, report.SourceSHA256); err == nil {
				t.Fatal("existing identity reassigned")
			}
			n, err := New(self, peers, dir, options)
			if err != nil {
				t.Fatal(err)
			}
			defer n.Shutdown()
			deadline := time.Now().Add(10 * time.Second)
			for !n.IsLeader() {
				if time.Now().After(deadline) {
					t.Fatal("migrated leader timeout")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err = n.Raft.Barrier(time.Second).Error(); err != nil {
				t.Fatal(err)
			}
			got, found := n.Get([]byte("retained"))
			if !found || string(got) != "ack" {
				t.Fatal("migration lost retained state")
			}
			if _, found = n.Get([]byte("deleted")); found {
				t.Fatal("migration resurrected deletion")
			}
			if err = n.Put([]byte("post-migration"), []byte("new"), time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMigrationRefusesChangedSourceAndMembership(t *testing.T) {
	self, peers, dir, options := legacyFixture(t, false)
	report, err := LegacyIdentityMigration(self, peers, dir, options, "")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "unreviewed-evidence"), []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = LegacyIdentityMigration(self, peers, dir, options, report.SourceSHA256); err == nil {
		t.Fatal("stale approval accepted")
	}
	altered := Peer{ID: self.ID, Addr: "127.0.0.1:29986"}
	if _, err = LegacyIdentityMigration(altered, []Peer{altered}, dir, options, ""); err == nil {
		t.Fatal("wrong persisted membership accepted")
	}
	before, _ := migrationFiles(dir)
	options.ClusterID = "different-identity"
	if _, err = LegacyIdentityMigration(self, peers, dir, options, report.SourceSHA256); err == nil {
		t.Fatal("approval reused for other identity")
	}
	after, _ := migrationFiles(dir)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("refused migration changed evidence")
	}
}

func TestMigrationRejectsActiveOwnerAndFatalOrCorruptState(t *testing.T) {
	self, peers, dir, options := legacyFixture(t, false)
	// Only a synthetic directory is opened. Migration never attaches to this node.
	// Existing-state startup intentionally refuses without a manifest, so retain
	// a direct ownership handle to test the same OS exclusion boundary.
	lock, err := memberlock.Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = LegacyIdentityMigration(self, peers, dir, options, ""); err == nil {
		lock.Close()
		t.Fatal("active owner bypassed")
	}
	lock.Close()
	if err = writeFatal(dir, "synthetic", 1); err != nil {
		t.Fatal(err)
	}
	if _, err = LegacyIdentityMigration(self, peers, dir, options, ""); err == nil {
		t.Fatal("fatal marker bypassed")
	}
	if err = checkFatal(dir, true); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(dir, "store.wal")
	raw, _ := os.ReadFile(walPath)
	raw[len(raw)-1] ^= 0xff
	os.WriteFile(walPath, raw, 0600)
	if _, err = LegacyIdentityMigration(self, peers, dir, options, ""); err == nil {
		t.Fatal("corruption migration accepted")
	}
	after, _ := os.ReadFile(walPath)
	if !bytes.Equal(raw, after) {
		t.Fatal("migration repaired corrupt evidence")
	}
}

func TestOfflineSnapshotInspectionRejectsChangedPayload(t *testing.T) {
	self, peers, dir, options := legacyFixture(t, true)
	paths, _ := filepath.Glob(filepath.Join(dir, "snapshots", "*", "state.bin"))
	if len(paths) == 0 {
		t.Fatal("missing snapshot")
	}
	raw, _ := os.ReadFile(paths[0])
	raw[len(raw)-1] ^= 0xff
	os.WriteFile(paths[0], raw, 0600)
	if _, err := LegacyIdentityMigration(self, peers, dir, options, ""); err == nil {
		t.Fatal("corrupt snapshot accepted")
	}
}

func TestMigrationRefusesIncompleteTailWithoutRepair(t *testing.T) {
	self, peers, dir, options := legacyFixture(t, false)
	path := filepath.Join(dir, "store.wal")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	file.Write([]byte{1, 2})
	file.Close()
	before, _ := os.ReadFile(path)
	if _, err = LegacyIdentityMigration(self, peers, dir, options, ""); err == nil {
		t.Fatal("incomplete-tail migration accepted")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("migration repaired tail without authority")
	}
}

func TestMigrationMissingRaftBucketsReturnsRefusal(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(filepath.Join(dir, "store.wal"))
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	db, err := bbolt.Open(filepath.Join(dir, "raft-log.bolt"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	peer := Peer{ID: "one", Addr: "127.0.0.1:29984"}
	if _, err = LegacyIdentityMigration(peer, []Peer{peer}, dir, StartOptions{ClusterID: "broken-layout"}, ""); err == nil {
		t.Fatal("missing Raft buckets accepted")
	}
}
