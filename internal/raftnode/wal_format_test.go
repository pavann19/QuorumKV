package raftnode

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/diskhealth"
	"github.com/pavann19/quorumkv/internal/wal"
)

func TestWALFormatValidationBeforeStorage(t *testing.T) {
	peer := Peer{ID: "one", Addr: "127.0.0.1:29974"}
	dir := filepath.Join(t.TempDir(), "must-not-exist")
	options := StartOptions{ClusterID: "format-validation", WALFormatVersion: 2, Bootstrap: true, BootstrapExpect: 1}
	if n, err := New(peer, []Peer{peer}, dir, options); err == nil {
		n.Shutdown()
		t.Fatal("unsupported WAL version opened")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("unsupported WAL version touched member storage")
	}
	options.WALFormatVersion = 1
	options.Bootstrap = false
	if _, err := LegacyIdentityMigration(peer, []Peer{peer}, dir, options, ""); err == nil {
		t.Fatal("identity migration authorized a format upgrade")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("identity migration touched refused source")
	}
}

func TestVersionedWALNodeSnapshotRestartAndDowngradeRefusal(t *testing.T) {
	peer := Peer{ID: "wal-format", Addr: "127.0.0.1:29973"}
	peers := []Peer{peer}
	dir := t.TempDir()
	options := StartOptions{WALFormatVersion: 1, SnapshotFormatVersion: 1, ClusterID: "versioned-wal-test", Bootstrap: true, BootstrapExpect: 1, DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}}
	n, err := New(peer, peers, dir, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if n != nil {
			n.Shutdown()
		}
	}()
	wait := func() {
		deadline := time.Now().Add(5 * time.Second)
		for !n.IsLeader() {
			if time.Now().After(deadline) {
				t.Fatal("leader not ready")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	wait()
	if err = n.Put([]byte("keep"), []byte("ack"), time.Second); err != nil {
		t.Fatal(err)
	}
	n.Put([]byte("remove"), []byte("old"), time.Second)
	if err = n.Delete([]byte("remove"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err = n.Raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	n.Shutdown()
	n = nil
	path := filepath.Join(dir, "store.wal")
	before, _ := os.ReadFile(path)
	legacy := options
	legacy.Bootstrap = false
	legacy.WALFormatVersion = 0
	if old, err := New(peer, peers, dir, legacy); err == nil {
		old.Shutdown()
		t.Fatal("downgrade accepted manifest")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("downgrade refusal changed WAL")
	}
	options.Bootstrap = false
	n, err = New(peer, peers, dir, options)
	if err != nil {
		t.Fatal(err)
	}
	wait()
	if err = n.Raft.Barrier(time.Second).Error(); err != nil {
		t.Fatal(err)
	}
	if value, found := n.Get([]byte("keep")); !found || string(value) != "ack" {
		t.Fatal("snapshot restart lost ack")
	}
	if _, found := n.Get([]byte("remove")); found {
		t.Fatal("snapshot restart resurrected deletion")
	}
	if err = n.Put([]byte("after-restart"), []byte("ack"), time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err = wal.InspectFormat(path, wal.Format{Version: 1, ClusterID: options.ClusterID}, nil); err != nil {
		t.Fatal("restart/compaction dropped WAL header", err)
	}
}
