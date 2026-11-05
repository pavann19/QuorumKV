package raftnode

import (
	"github.com/pavann19/quorumkv/internal/diskhealth"
	"os"
	"path/filepath"
	"testing"
)

func TestBootstrapRequiresExplicitIntentAndPreservesIdentity(t *testing.T) {
	self := Peer{ID: "one", Addr: "127.0.0.1:29991"}
	peers := []Peer{self}
	dir := filepath.Join(t.TempDir(), "member")
	options := StartOptions{DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}, ClusterID: "synthetic-cluster"}
	if n, err := New(self, peers, dir, options); err == nil {
		n.Shutdown()
		t.Fatal("bootstrapped without intent")
	}
	options.Bootstrap = true
	options.BootstrapExpect = 2
	if n, err := New(self, peers, dir, options); err == nil {
		n.Shutdown()
		t.Fatal("accepted incorrect voter count")
	}
	options.BootstrapExpect = 1
	n, err := New(self, peers, dir, options)
	if err != nil {
		t.Fatal(err)
	}
	if err = n.Shutdown(); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(dir, "cluster.json"))
	if err != nil {
		t.Fatal(err)
	}
	options.Bootstrap = false
	n, err = New(self, peers, dir, options)
	if err != nil {
		t.Fatal(err)
	}
	n.Shutdown()
	for _, change := range []string{"cluster", "peers", "node"} {
		changed := options
		ps := append([]Peer(nil), peers...)
		local := self
		switch change {
		case "cluster":
			changed.ClusterID = "wrong"
		case "peers":
			ps[0].Addr = "127.0.0.1:29992"
			local = ps[0]
		case "node":
			ps[0].ID = "wrong"
			local = ps[0]
		}
		if n, err := New(local, ps, dir, changed); err == nil {
			n.Shutdown()
			t.Fatalf("accepted %s drift", change)
		}
	}
	after, _ := os.ReadFile(filepath.Join(dir, "cluster.json"))
	if string(after) != string(original) {
		t.Fatal("manifest changed during refusal")
	}
	empty := filepath.Join(t.TempDir(), "replacement")
	if n, err := New(self, peers, empty, options); err == nil {
		n.Shutdown()
		t.Fatal("disk loss bootstrapped replacement")
	}
}

func TestExistingStateWithoutManifestRefused(t *testing.T) {
	self := Peer{ID: "one", Addr: "127.0.0.1:29993"}
	dir := t.TempDir()
	o := StartOptions{DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}, ClusterID: "synthetic", Bootstrap: true, BootstrapExpect: 1}
	n, err := New(self, []Peer{self}, dir, o)
	if err != nil {
		t.Fatal(err)
	}
	n.Shutdown()
	if err = os.Remove(filepath.Join(dir, "cluster.json")); err != nil {
		t.Fatal(err)
	}
	if n, err = New(self, []Peer{self}, dir, o); err == nil {
		n.Shutdown()
		t.Fatal("silently assigned identity to legacy state")
	}
}

func TestMemberOwnershipBeforeStoreOpen(t *testing.T) {
	self := Peer{ID: "one", Addr: "127.0.0.1:29994"}
	dir := t.TempDir()
	o := StartOptions{DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}, ClusterID: "synthetic", Bootstrap: true, BootstrapExpect: 1}
	first, err := New(self, []Peer{self}, dir, o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if first != nil {
			first.Shutdown()
		}
	}()
	manifest, err := os.ReadFile(filepath.Join(dir, "cluster.json"))
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "store.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if second, err := New(self, []Peer{self}, dir, o); err == nil {
		second.Shutdown()
		t.Fatal("two live owners")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "store.wal"))
	if string(after) != string(before) {
		t.Fatal("rejected owner modified store")
	}
	after, _ = os.ReadFile(filepath.Join(dir, "cluster.json"))
	if string(after) != string(manifest) {
		t.Fatal("rejected owner modified identity")
	}
	first.Shutdown()
	first = nil
	o.Bootstrap = false
	next, err := New(self, []Peer{self}, dir, o)
	if err != nil {
		t.Fatal(err)
	}
	next.Shutdown()
}

func TestStartupFailureReleasesMemberOwnership(t *testing.T) {
	self := Peer{ID: "one", Addr: "127.0.0.1:29995"}
	dir := t.TempDir()
	o := StartOptions{DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}, ClusterID: "synthetic"}
	if n, err := New(self, []Peer{self}, dir, o); err == nil {
		n.Shutdown()
		t.Fatal("empty state accepted")
	}
	o.Bootstrap = true
	o.BootstrapExpect = 1
	n, err := New(self, []Peer{self}, dir, o)
	if err != nil {
		t.Fatal("failed startup leaked ownership", err)
	}
	n.Shutdown()
}
