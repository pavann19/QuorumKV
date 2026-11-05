package raftnode

import (
	"encoding/json"
	"errors"
	"github.com/pavann19/quorumkv/internal/diskhealth"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pavann19/quorumkv/internal/store"
)

func TestTinyCapacitySurvivesReplayAndSnapshotRestart(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "log_replay", true: "snapshot"}[snapshot], func(t *testing.T) {
			dir := t.TempDir()
			self := Peer{ID: "one", Addr: "127.0.0.1:29998"}
			options := StartOptions{DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}, ClusterID: "tiny-fixture", Bootstrap: true, BootstrapExpect: 1, StoreLimits: store.Limits{MaxBytes: 8, MaxEntries: 2}}
			open := func() *Node {
				n, err := New(self, []Peer{self}, dir, options)
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
				return n
			}
			n := open()
			defer func() {
				if n != nil {
					n.Shutdown()
				}
			}()
			for _, pair := range [][2]string{{"a", "123"}, {"a", "x"}, {"b", "45678"}} {
				if err := n.Put([]byte(pair[0]), []byte(pair[1]), time.Second); err != nil {
					t.Fatal(err)
				}
			}
			before := n.Raft.LastIndex()
			if err := n.Put([]byte("c"), []byte("blocked"), time.Second); !errors.Is(err, store.ErrCapacity) {
				t.Fatal("overload not rejected", err)
			}
			if n.Raft.LastIndex() != before {
				t.Fatal("preflight rejection appended a log entry")
			}
			// Exercise authoritative apply-time rejection bypassing leader reservations.
			if err := n.apply(command{Op: opPut, Key: []byte("c"), Value: []byte("blocked")}, time.Second); !errors.Is(err, store.ErrCapacity) {
				t.Fatal("FSM capacity guard failed", err)
			}
			if snapshot {
				if err := n.Raft.Snapshot().Error(); err != nil {
					t.Fatal(err)
				}
			}
			n.Shutdown()
			n = nil
			options.Bootstrap = false
			n = open()
			if err := n.Raft.Barrier(time.Second).Error(); err != nil {
				t.Fatal(err)
			}
			for k, want := range map[string]string{"a": "x", "b": "45678"} {
				got, ok := n.Get([]byte(k))
				if !ok || string(got) != want {
					t.Fatalf("restart lost %s: %q", k, got)
				}
			}
			if _, ok := n.Get([]byte("c")); ok {
				t.Fatal("rejected command appeared on replay")
			}
			if u := n.Store.Usage(); u.Bytes != 8 || u.Entries != 2 {
				t.Fatalf("bad replay usage %+v", u)
			}
			if err := n.Delete([]byte("b"), time.Second); err != nil {
				t.Fatal(err)
			}
			if err := n.Put([]byte("c"), []byte("new"), time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCapacityNoOpMatchesReplicaState(t *testing.T) {
	// Two independently opened FSMs apply the identical ordered commands.
	var fsms []*FSM
	for i := 0; i < 2; i++ {
		s, err := store.OpenWithLimits(t.TempDir()+"/store.wal", store.Limits{MaxBytes: 4, MaxEntries: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		fsms = append(fsms, &FSM{Store: s})
	}
	for index, c := range []command{{Op: opPut, Key: []byte("a"), Value: []byte("123")}, {Op: opPut, Key: []byte("b"), Value: []byte("x")}, {Op: opDelete, Key: []byte("a")}, {Op: opPut, Key: []byte("b"), Value: []byte("x")}} {
		c.Limits = store.Limits{MaxBytes: 4, MaxEntries: 1}
		raw, _ := encodeCommand(c)
		for _, f := range fsms {
			result := f.Apply(&raft.Log{Index: uint64(index + 1), Data: raw})
			if index == 1 {
				err, ok := result.(error)
				if !ok || !errors.Is(err, store.ErrCapacity) {
					t.Fatal("rejection not deterministic", result)
				}
			} else if result != nil {
				t.Fatal(result)
			}
		}
	}
	for _, f := range fsms {
		if value, ok := f.Store.Get([]byte("b")); !ok || string(value) != "x" {
			t.Fatal("replica divergence")
		}
	}
}

func TestLimitConfigurationDriftRefused(t *testing.T) {
	dir := t.TempDir()
	self := Peer{ID: "one", Addr: "127.0.0.1:29999"}
	options := StartOptions{DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}, ClusterID: "limits-drift", Bootstrap: true, BootstrapExpect: 1, StoreLimits: store.Limits{MaxBytes: 8, MaxEntries: 2}}
	n, err := New(self, []Peer{self}, dir, options)
	if err != nil {
		t.Fatal(err)
	}
	n.Shutdown()
	options.StoreLimits.MaxBytes = 9
	if n, err = New(self, []Peer{self}, dir, options); err == nil {
		n.Shutdown()
		t.Fatal("changed deterministic limits accepted")
	}
}

func TestPersistedMembershipDriftRefusedBeforeWALReconstruction(t *testing.T) {
	dir := t.TempDir()
	self := Peer{ID: "one", Addr: "127.0.0.1:29990"}
	options := StartOptions{DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}, ClusterID: "inspect-fixture", Bootstrap: true, BootstrapExpect: 1}
	n, err := New(self, []Peer{self}, dir, options)
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
	if err = n.Put([]byte("preserve"), []byte("ack"), time.Second); err != nil {
		n.Shutdown()
		t.Fatal(err)
	}
	n.Shutdown()
	original, err := os.ReadFile(filepath.Join(dir, "store.wal"))
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an operator editing the manifest and flags to agree with each
	// other while disagreeing with the actual persisted Raft configuration.
	changed := Peer{ID: self.ID, Addr: "127.0.0.1:29989"}
	options.StoreLimits = store.DefaultLimits()
	raw, _ := json.Marshal(desiredManifest(changed, []Peer{changed}, options))
	if err = os.WriteFile(filepath.Join(dir, "cluster.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if n, err = New(changed, []Peer{changed}, dir, options); err == nil {
		n.Shutdown()
		t.Fatal("persisted membership mismatch accepted")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "store.wal"))
	if string(after) != string(original) {
		t.Fatal("refused startup reconstructed or changed WAL")
	}
}
