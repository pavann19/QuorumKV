package raftnode

import (
	"errors"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/diskhealth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDiskPressureRejectsBeforeLogAndRecovers(t *testing.T) {
	dir := t.TempDir()
	self := Peer{ID: "one", Addr: "127.0.0.1:29988"}
	n, err := New(self, []Peer{self}, dir, StartOptions{DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}, ClusterID: "disk-fixture", Bootstrap: true, BootstrapExpect: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()
	deadline := time.Now().Add(10 * time.Second)
	for !n.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("leader timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Injectable observations affect client admission only, never FSM application.
	n.diskProbe = func(string) (diskhealth.Space, error) { return diskhealth.Space{Available: 1, Total: 100}, nil }
	before := n.Raft.LastIndex()
	for _, op := range []func() error{func() error { return n.Put([]byte("blocked"), []byte("v"), time.Second) }, func() error { return n.Delete([]byte("blocked"), time.Second) }} {
		err := op()
		if !errors.Is(err, diskhealth.ErrPressure) {
			t.Fatalf("pressure bypass: %v", err)
		}
		rpcErr := NewKVServer(n).applyError(err)
		if status.Code(rpcErr) != codes.ResourceExhausted || outcomeInfo(t, rpcErr).Metadata["outcome"] != "not_submitted" {
			t.Fatal(rpcErr)
		}
	}
	if n.Raft.LastIndex() != before {
		t.Fatal("disk denial appended Raft log")
	}
	if u := n.Store.Usage(); u.ReservedBytes != 0 || u.ReservedEntries != 0 {
		t.Fatal("disk denial reserved memory")
	}
	n.diskProbe = func(string) (diskhealth.Space, error) { return diskhealth.Space{}, errors.New("FAKE_PRIVATE_MOUNT") }
	err = n.Put([]byte("blocked"), nil, time.Second)
	if !errors.Is(err, diskhealth.ErrUnknown) {
		t.Fatal("probe outage allowed write", err)
	}
	if status.Code(NewKVServer(n).applyError(err)) != codes.Unavailable {
		t.Fatal("wrong unknown code")
	}
	n.diskProbe = func(string) (diskhealth.Space, error) {
		return diskhealth.Space{Available: 1 << 30, Total: 2 << 30}, nil
	}
	if err = n.Put([]byte("recovered"), []byte("ack"), time.Second); err != nil {
		t.Fatal(err)
	}
	if value, found := n.Get([]byte("recovered")); !found || string(value) != "ack" {
		t.Fatal("recovered write lost")
	}
	if _, found := n.Get([]byte("blocked")); found {
		t.Fatal("denied write applied")
	}
}
