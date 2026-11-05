package raftnode

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pavann19/quorumkv/internal/diskhealth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestSnapshotInstallAndRestart(t *testing.T) {
	for _, format := range []uint32{0, 1} {
		t.Run(fmt.Sprint("format-", format), func(t *testing.T) { testSnapshotInstallAndRestart(t, format) })
	}
}
func testSnapshotInstallAndRestart(t *testing.T, format uint32) {
	listeners := make([]net.Listener, 3)
	peers := make([]Peer, 3)
	for i := range listeners {
		var err error
		listeners[i], err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		peers[i] = Peer{ID: fmt.Sprintf("node%d", i), Addr: listeners[i].Addr().String()}
	}
	root := t.TempDir()
	nodes := make([]*Node, 3)
	servers := make([]*grpc.Server, 3)
	start := func(i int, lis net.Listener) {
		cfg := raft.DefaultConfig()
		cfg.LogOutput = io.Discard
		cfg.TrailingLogs = 0
		cfg.SnapshotInterval = time.Hour
		var err error
		nodes[i], err = newNodeTransport(peers[i], peers, filepath.Join(root, peers[i].ID), cfg, insecure.NewCredentials(), StartOptions{SnapshotFormatVersion: format, ClusterID: "snapshot-fixture", Bootstrap: true, BootstrapExpect: len(peers), DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}})
		if err != nil {
			t.Fatal(err)
		}
		servers[i] = grpc.NewServer()
		nodes[i].RegisterOn(servers[i])
		go servers[i].Serve(lis)
	}
	stop := func(i int) {
		if servers[i] != nil {
			servers[i].Stop()
			servers[i] = nil
		}
		if nodes[i] != nil {
			if err := nodes[i].Shutdown(); err != nil {
				t.Error(err)
			}
			nodes[i] = nil
		}
	}
	t.Cleanup(func() {
		for i := range nodes {
			stop(i)
			listeners[i].Close()
		}
	})
	for i := range nodes {
		start(i, listeners[i])
	}
	wait := func(label string, condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if condition() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("timeout: %s", label)
	}
	leader := -1
	wait("leader", func() bool {
		for i, n := range nodes {
			if n.IsLeader() {
				leader = i
				return true
			}
		}
		return false
	})
	lag := (leader + 1) % 3
	// Plant local-only stale data. Installing the snapshot must replace it.
	if err := nodes[lag].Store.Put([]byte("obsolete"), []byte("stale")); err != nil {
		t.Fatal(err)
	}
	lagIndex := nodes[lag].Raft.LastIndex()
	stop(lag)
	want := map[string]string{}
	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("key-%03d", i)
		value := fmt.Sprintf("value-%03d", i)
		if err := nodes[leader].Put([]byte(key), []byte(value), time.Second*5); err != nil {
			t.Fatal(err)
		}
		want[key] = value
	}
	for i := 0; i < 16; i++ {
		key := fmt.Sprintf("key-%03d", i)
		if err := nodes[leader].Delete([]byte(key), time.Second*5); err != nil {
			t.Fatal(err)
		}
		delete(want, key)
	}
	if err := nodes[leader].Raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	snapshotIndex, err := strconv.ParseUint(nodes[leader].Raft.Stats()["last_snapshot_index"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	first, err := nodes[leader].logStore.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if snapshotIndex <= lagIndex || (first != 0 && first <= lagIndex) {
		t.Fatalf("log was not compacted past lagging replica: first=%d lag=%d", first, lagIndex)
	}
	restart := func() {
		lis, err := net.Listen("tcp", peers[lag].Addr)
		if err != nil {
			t.Fatal(err)
		}
		start(lag, lis)
	}
	restart()
	wait("snapshot installation and exact state", func() bool {
		index, _ := strconv.ParseUint(nodes[lag].Raft.Stats()["last_snapshot_index"], 10, 64)
		actual := map[string]string{}
		for _, e := range nodes[lag].Store.Dump() {
			actual[string(e.Key)] = string(e.Value)
		}
		return index >= snapshotIndex && reflect.DeepEqual(actual, want)
	})
	stop(lag)
	restart()
	wait("restarted snapshot state", func() bool {
		actual := map[string]string{}
		for _, e := range nodes[lag].Store.Dump() {
			actual[string(e.Key)] = string(e.Value)
		}
		return reflect.DeepEqual(actual, want)
	})
	if err := nodes[leader].Put([]byte("after-restart"), []byte("new"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	wait("replication after restored restart", func() bool {
		value, ok := nodes[lag].Store.Get([]byte("after-restart"))
		return ok && string(value) == "new"
	})
	t.Logf("installed_snapshot_index=%d verified_keys=%d deleted_keys=16 stale_keys_removed=1 restarts=2", snapshotIndex, len(want))
}
