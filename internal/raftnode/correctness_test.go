package raftnode

import (
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pavann19/quorumkv/internal/store"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

func TestCommittedStorageFailureStopsFSM(t *testing.T) {
	s, err := store.Open(filepath.Join(t.TempDir(), "store.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := encodeCommand(command{Op: opPut, Key: []byte("key"), Value: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() == nil {
			t.Error("committed storage failure must stop the node, not silently advance the FSM")
		}
	}()
	(&FSM{Store: s}).Apply(&raft.Log{Data: data})
}

func TestIsolatedLeaderCannotServeGet(t *testing.T) {
	var nodes []*Node
	var transports []*raft.InmemTransport
	configuration := raft.Configuration{}
	for i := 0; i < 3; i++ {
		addr, transport := raft.NewInmemTransport("")
		transports = append(transports, transport)
		configuration.Servers = append(configuration.Servers, raft.Server{ID: raft.ServerID(addr), Address: addr, Suffrage: raft.Voter})
	}
	for i, tr := range transports {
		for j, other := range transports {
			if i != j {
				tr.Connect(other.LocalAddr(), other)
			}
		}
		cfg := raft.DefaultConfig()
		cfg.LocalID = configuration.Servers[i].ID
		cfg.LogOutput = io.Discard
		cfg.HeartbeatTimeout = time.Second
		cfg.ElectionTimeout = time.Second
		cfg.LeaderLeaseTimeout = 500 * time.Millisecond
		st, err := store.Open(filepath.Join(t.TempDir(), "store.wal"))
		if err != nil {
			t.Fatal(err)
		}
		logs := raft.NewInmemStore()
		r, err := raft.NewRaft(cfg, &FSM{Store: st}, logs, logs, raft.NewInmemSnapshotStore(), tr)
		if err != nil {
			t.Fatal(err)
		}
		node := &Node{Raft: r, Store: st}
		nodes = append(nodes, node)
		t.Cleanup(func() { r.Shutdown().Error(); st.Close(); tr.Close() })
		if err := r.BootstrapCluster(configuration).Error(); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i, n := range nodes {
			if !n.IsLeader() {
				continue
			}
			if err := n.Put([]byte("key"), []byte("old"), time.Second); err != nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			response, err := NewKVServer(n).Get(ctx, &pb.GetRequest{Key: []byte("key")})
			cancel()
			if err != nil || !response.GetFound() || string(response.GetValue()) != "old" {
				t.Fatalf("healthy leader read failed: response=%v error=%v", response, err)
			}
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := NewKVServer(n).Get(canceled, &pb.GetRequest{Key: []byte("key")}); err == nil {
				t.Fatal("canceled read succeeded")
			}
			// Disconnect immediately while the old node still believes it leads.
			transports[i].DisconnectAll()
			for j, tr := range transports {
				if i != j {
					tr.Disconnect(transports[i].LocalAddr())
				}
			}
			ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if response, err := NewKVServer(n).Get(ctx, &pb.GetRequest{Key: []byte("key")}); err == nil {
				t.Fatalf("isolated leader served unfenced read: %+v", response)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no writable leader")
}
