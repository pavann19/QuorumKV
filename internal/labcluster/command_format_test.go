package labcluster

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	transportpb "github.com/Jille/raft-grpc-transport/proto"
	"github.com/pavann19/quorumkv/internal/raftnode"
	"github.com/pavann19/quorumkv/internal/requestlimits"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestVersionedCommandsSnapshotCatchupAndPeerRefusal(t *testing.T) {
	settings := raftnode.DefaultRaftSettings()
	settings.TrailingLogs = 0
	settings.SnapshotMS = time.Hour.Milliseconds()
	probe := &appendWireProbe{}
	c, err := newModeStorageProfile(t.TempDir(), true, 1, 1, 1, "quorum", settings, grpc.ChainUnaryInterceptor(probe.unary), grpc.ChainStreamInterceptor(probe.stream))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	leader, err := c.Leader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	put := func(key string, value []byte) {
		t.Helper()
		if _, err = c.Clients[leader].Put(ctx, &pb.PutRequest{Key: []byte(key), Value: value}); err != nil {
			t.Fatal(err)
		}
	}
	put("keep", []byte("old"))
	put("deleted", []byte("seed"))
	lag := (leader + 1) % 3
	for deadline := time.Now().Add(5 * time.Second); ; {
		if v, ok := c.Nodes[lag].Get([]byte("deleted")); ok && string(v) == "seed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lag member never received seeded stale state")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = c.Stop(lag); err != nil {
		t.Fatal(err)
	}
	put("keep", []byte("latest"))
	if _, err = c.Clients[leader].Delete(ctx, &pb.DeleteRequest{Key: []byte("deleted")}); err != nil {
		t.Fatal(err)
	}
	maximum := bytes.Repeat([]byte{0x91}, requestlimits.MaxValueBytes)
	put("maximum", maximum)
	if err = c.Nodes[leader].Raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	if err = c.Restart(lag); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; {
		v, found := c.Nodes[lag].Get([]byte("maximum"))
		keep, hasKeep := c.Nodes[lag].Get([]byte("keep"))
		_, deleted := c.Nodes[lag].Get([]byte("deleted"))
		if found && bytes.Equal(v, maximum) && hasKeep && string(keep) == "latest" && !deleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("versioned snapshot catchup did not produce exact state")
		}
		time.Sleep(10 * time.Millisecond)
	}
	probe.Lock()
	installs := probe.installs
	probe.Unlock()
	if installs == 0 {
		t.Fatal("fixture did not exercise actual snapshot transfer")
	}
	if c.Nodes[lag].CommandFormatVersion() != 1 || c.Nodes[lag].WALFormatVersion() != 1 || c.Nodes[lag].PeerProtocolVersion() != raftnode.PeerCommandProtocolVersion {
		t.Fatal("restart lost storage/peer profile")
	}
	// The same trusted peer certificate cannot bypass the command format fence.
	sender := (leader + 2) % 3
	conn, err := grpc.NewClient(c.Peers[leader].Addr, grpc.WithTransportCredentials(c.identities[sender].ClientCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	peer := transportpb.NewRaftTransportClient(conn)
	term, _ := strconv.ParseUint(c.Nodes[leader].Raft.Stats()["term"], 10, 64)
	index := c.Nodes[leader].Raft.LastIndex()
	header := &transportpb.RPCHeader{ProtocolVersion: 3, Id: []byte(c.Peers[sender].ID), Addr: []byte(c.Peers[sender].Addr)}
	for _, binding := range []metadata.MD{metadata.Pairs(raftnode.PeerClusterHeader, c.clusterID, raftnode.PeerProtocolHeader, "2", raftnode.PeerSnapshotFormatHeader, "1"), metadata.Pairs(raftnode.PeerClusterHeader, c.clusterID, raftnode.PeerProtocolHeader, "3", raftnode.PeerSnapshotFormatHeader, "1"), metadata.Pairs(raftnode.PeerClusterHeader, c.clusterID, raftnode.PeerProtocolHeader, "3", raftnode.PeerSnapshotFormatHeader, "1", raftnode.PeerCommandFormatHeader, "0"), metadata.Pairs(raftnode.PeerClusterHeader, c.clusterID, raftnode.PeerProtocolHeader, "3", raftnode.PeerSnapshotFormatHeader, "1", raftnode.PeerCommandFormatHeader, "1", raftnode.PeerCommandFormatHeader, "1")} {
		call := metadata.NewOutgoingContext(ctx, binding)
		var failures []error
		_, e := peer.RequestVote(call, &transportpb.RequestVoteRequest{RpcHeader: header, Term: term + 1000})
		failures = append(failures, e)
		_, e = peer.RequestPreVote(call, &transportpb.RequestPreVoteRequest{RpcHeader: header, Term: term + 1000})
		failures = append(failures, e)
		_, e = peer.AppendEntries(call, &transportpb.AppendEntriesRequest{RpcHeader: header, Term: term + 1000})
		failures = append(failures, e)
		_, e = peer.TimeoutNow(call, &transportpb.TimeoutNowRequest{RpcHeader: header})
		failures = append(failures, e)
		pipeline, e := peer.AppendEntriesPipeline(call)
		if e == nil {
			pipeline.Send(&transportpb.AppendEntriesRequest{RpcHeader: header, Term: term + 1000})
			_, e = pipeline.Recv()
			pipeline.CloseSend()
		}
		failures = append(failures, e)
		snapshot, e := peer.InstallSnapshot(call)
		if e == nil {
			snapshot.Send(&transportpb.InstallSnapshotRequest{RpcHeader: header, Term: term + 1000})
			_, e = snapshot.CloseAndRecv()
		}
		failures = append(failures, e)
		for _, e = range failures {
			if status.Code(e) != codes.PermissionDenied {
				t.Fatal("incompatible command peer reached transport", e)
			}
		}
		if c.Nodes[leader].Raft.Stats()["term"] != strconv.FormatUint(term, 10) || c.Nodes[leader].Raft.LastIndex() != index || !c.Nodes[leader].IsLeader() {
			t.Fatal("denied command profile changed consensus")
		}
	}
	if err = c.Stop(leader); err != nil {
		t.Fatal(err)
	}
	leader, err = c.Leader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	put("after-failover", []byte("ack"))
	response, err := c.Clients[leader].Get(ctx, &pb.GetRequest{Key: []byte("after-failover")})
	if err != nil || !response.Found || string(response.Value) != "ack" {
		t.Fatal("new command profile failed after leader kill", err)
	}
	t.Logf("command_format=1 wal_format=1 snapshot_format=1 peer_protocol=3 maximum_value_bytes=%d snapshot_chunks=%d exact_catchup=true old_peer_refused=true post_failover_write=true", len(maximum), installs)
}
