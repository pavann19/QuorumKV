package labcluster

import (
	"context"
	"strconv"
	"testing"
	"time"

	transportpb "github.com/Jille/raft-grpc-transport/proto"
	"github.com/pavann19/quorumkv/internal/raftnode"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthenticatedPeerWrongClusterCannotChangeConsensus(t *testing.T) {
	c, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader, err := c.Leader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Clients[leader].Put(ctx, &pb.PutRequest{Key: []byte("before-binding-test"), Value: []byte("ack")}); err != nil {
		t.Fatal(err)
	}
	sender := (leader + 1) % len(c.Peers)
	conn, err := grpc.NewClient(c.Peers[leader].Addr, grpc.WithTransportCredentials(c.identities[sender].ClientCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := transportpb.NewRaftTransportClient(conn)
	header := &transportpb.RPCHeader{ProtocolVersion: 3, Id: []byte(c.Peers[sender].ID), Addr: []byte(c.Peers[sender].Addr)}
	term, err := strconv.ParseUint(c.Nodes[leader].Raft.Stats()["term"], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	lastIndex := c.Nodes[leader].Raft.LastIndex()
	contexts := map[string]metadata.MD{
		"absent":                {},
		"new_command_profile":   metadata.Pairs(raftnode.PeerClusterHeader, c.clusterID, raftnode.PeerProtocolHeader, raftnode.PeerCommandProtocolVersion, raftnode.PeerSnapshotFormatHeader, "0", raftnode.PeerCommandFormatHeader, "1"),
		"wrong_snapshot_format": metadata.Pairs(raftnode.PeerClusterHeader, c.clusterID, raftnode.PeerProtocolHeader, raftnode.PeerProtocolVersion, raftnode.PeerSnapshotFormatHeader, "1"),
		"wrong_cluster":         metadata.Pairs(raftnode.PeerClusterHeader, "other-cluster", raftnode.PeerProtocolHeader, raftnode.PeerProtocolVersion, raftnode.PeerSnapshotFormatHeader, "0"),
		"missing_version":       metadata.Pairs(raftnode.PeerClusterHeader, c.clusterID),
		"wrong_version":         metadata.Pairs(raftnode.PeerClusterHeader, c.clusterID, raftnode.PeerProtocolHeader, "999", raftnode.PeerSnapshotFormatHeader, "0"),
		"duplicate_cluster":     metadata.Pairs(raftnode.PeerClusterHeader, c.clusterID, raftnode.PeerClusterHeader, c.clusterID, raftnode.PeerProtocolHeader, raftnode.PeerProtocolVersion, raftnode.PeerSnapshotFormatHeader, "0"),
	}
	for name, md := range contexts {
		t.Run(name, func(t *testing.T) {
			callCtx := metadata.NewOutgoingContext(ctx, md)
			var errors []error
			_, e := client.RequestVote(callCtx, &transportpb.RequestVoteRequest{RpcHeader: header, Term: term + 1000, Candidate: []byte(c.Peers[sender].Addr)})
			errors = append(errors, e)
			_, e = client.RequestPreVote(callCtx, &transportpb.RequestPreVoteRequest{RpcHeader: header, Term: term + 1000})
			errors = append(errors, e)
			_, e = client.AppendEntries(callCtx, &transportpb.AppendEntriesRequest{RpcHeader: header, Term: term + 1000, Leader: []byte(c.Peers[sender].Addr)})
			errors = append(errors, e)
			_, e = client.TimeoutNow(callCtx, &transportpb.TimeoutNowRequest{RpcHeader: header})
			errors = append(errors, e)
			pipeline, e := client.AppendEntriesPipeline(callCtx)
			if e == nil {
				pipeline.Send(&transportpb.AppendEntriesRequest{RpcHeader: header, Term: term + 1000})
				_, e = pipeline.Recv()
				pipeline.CloseSend()
			}
			errors = append(errors, e)
			snapshot, e := client.InstallSnapshot(callCtx)
			if e == nil {
				snapshot.Send(&transportpb.InstallSnapshotRequest{RpcHeader: header, Term: term + 1000, SnapshotVersion: 1})
				_, e = snapshot.CloseAndRecv()
			}
			errors = append(errors, e)
			for index, err := range errors {
				if status.Code(err) != codes.PermissionDenied {
					t.Fatalf("transport method %d not rejected: %v", index, err)
				}
			}
			if c.Nodes[leader].Raft.Stats()["term"] != strconv.FormatUint(term, 10) || !c.Nodes[leader].IsLeader() || c.Nodes[leader].Raft.LastIndex() != lastIndex {
				t.Fatal("denied peer request affected consensus term/leadership/log")
			}
		})
	}
	// Matching binding and the same authenticated peer reach the adopted
	// transport. A stale pre-vote is harmless and need not change the term.
	valid := metadata.NewOutgoingContext(ctx, metadata.Pairs(raftnode.PeerClusterHeader, c.clusterID, raftnode.PeerProtocolHeader, raftnode.PeerProtocolVersion, raftnode.PeerSnapshotFormatHeader, "0"))
	if _, err = client.RequestPreVote(valid, &transportpb.RequestPreVoteRequest{RpcHeader: header, Term: term - 1}); err != nil {
		t.Fatalf("valid authenticated peer rejected: %v", err)
	}
	if _, err = c.Clients[leader].Put(ctx, &pb.PutRequest{Key: []byte("after-binding-test"), Value: []byte("new-ack")}); err != nil {
		t.Fatal(err)
	}
	got, err := c.Clients[leader].Get(ctx, &pb.GetRequest{Key: []byte("after-binding-test")})
	if err != nil || !got.Found || string(got.Value) != "new-ack" {
		t.Fatal("legitimate operations failed after rejected peer requests", err)
	}
}
