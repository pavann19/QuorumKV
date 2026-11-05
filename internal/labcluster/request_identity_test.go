package labcluster

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/raftnode"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRequestIdentityWireContractDoesNotDeduplicate(t *testing.T) {
	c, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	leader, err := c.Leader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id := "01890f2e-7c00-7000-8000-000000000001"
	requestCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(raftnode.RequestIDHeader, id))
	for _, value := range []string{"first", "second"} {
		var header, trailer metadata.MD
		if _, err = c.Clients[leader].Put(requestCtx, &pb.PutRequest{Key: []byte("correlated"), Value: []byte(value)}, grpc.Header(&header), grpc.Trailer(&trailer)); err != nil {
			t.Fatal(err)
		}
		if header.Get(raftnode.RequestIDHeader)[0] != id || trailer.Get(raftnode.RequestIDHeader)[0] != id {
			t.Fatal("request ID not echoed")
		}
	}
	result, err := c.Clients[leader].Get(ctx, &pb.GetRequest{Key: []byte("correlated")})
	if err != nil || !result.Found || string(result.Value) != "second" {
		t.Fatal("correlation ID incorrectly acted as deduplication", err)
	}
	before := c.Nodes[leader].Raft.LastIndex()
	for _, ids := range [][]string{{"FAKE_SECRET"}, {id, id}, {strings.Repeat("x", 512)}, {"01890f2e-7c00-4000-8000-000000000001"}} {
		badCtx := metadata.NewOutgoingContext(ctx, metadata.MD{raftnode.RequestIDHeader: ids})
		var trailer metadata.MD
		_, err := c.Clients[leader].Put(badCtx, &pb.PutRequest{Key: []byte("must-not-apply"), Value: []byte("v")}, grpc.Trailer(&trailer))
		if status.Code(err) != codes.InvalidArgument {
			t.Fatal("bad request ID accepted", err)
		}
		if len(trailer.Get(raftnode.RequestIDHeader)) != 0 || strings.Contains(err.Error(), "FAKE_SECRET") {
			t.Fatal("invalid metadata reflected")
		}
	}
	if c.Nodes[leader].Raft.LastIndex() != before {
		t.Fatal("invalid request ID changed consensus log")
	}
	follower := (leader + 1) % len(c.Nodes)
	_, err = c.Clients[follower].Put(requestCtx, &pb.PutRequest{Key: []byte("fenced"), Value: []byte("v")})
	var info *errdetails.ErrorInfo
	var retry *errdetails.RetryInfo
	for _, detail := range status.Convert(err).Details() {
		switch detail := detail.(type) {
		case *errdetails.ErrorInfo:
			info = detail
		case *errdetails.RetryInfo:
			retry = detail
		}
	}
	if info == nil || info.Metadata["request_id"] != id || info.Metadata["outcome"] != "not_submitted" || info.Metadata["term"] == "" || retry == nil {
		t.Fatalf("fenced request lost structured identity/retry information: %v", err)
	}
	var generated metadata.MD
	if _, err = c.Clients[leader].Get(ctx, &pb.GetRequest{Key: []byte("correlated")}, grpc.Header(&generated)); err != nil {
		t.Fatal(err)
	}
	ids := generated.Get(raftnode.RequestIDHeader)
	if len(ids) != 1 || len(ids[0]) != 36 || ids[0][14] != '7' {
		t.Fatal("missing server fallback UUIDv7")
	}
	if _, found := c.Nodes[leader].Store.Get([]byte("must-not-apply")); found {
		t.Fatal("invalid request mutation occurred")
	}
}
