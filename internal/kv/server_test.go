package kv_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pavann19/quorumkv/internal/kv"
	"github.com/pavann19/quorumkv/internal/store"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

// startTestServer boots a real gRPC server (over an in-memory bufconn
// listener, not a real socket) backed by a real, WAL-durable store, and
// returns a connected client plus a cleanup func.
func startTestServer(t *testing.T) pb.KVClient {
	t.Helper()

	walPath := filepath.Join(t.TempDir(), "test.wal")
	s, err := store.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterKVServer(grpcServer, kv.New(s))
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return pb.NewKVClient(conn)
}

func TestGetPutDelete_RoundTrip(t *testing.T) {
	client := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.Put(ctx, &pb.PutRequest{Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}

	resp, err := client.Get(ctx, &pb.GetRequest{Key: []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Found || string(resp.Value) != "1" {
		t.Fatalf("expected a=1, got found=%v value=%q", resp.Found, resp.Value)
	}

	if _, err := client.Delete(ctx, &pb.DeleteRequest{Key: []byte("a")}); err != nil {
		t.Fatal(err)
	}

	resp, err = client.Get(ctx, &pb.GetRequest{Key: []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Found {
		t.Fatal("expected key to be gone after Delete")
	}
}

func TestGet_MissingKeyReturnsFoundFalse(t *testing.T) {
	client := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Get(ctx, &pb.GetRequest{Key: []byte("nope")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Found {
		t.Fatal("expected Found=false for a missing key")
	}
}

func TestPut_EmptyKeyIsRejected(t *testing.T) {
	client := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.Put(ctx, &pb.PutRequest{Key: nil, Value: []byte("v")}); err == nil {
		t.Fatal("expected an empty key to be rejected")
	}
}

func TestDelete_EmptyKeyIsRejected(t *testing.T) {
	client := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.Delete(ctx, &pb.DeleteRequest{Key: nil}); err == nil {
		t.Fatal("expected an empty key to be rejected")
	}
}
