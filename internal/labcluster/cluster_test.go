package labcluster

import (
	"context"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"testing"
	"time"
)

func TestAuthenticatedReplicationRotationAndRestore(t *testing.T) {
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
	if _, err = c.Clients[leader].Put(ctx, &pb.PutRequest{Key: []byte("retained"), Value: []byte("acknowledged")}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Clients[leader].Put(ctx, &pb.PutRequest{Key: []byte("deleted"), Value: []byte("gone")}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Clients[leader].Delete(ctx, &pb.DeleteRequest{Key: []byte("deleted")}); err != nil {
		t.Fatal(err)
	}
	if err = c.Nodes[leader].Raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	if err = c.Rotate(leader); err != nil {
		t.Fatal(err)
	}
	// Restart forces transport handshakes with the rotated cert and persisted data.
	if err = c.Stop(leader); err != nil {
		t.Fatal(err)
	}
	if err = c.Restart(leader); err != nil {
		t.Fatal(err)
	}
	for {
		leader, err = c.Leader(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, e := c.Clients[leader].Get(ctx, &pb.GetRequest{Key: []byte("retained")})
		if e == nil && result.Found && string(result.Value) == "acknowledged" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("restore: %v", e)
		case <-time.After(30 * time.Millisecond):
		}
	}
	deleted, err := c.Clients[leader].Get(ctx, &pb.GetRequest{Key: []byte("deleted")})
	if err != nil || deleted.Found {
		t.Fatalf("deleted key restored: %v %v", deleted, err)
	}
	if _, err = c.Clients[leader].Put(ctx, &pb.PutRequest{Key: []byte("after"), Value: []byte("rotation")}); err != nil {
		t.Fatal(err)
	}
}
