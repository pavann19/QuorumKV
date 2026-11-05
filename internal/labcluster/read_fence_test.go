package labcluster

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

func TestAuthenticatedReadOnlyLoadDoesNotGrowRaftLog(t *testing.T) {
	c, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	leader, err := c.Leader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Clients[leader].Put(ctx, &pb.PutRequest{Key: []byte("read-only"), Value: []byte("acknowledged")}); err != nil {
		t.Fatal(err)
	}
	// A successful warmup also proves current-term quorum and completed apply.
	if _, err = c.Clients[leader].Get(ctx, &pb.GetRequest{Key: []byte("read-only")}); err != nil {
		t.Fatal(err)
	}
	committed := c.Nodes[leader].Raft.CommitIndex()
	for _, node := range c.Nodes {
		for node.Raft.CommitIndex() < committed && ctx.Err() == nil {
			time.Sleep(time.Millisecond)
		}
		if ctx.Err() != nil {
			t.Fatal("followers did not catch up", ctx.Err())
		}
	}
	indices := make([]uint64, len(c.Nodes))
	for i, node := range c.Nodes {
		indices[i] = node.Raft.LastIndex()
	}
	errs := make(chan error, 4)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 16; i++ {
				result, err := c.Clients[leader].Get(ctx, &pb.GetRequest{Key: []byte("read-only")})
				if err != nil {
					errs <- err
					return
				}
				if !result.GetFound() || string(result.GetValue()) != "acknowledged" {
					t.Error("incorrect authenticated readback")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	for i, node := range c.Nodes {
		if node.Raft.LastIndex() != indices[i] {
			t.Fatalf("node %d log grew under authenticated read-only load: %d -> %d", i, indices[i], node.Raft.LastIndex())
		}
	}
}
