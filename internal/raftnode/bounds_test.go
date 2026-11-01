package raftnode

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
	"time"
)

func TestBoundsAndCancellationKeepSubmittedEffect(t *testing.T) {
	if validateSize(make([]byte, MaxKeyBytes+1), nil) == nil || validateSize([]byte("k"), make([]byte, MaxValueBytes+1)) == nil {
		t.Fatal("oversized input accepted")
	}
	if validateSize([]byte("k"), make([]byte, MaxValueBytes)) != nil {
		t.Fatal("legal bound rejected")
	}
	s := &KVServer{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	applied := make(chan struct{})
	release := make(chan struct{})
	err := s.mutate(ctx, func() error { <-release; close(applied); return nil })
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("wrong uncertain outcome: %v", err)
	}
	s.mu.Lock()
	n := s.inflight
	s.mu.Unlock()
	if n != 1 {
		t.Fatal("pending effect lost its bound")
	}
	close(release)
	select {
	case <-applied:
	case <-time.After(time.Second):
		t.Fatal("submitted effect canceled")
	}
}
