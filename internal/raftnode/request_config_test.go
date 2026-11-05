package raftnode

import (
	"context"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/requestlimits"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConfiguredRequestBoundsAndSharedAdmission(t *testing.T) {
	limits := requestlimits.Defaults()
	limits.KeyBytes = 2
	limits.ValueBytes = 3
	limits.Inflight = 2
	limits.TimeoutMS = 20
	n := &Node{requestLimits: limits}
	s := NewKVServer(n)
	if s.validateSize([]byte("abc"), nil) == nil || s.validateSize([]byte("a"), []byte("1234")) == nil {
		t.Fatal("configured size bound bypassed")
	}
	if s.validateSize([]byte("ab"), []byte("123")) != nil {
		t.Fatal("legal configured input refused")
	}
	if !s.acquire() || !s.acquire() {
		t.Fatal("configured admission refused early")
	}
	if NewKVServer(n).acquire() {
		t.Fatal("configured admission bypassed")
	}
	s.release()
	s.release()
	release := make(chan struct{})
	started := time.Now()
	err := s.mutate(context.Background(), func() error { <-release; return nil })
	close(release)
	if status.Code(err) != codes.DeadlineExceeded || time.Since(started) > time.Second {
		t.Fatalf("configured wait ignored: %v", err)
	}
}
