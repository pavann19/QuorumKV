package raftnode

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/requestlimits"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWriteReadGraceCancellationPreventsSubmission(t *testing.T) {
	limits := requestlimits.Defaults()
	limits.WriteReadGraceMS = 5
	server := NewKVServer(&Node{requestLimits: limits})
	server.pendingReads.Store(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	var invoked atomic.Bool
	err := server.mutate(ctx, func() error { invoked.Store(true); return nil })
	if invoked.Load() || status.Code(err) != codes.DeadlineExceeded {
		t.Fatal("grace cancellation submitted mutation", invoked.Load(), err)
	}
	outcome := ""
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			outcome = info.Metadata["outcome"]
		}
	}
	if outcome != "not_submitted" {
		t.Fatal("pre-submission cancellation became uncertain", outcome)
	}
	server.mu.Lock()
	inflight := server.inflight
	server.mu.Unlock()
	if inflight != 0 {
		t.Fatal("canceled grace leaked admission", inflight)
	}
}
func TestWriteReadGraceCannotStarveMutation(t *testing.T) {
	limits := requestlimits.Defaults()
	limits.WriteReadGraceMS = 5
	server := NewKVServer(&Node{requestLimits: limits})
	server.pendingReads.Store(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var invoked atomic.Bool
	if err := server.mutate(ctx, func() error { invoked.Store(true); return nil }); err != nil || !invoked.Load() {
		t.Fatal("persistent reader pressure starved a write", err)
	}
	if server.pendingReads.Load() != 1 {
		t.Fatal("mutation changed reader ownership")
	}
}
func TestWriteReadGraceDisabledDoesNotWaitForReaders(t *testing.T) {
	limits := requestlimits.Defaults()
	limits.WriteReadGraceMS = 0
	server := NewKVServer(&Node{requestLimits: limits})
	server.pendingReads.Store(1)
	called := false
	if err := server.mutate(context.Background(), func() error { called = true; return nil }); err != nil || !called {
		t.Fatal("disabled grace prevented submission", err)
	}
}
