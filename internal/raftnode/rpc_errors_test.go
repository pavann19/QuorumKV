package raftnode

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func outcomeInfo(t *testing.T, err error) *errdetails.ErrorInfo {
	t.Helper()
	// Round-trip the wire status representation rather than inspecting only text.
	st := status.FromProto(status.Convert(err).Proto())
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info
		}
	}
	t.Fatal("missing structured error details")
	return nil
}

func TestMutationErrorContract(t *testing.T) {
	cases := []struct {
		name            string
		err             error
		code            codes.Code
		reason, outcome string
	}{
		{"fenced", raft.ErrNotLeader, codes.FailedPrecondition, "NOT_LEADER", "not_submitted"},
		{"transfer", raft.ErrLeadershipTransferInProgress, codes.Unavailable, "LEADERSHIP_TRANSFER", "not_submitted"},
		{"enqueue", raft.ErrEnqueueTimeout, codes.Unavailable, "ENQUEUE_TIMEOUT", "not_submitted"},
		{"lost", raft.ErrLeadershipLost, codes.Unavailable, "APPLY_INTERRUPTED", "uncertain"},
		{"restore", raft.ErrAbortedByRestore, codes.Unavailable, "APPLY_INTERRUPTED", "uncertain"},
		{"shutdown", raft.ErrRaftShutdown, codes.Unavailable, "APPLY_INTERRUPTED", "uncertain"},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded, "CONTEXT_ENDED", "uncertain"},
		{"cancel", context.Canceled, codes.Canceled, "CONTEXT_ENDED", "uncertain"},
		{"unknown", errors.New("FAKE_PRIVATE_STORAGE_PATH"), codes.Internal, "APPLY_FAILED", "uncertain"},
	}
	s := &KVServer{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.applyError(fmt.Errorf("FAKE_SECRET: %w", tc.err))
			if status.Code(err) != tc.code {
				t.Fatalf("code %v", err)
			}
			info := outcomeInfo(t, err)
			if info.Domain != "quorumkv" || info.Reason != tc.reason || info.Metadata["outcome"] != tc.outcome {
				t.Fatalf("wrong contract %v", info)
			}
			if strings.Contains(err.Error(), "FAKE_") {
				t.Fatal("raw error leaked")
			}
		})
	}
}

func TestMutationRejectionNeverInvokesApply(t *testing.T) {
	for _, which := range []string{"canceled", "full"} {
		t.Run(which, func(t *testing.T) {
			s := &KVServer{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if which == "canceled" {
				cancel()
			} else {
				s.inflight = 64
			}
			called := false
			err := s.mutate(ctx, func() error { called = true; return nil })
			if called {
				t.Fatal("rejected invocation submitted")
			}
			if outcomeInfo(t, err).Metadata["outcome"] != "not_submitted" {
				t.Fatal("wrong rejection outcome")
			}
		})
	}
}

func TestDeadlineAfterSubmissionRetainsUncertainEffect(t *testing.T) {
	s := &KVServer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, finish, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() { done <- s.mutate(ctx, func() error { close(entered); <-finish; return nil }) }()
	<-entered
	cancel()
	err := <-done
	if status.Code(err) != codes.Canceled || outcomeInfo(t, err).Metadata["outcome"] != "uncertain" {
		t.Fatalf("false rejection: %v", err)
	}
	s.mu.Lock()
	pending := s.inflight
	s.mu.Unlock()
	if pending != 1 {
		t.Fatal("lost pending effect")
	}
	close(finish)
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		pending = s.inflight
		s.mu.Unlock()
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("admission leaked")
		}
		time.Sleep(time.Millisecond)
	}
}
