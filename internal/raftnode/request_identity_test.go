package raftnode

import (
	"context"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRequestIDValidationAndGeneration(t *testing.T) {
	id, err := newRequestID()
	if err != nil || !validRequestID(id) {
		t.Fatal("invalid generated v7 ID", id, err)
	}
	for _, bad := range []string{"", "FAKE_SECRET", "01890f2e-7c00-4000-8000-000000000001", "01890f2e-7c00-7000-0000-000000000001", "01890F2E-7c00-7000-8000-000000000001"} {
		if validRequestID(bad) {
			t.Fatal("invalid request ID accepted")
		}
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(RequestIDHeader, id, RequestIDHeader, id))
	if _, err := beginRequest(ctx); status.Code(err) != codes.InvalidArgument {
		t.Fatal("duplicate ID accepted", err)
	}
}
func TestRequestDetailsPreserveUncertaintyAndRetryAdvice(t *testing.T) {
	id := "01890f2e-7c00-7000-8000-000000000001"
	for _, tc := range []struct {
		reason, outcome, action string
		delay                   time.Duration
	}{
		{"ADMISSION_FULL", "not_submitted", "bounded_retry_if_no_prior_uncertainty", 100 * time.Millisecond},
		{"DISK_PRESSURE", "not_submitted", "bounded_retry_if_no_prior_uncertainty", time.Second},
		{"KEYSPACE_FULL", "not_applied", "review_capacity", 0},
		{"APPLY_INTERRUPTED", "uncertain", "reconcile", 0},
		{"CONTEXT_ENDED", "uncertain", "reconcile", 0},
	} {
		original := rpcFailure(codes.Unavailable, tc.reason, tc.outcome, nil)
		err := finishRequest(id, original, true)
		info := outcomeInfo(t, err)
		if info.Metadata["request_id"] != id || info.Metadata["outcome"] != tc.outcome || info.Metadata["next_action"] != tc.action {
			t.Fatalf("lost contract %v", info)
		}
		if _, exists := outcomeInfo(t, original).Metadata["request_id"]; exists {
			t.Fatal("decorator mutated original status")
		}
		var delay time.Duration
		for _, detail := range status.Convert(err).Details() {
			if retry, ok := detail.(*errdetails.RetryInfo); ok {
				delay = retry.RetryDelay.AsDuration()
			}
		}
		if delay != tc.delay {
			t.Fatalf("wrong retry delay: %s %v", tc.reason, delay)
		}
	}
}

func TestUnknownMutationStatusNeverBecomesKnownRejection(t *testing.T) {
	id := "01890f2e-7c00-7000-8000-000000000001"
	err := finishRequest(id, status.Error(codes.Internal, "FAKE_PRIVATE_STORAGE_PATH"), true)
	info := outcomeInfo(t, err)
	if info.Metadata["outcome"] != "uncertain" || info.Metadata["next_action"] != "reconcile" {
		t.Fatal("unknown status weakened uncertainty", info)
	}
	if status.Convert(err).Message() != "quorumkv: RPC_FAILURE" {
		t.Fatal("unknown internal message exposed")
	}
	for _, detail := range status.Convert(err).Details() {
		if _, ok := detail.(*errdetails.RetryInfo); ok {
			t.Fatal("uncertain mutation received retry advice")
		}
	}
}
