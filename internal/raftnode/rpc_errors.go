package raftnode

import (
	"context"
	"errors"

	"github.com/hashicorp/raft"
	"github.com/pavann19/quorumkv/internal/diskhealth"
	"github.com/pavann19/quorumkv/internal/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rpcFailure gives clients a stable contract without exposing internal errors.
// outcome applies to this invocation only; a retry may have its own history.
func rpcFailure(code codes.Code, reason, outcome string, metadata map[string]string) error {
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata["outcome"] = outcome
	st := status.New(code, "quorumkv: "+reason)
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "quorumkv", Metadata: metadata})
	if err != nil {
		return st.Err()
	}
	return detailed.Err()
}

func mutationContextError(err error, submitted bool) error {
	outcome := "not_submitted"
	if submitted {
		outcome = "uncertain"
	}
	return rpcFailure(status.Code(status.FromContextError(err).Err()), "CONTEXT_ENDED", outcome, nil)
}

func (s *KVServer) applyError(err error) error {
	switch {
	case errors.Is(err, diskhealth.ErrPressure):
		return rpcFailure(codes.ResourceExhausted, "DISK_PRESSURE", "not_submitted", nil)
	case errors.Is(err, diskhealth.ErrUnknown):
		return rpcFailure(codes.Unavailable, "DISK_STATUS_UNKNOWN", "not_submitted", nil)
	case errors.Is(err, store.ErrCapacity):
		return rpcFailure(codes.ResourceExhausted, "KEYSPACE_FULL", "not_applied", nil)
	case errors.Is(err, raft.ErrNotLeader):
		return rpcFailure(codes.FailedPrecondition, "NOT_LEADER", "not_submitted", nil)
	case errors.Is(err, raft.ErrLeadershipTransferInProgress):
		return rpcFailure(codes.Unavailable, "LEADERSHIP_TRANSFER", "not_submitted", nil)
	case errors.Is(err, raft.ErrEnqueueTimeout):
		return rpcFailure(codes.Unavailable, "ENQUEUE_TIMEOUT", "not_submitted", nil)
	case errors.Is(err, raft.ErrLeadershipLost), errors.Is(err, raft.ErrAbortedByRestore), errors.Is(err, raft.ErrRaftShutdown):
		return rpcFailure(codes.Unavailable, "APPLY_INTERRUPTED", "uncertain", nil)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return mutationContextError(err, true)
	default:
		return rpcFailure(codes.Internal, "APPLY_FAILED", "uncertain", nil)
	}
}
