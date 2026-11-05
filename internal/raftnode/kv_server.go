package raftnode

import (
	"bytes"
	"context"
	"github.com/pavann19/quorumkv/internal/requestlimits"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

// applyTimeout bounds Raft enqueue waits; it does not cancel committed writes.
const applyTimeout = requestlimits.RPCWait
const MaxKeyBytes = requestlimits.MaxKeyBytes
const MaxValueBytes = requestlimits.MaxValueBytes

// KVServer implements pb.KVServer backed by a Raft-replicated Node,
// instead of internal/kv.Server's direct-to-store implementation used by
// the single-node MVP. Writes only succeed on the leader; a non-leader
// returns an error naming the current leader, so a client can retry there.
type KVServer struct {
	pb.UnimplementedKVServer
	Node          *Node
	mu            sync.Mutex
	inflight      int
	pendingReads  atomic.Int64
	readCompleted chan struct{}
}

func NewKVServer(n *Node) *KVServer {
	// All entry points, including HTTP readiness, share outstanding futures.
	// Recreating wrappers must not reset the node's admission budget.
	n.kvOnce.Do(func() { n.kvServer = &KVServer{Node: n} })
	return n.kvServer
}

// Get fences reads against a fresh quorum and completed local FSM application.
func (s *KVServer) Get(ctx context.Context, req *pb.GetRequest) (response *pb.GetResponse, resultErr error) {
	id, err := beginRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = s.finishRPC(id, resultErr, false) }()
	if err := s.validateSize(req.GetKey(), nil); err != nil {
		return nil, err
	}
	if !s.Node.IsLeader() {
		return nil, s.notLeaderError(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, s.limits().Wait())
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if !s.acquire() {
		return nil, status.Error(codes.ResourceExhausted, "request limit")
	}
	type readResult struct {
		response *pb.GetResponse
		err      error
	}
	key := bytes.Clone(req.GetKey())
	done := make(chan readResult, 1)
	s.pendingReads.Add(1)
	go func() {
		// Retain the admission slot until any submitted quorum/barrier future ends,
		// even if its caller has already timed out.
		defer s.release()
		defer s.finishRead()
		response, err := s.Node.fencedRead(ctx, key)
		done <- readResult{response, err}
	}()
	select {
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case result := <-done:
		if result.err != nil {
			if ctx.Err() != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			return nil, rpcFailure(codes.Unavailable, "READ_FENCE_FAILED", "no_mutation", nil)
		}
		return result.response, nil
	}
}

func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (response *pb.PutResponse, resultErr error) {
	id, err := beginRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = s.finishRPC(id, resultErr, true) }()
	if err := s.validateSize(req.GetKey(), req.GetValue()); err != nil {
		return nil, err
	}
	if !s.Node.IsLeader() {
		return nil, s.notLeaderError(ctx)
	}
	if err := s.mutate(ctx, func() error { return s.Node.Put(req.GetKey(), req.GetValue(), s.limits().Wait()) }); err != nil {
		return nil, err
	}
	return &pb.PutResponse{}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (response *pb.DeleteResponse, resultErr error) {
	id, err := beginRequest(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = s.finishRPC(id, resultErr, true) }()
	if err := s.validateSize(req.GetKey(), nil); err != nil {
		return nil, err
	}
	if !s.Node.IsLeader() {
		return nil, s.notLeaderError(ctx)
	}
	if err := s.mutate(ctx, func() error { return s.Node.Delete(req.GetKey(), s.limits().Wait()) }); err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{}, nil
}

func validateSize(key, value []byte) error {
	if len(key) == 0 || len(key) > MaxKeyBytes || len(value) > MaxValueBytes {
		return rpcFailure(codes.InvalidArgument, "REQUEST_SIZE_INVALID", "not_submitted", nil)
	}
	return nil
}

// A deadline ends the RPC wait, not the submitted Raft operation. Keep its slot
// until the future resolves and return a conservative uncertain error.
func (s *KVServer) mutate(ctx context.Context, apply func() error) error {
	ctx, cancel := context.WithTimeout(ctx, s.limits().Wait())
	defer cancel()
	if err := ctx.Err(); err != nil {
		return mutationContextError(err, false)
	}
	if !s.acquire() {
		return rpcFailure(codes.ResourceExhausted, "ADMISSION_FULL", "not_submitted", nil)
	}
	// A short, bounded coalescing interval gives pending quorum confirmations
	// room to run before another client write burst. It does not delay FSM apply
	// or revoke submitted work, and cannot starve writers while readers persist.
	if grace := s.limits().WriteReadGraceMS; grace > 0 {
		if ready := s.readGraceSignal(); ready != nil {
			timer := time.NewTimer(time.Duration(grace) * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				s.release()
				return mutationContextError(ctx.Err(), false)
			case <-ready:
				timer.Stop()
			case <-timer.C:
			}
			if err := ctx.Err(); err != nil {
				s.release()
				return mutationContextError(err, false)
			}
		}
	}

	done := make(chan error, 1)
	go func() { err := apply(); s.release(); done <- err }()
	select {
	case <-ctx.Done():
		return mutationContextError(ctx.Err(), true)
	case err := <-done:
		if err != nil {
			return s.applyError(err)
		}
		return nil
	}
}

func (s *KVServer) readGraceSignal() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingReads.Load() == 0 {
		return nil
	}
	if s.readCompleted == nil {
		s.readCompleted = make(chan struct{})
	}
	return s.readCompleted
}
func (s *KVServer) finishRead() {
	s.pendingReads.Add(-1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingReads.Load() == 0 && s.readCompleted != nil {
		close(s.readCompleted)
		s.readCompleted = nil
	}
}

func (s *KVServer) notLeaderError(ctx context.Context) error {
	leader := s.Node.LeaderAddr()
	if leader == "" {
		return rpcFailure(codes.Unavailable, "NO_LEADER", "not_submitted", nil)
	}
	// A leader hint is advisory routing information, not an authorization claim.
	grpc.SetTrailer(ctx, metadata.Pairs("quorumkv-leader", leader))
	return rpcFailure(codes.FailedPrecondition, "NOT_LEADER", "not_submitted", map[string]string{"leader": leader})
}

func (s *KVServer) acquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight >= s.limits().Inflight {
		return false
	}
	s.inflight++
	return true
}
func (s *KVServer) release() { s.mu.Lock(); s.inflight--; s.mu.Unlock() }

func (s *KVServer) limits() requestlimits.Limits {
	if s.Node == nil {
		return requestlimits.Defaults()
	}
	return s.Node.RequestLimits()
}
func (s *KVServer) validateSize(key, value []byte) error {
	limits := s.limits()
	if len(key) == 0 || len(key) > limits.KeyBytes || len(value) > limits.ValueBytes {
		return rpcFailure(codes.InvalidArgument, "REQUEST_SIZE_INVALID", "not_submitted", nil)
	}
	return nil
}
