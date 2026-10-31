package raftnode

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

// applyTimeout bounds Raft enqueue waits; it does not cancel committed writes.
const applyTimeout = 5 * time.Second
const MaxKeyBytes = 4096
const MaxValueBytes = 1 << 20

// KVServer implements pb.KVServer backed by a Raft-replicated Node,
// instead of internal/kv.Server's direct-to-store implementation used by
// the single-node MVP. Writes only succeed on the leader; a non-leader
// returns an error naming the current leader, so a client can retry there.
type KVServer struct {
	pb.UnimplementedKVServer
	Node     *Node
	mu       sync.Mutex
	inflight int
}

func NewKVServer(n *Node) *KVServer {
	return &KVServer{Node: n}
}

// Get commits a Raft barrier before reading, fencing isolated leaders and
// waiting until preceding committed writes have reached the local FSM.
func (s *KVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if err := validateSize(req.GetKey(), nil); err != nil {
		return nil, err
	}
	if !s.Node.IsLeader() {
		return nil, s.notLeaderError()
	}
	ctx, cancel := context.WithTimeout(ctx, applyTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	deadline, _ := ctx.Deadline()
	if !s.acquire() {
		return nil, status.Error(codes.ResourceExhausted, "request limit")
	}
	future := s.Node.Raft.Barrier(time.Until(deadline))
	done := make(chan error, 1)
	go func() { err := future.Error(); s.release(); done <- err }()
	select {
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case err := <-done:
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "read barrier failed: %v", err)
		}
	}
	value, found := s.Node.Get(req.GetKey())
	return &pb.GetResponse{Value: value, Found: found}, nil
}

func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if err := validateSize(req.GetKey(), req.GetValue()); err != nil {
		return nil, err
	}
	if !s.Node.IsLeader() {
		return nil, s.notLeaderError()
	}
	if err := s.mutate(ctx, func() error { return s.Node.Put(req.GetKey(), req.GetValue(), applyTimeout) }); err != nil {
		return nil, err
	}
	return &pb.PutResponse{}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := validateSize(req.GetKey(), nil); err != nil {
		return nil, err
	}
	if !s.Node.IsLeader() {
		return nil, s.notLeaderError()
	}
	if err := s.mutate(ctx, func() error { return s.Node.Delete(req.GetKey(), applyTimeout) }); err != nil {
		return nil, err
	}
	return &pb.DeleteResponse{}, nil
}

func validateSize(key, value []byte) error {
	if len(key) == 0 || len(key) > MaxKeyBytes || len(value) > MaxValueBytes {
		return status.Error(codes.InvalidArgument, "key/value size outside configured bounds")
	}
	return nil
}

// A deadline ends the RPC wait, not the submitted Raft operation. Keep its slot
// until the future resolves and return a conservative uncertain error.
func (s *KVServer) mutate(ctx context.Context, apply func() error) error {
	ctx, cancel := context.WithTimeout(ctx, applyTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if !s.acquire() {
		return status.Error(codes.ResourceExhausted, "mutation limit")
	}
	done := make(chan error, 1)
	go func() { err := apply(); s.release(); done <- err }()
	select {
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case err := <-done:
		if err != nil {
			return s.applyError(err)
		}
		return nil
	}
}

func (s *KVServer) notLeaderError() error {
	leader := s.Node.LeaderAddr()
	if leader == "" {
		return status.Error(codes.Unavailable, "not the leader, and no leader is currently known (election in progress?)")
	}
	return status.Errorf(codes.FailedPrecondition, "not the leader; current leader is at %s", leader)
}

func (s *KVServer) acquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight >= 64 {
		return false
	}
	s.inflight++
	return true
}
func (s *KVServer) release() { s.mu.Lock(); s.inflight--; s.mu.Unlock() }

func (s *KVServer) applyError(err error) error {
	return status.Errorf(codes.Internal, "applying to raft: %v", err)
}
