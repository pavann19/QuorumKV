package raftnode

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

// applyTimeout bounds Raft enqueue waits; it does not cancel committed writes.
const applyTimeout = 5 * time.Second

// KVServer implements pb.KVServer backed by a Raft-replicated Node,
// instead of internal/kv.Server's direct-to-store implementation used by
// the single-node MVP. Writes only succeed on the leader; a non-leader
// returns an error naming the current leader, so a client can retry there.
type KVServer struct {
	pb.UnimplementedKVServer
	Node *Node
}

func NewKVServer(n *Node) *KVServer {
	return &KVServer{Node: n}
}

// Get commits a Raft barrier before reading, fencing isolated leaders and
// waiting until preceding committed writes have reached the local FSM.
func (s *KVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if !s.Node.IsLeader() {
		return nil, s.notLeaderError()
	}
	ctx, cancel := context.WithTimeout(ctx, applyTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	deadline, _ := ctx.Deadline()
	future := s.Node.Raft.Barrier(time.Until(deadline))
	done := make(chan error, 1)
	go func() { done <- future.Error() }()
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
	if len(req.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if !s.Node.IsLeader() {
		return nil, s.notLeaderError()
	}
	if err := s.Node.Put(req.GetKey(), req.GetValue(), applyTimeout); err != nil {
		return nil, s.applyError(err)
	}
	return &pb.PutResponse{}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if len(req.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if !s.Node.IsLeader() {
		return nil, s.notLeaderError()
	}
	if err := s.Node.Delete(req.GetKey(), applyTimeout); err != nil {
		return nil, s.applyError(err)
	}
	return &pb.DeleteResponse{}, nil
}

func (s *KVServer) notLeaderError() error {
	leader := s.Node.LeaderAddr()
	if leader == "" {
		return status.Error(codes.Unavailable, "not the leader, and no leader is currently known (election in progress?)")
	}
	return status.Errorf(codes.FailedPrecondition, "not the leader; current leader is at %s", leader)
}

func (s *KVServer) applyError(err error) error {
	return status.Errorf(codes.Internal, "applying to raft: %v", err)
}
