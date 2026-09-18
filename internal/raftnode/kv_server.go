package raftnode

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

// applyTimeout bounds how long a Put/Delete waits for Raft to commit and
// apply it before giving up and returning an error to the client.
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

// Get reads from this node's local store. Per the build plan's "start
// simple" option, this is leader-only: a follower rejects Get rather than
// silently serving a potentially stale read, since stale-follower-reads
// are explicitly meant to be a separate, opt-in mode (not yet built) per
// the plan, not the default.
func (s *KVServer) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if !s.Node.IsLeader() {
		return nil, s.notLeaderError()
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
