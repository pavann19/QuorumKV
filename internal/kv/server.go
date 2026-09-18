// Package kv implements the gRPC KV service (proto/kv.proto) as a thin
// layer over internal/store: it does request/response translation only,
// with no logic of its own to get wrong -- durability and correctness are
// entirely internal/store's (and, beneath that, internal/wal's) job.
package kv

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pavann19/quorumkv/internal/store"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

// Server implements pb.KVServer.
type Server struct {
	pb.UnimplementedKVServer
	Store *store.Store
}

func New(s *store.Store) *Server {
	return &Server{Store: s}
}

func (s *Server) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, found := s.Store.Get(req.GetKey())
	return &pb.GetResponse{Value: value, Found: found}, nil
}

func (s *Server) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if len(req.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if err := s.Store.Put(req.GetKey(), req.GetValue()); err != nil {
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}
	return &pb.PutResponse{}, nil
}

func (s *Server) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if len(req.GetKey()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if err := s.Store.Delete(req.GetKey()); err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	return &pb.DeleteResponse{}, nil
}
