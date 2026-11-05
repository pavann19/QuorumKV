package raftnode

import (
	"context"
	"sync"

	transportpb "github.com/Jille/raft-grpc-transport/proto"
	"github.com/pavann19/quorumkv/internal/requestlimits"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const MaxReplicationEntries = 8
const MaxLogicalAppendBytes = MaxReplicationEntries * requestlimits.MaxMessageBytes
const PipelineAppendByteBudget = MaxLogicalAppendBytes
const appendUnaryMethod = "/RaftTransport/AppendEntries"
const appendPipelineMethod = "/RaftTransport/AppendEntriesPipeline"

// Preserve the adopted wire schema and receive ceiling. Logical groups may be
// larger than a physical RPC, but every piece has a valid preceding log link.
// Validate the entire plan before sending any of it.
func splitAppendRequest(request *transportpb.AppendEntriesRequest) ([]*transportpb.AppendEntriesRequest, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "peer append request missing")
	}
	if len(request.Entries) > MaxReplicationEntries {
		return nil, status.Error(codes.ResourceExhausted, "peer append group exceeds entry bound")
	}
	if proto.Size(request) <= requestlimits.MaxMessageBytes {
		return []*transportpb.AppendEntriesRequest{request}, nil
	}
	if len(request.Entries) == 0 || len(request.Entries) > MaxReplicationEntries || proto.Size(request) > MaxLogicalAppendBytes {
		return nil, status.Error(codes.ResourceExhausted, "peer append group exceeds bound")
	}
	if len(request.ProtoReflect().GetUnknown()) != 0 || request.ProtoReflect().Descriptor().Fields().Len() != 7 {
		return nil, status.Error(codes.FailedPrecondition, "peer append schema requires adapter upgrade")
	}
	previous := request.PrevLogEntry
	for _, entry := range request.Entries {
		if entry == nil || entry.Index <= previous || entry.Index-previous != 1 {
			return nil, status.Error(codes.InvalidArgument, "peer append log sequence invalid")
		}
		previous = entry.Index
	}
	parts := []*transportpb.AppendEntriesRequest{}
	previous, previousTerm := request.PrevLogEntry, request.PrevLogTerm
	for start := 0; start < len(request.Entries); {
		part := &transportpb.AppendEntriesRequest{RpcHeader: request.RpcHeader, Term: request.Term, Leader: request.Leader, PrevLogEntry: previous, PrevLogTerm: previousTerm, LeaderCommitIndex: request.LeaderCommitIndex}
		end := start
		for end < len(request.Entries) {
			part.Entries = request.Entries[start : end+1]
			if proto.Size(part) > requestlimits.MaxMessageBytes {
				break
			}
			end++
		}
		if end == start {
			return nil, status.Error(codes.ResourceExhausted, "peer append entry exceeds wire bound")
		}
		part.Entries = request.Entries[start:end]
		parts = append(parts, part)
		last := request.Entries[end-1]
		previous, previousTerm = last.Index, last.Term
		start = end
	}
	return parts, nil
}
func copyAppendResponse(destination, source *transportpb.AppendEntriesResponse) {
	proto.Reset(destination)
	proto.Merge(destination, source)
}
func invokeSizedAppend(ctx context.Context, method string, request, response any, connection *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
	if method != appendUnaryMethod {
		return invoke(ctx, method, request, response, connection, options...)
	}
	input, ok := request.(*transportpb.AppendEntriesRequest)
	output, outputOK := response.(*transportpb.AppendEntriesResponse)
	if !ok || !outputOK {
		return status.Error(codes.Internal, "peer append type invalid")
	}
	parts, err := splitAppendRequest(input)
	if err != nil {
		return err
	}
	if len(parts) == 1 {
		return invoke(ctx, method, request, response, connection, options...)
	}
	for _, part := range parts {
		received := new(transportpb.AppendEntriesResponse)
		if err := invoke(ctx, method, part, received, connection, options...); err != nil {
			return err
		}
		// A rejection/higher term ends this sequential attempt. Already sent pieces
		// remain uncertain; the adopted Raft leader controls all subsequent retry.
		if !received.Success || received.Term > input.Term {
			copyAppendResponse(output, received)
			return nil
		}
		response = received
	}
	copyAppendResponse(output, response.(*transportpb.AppendEntriesResponse))
	return nil
}

type appendByteBudget struct {
	sync.Mutex
	used    int
	limit   int
	changed chan struct{}
}

func (b *appendByteBudget) acquire(ctx context.Context, size int) error {
	if size > b.limit {
		return status.Error(codes.ResourceExhausted, "peer append byte budget exceeded")
	}
	for {
		b.Lock()
		if err := ctx.Err(); err != nil {
			b.Unlock()
			return err
		}
		if size <= b.limit-b.used {
			b.used += size
			b.Unlock()
			return nil
		}
		if b.changed == nil {
			b.changed = make(chan struct{})
		}
		changed := b.changed
		b.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
func (b *appendByteBudget) release(size int) {
	b.Lock()
	defer b.Unlock()
	b.used -= size
	if b.changed != nil {
		close(b.changed)
		b.changed = nil
	}
}

type appendResponsePlan struct {
	pieces, bytes int
	term          uint64
}
type sizedAppendStream struct {
	grpc.ClientStream
	cancel context.CancelFunc
	sendMu sync.Mutex
	plans  chan appendResponsePlan
	budget appendByteBudget
}

func newSizedAppendStream(stream grpc.ClientStream, cancel context.CancelFunc) *sizedAppendStream {
	return &sizedAppendStream{ClientStream: stream, cancel: cancel, plans: make(chan appendResponsePlan, 32), budget: appendByteBudget{limit: PipelineAppendByteBudget}}
}
func (s *sizedAppendStream) SendMsg(message any) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	request, ok := message.(*transportpb.AppendEntriesRequest)
	if !ok {
		s.cancel()
		return status.Error(codes.Internal, "peer append stream request type invalid")
	}
	parts, err := splitAppendRequest(request)
	if err != nil {
		s.cancel()
		return err
	}
	size := max(1, proto.Size(request))
	if err := s.budget.acquire(s.Context(), size); err != nil {
		return err
	}
	for _, part := range parts {
		if err := s.ClientStream.SendMsg(part); err != nil {
			s.budget.release(size)
			s.cancel()
			return err
		}
	}
	select {
	case s.plans <- appendResponsePlan{pieces: len(parts), bytes: size, term: request.Term}:
		return nil
	case <-s.Context().Done():
		s.budget.release(size)
		s.cancel()
		return s.Context().Err()
	}
}
func (s *sizedAppendStream) RecvMsg(message any) error {
	response, ok := message.(*transportpb.AppendEntriesResponse)
	if !ok {
		s.cancel()
		return status.Error(codes.Internal, "peer append stream response type invalid")
	}
	var plan appendResponsePlan
	select {
	case plan = <-s.plans:
	case <-s.Context().Done():
		return s.Context().Err()
	}
	defer s.budget.release(plan.bytes)
	if plan.pieces == 1 {
		if err := s.ClientStream.RecvMsg(response); err != nil {
			s.cancel()
			return err
		}
		return nil
	}
	var selected *transportpb.AppendEntriesResponse
	success := true
	for i := 0; i < plan.pieces; i++ {
		received := new(transportpb.AppendEntriesResponse)
		if err := s.ClientStream.RecvMsg(received); err != nil {
			s.cancel()
			return err
		}
		success = success && received.Success && received.Term == plan.term
		// Keep the highest term. At an equal term preserve the first rejection for
		// Raft's backoff hint, or the latest success when all preceding ones succeed.
		if selected == nil || received.Term > selected.Term || (received.Term == selected.Term && selected.Success) {
			selected = received
		}
	}
	selected.Success = success
	copyAppendResponse(response, selected)
	return nil
}
