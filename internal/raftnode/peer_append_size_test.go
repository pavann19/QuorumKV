package raftnode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	transportpb "github.com/Jille/raft-grpc-transport/proto"
	"github.com/pavann19/quorumkv/internal/requestlimits"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func largeAppendFixture() *transportpb.AppendEntriesRequest {
	request := &transportpb.AppendEntriesRequest{RpcHeader: &transportpb.RPCHeader{ProtocolVersion: 3, Id: []byte("source"), Addr: []byte("127.0.0.1:12345")}, Term: 9, Leader: []byte("leader"), PrevLogEntry: 40, PrevLogTerm: 8, LeaderCommitIndex: 48}
	for i := 0; i < 8; i++ {
		request.Entries = append(request.Entries, &transportpb.Log{Index: uint64(41 + i), Term: 9, Data: bytes.Repeat([]byte{byte(i)}, requestlimits.MaxValueBytes+4096)})
	}
	return request
}
func TestAppendWireSplittingPreservesLinksAndCompleteInput(t *testing.T) {
	request := largeAppendFixture()
	before, _ := proto.Marshal(request)
	parts, err := splitAppendRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 3 {
		t.Fatal("maximum-size group not split", len(parts))
	}
	index, term, seen := request.PrevLogEntry, request.PrevLogTerm, 0
	for _, part := range parts {
		encoded, err := proto.Marshal(part)
		if err != nil || len(encoded) > requestlimits.MaxMessageBytes {
			t.Fatal("oversized physical frame", len(encoded), err)
		}
		if part.PrevLogEntry != index || part.PrevLogTerm != term || part.LeaderCommitIndex != request.LeaderCommitIndex || !proto.Equal(part.RpcHeader, request.RpcHeader) || !bytes.Equal(part.Leader, request.Leader) || part.Term != request.Term {
			t.Fatal("append header/link changed")
		}
		for _, entry := range part.Entries {
			if !proto.Equal(entry, request.Entries[seen]) {
				t.Fatal("entry changed/reordered", seen)
			}
			seen++
		}
		last := part.Entries[len(part.Entries)-1]
		index, term = last.Index, last.Term
	}
	after, _ := proto.Marshal(request)
	if seen != 8 || !bytes.Equal(before, after) {
		t.Fatal("input mutated or entries lost")
	}
}
func TestAppendInvalidGroupRefusedBeforeInvoke(t *testing.T) {
	cases := map[string]*transportpb.AppendEntriesRequest{"nil": nil, "single_oversize": {Entries: []*transportpb.Log{{Index: 1, Data: make([]byte, requestlimits.MaxMessageBytes)}}}, "oversize_header": {Leader: make([]byte, requestlimits.MaxMessageBytes+1)}}
	gap := largeAppendFixture()
	gap.Entries[4].Index++
	cases["gap"] = gap
	unknown := largeAppendFixture()
	unknown.ProtoReflect().SetUnknown([]byte{0x48, 1})
	cases["unknown_schema"] = unknown
	for name, request := range cases {
		t.Run(name, func(t *testing.T) {
			invoked := false
			err := invokeSizedAppend(context.Background(), appendUnaryMethod, request, new(transportpb.AppendEntriesResponse), nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				invoked = true
				return nil
			})
			if err == nil || invoked {
				t.Fatal("invalid append had a wire effect", err)
			}
		})
	}
}
func TestUnaryAppendOnlyAcknowledgesCompleteSuccessfulGroup(t *testing.T) {
	for _, failAt := range []int{-1, 0, 1, 2} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			request := largeAppendFixture()
			calls := 0
			response := new(transportpb.AppendEntriesResponse)
			err := invokeSizedAppend(context.Background(), appendUnaryMethod, request, response, nil, func(_ context.Context, _ string, input, output any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				part := input.(*transportpb.AppendEntriesRequest)
				result := output.(*transportpb.AppendEntriesResponse)
				if proto.Size(part) > requestlimits.MaxMessageBytes {
					t.Fatal("oversized invocation")
				}
				current := calls
				calls++
				if current == failAt {
					return io.ErrUnexpectedEOF
				}
				result.Term = request.Term
				result.Success = true
				result.LastLog = part.Entries[len(part.Entries)-1].Index
				return nil
			})
			if failAt < 0 {
				if err != nil || calls != 3 || !response.Success || response.LastLog != 48 {
					t.Fatal("full successful group", calls, response, err)
				}
			} else if !errors.Is(err, io.ErrUnexpectedEOF) || calls != failAt+1 || response.Success {
				t.Fatal("partial group acknowledged", calls, response, err)
			}
		})
	}
	for _, higherTerm := range []bool{false, true} {
		request := largeAppendFixture()
		calls := 0
		response := new(transportpb.AppendEntriesResponse)
		err := invokeSizedAppend(context.Background(), appendUnaryMethod, request, response, nil, func(_ context.Context, _ string, _ any, output any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			result := output.(*transportpb.AppendEntriesResponse)
			calls++
			result.Term = request.Term
			result.Success = calls != 2
			if higherTerm && calls == 2 {
				result.Term++
				result.Success = true
			}
			return nil
		})
		if err != nil || calls != 2 || (!higherTerm && response.Success) || (higherTerm && response.Term != request.Term+1) {
			t.Fatal("rejection/higher term not propagated", calls, response, err)
		}
	}
}

type appendFakeStream struct {
	ctx                            context.Context
	mu                             sync.Mutex
	sent                           []*transportpb.AppendEntriesRequest
	responses                      []*transportpb.AppendEntriesResponse
	recvAt, failSendAt, failRecvAt int
}

func (s *appendFakeStream) Header() (metadata.MD, error) { return nil, nil }
func (s *appendFakeStream) Trailer() metadata.MD         { return nil }
func (s *appendFakeStream) CloseSend() error             { return nil }
func (s *appendFakeStream) Context() context.Context     { return s.ctx }
func (s *appendFakeStream) SendMsg(message any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failSendAt > 0 && len(s.sent)+1 == s.failSendAt {
		return io.ErrClosedPipe
	}
	s.sent = append(s.sent, proto.Clone(message.(*transportpb.AppendEntriesRequest)).(*transportpb.AppendEntriesRequest))
	return nil
}
func (s *appendFakeStream) RecvMsg(message any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failRecvAt > 0 && s.recvAt+1 == s.failRecvAt {
		return io.ErrUnexpectedEOF
	}
	if s.recvAt >= len(s.responses) {
		return io.EOF
	}
	copyAppendResponse(message.(*transportpb.AppendEntriesResponse), s.responses[s.recvAt])
	s.recvAt++
	return nil
}
func fakeAppendResponses(count int) []*transportpb.AppendEntriesResponse {
	result := []*transportpb.AppendEntriesResponse{}
	for i := 0; i < count; i++ {
		result = append(result, &transportpb.AppendEntriesResponse{Term: 9, Success: true, LastLog: uint64(41 + i*3)})
	}
	return result
}
func TestAppendPipelineDrainsWholeGroupAndPreservesNextResponse(t *testing.T) {
	for _, rejectAt := range []int{-1, 0, 1, 2} {
		ctx, cancel := context.WithCancel(context.Background())
		fake := &appendFakeStream{ctx: ctx, responses: fakeAppendResponses(4)}
		if rejectAt >= 0 {
			fake.responses[rejectAt].Success = false
			fake.responses[rejectAt].LastLog = 17
			fake.responses[rejectAt].NoRetryBackoff = true
		}
		stream := newSizedAppendStream(fake, cancel)
		if err := stream.SendMsg(largeAppendFixture()); err != nil {
			t.Fatal(err)
		}
		if err := stream.SendMsg(&transportpb.AppendEntriesRequest{Term: 9}); err != nil {
			t.Fatal(err)
		}
		response := new(transportpb.AppendEntriesResponse)
		if err := stream.RecvMsg(response); err != nil {
			t.Fatal(err)
		}
		if fake.recvAt != 3 || (rejectAt < 0) != response.Success || (rejectAt >= 0 && (response.LastLog != 17 || !response.NoRetryBackoff)) {
			t.Fatal("partial response lost/rejected group acknowledged", rejectAt, response, fake.recvAt)
		}
		next := new(transportpb.AppendEntriesResponse)
		if err := stream.RecvMsg(next); err != nil || !next.Success || fake.recvAt != 4 {
			t.Fatal("next logical response misaligned", next, err)
		}
		if stream.budget.used != 0 {
			t.Fatal("byte credits leaked")
		}
		cancel()
	}
}
func TestAppendPipelineHigherTermAndLostTailCannotAcknowledge(t *testing.T) {
	for _, lostTail := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		fake := &appendFakeStream{ctx: ctx, responses: fakeAppendResponses(3)}
		if lostTail {
			fake.failRecvAt = 3
		} else {
			fake.responses[0].Term = 11
			fake.responses[0].Success = false
		}
		stream := newSizedAppendStream(fake, cancel)
		if err := stream.SendMsg(largeAppendFixture()); err != nil {
			t.Fatal(err)
		}
		response := new(transportpb.AppendEntriesResponse)
		err := stream.RecvMsg(response)
		if lostTail {
			if !errors.Is(err, io.ErrUnexpectedEOF) || response.Success {
				t.Fatal("lost tail acknowledged", response, err)
			}
		} else if err != nil || response.Term != 11 || response.Success || fake.recvAt != 3 {
			t.Fatal("higher term lost", response, err)
		}
		if stream.budget.used != 0 {
			t.Fatal("failed group leaked credits")
		}
		cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &appendFakeStream{ctx: ctx, failSendAt: 2}
	stream := newSizedAppendStream(fake, cancel)
	if err := stream.SendMsg(largeAppendFixture()); !errors.Is(err, io.ErrClosedPipe) || stream.budget.used != 0 || ctx.Err() == nil {
		t.Fatal("partial send did not cancel/release", err)
	}
}
func TestAppendPipelineByteBudgetBackpressureAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &appendFakeStream{ctx: ctx, responses: fakeAppendResponses(6)}
	stream := newSizedAppendStream(fake, cancel)
	request := largeAppendFixture()
	stream.budget.limit = proto.Size(request)
	if err := stream.SendMsg(request); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- stream.SendMsg(request) }()
	select {
	case err := <-done:
		t.Fatal("byte budget bypassed", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := stream.RecvMsg(new(transportpb.AppendEntriesResponse)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("byte release did not wake sender")
	}
	go func() { done <- stream.SendMsg(request) }()
	select {
	case err := <-done:
		t.Fatal("second budget bypass", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("budget cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled sender did not stop")
	}
}
