package client

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func responseError(ctx context.Context, reason, outcome, leader string, code codes.Code) error {
	md, _ := metadata.FromOutgoingContext(ctx)
	s, _ := status.New(code, "fixture").WithDetails(&errdetails.ErrorInfo{Domain: "quorumkv", Reason: reason, Metadata: map[string]string{"request_id": md.Get(requestIDHeader)[0], "outcome": outcome, "leader": leader}})
	return s.Err()
}

type fakeAPI struct {
	mu  sync.Mutex
	ids []string
	run func(context.Context) error
}

func (f *fakeAPI) invoke(ctx context.Context) error {
	md, _ := metadata.FromOutgoingContext(ctx)
	f.mu.Lock()
	f.ids = append(f.ids, md.Get(requestIDHeader)[0])
	f.mu.Unlock()
	return f.run(ctx)
}
func (f *fakeAPI) Get(ctx context.Context, _ *pb.GetRequest, _ ...grpc.CallOption) (*pb.GetResponse, error) {
	if err := f.invoke(ctx); err != nil {
		return nil, err
	}
	return &pb.GetResponse{Value: []byte("v"), Found: true}, nil
}
func (f *fakeAPI) Put(ctx context.Context, _ *pb.PutRequest, _ ...grpc.CallOption) (*pb.PutResponse, error) {
	return &pb.PutResponse{}, f.invoke(ctx)
}
func (f *fakeAPI) Delete(ctx context.Context, _ *pb.DeleteRequest, _ ...grpc.CallOption) (*pb.DeleteResponse, error) {
	return &pb.DeleteResponse{}, f.invoke(ctx)
}
func testClient(fs ...*fakeAPI) *Client {
	c := &Client{config: Config{MaxAttempts: 3, AttemptTimeout: time.Second, Timeout: 3 * time.Second}}
	for i, f := range fs {
		address := []string{"127.0.0.1:21001", "127.0.0.1:21002", "127.0.0.1:21003"}[i]
		c.endpoints = append(c.endpoints, endpoint{address: address, api: f})
	}
	return c
}

func TestMutationUnknownAndUncertainErrorsNeverRetry(t *testing.T) {
	for _, kind := range []string{"transport", "deadline", "wrong-id", "wrong-code", "uncertain", "capacity"} {
		t.Run(kind, func(t *testing.T) {
			f := &fakeAPI{run: func(ctx context.Context) error {
				switch kind {
				case "transport":
					return status.Error(codes.Unavailable, "reply lost")
				case "deadline":
					return status.Error(codes.DeadlineExceeded, "reply lost")
				case "wrong-id":
					ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(requestIDHeader, "different"))
					return responseError(ctx, "NOT_LEADER", "not_submitted", "", codes.FailedPrecondition)
				case "wrong-code":
					return responseError(ctx, "NOT_LEADER", "not_submitted", "", codes.Internal)
				case "capacity":
					return responseError(ctx, "KEYSPACE_FULL", "not_applied", "", codes.ResourceExhausted)
				default:
					return responseError(ctx, "APPLY_INTERRUPTED", "uncertain", "", codes.Unavailable)
				}
			}}
			r, err := testClient(f).Put(context.Background(), []byte("k"), []byte("v"))
			want := "uncertain"
			if kind == "capacity" {
				want = "not_applied"
			}
			if err == nil || r.Outcome != want || len(r.Attempts) != 1 || len(f.ids) != 1 {
				t.Fatalf("unsafe mutation retry/outcome: %+v %v", r, err)
			}
			var typed *Error
			if !errors.As(err, &typed) || typed.Receipt.OperationID != r.OperationID {
				t.Fatal("missing retained receipt")
			}
		})
	}
}

func TestConfirmedRejectionRedirectsOnlyToConfiguredEndpoints(t *testing.T) {
	for _, hint := range []string{"127.0.0.1:21003", "dns:///attacker:443"} {
		t.Run(hint, func(t *testing.T) {
			f := &fakeAPI{run: func(ctx context.Context) error {
				return responseError(ctx, "NOT_LEADER", "not_submitted", hint, codes.FailedPrecondition)
			}}
			g := &fakeAPI{run: func(context.Context) error { return nil }}
			h := &fakeAPI{run: func(context.Context) error { return nil }}
			r, err := testClient(f, g, h).Delete(context.Background(), []byte("k"))
			if err != nil || r.Outcome != "acknowledged" || len(r.Attempts) != 2 || r.Attempts[0].Outcome != "not_submitted" {
				t.Fatalf("redirect failed %+v %v", r, err)
			}
			want := "127.0.0.1:21002"
			if hint == "127.0.0.1:21003" {
				want = hint
			}
			if r.Attempts[1].Endpoint != want || r.Attempts[0].ID == r.Attempts[1].ID || r.OperationID == r.Attempts[0].ID {
				t.Fatalf("unsafe redirect/identity %+v", r)
			}
		})
	}
}

func TestReadRetryAndAttemptCeilings(t *testing.T) {
	f := &fakeAPI{run: func(context.Context) error { return status.Error(codes.Unavailable, "offline") }}
	c := testClient(f)
	c.config.MaxAttempts = 2
	_, r, err := c.Get(context.Background(), []byte("k"))
	if err == nil || r.Outcome != "no_mutation" || len(r.Attempts) != 2 {
		t.Fatalf("read retry bounds %+v %v", r, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err = c.Put(ctx, []byte("k"), []byte("v"))
	if !errors.Is(err, context.Canceled) || len(r.Attempts) != 0 || r.Outcome != "not_submitted" {
		t.Fatalf("pre-send cancellation %+v %v", r, err)
	}
}

func TestStaleLeaderHintCannotTrapDiscoveryAtStoppedMember(t *testing.T) {
	stopped := &fakeAPI{run: func(context.Context) error { return status.Error(codes.Unavailable, "stopped") }}
	follower := &fakeAPI{run: func(ctx context.Context) error {
		return responseError(ctx, "NOT_LEADER", "not_submitted", "127.0.0.1:21001", codes.FailedPrecondition)
	}}
	leader := &fakeAPI{run: func(context.Context) error { return nil }}
	response, r, err := testClient(stopped, follower, leader).Get(context.Background(), []byte("k"))
	if err != nil || response == nil || !response.Found || len(r.Attempts) != 3 || r.Attempts[2].Endpoint != "127.0.0.1:21003" {
		t.Fatalf("stale hint trapped discovery %+v %v", r, err)
	}
	if len(stopped.ids) != 1 || len(follower.ids) != 1 || len(leader.ids) != 1 {
		t.Fatal("configured members starved")
	}
}

func TestRetryWaitCancellationPreservesKnownOutcome(t *testing.T) {
	f := &fakeAPI{run: func(ctx context.Context) error {
		return responseError(ctx, "ADMISSION_FULL", "not_submitted", "", codes.ResourceExhausted)
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	r, err := testClient(f).Put(ctx, []byte("k"), []byte("v"))
	if !errors.Is(err, context.DeadlineExceeded) || len(r.Attempts) != 1 || r.Outcome != "not_submitted" {
		t.Fatalf("backoff cancellation %+v %v", r, err)
	}
}

func TestUnreasonableRetryAdviceAndDuplicateDetailsStop(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		f := &fakeAPI{run: func(ctx context.Context) error {
			err := responseError(ctx, "ADMISSION_FULL", "not_submitted", "", codes.ResourceExhausted)
			s := status.Convert(err)
			if duplicate {
				s, _ = s.WithDetails(s.Details()[0].(*errdetails.ErrorInfo))
			} else {
				s, _ = s.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(10 * time.Second)})
			}
			return s.Err()
		}}
		r, err := testClient(f).Put(context.Background(), []byte("k"), []byte("v"))
		if err == nil || len(r.Attempts) != 1 {
			t.Fatalf("unbounded advice retried %+v %v", r, err)
		}
		if duplicate && r.Outcome != "uncertain" {
			t.Fatal("ambiguous details treated as rejection")
		}
	}
}

func TestClientValidationAndClosedClientHaveNoAttempts(t *testing.T) {
	for _, addr := range []string{"dns:///host:443", "user@host:443", "host:0", "host:080", "host/path:443", "bad..host:443", "host:443?x"} {
		if _, err := normalized(Config{Endpoints: []Endpoint{{Address: addr}}}); err == nil {
			t.Fatal("invalid address accepted", addr)
		}
	}
	if _, err := New(Config{Endpoints: []Endpoint{{Address: "127.0.0.1:21001", Identity: "spiffe://quorumkv/peer/one"}}}); err == nil {
		t.Fatal("missing TLS accepted")
	}
	f := &fakeAPI{run: func(context.Context) error { return nil }}
	c := testClient(f)
	r, err := c.Put(context.Background(), nil, nil)
	if err == nil || len(r.Attempts) != 0 || r.Outcome != "not_submitted" {
		t.Fatal("invalid size sent")
	}
	c.Close()
	c.Close()
	r, err = c.Delete(context.Background(), []byte("k"))
	if err == nil || len(r.Attempts) != 0 || len(f.ids) != 0 {
		t.Fatal("closed client sent")
	}
}
