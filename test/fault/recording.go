package fault

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Recorder stores every attempt, including requests whose reply does not reveal
// whether a mutation committed. IDs describe observations, not server deduplication.
type Recorder struct {
	beforeOperation func(int, int)
	mu              sync.Mutex
	history         []Op
	operations      atomic.Int64
	attempts        atomic.Int64
	clock           atomic.Int64
	events          []FaultEvent
}

type FaultEvent struct {
	Kind  string `json:"kind"`
	Node  string `json:"node"`
	Event int64  `json:"event"`
}

func (r *Recorder) Mark(kind, node string) {
	event := FaultEvent{Kind: kind, Node: node, Event: r.tick()}
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *Recorder) Events() []FaultEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]FaultEvent(nil), r.events...)
}

func NewRecorder() *Recorder { return &Recorder{} }

// A logical event clock avoids wall-clock jumps and gives calls/responses a
// strict order across clients. CallNS/ReturnNS retain their legacy JSON names.
func (r *Recorder) tick() int64 { return r.clock.Add(1) }

func (r *Recorder) Execute(ctx context.Context, clientID int, kind, key, value string, nodes []string, clients []pb.KVClient, attemptTimeout time.Duration) error {
	return r.execute(ctx, clientID, kind, key, value, nodes, clients, attemptTimeout, true)
}

func (r *Recorder) ExecuteOnce(ctx context.Context, clientID int, kind, key, value, node string, client pb.KVClient) error {
	return r.execute(ctx, clientID, kind, key, value, []string{node}, []pb.KVClient{client}, time.Second*5, false)
}

func (r *Recorder) execute(ctx context.Context, clientID int, kind, key, value string, nodes []string, clients []pb.KVClient, attemptTimeout time.Duration, retry bool) error {
	if len(clients) == 0 || len(nodes) != len(clients) {
		return fmt.Errorf("invalid RPC targets")
	}
	operation := r.operations.Add(1)
	var lastErr error
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		target := (attempt - 1) % len(clients)
		op := Op{OperationID: operation, AttemptID: r.attempts.Add(1), Attempt: attempt,
			ClientID: clientID, Kind: kind, Key: key, Value: value, Node: nodes[target], CallNS: r.tick()}
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		switch kind {
		case "put":
			_, lastErr = clients[target].Put(attemptCtx, &pb.PutRequest{Key: []byte(key), Value: []byte(value)})
		case "delete":
			_, lastErr = clients[target].Delete(attemptCtx, &pb.DeleteRequest{Key: []byte(key)})
		case "get":
			var response *pb.GetResponse
			response, lastErr = clients[target].Get(attemptCtx, &pb.GetRequest{Key: []byte(key)})
			if lastErr == nil {
				op.Found, op.GotValue = response.Found, string(response.Value)
			}
		default:
			cancel()
			return fmt.Errorf("unknown operation %q", kind)
		}
		cancel()
		op.ReturnNS = r.tick()
		op.Outcome = attemptOutcome(kind, lastErr)
		op.Acknowledged = lastErr == nil
		if lastErr != nil {
			op.Err = lastErr.Error()
		}
		r.mu.Lock()
		r.history = append(r.history, op)
		r.mu.Unlock()
		if lastErr == nil || !retry {
			return lastErr
		}
		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func attemptOutcome(kind string, err error) string {
	if err == nil {
		return "acknowledged"
	}
	if kind == "get" {
		return "failed_read"
	}
	switch status.Code(err) {
	case codes.InvalidArgument, codes.FailedPrecondition:
		return "rejected"
	default:
		// Unavailable/Internal/DeadlineExceeded do not establish non-commitment.
		return "uncertain"
	}
}

func (r *Recorder) History() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := append([]Op(nil), r.history...)
	sort.Slice(result, func(i, j int) bool { return result[i].CallNS < result[j].CallNS })
	return result
}

func workloadOperation(client, index int, keys []string) (string, string) {
	// Each key gets Put/Get/Delete/Get cycles. Key choice is independent of
	// operation parity, including when the keyspace has an even size.
	kinds := [...]string{"put", "get", "delete", "get"}
	return kinds[index%len(kinds)], keys[(index/len(kinds)+client)%len(keys)]
}
