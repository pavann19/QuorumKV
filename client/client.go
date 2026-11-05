// Package client provides bounded, authenticated access to a fixed QuorumKV cluster.
// It never retries a mutation with an uncertain outcome. Request IDs correlate
// attempts; they are not durable idempotency keys.
package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pavann19/quorumkv/internal/identity"
	"github.com/pavann19/quorumkv/internal/requestlimits"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const requestIDHeader = "quorumkv-request-id"

type Endpoint struct {
	Address string
	// Identity is the exact spiffe://quorumkv/peer/<member> certificate URI.
	Identity string
}

type Config struct {
	Endpoints                                     []Endpoint
	IdentityURI, CertificateFile, KeyFile, CAFile string
	// Zero values select 3 attempts, 1 second per attempt, and 5 seconds total.
	MaxAttempts             int
	AttemptTimeout, Timeout time.Duration
}

type Attempt struct {
	ID, Endpoint      string
	Started, Finished time.Time
	Code              codes.Code
	Reason, Outcome   string
}

// Receipt contains no keys or values. Persist it in the application's operation
// journal when an uncertain result needs human or application reconciliation.
type Receipt struct {
	OperationID string
	Outcome     string
	Attempts    []Attempt
}

type Error struct {
	Receipt Receipt
	Cause   error
}

func (e *Error) Error() string              { return "quorumkv client: " + e.Receipt.Outcome }
func (e *Error) Unwrap() error              { return e.Cause }
func (e *Error) GRPCStatus() *status.Status { return status.Convert(e.Cause) }

type endpoint struct {
	address string
	api     pb.KVClient
	conn    *grpc.ClientConn
}

// Client is safe for concurrent calls. Close terminates its owned connections.
// It does not cancel already submitted server-side mutations.
type Client struct {
	endpoints []endpoint
	config    Config
	mu        sync.Mutex
	preferred int
	closed    bool
}

func normalized(cfg Config) (Config, error) {
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.AttemptTimeout == 0 {
		cfg.AttemptTimeout = time.Second
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.MaxAttempts < 1 || cfg.MaxAttempts > 8 || cfg.AttemptTimeout < time.Millisecond || cfg.AttemptTimeout > 5*time.Second || cfg.Timeout < time.Millisecond || cfg.Timeout > 30*time.Second {
		return cfg, errors.New("invalid client attempt or deadline bounds")
	}
	if len(cfg.Endpoints) < 1 || len(cfg.Endpoints) > 16 {
		return cfg, errors.New("configure 1..16 fixed endpoints")
	}
	seen := map[string]bool{}
	for _, ep := range cfg.Endpoints {
		host, port, err := net.SplitHostPort(ep.Address)
		n, portErr := strconv.Atoi(port)
		if err != nil || host == "" || portErr != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port || seen[ep.Address] || len(ep.Address) > 320 {
			return cfg, errors.New("invalid or duplicate endpoint")
		}
		// Use a literal passthrough target; resolver syntax, paths and credentials
		// are never accepted through an address or a server redirect.
		if net.ParseIP(host) == nil {
			if len(host) > 253 {
				return cfg, errors.New("invalid endpoint host")
			}
			for _, label := range strings.Split(host, ".") {
				if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
					return cfg, errors.New("invalid endpoint host")
				}
				for _, c := range label {
					if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
						return cfg, errors.New("invalid endpoint host")
					}
				}
			}
		}
		seen[ep.Address] = true
	}
	cfg.Endpoints = append([]Endpoint(nil), cfg.Endpoints...)
	return cfg, nil
}

// New creates lazy connections with mTLS and exact endpoint/peer-URI bindings.
// There is no plaintext fallback, arbitrary dial option or dynamic discovery.
func New(cfg Config) (*Client, error) {
	cfg, err := normalized(cfg)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(cfg.IdentityURI, "spiffe://quorumkv/client/") {
		return nil, errors.New("client identity required")
	}
	bindings := map[string]string{}
	for _, ep := range cfg.Endpoints {
		bindings[ep.Address] = ep.Identity
	}
	id, err := identity.New(identity.Config{Self: cfg.IdentityURI, Certificate: cfg.CertificateFile, Key: cfg.KeyFile, CA: cfg.CAFile, Peers: bindings})
	if err != nil {
		return nil, errors.New("invalid client TLS identity or trust configuration")
	}
	c := &Client{config: cfg}
	for _, ep := range cfg.Endpoints {
		conn, err := grpc.NewClient("passthrough:///"+ep.Address, grpc.WithAuthority(ep.Address), grpc.WithTransportCredentials(id.ClientCredentials()), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(requestlimits.MaxMessageBytes), grpc.MaxCallSendMsgSize(requestlimits.MaxMessageBytes)))
		if err != nil {
			c.Close()
			return nil, err
		}
		c.endpoints = append(c.endpoints, endpoint{ep.Address, pb.NewKVClient(conn), conn})
	}
	return c, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	var errs []error
	for _, ep := range c.endpoints {
		if ep.conn != nil {
			errs = append(errs, ep.conn.Close())
		}
	}
	return errors.Join(errs...)
}

func (c *Client) Get(ctx context.Context, key []byte) (*pb.GetResponse, Receipt, error) {
	if err := validate(key, nil); err != nil {
		r, e := rejected(err, false)
		return nil, r, e
	}
	key = bytes.Clone(key)
	var response *pb.GetResponse
	r, err := c.call(ctx, false, func(ctx context.Context, api pb.KVClient) error {
		var err error
		response, err = api.Get(ctx, &pb.GetRequest{Key: key})
		return err
	})
	return response, r, err
}
func (c *Client) Put(ctx context.Context, key, value []byte) (Receipt, error) {
	if err := validate(key, value); err != nil {
		return rejected(err, true)
	}
	key, value = bytes.Clone(key), bytes.Clone(value)
	return c.call(ctx, true, func(ctx context.Context, api pb.KVClient) error {
		_, err := api.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		return err
	})
}
func (c *Client) Delete(ctx context.Context, key []byte) (Receipt, error) {
	if err := validate(key, nil); err != nil {
		return rejected(err, true)
	}
	key = bytes.Clone(key)
	return c.call(ctx, true, func(ctx context.Context, api pb.KVClient) error {
		_, err := api.Delete(ctx, &pb.DeleteRequest{Key: key})
		return err
	})
}

func validate(key, value []byte) error {
	if len(key) < 1 || len(key) > requestlimits.MaxKeyBytes || len(value) > requestlimits.MaxValueBytes {
		return status.Error(codes.InvalidArgument, "request exceeds client size limits")
	}
	return nil
}
func rejected(err error, mutation bool) (Receipt, error) {
	r := Receipt{Outcome: "no_mutation"}
	if mutation {
		r.Outcome = "not_submitted"
	}
	var idErr error
	r.OperationID, idErr = uuid7()
	if idErr != nil {
		err = idErr
	}
	return r, &Error{Receipt: r, Cause: err}
}

func uuid7() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	n := uint64(time.Now().UnixMilli())
	for i := 5; i >= 0; i-- {
		b[i] = byte(n)
		n >>= 8
	}
	b[6] = 0x70 | b[6]&15
	b[8] = 0x80 | b[8]&63
	h := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[:8], h[8:12], h[12:16], h[16:20], h[20:]), nil
}

type decision struct {
	reason, outcome, leader string
	retry                   bool
	delay                   time.Duration
}

// Only the exact structured status for this attempt can prove a rejection.
// Multiple ErrorInfo records, unrecognized pairs or missing identity fail closed.
func classify(err error, id string, mutation bool) decision {
	d := decision{outcome: "no_mutation"}
	if mutation {
		d.outcome = "uncertain"
	}
	if err == nil {
		d.outcome = "acknowledged"
		return d
	}
	var info *errdetails.ErrorInfo
	for _, v := range status.Convert(err).Details() {
		if x, ok := v.(*errdetails.ErrorInfo); ok && x.Domain == "quorumkv" {
			if info != nil {
				return d
			}
			info = x
		}
		if x, ok := v.(*errdetails.RetryInfo); ok {
			if x.RetryDelay == nil || x.RetryDelay.CheckValid() != nil {
				return d
			}
			d.delay = max(d.delay, x.RetryDelay.AsDuration())
		}
	}
	code := status.Code(err)
	if info != nil && info.Metadata["request_id"] == id {
		expected := map[string]codes.Code{"NOT_LEADER": codes.FailedPrecondition, "NO_LEADER": codes.Unavailable, "LEADERSHIP_TRANSFER": codes.Unavailable, "ENQUEUE_TIMEOUT": codes.Unavailable, "ADMISSION_FULL": codes.ResourceExhausted, "DISK_PRESSURE": codes.ResourceExhausted, "DISK_STATUS_UNKNOWN": codes.Unavailable}
		if want, ok := expected[info.Reason]; ok && want == code && info.Metadata["outcome"] == "not_submitted" {
			d.reason, d.leader, d.retry = info.Reason, info.Metadata["leader"], true
			if mutation {
				d.outcome = "not_submitted"
			}
			return d
		}
		// A capacity rejection is a committed no-op or rejected reservation;
		// neither permits an automatic retry loop.
		if mutation && info.Reason == "KEYSPACE_FULL" && code == codes.ResourceExhausted && info.Metadata["outcome"] == "not_applied" {
			d.reason, d.outcome = info.Reason, "not_applied"
		}
		if mutation && info.Reason == "REQUEST_SIZE_INVALID" && code == codes.InvalidArgument && info.Metadata["outcome"] == "not_submitted" {
			d.reason, d.outcome = info.Reason, "not_submitted"
		}
	}
	if !mutation && (code == codes.Unavailable || code == codes.DeadlineExceeded) {
		d.retry = true
	}
	return d
}

func (c *Client) call(parent context.Context, mutation bool, invoke func(context.Context, pb.KVClient) error) (r Receipt, resultErr error) {
	r.Outcome = "not_submitted"
	if !mutation {
		r.Outcome = "no_mutation"
	}
	fail := func(err error) (Receipt, error) { return r, &Error{Receipt: r, Cause: err} }
	var err error
	r.OperationID, err = uuid7()
	if err != nil {
		return fail(err)
	}
	c.mu.Lock()
	index, closed := c.preferred, c.closed
	c.mu.Unlock()
	if closed {
		return fail(errors.New("client closed"))
	}
	ctx, cancel := context.WithTimeout(parent, c.config.Timeout)
	defer cancel()
	failed := make([]bool, len(c.endpoints))
	visits := make([]int, len(c.endpoints))
	for n := 0; n < c.config.MaxAttempts; n++ {
		if err = ctx.Err(); err != nil {
			return fail(err)
		}
		id, e := uuid7()
		if e != nil {
			return fail(e)
		}
		a := Attempt{ID: id, Endpoint: c.endpoints[index].address, Started: time.Now()}
		attemptCtx, stop := context.WithTimeout(ctx, c.config.AttemptTimeout)
		md, _ := metadata.FromOutgoingContext(attemptCtx)
		md = md.Copy()
		md.Set(requestIDHeader, id)
		err = invoke(metadata.NewOutgoingContext(attemptCtx, md), c.endpoints[index].api)
		stop()
		d := classify(err, id, mutation)
		visits[index]++
		a.Finished, a.Code, a.Reason, a.Outcome = time.Now(), status.Code(err), d.reason, d.outcome
		r.Attempts = append(r.Attempts, a)
		r.Outcome = d.outcome
		if err == nil {
			c.mu.Lock()
			c.preferred = index
			c.mu.Unlock()
			return r, nil
		}
		if !d.retry || n+1 == c.config.MaxAttempts {
			return fail(err)
		}
		// A stale hint must not trap discovery at a failed address or starve
		// other configured members. Failure memory is local to this operation.
		if status.Code(err) == codes.Unavailable && d.reason == "" {
			failed[index] = true
		}
		index = (index + 1) % len(c.endpoints)
		for offset := 0; offset < len(c.endpoints); offset++ {
			candidate := (index + offset) % len(c.endpoints)
			if !failed[candidate] {
				index = candidate
				break
			}
		}
		for i, ep := range c.endpoints {
			if ep.address == d.leader && !failed[i] && visits[i] <= visits[index] {
				index = i
				break
			}
		}
		// An exponential minimum avoids burning all attempts before an election;
		// positive jitter spreads retries and server RetryInfo remains a floor.
		// Stop if the hint exceeds this bounded policy instead of retrying early.
		window := min(time.Second, 100*time.Millisecond*time.Duration(1<<n))
		if d.delay < 0 || d.delay > time.Second {
			return fail(err)
		}
		var jitter [2]byte
		if _, e = rand.Read(jitter[:]); e != nil {
			return fail(e)
		}
		delay := max(d.delay, window) + time.Duration(uint16(jitter[0])<<8|uint16(jitter[1]))*window/65536
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fail(ctx.Err())
		case <-timer.C:
		}
	}
	return fail(err)
}
