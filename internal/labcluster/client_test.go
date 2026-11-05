package labcluster

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	qclient "github.com/pavann19/quorumkv/client"
	"github.com/pavann19/quorumkv/internal/identity"
	"github.com/pavann19/quorumkv/internal/raftnode"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func clusterClientConfig(t *testing.T, c *Cluster, first int) qclient.Config {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(c.Root, "client-identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	var id identity.Config
	if err = json.Unmarshal(raw, &id); err != nil {
		t.Fatal(err)
	}
	cfg := qclient.Config{IdentityURI: id.Self, CertificateFile: id.Certificate, KeyFile: id.Key, CAFile: id.CA, MaxAttempts: 8, AttemptTimeout: 500 * time.Millisecond, Timeout: 15 * time.Second}
	for n := range c.Peers {
		p := c.Peers[(first+n)%len(c.Peers)]
		cfg.Endpoints = append(cfg.Endpoints, qclient.Endpoint{Address: p.Addr, Identity: id.Peers[p.Addr]})
	}
	return cfg
}

func TestFirstPartyClientMTLSRedirectLeaderKillAndLostReply(t *testing.T) {
	var drop atomic.Bool
	var applied atomic.Int64
	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		response, err := next(ctx, req)
		if put, ok := req.(*pb.PutRequest); ok && string(put.Key) == "lost-reply" && err == nil {
			applied.Add(1)
			if drop.CompareAndSwap(true, false) {
				return nil, status.Error(codes.Unavailable, "synthetic lost reply after commit")
			}
		}
		return response, err
	}
	c, err := newModeRaftProfile(t.TempDir(), true, 0, "quorum", raftnode.DefaultRaftSettings(), grpc.ChainUnaryInterceptor(interceptor))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader, err := c.Leader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Begin at a follower with current leader knowledge, exercising authenticated
	// ErrorInfo/request-ID correlation and a redirect to the static allowlist.
	follower := (leader + 1) % 3
	for deadline := time.Now().Add(3 * time.Second); c.Nodes[follower].Raft.Leader() == ""; {
		if time.Now().After(deadline) {
			t.Fatal("follower did not learn leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
	client, err := qclient.New(clusterClientConfig(t, c, follower))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r, err := client.Put(ctx, []byte("stable"), []byte("acknowledged"))
	if err != nil || r.Outcome != "acknowledged" || len(r.Attempts) < 2 || r.Attempts[0].Outcome != "not_submitted" {
		t.Fatalf("follower redirect %+v %v", r, err)
	}
	drop.Store(true)
	r, err = client.Put(ctx, []byte("lost-reply"), []byte("committed-once"))
	if err == nil || r.Outcome != "uncertain" || len(r.Attempts) != 1 || applied.Load() != 1 {
		t.Fatalf("lost reply replayed or hidden %+v %v applied=%d", r, err, applied.Load())
	}
	value, readReceipt, err := client.Get(ctx, []byte("lost-reply"))
	if err != nil || !value.Found || string(value.Value) != "committed-once" {
		t.Fatalf("lost reply not actually committed %+v %v", readReceipt, err)
	}
	// Readback proves current state only. It cannot prove arbitrary operation
	// identity in the presence of concurrent writers; the mutation stays uncertain.
	if r.Outcome != "uncertain" {
		t.Fatal("readback rewrote mutation history")
	}
	if err = c.Stop(leader); err != nil {
		t.Fatal(err)
	}
	value, readReceipt, err = client.Get(ctx, []byte("stable"))
	if err != nil || !value.Found || string(value.Value) != "acknowledged" || len(readReceipt.Attempts) < 2 {
		t.Fatalf("leader-kill read recovery %+v %v", readReceipt, err)
	}
	r, err = client.Delete(ctx, []byte("stable"))
	if err != nil || r.Outcome != "acknowledged" {
		t.Fatalf("new leader write %+v %v", r, err)
	}
	value, _, err = client.Get(ctx, []byte("stable"))
	if err != nil || value.Found {
		t.Fatal("new leader deletion missing", err)
	}
	t.Logf("mtls=true redirect_confirmed=true lost_reply_attempts=1 lost_reply_actual_applications=%d leader_kill_read_attempts=%d mutation_uncertainty_preserved=true", applied.Load(), len(readReceipt.Attempts))
}

func TestFirstPartyClientRejectsWrongPeerBinding(t *testing.T) {
	c, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	leader, err := c.Leader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cfg := clusterClientConfig(t, c, leader)
	cfg.Endpoints = cfg.Endpoints[:1]
	cfg.Endpoints[0].Identity = "spiffe://quorumkv/peer/impostor"
	cfg.MaxAttempts = 1
	client, err := qclient.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r, err := client.Put(ctx, []byte("must-not-apply"), []byte("secret"))
	if err == nil || r.Outcome != "uncertain" || len(r.Attempts) != 1 {
		t.Fatalf("wrong peer allowed %+v %v", r, err)
	}
	if _, found := c.Nodes[leader].Get([]byte("must-not-apply")); found {
		t.Fatal("wrong-peer request reached FSM")
	}
}
