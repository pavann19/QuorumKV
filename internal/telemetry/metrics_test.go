package telemetry_test

import (
	"context"
	"github.com/pavann19/quorumkv/internal/labcluster"
	"github.com/pavann19/quorumkv/internal/telemetry"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/grpc"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBoundedMetricsAndHealth(t *testing.T) {
	c, err := labcluster.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	leader, err := c.Leader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	m := &telemetry.Metrics{}
	_, err = m.Unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/quorumkv.v1.KV/Put"}, func(ctx context.Context, _ any) (any, error) {
		return c.Clients[leader].Put(ctx, &pb.PutRequest{Key: []byte("PRIVATE_KEY_VALUE"), Value: []byte("FAKE_SECRET")})
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(m.Handler(c.Nodes[leader], "source-hash"))
	defer server.Close()
	for _, path := range []string{"/metrics", "/ready", "/live", "/startup", "/effective-config"} {
		resp, e := server.Client().Get(server.URL + path)
		if e != nil {
			t.Fatal(e)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %s", path, raw)
		}
		if strings.Contains(string(raw), "FAKE_SECRET") || strings.Contains(string(raw), "PRIVATE_KEY_VALUE") {
			t.Fatal("secret telemetry")
		}
		if path == "/metrics" && (!strings.Contains(string(raw), "quorumkv_wal_syncs_total 1") || !strings.Contains(string(raw), `quorumkv_rpc_total{method="put",code="0"} 1`)) {
			t.Fatalf("metrics missing actual effects: %s", raw)
		}
	}
}
