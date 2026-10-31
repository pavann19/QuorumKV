//go:build bench

package bench

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"github.com/pavann19/quorumkv/test/fault"
	"google.golang.org/grpc/status"
)

type writeSample struct {
	OperationID  int    `json:"operation_id"`
	ClientID     int    `json:"client_id"`
	Key          string `json:"key"`
	CallNS       int64  `json:"call_ns"`
	ReturnNS     int64  `json:"return_ns"`
	LatencyNS    int64  `json:"latency_ns"`
	Acknowledged bool   `json:"acknowledged"`
	ErrorCode    string `json:"error_code"`
}

type concurrentRun struct {
	Clients           int           `json:"clients"`
	Repetition        int           `json:"repetition"`
	Operations        int           `json:"operations"`
	PayloadBytes      int           `json:"payload_bytes"`
	WarmupOperations  int           `json:"warmup_operations"`
	DurationSeconds   float64       `json:"duration_seconds"`
	Throughput        float64       `json:"successful_ops_per_second"`
	P50MS             float64       `json:"p50_ms"`
	P95MS             float64       `json:"p95_ms"`
	P99MS             float64       `json:"p99_ms"`
	Errors            int           `json:"errors"`
	VerifiedKeys      int           `json:"verified_keys"`
	VerificationError string        `json:"verification_error,omitempty"`
	Samples           []writeSample `json:"samples"`
}

func benchSetting(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 {
		t.Fatalf("%s must be a positive integer", name)
	}
	return n
}

// Fixed operation and payload counts keep baseline and candidate workloads
// identical. Unique keys give every acknowledged write an exact readback oracle.
func TestMeasureConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("opt-in measurements")
	}
	operations := benchSetting(t, "QUORUMKV_BENCH_OPS", 256)
	repeats := benchSetting(t, "QUORUMKV_BENCH_REPEATS", 3)
	payload := benchSetting(t, "QUORUMKV_BENCH_VALUE_BYTES", 1024)
	label := os.Getenv("QUORUMKV_BENCH_LABEL")
	if label == "" {
		label = "candidate"
	}
	for _, ch := range label {
		if !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-", ch) {
			t.Fatal("invalid benchmark label")
		}
	}
	if os.Getenv("GOFLAGS") != "" || os.Getenv("GORACE") != "" {
		t.Fatal("run measurements without GOFLAGS or GORACE instrumentation")
	}
	revision := os.Getenv("QUORUMKV_SERVER_REVISION")
	if revision == "" {
		out, err := exec.Command("git", "rev-parse", "HEAD").Output()
		if err != nil {
			t.Fatal(err)
		}
		revision = strings.TrimSpace(string(out))
		if exec.Command("git", "diff", "--quiet").Run() != nil {
			revision += "+working-tree"
		}
	}
	result := struct {
		SchemaVersion  int             `json:"schema_version"`
		Timestamp      time.Time       `json:"timestamp"`
		ServerRevision string          `json:"server_revision"`
		GoVersion      string          `json:"go_version"`
		OS             string          `json:"os"`
		Architecture   string          `json:"architecture"`
		LogicalCPUs    int             `json:"logical_cpus"`
		Nodes          int             `json:"nodes"`
		Seed           int             `json:"seed"`
		Method         string          `json:"method"`
		Runs           []concurrentRun `json:"runs"`
	}{SchemaVersion: 1, Timestamp: time.Now().UTC(), ServerRevision: revision, GoVersion: runtime.Version(), OS: runtime.GOOS, Architecture: runtime.GOARCH, LogicalCPUs: runtime.NumCPU(), Nodes: 3, Seed: 1, Method: "fixed-count closed-loop unique-key Put; 32 warmup writes; no retries; all latency samples include failures; quantiles use floor(p*(n-1)); readback outside timed phase"}
	for _, clients := range []int{1, 4, 16} {
		for repeat := 0; repeat < repeats; repeat++ {
			t.Run(fmt.Sprintf("clients%d/repeat%d", clients, repeat), func(t *testing.T) {
				c := fault.Start(t, 3, 19601)
				leader := c.FindLeader(t, c.AllIDs(), 20*time.Second)
				client := c.Client(t, leader)
				value := strings.Repeat("x", payload)
				for i := 0; i < 32; i++ {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					_, err := client.Put(ctx, &pb.PutRequest{Key: []byte(fmt.Sprintf("warmup-%d", i)), Value: []byte(value)})
					cancel()
					if err != nil {
						t.Fatal(err)
					}
				}
				run := concurrentRun{Clients: clients, Repetition: repeat, Operations: operations, PayloadBytes: payload, WarmupOperations: 32, Samples: make([]writeSample, operations)}
				gate := make(chan struct{})
				var wg sync.WaitGroup
				var start time.Time
				for worker := 0; worker < clients; worker++ {
					wg.Add(1)
					go func(worker int) {
						defer wg.Done()
						<-gate
						for i := worker; i < operations; i += clients {
							key := fmt.Sprintf("concurrent-%06d", i)
							call := time.Since(start).Nanoseconds()
							ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
							_, err := client.Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte(value)})
							cancel()
							returned := time.Since(start).Nanoseconds()
							run.Samples[i] = writeSample{OperationID: i + 1, ClientID: worker, Key: key, CallNS: call, ReturnNS: returned, LatencyNS: returned - call, Acknowledged: err == nil, ErrorCode: status.Code(err).String()}
						}
					}(worker)
				}
				start = time.Now()
				close(gate)
				wg.Wait()
				run.DurationSeconds = time.Since(start).Seconds()
				latencies := make([]float64, 0, operations)
				for _, sample := range run.Samples {
					latencies = append(latencies, float64(sample.LatencyNS)/1e6)
					if !sample.Acknowledged {
						run.Errors++
					}
				}
				sort.Float64s(latencies)
				run.P50MS = percentile(latencies, .50)
				run.P95MS = percentile(latencies, .95)
				run.P99MS = percentile(latencies, .99)
				run.Throughput = float64(operations-run.Errors) / run.DurationSeconds
				for _, sample := range run.Samples {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					response, err := client.Get(ctx, &pb.GetRequest{Key: []byte(sample.Key)})
					cancel()
					if sample.Acknowledged && (err != nil || !response.GetFound() || string(response.GetValue()) != value) {
						run.VerificationError = fmt.Sprintf("acknowledged key %s failed readback: %v", sample.Key, err)
						break
					}
					if sample.Acknowledged {
						run.VerifiedKeys++
					}
				}
				result.Runs = append(result.Runs, run)
				if err := writeResult("concurrent_"+label+".json", result); err != nil {
					t.Fatal(err)
				}
				t.Logf("clients=%d repetition=%d successful_ops_s=%.2f p50_ms=%.3f p95_ms=%.3f p99_ms=%.3f errors=%d verified=%d", clients, repeat, run.Throughput, run.P50MS, run.P95MS, run.P99MS, run.Errors, run.VerifiedKeys)
				if run.Errors > 0 || run.VerificationError != "" {
					t.Fatalf("measurement failed: errors=%d verification=%s", run.Errors, run.VerificationError)
				}
			})
		}
	}
}
