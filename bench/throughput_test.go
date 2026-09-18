// M3's throughput/latency measurement: "throughput/latency at cluster
// sizes 3 and 5."
package bench

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"github.com/pavann19/quorumkv/test/fault"
)

const opsPerThroughputRun = 100

type throughputResult struct {
	Timestamp        time.Time `json:"timestamp"`
	ClusterSize      int       `json:"cluster_size"`
	SampleSize       int       `json:"sample_size"`
	MinMS            float64   `json:"min_ms"`
	P50MS            float64   `json:"p50_ms"`
	P90MS            float64   `json:"p90_ms"`
	P99MS            float64   `json:"p99_ms"`
	MaxMS            float64   `json:"max_ms"`
	MeanMS           float64   `json:"mean_ms"`
	TotalDurationMS  float64   `json:"total_duration_ms"`
	ThroughputPerSec float64   `json:"throughput_per_sec"`
	Environment      string    `json:"environment"`
}

func measureThroughput(t *testing.T, clusterSize, basePort int) throughputResult {
	t.Helper()
	c := fault.Start(t, clusterSize, basePort)
	leader := c.FindLeader(t, c.AllIDs(), 20*time.Second)
	client := c.Client(t, leader)

	samples := make([]float64, 0, opsPerThroughputRun)
	start := time.Now()
	for i := 0; i < opsPerThroughputRun; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		value := fmt.Sprintf("bench-value-%d", i)
		t0 := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte(value)})
		cancel()
		if err != nil {
			t.Fatalf("op %d: unexpected error on a healthy %d-node cluster: %v", i, clusterSize, err)
		}
		samples = append(samples, float64(time.Since(t0).Microseconds())/1000.0)
	}
	total := time.Since(start)

	sort.Float64s(samples)
	sum := 0.0
	for _, v := range samples {
		sum += v
	}
	n := len(samples)

	return throughputResult{
		Timestamp:        time.Now().UTC(),
		ClusterSize:      clusterSize,
		SampleSize:       n,
		MinMS:            samples[0],
		P50MS:            percentile(samples, 0.50),
		P90MS:            percentile(samples, 0.90),
		P99MS:            percentile(samples, 0.99),
		MaxMS:            samples[n-1],
		MeanMS:           sum / float64(n),
		TotalDurationMS:  float64(total.Microseconds()) / 1000.0,
		ThroughputPerSec: float64(n) / total.Seconds(),
		Environment:      "multi-process cluster on localhost (real Raft consensus, real gRPC, no container/kubelet overhead)",
	}
}

func TestMeasureThroughputAndLatency_3Nodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput benchmark in -short mode")
	}
	result := measureThroughput(t, 3, 19301)
	t.Logf("3-node cluster: p50=%.2fms p90=%.2fms p99=%.2fms throughput=%.1f ops/sec",
		result.P50MS, result.P90MS, result.P99MS, result.ThroughputPerSec)
	if err := writeResult("throughput_3nodes.json", result); err != nil {
		t.Fatalf("writing throughput_3nodes.json: %v", err)
	}
}

func TestMeasureThroughputAndLatency_5Nodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput benchmark in -short mode")
	}
	result := measureThroughput(t, 5, 19401)
	t.Logf("5-node cluster: p50=%.2fms p90=%.2fms p99=%.2fms throughput=%.1f ops/sec",
		result.P50MS, result.P90MS, result.P99MS, result.ThroughputPerSec)
	if err := writeResult("throughput_5nodes.json", result); err != nil {
		t.Fatalf("writing throughput_5nodes.json: %v", err)
	}
}
