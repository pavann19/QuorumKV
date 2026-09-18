// M3's failover-time measurement: "time from leader kill to a new leader
// accepting writes again," run multiple times and reported as a
// distribution, not a single cherry-picked number -- the same discipline
// used for ModelGate's admission-latency benchmark and LedgerLine's
// bench/results/ pattern.
package bench

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"github.com/pavann19/quorumkv/test/fault"
)

const failoverTrials = 10

type failoverResult struct {
	Timestamp   time.Time `json:"timestamp"`
	SampleSize  int       `json:"sample_size"`
	MinMS       float64   `json:"min_ms"`
	P50MS       float64   `json:"p50_ms"`
	P90MS       float64   `json:"p90_ms"`
	MaxMS       float64   `json:"max_ms"`
	MeanMS      float64   `json:"mean_ms"`
	SamplesMS   []float64 `json:"samples_ms"`
	Environment string    `json:"environment"`
}

func TestMeasureFailoverTime(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping failover benchmark in -short mode")
	}

	samples := make([]float64, 0, failoverTrials)
	basePort := 19001

	for trial := 0; trial < failoverTrials; trial++ {
		t.Run("", func(t *testing.T) {
			c := fault.Start(t, 3, basePort+trial*10)
			leader := c.FindLeader(t, c.AllIDs(), 20*time.Second)

			majority := make([]string, 0, 2)
			for _, id := range c.AllIDs() {
				if id != leader {
					majority = append(majority, id)
				}
			}
			clients := make([]pb.KVClient, len(majority))
			for i, id := range majority {
				clients[i] = c.Client(t, id)
			}

			killTime := time.Now()
			c.Kill(leader)

			deadline := time.Now().Add(20 * time.Second)
			var acceptedAt time.Time
			i := 0
			for time.Now().Before(deadline) {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				_, err := clients[i%len(clients)].Put(ctx, &pb.PutRequest{
					Key: []byte("failover-probe"), Value: []byte("x"),
				})
				cancel()
				if err == nil {
					acceptedAt = time.Now()
					break
				}
				i++
				time.Sleep(50 * time.Millisecond)
			}
			if acceptedAt.IsZero() {
				t.Fatalf("trial %d: cluster never accepted a write again within timeout", trial)
			}

			failoverMS := float64(acceptedAt.Sub(killTime).Microseconds()) / 1000.0
			t.Logf("trial %d: failover time = %.2fms", trial, failoverMS)
			samples = append(samples, failoverMS)
		})
	}

	sort.Float64s(samples)
	sum := 0.0
	for _, v := range samples {
		sum += v
	}
	n := len(samples)
	result := failoverResult{
		Timestamp:   time.Now().UTC(),
		SampleSize:  n,
		MinMS:       samples[0],
		P50MS:       percentile(samples, 0.50),
		P90MS:       percentile(samples, 0.90),
		MaxMS:       samples[n-1],
		MeanMS:      sum / float64(n),
		SamplesMS:   samples,
		Environment: "3-process cluster on localhost, no kind/kubelet/container overhead",
	}
	t.Logf("failover time over %d trials: min=%.1fms p50=%.1fms p90=%.1fms max=%.1fms mean=%.1fms",
		n, result.MinMS, result.P50MS, result.P90MS, result.MaxMS, result.MeanMS)

	if err := writeResult("failover_time.json", result); err != nil {
		t.Fatalf("writing failover_time.json: %v", err)
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 1 {
		return sorted[0]
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func writeResult(name string, v any) error {
	if err := os.MkdirAll("results", 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join("results", name), data, 0o644)
}
