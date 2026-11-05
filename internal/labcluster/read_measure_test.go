//go:build measure

package labcluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

type readMeasurement struct {
	Transport    string   `json:"transport"`
	Mode         string   `json:"read_mode"`
	Clients      int      `json:"clients"`
	Repetition   int      `json:"repetition"`
	SamplesNS    []int64  `json:"warm_samples_ns"`
	ColdNS       []int64  `json:"cold_connection_samples_ns"`
	ElapsedNS    int64    `json:"warm_elapsed_ns"`
	OpsPerSecond float64  `json:"warm_ops_per_second"`
	P50NS        int64    `json:"warm_p50_ns"`
	P95NS        int64    `json:"warm_p95_ns"`
	P99NS        int64    `json:"warm_p99_ns"`
	LogGrowth    []uint64 `json:"replica_log_growth"`
	Error        string   `json:"error,omitempty"`
}

func readPercentile(values []int64, fraction float64) int64 {
	copied := append([]int64(nil), values...)
	sort.Slice(copied, func(i, j int) bool { return copied[i] < copied[j] })
	return copied[int(math.Ceil(float64(len(copied))*fraction))-1]
}
func measureReadProfile(t *testing.T, mtls bool, mode string, clients, rep int) (result readMeasurement) {
	t.Helper()
	result = readMeasurement{Transport: "plaintext", Mode: mode, Clients: clients, Repetition: rep}
	if mtls {
		result.Transport = "mtls"
	}
	c, err := newModeReadProfile(t.TempDir(), mtls, 0, mode)
	if err != nil {
		result.Error = err.Error()
		return
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	leader, err := c.Leader(ctx)
	if err != nil {
		result.Error = err.Error()
		return
	}
	value := bytes.Repeat([]byte("v"), 1024)
	key := []byte("read-measure")
	if _, err = c.Clients[leader].Put(ctx, &pb.PutRequest{Key: key, Value: value}); err != nil {
		result.Error = err.Error()
		return
	}
	read := func(client pb.KVClient) error {
		response, err := client.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			return err
		}
		if !response.GetFound() || !bytes.Equal(response.GetValue(), value) {
			return fmt.Errorf("incorrect acknowledged readback")
		}
		return nil
	}
	for i := 0; i < 8; i++ {
		if err := read(c.Clients[leader]); err != nil {
			result.Error = err.Error()
			return
		}
	}
	settled := func() bool {
		last := c.Nodes[leader].Raft.LastIndex()
		for _, node := range c.Nodes {
			observed, found := node.Get(key)
			if !found || !bytes.Equal(observed, value) || node.Raft.LastIndex() != last {
				return false
			}
		}
		return true
	}
	for !settled() && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	if ctx.Err() != nil {
		result.Error = ctx.Err().Error()
		return
	}
	before := make([]uint64, len(c.Nodes))
	walBefore := make([]uint64, len(c.Nodes))
	for i, node := range c.Nodes {
		before[i] = node.Raft.LastIndex()
		walBefore[i] = node.Store.WALStats().Syncs
	}
	term := c.Nodes[leader].Raft.Stats()["term"]
	for i := 0; i < 8; i++ {
		client, closeClient, err := c.ColdClient(leader)
		if err != nil {
			result.Error = err.Error()
			return
		}
		start := time.Now()
		err = read(client)
		result.ColdNS = append(result.ColdNS, time.Since(start).Nanoseconds())
		closeClient()
		if err != nil {
			result.Error = err.Error()
			return
		}
	}
	const samples = 128
	result.SamplesNS = make([]int64, samples)
	errs := make(chan error, clients)
	var wg sync.WaitGroup
	start := time.Now()
	for worker := 0; worker < clients; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < samples; i += clients {
				started := time.Now()
				err := read(c.Clients[leader])
				result.SamplesNS[i] = time.Since(started).Nanoseconds()
				if err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	result.ElapsedNS = time.Since(start).Nanoseconds()
	close(errs)
	for err := range errs {
		result.Error = err.Error()
	}
	if result.Error != "" {
		return
	}
	for !settled() && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	if ctx.Err() != nil {
		result.Error = ctx.Err().Error()
		return
	}
	if !c.Nodes[leader].IsLeader() || c.Nodes[leader].Raft.Stats()["term"] != term {
		result.Error = "leadership changed during matched healthy measurement"
		return
	}
	for i, node := range c.Nodes {
		growth := node.Raft.LastIndex() - before[i]
		result.LogGrowth = append(result.LogGrowth, growth)
		expected := uint64(0)
		if mode == "barrier" {
			expected = samples + 8
		}
		if growth != expected {
			result.Error = fmt.Sprintf("replica %d log growth %d expected %d", i, growth, expected)
			return
		}
		if node.Store.WALStats().Syncs != walBefore[i] {
			result.Error = "read-only run changed store WAL sync count"
			return
		}
	}
	result.OpsPerSecond = float64(samples) / (float64(result.ElapsedNS) / 1e9)
	result.P50NS = readPercentile(result.SamplesNS, .5)
	result.P95NS = readPercentile(result.SamplesNS, .95)
	result.P99NS = readPercentile(result.SamplesNS, .99)
	return
}

func TestReadFencePairedMeasurement(t *testing.T) {
	report := map[string]any{
		"status": "FAIL_OR_INCOMPLETE", "comparison": "same-candidate barrier control versus quorum read path; not historical executables",
		"profile":           map[string]any{"nodes": 3, "value_bytes": 1024, "warm_samples_per_run": 128, "cold_samples_per_run": 8, "warmup": 8, "paired_repetitions": 3, "clients": []int{1, 4}, "gomaxprocs": runtime.GOMAXPROCS(0), "go_version": runtime.Version(), "durability": "unchanged Raft Bolt and store WAL fsync; acknowledgements after apply"},
		"predeclared_gates": "exact acknowledged readback, no errors/elections, quorum zero log growth, barrier one entry/read on every replica; median warm p99 <= 1.20 times control and median warm throughput >= 0.80 times control per transport/concurrency profile",
		"limitations":       []string{"local Docker/WSL2 resource-limited fixtures only", "cold samples retained as observations with no statistical latency gate", "not production capacity or multi-host latency"},
	}
	runs := []readMeasurement{}
	defer func() {
		report["runs"] = runs
		path := os.Getenv("QUORUMKV_RESULTS_DIR")
		if path == "" {
			path = t.TempDir()
		}
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			t.Error(err)
			return
		}
		if err = os.MkdirAll(path, 0700); err == nil {
			err = os.WriteFile(filepath.Join(path, "read-fence-measurement.json"), append(data, '\n'), 0600)
		}
		if err != nil {
			t.Error(err)
		}
	}()
	comparisons := []map[string]any{}
	failures := false
	for _, mtls := range []bool{false, true} {
		for _, clients := range []int{1, 4} {
			groups := map[string][]readMeasurement{"barrier": {}, "quorum": {}}
			for rep := 0; rep < 3; rep++ {
				order := []string{"barrier", "quorum"}
				if rep%2 == 1 {
					order = []string{"quorum", "barrier"}
				}
				for _, mode := range order {
					result := measureReadProfile(t, mtls, mode, clients, rep)
					runs = append(runs, result)
					if result.Error != "" {
						t.Fatalf("%s/%s/%d/%d: %s", result.Transport, mode, clients, rep, result.Error)
					}
					groups[mode] = append(groups[mode], result)
					t.Logf("%s %s clients=%d rep=%d p99=%dns ops/s=%.2f", result.Transport, mode, clients, rep, result.P99NS, result.OpsPerSecond)
				}
			}
			median := func(values []float64) float64 { sort.Float64s(values); return values[len(values)/2] }
			p99 := func(mode string) float64 {
				v := []float64{}
				for _, r := range groups[mode] {
					v = append(v, float64(r.P99NS))
				}
				return median(v)
			}
			throughput := func(mode string) float64 {
				v := []float64{}
				for _, r := range groups[mode] {
					v = append(v, r.OpsPerSecond)
				}
				return median(v)
			}
			p99Ratio := p99("quorum") / p99("barrier")
			throughputRatio := throughput("quorum") / throughput("barrier")
			pass := p99Ratio <= 1.2 && throughputRatio >= .8
			comparisons = append(comparisons, map[string]any{"mtls": mtls, "clients": clients, "median_p99_ratio": p99Ratio, "median_throughput_ratio": throughputRatio, "pass": pass})
			if !pass {
				failures = true
				t.Errorf("performance gate mtls=%v clients=%d p99 ratio %.3f throughput ratio %.3f", mtls, clients, p99Ratio, throughputRatio)
			}
		}
	}
	report["comparisons"] = comparisons
	if !failures {
		report["status"] = "PASS_LOCAL_PAIRED_MEASUREMENT"
	}
}
