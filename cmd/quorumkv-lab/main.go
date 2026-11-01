// quorumkv-lab runs a bounded synthetic mTLS lab. It never accepts existing data
// or target addresses. A soak is 24 real hours; a smoke cannot be labelled soak.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/pavann19/quorumkv/internal/labcluster"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"github.com/pavann19/quorumkv/test/fault"
)

func writeJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err = os.WriteFile(tmp, raw, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	return values[int(p*float64(len(values)-1))]
}

func observedMetrics(c *labcluster.Cluster) map[string]uint64 {
	result := map[string]uint64{"leader": 0}
	for _, n := range c.Nodes {
		if n == nil || !n.IsLeader() {
			continue
		}
		result["leader"] = 1
		stats := n.Raft.Stats()
		for _, key := range []string{"term", "commit_index", "applied_index", "last_log_index", "last_snapshot_index", "num_peers"} {
			value, _ := strconv.ParseUint(stats[key], 10, 64)
			result[key] = value
		}
		wal := n.Store.WALStats()
		result["wal_appends_total"], result["wal_syncs_total"], result["wal_failures_total"], result["wal_bytes_total"] = wal.Appends, wal.Syncs, wal.Failures, wal.Bytes
		break
	}
	return result
}
func execute(ctx context.Context, c *labcluster.Cluster, r *fault.Recorder, client int, kind, key, value string) error {
	if kind != "put" {
		value = ""
	}
	ids := []string{}
	for _, p := range c.Peers {
		ids = append(ids, p.ID)
	}
	return r.Execute(ctx, client, kind, key, value, ids, c.Clients, 2*time.Second)
}
func maintenance(ctx context.Context, c *labcluster.Cluster, out string, index int) (float64, error) {
	r := fault.NewRecorder()
	leader, err := c.Leader(ctx)
	if err != nil {
		return 0, err
	}
	if err = execute(ctx, c, r, 99, "put", "restore-canary", "acknowledged"); err != nil {
		return 0, err
	}
	if err = c.Nodes[leader].Raft.Snapshot().Error(); err != nil {
		return 0, err
	}
	if err = c.Rotate(leader); err != nil {
		return 0, err
	}
	start := time.Now()
	if err = c.Stop(leader); err != nil {
		return 0, err
	}
	if err = c.Restart(leader); err != nil {
		return 0, err
	}
	if err = execute(ctx, c, r, 99, "put", "post-failover", "probe"); err != nil {
		return 0, err
	}
	elapsed := time.Since(start).Seconds()
	if elapsed > 15 {
		return elapsed, errors.New("restore/failover gate exceeded 15 seconds")
	}
	if err = execute(ctx, c, r, 99, "get", "restore-canary", ""); err != nil {
		return elapsed, err
	}
	h := r.History()
	last := h[len(h)-1]
	if !last.Found || last.GotValue != "acknowledged" {
		return elapsed, errors.New("acknowledged canary lost after restore")
	}
	for _, key := range []string{"restore-canary", "post-failover"} {
		if err = execute(ctx, c, r, 99, "delete", key, ""); err != nil {
			return elapsed, err
		}
	}
	if err = fault.CheckHistory(r.History()); err != nil {
		return elapsed, err
	}
	return elapsed, writeJSON(filepath.Join(out, fmt.Sprintf("restore-%06d.json", index)), map[string]any{"schema_version": 2, "ops": r.History(), "recovery_seconds": elapsed, "snapshot_restore": true, "certificate_rotation": true, "scope": "graceful localhost member restart; not RTO/RPO or host HA"})
}
func run() error {
	profile := flag.String("profile", "smoke", "smoke or soak")
	transportMode := flag.String("transport", "mtls", "mtls or plaintext (smoke comparison only)")
	duration := flag.Duration("duration", 60*time.Second, "smoke duration only; soak always requires 24 hours")
	out := flag.String("out", "", "new exclusive output directory")
	flag.Parse()
	if *profile != "smoke" && *profile != "soak" {
		return errors.New("unknown profile")
	}
	if (*transportMode != "mtls" && *transportMode != "plaintext") || (*profile == "soak" && *transportMode != "mtls") {
		return errors.New("soak requires mTLS; comparison mode is smoke-only")
	}
	required := *duration
	if *profile == "soak" {
		required = 24 * time.Hour
	} else if required < 10*time.Second || required > 10*time.Minute {
		return errors.New("smoke duration must be 10 seconds to 10 minutes")
	}
	if *out == "" {
		return errors.New("new output directory required")
	}
	if err := os.Mkdir(*out, 0700); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	c, err := labcluster.NewMode(*out, *transportMode == "mtls")
	if err != nil {
		return err
	}
	defer c.Close()
	start := time.Now()
	state := map[string]any{"profile": *profile, "required_seconds": required.Seconds(), "started_utc": start.UTC(), "status": "running", "transport": *transportMode, "nodes": 3, "clients": 4, "value_bytes": 1024, "keys": 8, "sync": "fsync every append", "tick_seconds": 5, "gates": map[string]any{"invariant_violations": 0, "failed_logical_operations": 0, "healthy_window_uncertain_mutations": 0, "p99_seconds": 5, "recovery_seconds": 15, "go_memory_bytes": 1 << 30}}
	defer func() {
		state["elapsed_seconds"] = time.Since(start).Seconds()
		if state["status"] == "running" {
			state["status"] = "FAIL"
		}
		writeJSON(filepath.Join(*out, "summary.json"), state)
	}()
	if err = writeJSON(filepath.Join(*out, "preflight.json"), state); err != nil {
		return err
	}
	probeCtx, probeStop := context.WithTimeout(ctx, 15*time.Second)
	leader, e := c.Leader(probeCtx)
	if e != nil {
		probeStop()
		return e
	}
	cold, closeCold, e := c.ColdClient(leader)
	if e != nil {
		probeStop()
		return e
	}
	coldHistory := fault.NewRecorder()
	coldStart := time.Now()
	e = coldHistory.ExecuteOnce(probeCtx, 100, "put", "cold-canary", fmt.Sprintf("%01024d", 1), c.Peers[leader].ID, cold)
	state["cold_first_ack_seconds"] = time.Since(coldStart).Seconds()
	if e == nil {
		e = coldHistory.ExecuteOnce(probeCtx, 100, "get", "cold-canary", "", c.Peers[leader].ID, cold)
	}
	if e == nil {
		e = coldHistory.ExecuteOnce(probeCtx, 100, "delete", "cold-canary", "", c.Peers[leader].ID, cold)
	}
	closeCold()
	probeStop()
	if e != nil {
		return e
	}
	if e = fault.CheckHistory(coldHistory.History()); e != nil {
		return e
	}
	if e = writeJSON(filepath.Join(*out, "cold-history.json"), map[string]any{"schema_version": 2, "ops": coldHistory.History()}); e != nil {
		return e
	}
	// No unlimited sample arrays: one JSON line per batch, exact histories retained
	// under a supervisor-enforced disk ceiling. Quantiles are per-window samples.
	samples, err := os.OpenFile(filepath.Join(*out, "samples.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer samples.Close()
	encoder := json.NewEncoder(samples)
	batches, attempts := 0, 0
	maintenanceRuns := 0
	for time.Since(start) < required {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if batches%360 == 0 {
			opCtx, stop := context.WithTimeout(ctx, 25*time.Second)
			recovery, e := maintenance(opCtx, c, *out, batches)
			stop()
			if e != nil {
				return fmt.Errorf("maintenance: %w", e)
			}
			maintenanceRuns++
			state["last_recovery_seconds"] = recovery
		}
		batchStart := time.Now()
		r := fault.NewRecorder()
		var wg sync.WaitGroup
		failures := make(chan error, 4)
		latency := make(chan float64, 16)
		for client := 0; client < 4; client++ {
			wg.Add(1)
			go func(client int) {
				defer wg.Done()
				key := fmt.Sprintf("key-%d", (batches+client)%8)
				value := fmt.Sprintf("%01024d", batches*4+client)
				for _, kind := range []string{"put", "get", "delete", "get"} {
					opCtx, stop := context.WithTimeout(ctx, 10*time.Second)
					s := time.Now()
					e := execute(opCtx, c, r, client, kind, key, value)
					latency <- time.Since(s).Seconds()
					stop()
					if e != nil {
						failures <- e
						return
					}
				}
			}(client)
		}
		wg.Wait()
		close(failures)
		close(latency)
		history := r.History()
		path := filepath.Join(*out, fmt.Sprintf("history-%06d.json", batches))
		if err = writeJSON(path, map[string]any{"schema_version": 2, "ops": history, "scope": "observed_rpc_attempts_only"}); err != nil {
			return err
		}
		if err = fault.CheckHistory(history); err != nil {
			return fmt.Errorf("batch %d invariant: %w", batches, err)
		}
		outcomes := map[string]int{}
		for _, op := range history {
			outcomes[op.Outcome]++
			if op.Outcome == "uncertain" {
				return errors.New("healthy-window uncertain mutation; history retained and run stopped")
			}
		}
		for e := range failures {
			return fmt.Errorf("failed logical operation (history retained): %w", e)
		}
		times := []float64{}
		for v := range latency {
			times = append(times, v)
		}
		p99 := percentile(append([]float64(nil), times...), .99)
		if p99 > 5 {
			return fmt.Errorf("latency gate failed: p99=%f", p99)
		}
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		if memory.Sys > 1<<30 {
			return errors.New("Go memory ceiling exceeded")
		}
		attempts += len(history)
		batches++
		sample := map[string]any{"elapsed_seconds": time.Since(start).Seconds(), "batch": batches, "attempts": len(history), "outcomes": outcomes, "logical_operation_seconds": times, "p50_seconds": percentile(append([]float64(nil), times...), .50), "p95_seconds": percentile(append([]float64(nil), times...), .95), "p99_seconds": p99, "go_memory_sys_bytes": memory.Sys, "go_heap_bytes": memory.HeapAlloc, "goroutines": runtime.NumGoroutine(), "checker": "PASS"}
		if err = encoder.Encode(sample); err != nil {
			return err
		}
		if err = samples.Sync(); err != nil {
			return err
		}
		if err = writeJSON(filepath.Join(*out, "latest-history.json"), map[string]any{"schema_version": 2, "ops": history, "metrics": observedMetrics(c), "linearizable": true}); err != nil {
			return err
		}
		state["batches"], state["attempts"], state["maintenance_runs"] = batches, attempts, maintenanceRuns
		if err = writeJSON(filepath.Join(*out, "progress.json"), state); err != nil {
			return err
		}
		wait := 5*time.Second - time.Since(batchStart)
		remaining := required - time.Since(start)
		if wait > remaining {
			wait = remaining
		}
		if wait > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}
	}
	if time.Since(start) < required {
		return errors.New("elapsed duration gate failed")
	}
	// Read from the real mTLS client once more before declaring success.
	opCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	leader, err = c.Leader(opCtx)
	if err != nil {
		return err
	}
	if _, err = c.Clients[leader].Get(opCtx, &pb.GetRequest{Key: []byte("restore-canary")}); err != nil {
		return err
	}
	state["status"] = "PASS"
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
