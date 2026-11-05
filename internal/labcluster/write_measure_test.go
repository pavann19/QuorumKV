//go:build measure

package labcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/raftnode"
	"github.com/pavann19/quorumkv/internal/requestlimits"
	"github.com/pavann19/quorumkv/test/fault"
	"google.golang.org/grpc"
)

type writeRunResources struct {
	CPU                                          map[string]uint64 `json:"cgroup_cpu"`
	CPUObservation                               string            `json:"cpu_observation"`
	NumGC                                        uint32            `json:"num_gc"`
	PauseTotalNS, TotalAlloc, HeapAlloc, HeapSys uint64
	Goroutines                                   int `json:"goroutines"`
}

func writeResources() writeRunResources {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	result := writeRunResources{CPU: map[string]uint64{}, NumGC: memory.NumGC, PauseTotalNS: memory.PauseTotalNs, TotalAlloc: memory.TotalAlloc, HeapAlloc: memory.HeapAlloc, HeapSys: memory.HeapSys, Goroutines: runtime.NumGoroutine()}
	var data []byte
	for _, path := range []string{"/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/cpu/cpu.stat", "/sys/fs/cgroup/cpu,cpuacct/cpu.stat"} {
		observed, err := os.ReadFile(path)
		if err == nil {
			data = observed
			result.CPUObservation = path
			break
		}
	}
	if data == nil {
		result.CPUObservation = "UNAVAILABLE"
		return result
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 {
			if value, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
				result.CPU[fields[0]] = value
			}
		}
	}
	if len(result.CPU) == 0 {
		result.CPUObservation = "INVALID_STATS"
	}

	return result
}

type writeBatchMeasurement struct {
	HistoryChecked                                bool                  `json:"history_checked"`
	RecoveryProgress                              []map[string]any      `json:"recovery_progress_at_timeout,omitempty"`
	OperationsPerClient                           int                   `json:"operations_per_client"`
	EffectiveRaft                                 raftnode.RaftSettings `json:"effective_raft"`
	EffectiveRequests                             requestlimits.Limits  `json:"effective_requests"`
	ResourcesBefore, ResourcesAfter               writeRunResources
	ReadPhasesBefore, ReadPhasesAfter             [4]raftnode.ReadPhaseSnapshot
	Status                                        string  `json:"status"`
	Name                                          string  `json:"name"`
	MTLS                                          bool    `json:"mtls"`
	ValueBytes                                    int     `json:"value_bytes"`
	AppendEntries                                 int     `json:"append_entries"`
	Repetition                                    int     `json:"repetition"`
	ColdWriteNS                                   int64   `json:"first_client_write_ns"`
	WriteNS                                       []int64 `json:"write_samples_ns"`
	ReadNS                                        []int64 `json:"read_samples_ns"`
	ElapsedNS                                     int64   `json:"mixed_elapsed_ns"`
	WritesPerSecond                               float64 `json:"completed_writes_per_second_under_mixed_load"`
	WriteP50NS                                    int64   `json:"write_p50_ns"`
	WriteP95NS                                    int64   `json:"write_p95_ns"`
	WriteP99NS                                    int64   `json:"write_p99_ns"`
	ReadP50NS                                     int64   `json:"read_p50_ns"`
	ReadP95NS                                     int64   `json:"read_p95_ns"`
	ReadP99NS                                     int64   `json:"read_p99_ns"`
	WALAppends, WALSyncs                          uint64
	RaftGrowth                                    uint64 `json:"raft_log_growth_during_mixed_load"`
	CatchupNS                                     int64  `json:"restart_to_exact_catchup_ns"`
	PeerMaxBytes, PeerMaxEntries, PeerMultiFrames int
	Error                                         string `json:"error,omitempty"`
}

func measureWriteBatchProfile(t *testing.T, mtls bool, valueBytes, appendEntries, repetition int) (result writeBatchMeasurement) {
	t.Helper()
	result = writeBatchMeasurement{Status: "INCOMPLETE", Name: fmt.Sprintf("write-mtls-%v-bytes-%d-append-%d-rep-%d", mtls, valueBytes, appendEntries, repetition), MTLS: mtls, ValueBytes: valueBytes, AppendEntries: appendEntries, Repetition: repetition}
	recorder := fault.NewRecorder()
	checked := false
	defer func() {
		if !checked && len(recorder.History()) > 0 {
			if err := fault.WriteHistory(result.Name, recorder.History(), false, recorder.Events()...); err != nil {
				t.Error(err)
			}
		}
		directory := os.Getenv("QUORUMKV_RESULTS_DIR")
		if directory != "" {
			raw, _ := json.MarshalIndent(result, "", "  ")
			if err := os.WriteFile(filepath.Join(directory, result.Name+"-measurement.json"), append(raw, '\n'), 0600); err != nil {
				t.Error(err)
			}
			for _, name := range []string{result.Name + ".json", result.Name + "-measurement.json"} {
				file, err := os.OpenFile(filepath.Join(directory, name), os.O_RDWR, 0)
				if err != nil {
					t.Error(err)
					continue
				}
				syncErr := file.Sync()
				closeErr := file.Close()
				if syncErr != nil {
					t.Error(syncErr)
				}
				if closeErr != nil {
					t.Error(closeErr)
				}
			}

		}
	}()
	settings := raftnode.DefaultRaftSettings()
	settings.MaxAppendEntries = appendEntries
	settings.SnapshotMS = time.Hour.Milliseconds()
	settings.SnapshotThreshold = 1 << 20
	probe := &appendWireProbe{phase: "warmup"}
	c, err := newModeRaftProfile(t.TempDir(), mtls, 0, "quorum", settings, grpcProbeOptions(probe)...)
	if err != nil {
		result.Error = err.Error()
		return
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	leader, err := c.Leader(ctx)
	if err != nil {
		result.Error = err.Error()
		return
	}
	client, node := c.Clients[leader], c.Peers[leader].ID
	result.EffectiveRaft = c.Nodes[leader].EffectiveRaftSettings()
	result.EffectiveRequests = c.Nodes[leader].RequestLimits()
	const keyCount = 16
	seed := strings.Repeat("s", valueBytes)
	for i := 0; i < keyCount; i++ {
		start := time.Now()
		err := recorder.ExecuteOnce(ctx, 1000, "put", fmt.Sprintf("shared-%02d", i), seed, node, client)
		if i == 0 {
			result.ColdWriteNS = time.Since(start).Nanoseconds()
		}
		if err != nil {
			result.Error = err.Error()
			return
		}
	}
	if err = recorder.ExecuteOnce(ctx, 1000, "put", "seed-delete", "seed", node, client); err != nil {
		result.Error = err.Error()
		return
	}
	for i := 0; i < keyCount; i++ {
		if err = recorder.ExecuteOnce(ctx, 1000, "get", fmt.Sprintf("shared-%02d", i), "", node, client); err != nil {
			result.Error = err.Error()
			return
		}
	}
	// No seed publication can spill into measured disk counters on any replica.
	for {
		settled := true
		for _, member := range c.Nodes {
			if member.Store.Usage().Entries != keyCount+1 {
				settled = false
			}
		}
		if settled {
			break
		}
		select {
		case <-ctx.Done():
			result.Error = "seed application did not settle"
			return
		case <-time.After(time.Millisecond):
		}
	}
	lag := (leader + 1) % len(c.Nodes)
	if err = c.Stop(lag); err != nil {
		result.Error = err.Error()
		return
	}
	before := c.Nodes[leader].Store.WALStats()
	beforeIndex := c.Nodes[leader].Raft.LastIndex()
	term := c.Nodes[leader].Raft.Stats()["term"]
	result.ResourcesBefore = writeResources()
	result.ReadPhasesBefore = c.Nodes[leader].ReadPhaseStats()
	const writers, readers = 8, 4
	operations := 32
	if valueBytes == 1024 {
		operations = 256
	}
	result.OperationsPerClient = operations
	result.WriteNS = make([]int64, writers*operations)
	result.ReadNS = make([]int64, readers*operations)
	startGate := make(chan struct{})
	errs := make(chan error, writers+readers)
	var group sync.WaitGroup
	for worker := 0; worker < writers; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			<-startGate
			for i := 0; i < operations; i++ {
				key := fmt.Sprintf("shared-%02d", (worker*4+i)%keyCount)
				prefix := fmt.Sprintf("writer-%d-seq-%d-", worker, i)
				value := prefix + strings.Repeat("v", valueBytes-len(prefix))
				start := time.Now()
				err := recorder.ExecuteOnce(ctx, worker, "put", key, value, node, client)
				result.WriteNS[worker*operations+i] = time.Since(start).Nanoseconds()
				if err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	for worker := 0; worker < readers; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			<-startGate
			for i := 0; i < operations; i++ {
				start := time.Now()
				err := recorder.ExecuteOnce(ctx, writers+worker, "get", fmt.Sprintf("shared-%02d", (worker+i)%keyCount), "", node, client)
				result.ReadNS[worker*operations+i] = time.Since(start).Nanoseconds()
				if err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	started := time.Now()
	close(startGate)
	group.Wait()
	result.ElapsedNS = time.Since(started).Nanoseconds()
	result.ReadPhasesAfter = c.Nodes[leader].ReadPhaseStats()
	result.ResourcesAfter = writeResources()
	close(errs)
	for err := range errs {
		result.Error = err.Error()
	}
	if result.Error != "" {
		return
	}
	if !c.Nodes[leader].IsLeader() || c.Nodes[leader].Raft.Stats()["term"] != term {
		result.Error = "leadership changed in healthy mixed measurement"
		return
	}
	after := c.Nodes[leader].Store.WALStats()
	result.WALAppends = after.Appends - before.Appends
	result.WALSyncs = after.Syncs - before.Syncs
	result.RaftGrowth = c.Nodes[leader].Raft.LastIndex() - beforeIndex
	if result.WALAppends != uint64(writers*operations) || result.RaftGrowth != uint64(writers*operations) || (appendEntries == 1 && result.WALSyncs != uint64(writers*operations)) || (appendEntries > 1 && result.WALSyncs >= uint64(writers*operations)) {
		result.Error = "write/log/sync accounting gate failed"
		return
	}
	result.WritesPerSecond = float64(writers*operations) / (float64(result.ElapsedNS) / 1e9)
	result.WriteP50NS = readPercentile(result.WriteNS, .50)
	result.WriteP95NS = readPercentile(result.WriteNS, .95)
	result.WriteP99NS = readPercentile(result.WriteNS, .99)
	result.ReadP50NS = readPercentile(result.ReadNS, .50)
	result.ReadP95NS = readPercentile(result.ReadNS, .95)
	result.ReadP99NS = readPercentile(result.ReadNS, .99)
	if err = recorder.ExecuteOnce(ctx, 1000, "delete", "seed-delete", "", node, client); err != nil {
		result.Error = err.Error()
		return
	}
	if err = recorder.ExecuteOnce(ctx, 1000, "get", "seed-delete", "", node, client); err != nil {
		result.Error = err.Error()
		return
	}
	for i := 0; i < keyCount; i++ {
		if err = recorder.ExecuteOnce(ctx, 1000, "get", fmt.Sprintf("shared-%02d", i), "", node, client); err != nil {
			result.Error = err.Error()
			return
		}
	}
	expected := exactDrillState(c.Nodes[leader])
	target := c.Nodes[leader].Raft.CommitIndex()
	probe.Lock()
	probe.phase = "catchup"
	probe.Unlock()
	started = time.Now()
	if err = c.Restart(lag); err != nil {
		result.Error = err.Error()
		return
	}
	for {
		settled := true
		for _, member := range c.Nodes {
			_, present := member.Store.Get([]byte("seed-delete"))
			if present || member.Store.Usage().Entries != keyCount || member.Raft.CommitIndex() < target {
				settled = false
			}
		}
		if settled {
			break
		}
		select {
		case <-ctx.Done():
			result.Error = "catchup fixture budget exhausted before acknowledged prefix applied"
			result.CatchupNS = time.Since(started).Nanoseconds()
			for index, member := range c.Nodes {
				stats := member.Raft.Stats()
				usage := member.Store.Usage()
				result.RecoveryProgress = append(result.RecoveryProgress, map[string]any{"member": c.Peers[index].ID, "term": stats["term"], "commit_index": stats["commit_index"], "applied_index": stats["applied_index"], "last_log_index": stats["last_log_index"], "live_entries": usage.Entries, "live_bytes": usage.Bytes})
			}
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
	for _, member := range c.Nodes {
		if !reflect.DeepEqual(exactDrillState(member), expected) {
			result.Error = "catchup state/deletion differs from acknowledged leader"
			return
		}
	}
	result.CatchupNS = time.Since(started).Nanoseconds()
	if !c.Nodes[leader].IsLeader() || c.Nodes[leader].Raft.Stats()["term"] != term {
		result.Error = "leadership changed in healthy catchup measurement"
		return
	}
	probe.Lock()
	result.PeerMaxBytes, result.PeerMaxEntries, result.PeerMultiFrames = probe.maxBytes, probe.maxEntries, probe.multiFrames
	installs := probe.installs
	probe.Unlock()
	if result.PeerMaxBytes > requestlimits.MaxMessageBytes || installs != 0 || (appendEntries > 1 && result.PeerMultiFrames == 0) {
		result.Error = "wire aggregation/no-snapshot gate failed"
		return
	}
	fault.CheckAndCommit(t, result.Name, recorder.History(), recorder.Events()...)
	checked = true
	result.HistoryChecked = true
	result.Status = "PASS_LOCAL_PROFILE"
	return
}
func grpcProbeOptions(probe *appendWireProbe) []grpc.ServerOption {
	return []grpc.ServerOption{grpc.ChainUnaryInterceptor(probe.unary), grpc.ChainStreamInterceptor(probe.stream)}
}
func TestWriteBatchPairedMeasurement(t *testing.T) {
	if os.Getenv("QUORUMKV_RESULTS_DIR") == "" {
		t.Setenv("QUORUMKV_RESULTS_DIR", t.TempDir())
	}
	report := map[string]any{"status": "FAIL_OR_INCOMPLETE", "comparison": "same-candidate logical append group 1 versus 8; both retain fsync and narrowed map lock", "profile": map[string]any{"nodes": 3, "writers": 8, "readers": 4, "operations_per_client_by_value_bytes": map[string]int{"1024": 256, "65536": 32}, "shared_keys": 16, "paired_repetitions": 3, "value_bytes": []int{1024, 65536}, "gomaxprocs": runtime.GOMAXPROCS(0), "go_version": runtime.Version(), "read_mode": "quorum", "lagging_member": "stopped before mixed phase, then exact log catch-up"}, "predeclared_gates": "zero RPC errors; original attempt history/linearizability oracle; exact all-replica state and deletion; no snapshot fallback; byte limits and sync accounting; median throughput >=1.10 control, median write p99 <=1.20 control, median mixed read p99 <1.00 control, median restart/catchup <=1.20 control per transport/value profile", "limitations": []string{"local Docker/WSL2 only; not production capacity/multi-host RTO", "first-client-write samples retained without a cold-latency gate", "both profiles use current narrowed map lock, so comparison isolates batching", "each profile runs in a fresh helper process; history artifacts fsynced before the next profile"}}
	runs := []writeBatchMeasurement{}
	comparisons := []map[string]any{}
	defer func() {
		report["runs"] = runs
		report["comparisons"] = comparisons
		directory := os.Getenv("QUORUMKV_RESULTS_DIR")
		if directory == "" {
			directory = t.TempDir()
		}
		raw, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(filepath.Join(directory, "write-batch-measurement.json"), append(raw, '\n'), 0600); err != nil {
			t.Error(err)
		}
	}()
	failed := false
	for _, mtls := range []bool{false, true} {
		for _, size := range []int{1024, 65536} {
			profiles := map[int][]writeBatchMeasurement{1: {}, 8: {}}
			for repetition := 0; repetition < 3; repetition++ {
				order := []int{1, 8}
				if repetition%2 == 1 {
					order = []int{8, 1}
				}
				for _, entries := range order {
					report["active_profile"] = fmt.Sprintf("mtls=%v bytes=%d entries=%d rep=%d", mtls, size, entries, repetition)
					result := measureFreshWriteBatchProfile(t, mtls, size, entries, repetition)
					runs = append(runs, result)
					if result.Error != "" {
						t.Fatal(result.Name, result.Error)
					}
					profiles[entries] = append(profiles[entries], result)
					t.Logf("%s writes/s=%.2f write_p99_ns=%d read_p99_ns=%d catchup_ns=%d syncs=%d", result.Name, result.WritesPerSecond, result.WriteP99NS, result.ReadP99NS, result.CatchupNS, result.WALSyncs)
				}
			}
			median := func(entries int, value func(writeBatchMeasurement) float64) float64 {
				values := []float64{}
				for _, result := range profiles[entries] {
					values = append(values, value(result))
				}
				sort.Float64s(values)
				return values[len(values)/2]
			}
			ratio := func(value func(writeBatchMeasurement) float64) float64 { return median(8, value) / median(1, value) }
			throughput := ratio(func(r writeBatchMeasurement) float64 { return r.WritesPerSecond })
			writeP99 := ratio(func(r writeBatchMeasurement) float64 { return float64(r.WriteP99NS) })
			readP99 := ratio(func(r writeBatchMeasurement) float64 { return float64(r.ReadP99NS) })
			catchup := ratio(func(r writeBatchMeasurement) float64 { return float64(r.CatchupNS) })
			pass := throughput >= 1.10 && writeP99 <= 1.20 && readP99 < 1.00 && catchup <= 1.20
			comparisons = append(comparisons, map[string]any{"mtls": mtls, "value_bytes": size, "median_throughput_ratio": throughput, "median_write_p99_ratio": writeP99, "median_read_p99_ratio": readP99, "median_restart_catchup_ratio": catchup, "pass": pass})
			if !pass {
				failed = true
				t.Errorf("paired gate mtls=%v bytes=%d throughput=%.3f write_p99=%.3f read_p99=%.3f catchup=%.3f", mtls, size, throughput, writeP99, readP99, catchup)
			}
		}
	}
	delete(report, "active_profile")
	if !failed {
		report["status"] = "PASS_LOCAL_PAIRED_BATCH_MEASUREMENT"
	}
}

// This is an observation profile for the previously failing mTLS/1 KiB case.
// It does not replace or relax the paired acceptance gates above.
func TestWriteBatchReadTailDiagnostic(t *testing.T) {
	if os.Getenv("QUORUMKV_RESULTS_DIR") == "" {
		t.Setenv("QUORUMKV_RESULTS_DIR", t.TempDir())
	}
	results := []writeBatchMeasurement{}
	defer func() {
		raw, _ := json.MarshalIndent(map[string]any{"status": "OBSERVATIONS_ONLY", "scope": "same workload and gates remain in TestWriteBatchPairedMeasurement; collect phase/CPU/GC observations for the failing profile", "runs": results}, "", "  ")
		if err := os.WriteFile(filepath.Join(os.Getenv("QUORUMKV_RESULTS_DIR"), "write-tail-diagnostic.json"), append(raw, '\n'), 0600); err != nil {
			t.Error(err)
		}
	}()
	for repetition := 0; repetition < 3; repetition++ {
		order := []int{1, 8}
		if repetition%2 == 1 {
			order = []int{8, 1}
		}
		for _, entries := range order {
			result := measureFreshWriteBatchProfile(t, true, 1024, entries, repetition)
			results = append(results, result)
			if result.Error != "" {
				t.Fatal(result.Name, result.Error)
			}
			t.Logf("diagnostic %s read_p99_ns=%d mixed_ns=%d", result.Name, result.ReadP99NS, result.ElapsedNS)
		}
	}
}

type writeProfileParameters struct {
	MTLS                            bool
	ValueBytes, Entries, Repetition int
}

func TestWriteBatchProfileHelper(t *testing.T) {
	raw := os.Getenv("QUORUMKV_WRITE_PROFILE_HELPER")
	if raw == "" {
		return
	}
	var parameters writeProfileParameters
	if len(raw) > 1024 || json.Unmarshal([]byte(raw), &parameters) != nil || (parameters.ValueBytes != 1024 && parameters.ValueBytes != 65536) || (parameters.Entries != 1 && parameters.Entries != 8) || parameters.Repetition < 0 || parameters.Repetition >= 3 {
		t.Fatal("invalid owned profile parameters")
	}
	result := measureWriteBatchProfile(t, parameters.MTLS, parameters.ValueBytes, parameters.Entries, parameters.Repetition)
	if result.Error != "" || result.Status != "PASS_LOCAL_PROFILE" {
		t.Fatal("profile did not pass correctness", result.Error)
	}
}
func measureFreshWriteBatchProfile(t *testing.T, mtls bool, valueBytes, entries, repetition int) writeBatchMeasurement {
	t.Helper()
	name := fmt.Sprintf("write-mtls-%v-bytes-%d-append-%d-rep-%d", mtls, valueBytes, entries, repetition)
	directory := os.Getenv("QUORUMKV_RESULTS_DIR")
	result := writeBatchMeasurement{Status: "INCOMPLETE", Name: name, MTLS: mtls, ValueBytes: valueBytes, AppendEntries: entries, Repetition: repetition}
	log, err := os.Create(filepath.Join(directory, name+"-child.log"))
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer log.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 140*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWriteBatchProfileHelper$", "-test.timeout=135s", "-test.v")
	parameters, _ := json.Marshal(writeProfileParameters{MTLS: mtls, ValueBytes: valueBytes, Entries: entries, Repetition: repetition})
	command.Env = append(os.Environ(), "QUORUMKV_WRITE_PROFILE_HELPER="+string(parameters))
	command.Stdout, command.Stderr = log, log
	runErr := command.Run()
	data, err := os.ReadFile(filepath.Join(directory, name+"-measurement.json"))
	if err != nil {
		result.Error = fmt.Sprintf("profile receipt missing: %v (process %v)", err, runErr)
		return result
	}
	if err = json.Unmarshal(data, &result); err != nil {
		result.Error = err.Error()
		return result
	}
	if runErr != nil {
		result.Error = fmt.Sprintf("profile process failed: %v; child detail: %s", runErr, result.Error)
	}
	if result.Name != name || result.MTLS != mtls || result.ValueBytes != valueBytes || result.AppendEntries != entries || result.Repetition != repetition {
		result.Error = "profile receipt identity mismatch"
	}
	if result.Status != "PASS_LOCAL_PROFILE" && result.Error == "" {
		result.Error = "profile did not finish correctness checks"
	}
	return result
}
