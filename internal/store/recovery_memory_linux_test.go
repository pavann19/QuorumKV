//go:build linux && measure

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/wal"
)

type recoveryMemorySample struct {
	Mode            string  `json:"mode"`
	PeakRSSBytes    uint64  `json:"peak_rss_bytes"`
	HeapAllocBytes  uint64  `json:"heap_alloc_bytes_at_observation"`
	TotalAllocBytes uint64  `json:"total_alloc_bytes"`
	GCCycles        uint32  `json:"gc_cycles"`
	OpenSeconds     float64 `json:"open_seconds"`
	Entries         int     `json:"entries"`
	LogicalBytes    int64   `json:"logical_bytes"`
	ValueSHA256     string  `json:"value_sha256"`
	GoVersion       string  `json:"go_version"`
}

func TestRecoveryMemoryHelper(t *testing.T) {
	mode := os.Getenv("QUORUMKV_RECOVERY_MEMORY_MODE")
	if mode == "" {
		return
	}
	path := os.Getenv("QUORUMKV_RECOVERY_MEMORY_PATH")
	var value []byte
	var entries int
	var logicalBytes int64
	var records []wal.Record
	var legacy map[string][]byte
	var candidate *Store
	var file *wal.WAL
	start := time.Now()
	switch mode {
	case "materializing":
		var err error
		records, err = wal.Replay(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		legacy = map[string][]byte{}
		for _, record := range records {
			switch record.Op {
			case wal.OpPut:
				legacy[string(record.Key)] = record.Value
			case wal.OpDelete:
				delete(legacy, string(record.Key))
			}
		}
		file, err = wal.Open(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
		value = legacy["same"]
		entries = len(legacy)
		logicalBytes = usage(legacy)
	case "streaming":
		var err error
		candidate, err = OpenWithLimits(path, Limits{MaxBytes: 65540, MaxEntries: 1})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(4)
		}
		value, _ = candidate.Get([]byte("same"))
		entries = int(candidate.Usage().Entries)
		logicalBytes = candidate.Usage().Bytes
	default:
		os.Exit(5)
	}
	elapsed := time.Since(start).Seconds()
	digest := sha256.Sum256(value)
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil || rusage.Maxrss < 0 {
		os.Exit(6)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	sample := recoveryMemorySample{Mode: mode, PeakRSSBytes: uint64(rusage.Maxrss) * 1024, HeapAllocBytes: memory.HeapAlloc, TotalAllocBytes: memory.TotalAlloc, GCCycles: memory.NumGC, OpenSeconds: elapsed, Entries: entries, LogicalBytes: logicalBytes, ValueSHA256: hex.EncodeToString(digest[:]), GoVersion: runtime.Version()}
	// Preserve the materializing algorithm's necessarily retained decode records
	// through the peak-memory observation; no steady-state heap claim is made.
	runtime.KeepAlive(records)
	runtime.KeepAlive(legacy)
	runtime.KeepAlive(candidate)
	if candidate != nil {
		candidate.Close()
	}
	if file != nil {
		file.Close()
	}
	json.NewEncoder(os.Stdout).Encode(sample)
	os.Exit(0)
}

func TestMeasureStreamingRecoveryMemory(t *testing.T) {
	if recoveryProfileRace {
		t.Fatal("recovery memory measurements require an uninstrumented binary")
	}
	const recordCount = 2048
	const valueBytes = 65536
	const peakCeiling = 64 << 20
	const maxPeakFraction = 0.60
	report := map[string]any{"schema": 1, "status": "INCOMPLETE", "comparison": "same current decoder and I/O semantics; original materializing recovery algorithm vs streaming candidate, not a historical executable comparison", "records": recordCount, "value_bytes": valueBytes, "expected_entries": 1, "logical_bytes": valueBytes + 4, "streaming_peak_rss_ceiling_bytes": peakCeiling, "streaming_to_materializing_peak_fraction_ceiling": maxPeakFraction, "environment": map[string]any{"GOMAXPROCS": 2, "GOGC": 100, "GOMEMLIMIT": "256MiB", "filesystem": "identified by enclosing Linux runner", "cache": "same generated file, paired alternating order, page cache uncontrolled", "race": recoveryProfileRace}, "latency_capacity_claim": "NONE", "scope": "bounded overwrite-history startup memory, not sustained or all-keyspace memory"}
	samples := []recoveryMemorySample{}
	artifact := ""
	if output := os.Getenv("QUORUMKV_RESULTS_DIR"); output != "" {
		if err := os.MkdirAll(output, 0700); err != nil {
			t.Fatal(err)
		}
		var err error
		artifact, err = os.MkdirTemp(output, "recovery-memory-")
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		if t.Failed() {
			report["status"] = "FAIL"
		}
		report["samples"] = samples
		raw, _ := json.MarshalIndent(report, "", "  ")
		if artifact != "" {
			if err := os.WriteFile(filepath.Join(artifact, "report.json"), append(raw, '\n'), 0600); err != nil {
				t.Error(err)
			}
		}
		t.Logf("recovery_memory_profile=%s", raw)
	}()
	path := filepath.Join(t.TempDir(), "history.wal")
	file, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte{0x5a}, valueBytes)
	records := make([]wal.Record, recordCount)
	for i := range records {
		records[i] = wal.Record{Op: wal.OpPut, Key: []byte("same"), Value: payload}
	}
	if err = file.Reset(records); err != nil {
		file.Close()
		t.Fatal(err)
	}
	file.Close()
	input, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	size, err := io.Copy(hash, input)
	input.Close()
	if err != nil {
		t.Fatal(err)
	}
	report["wal_file_bytes"] = size
	report["wal_sha256"] = hex.EncodeToString(hash.Sum(nil))
	expectedHash := sha256.Sum256(payload)
	for repetition := 0; repetition < 3; repetition++ {
		order := []string{"streaming", "materializing"}
		if repetition%2 == 1 {
			order = []string{"materializing", "streaming"}
		}
		paired := map[string]recoveryMemorySample{}
		for _, mode := range order {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoveryMemoryHelper$")
			command.Env = append(os.Environ(), "QUORUMKV_RECOVERY_MEMORY_MODE="+mode, "QUORUMKV_RECOVERY_MEMORY_PATH="+path, "GOMAXPROCS=2", "GOGC=100", "GOMEMLIMIT=256MiB")
			raw, runErr := command.CombinedOutput()
			cancel()
			if runErr != nil {
				report["failed_helper_output"] = string(raw)
				t.Fatalf("%s helper failed: %v", mode, runErr)
			}
			var sample recoveryMemorySample
			if err = json.Unmarshal(raw, &sample); err != nil {
				t.Fatalf("invalid sample: %s %v", raw, err)
			}
			samples = append(samples, sample)
			paired[mode] = sample
			if sample.Entries != 1 || sample.LogicalBytes != valueBytes+4 || sample.ValueSHA256 != hex.EncodeToString(expectedHash[:]) {
				t.Fatalf("recovery state mismatch: %+v", sample)
			}
			if sample.Mode != mode || sample.GoVersion != runtime.Version() || sample.PeakRSSBytes == 0 {
				t.Fatalf("sample provenance mismatch: %+v", sample)
			}
		}
		stream, baseline := paired["streaming"], paired["materializing"]
		if stream.PeakRSSBytes > peakCeiling || float64(stream.PeakRSSBytes) > float64(baseline.PeakRSSBytes)*maxPeakFraction {
			t.Fatalf("predeclared memory gate failed: streaming=%d materializing=%d", stream.PeakRSSBytes, baseline.PeakRSSBytes)
		}
	}
	after, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	afterHash := sha256.New()
	afterSize, err := io.Copy(afterHash, after)
	after.Close()
	if err != nil || afterSize != size || hex.EncodeToString(afterHash.Sum(nil)) != report["wal_sha256"] {
		t.Fatal("measurement changed input WAL", err)
	}
	report["source_wal_unchanged"] = true
	report["status"] = "PASS_LOCAL_LINUX_MEMORY_PROFILE"
}
