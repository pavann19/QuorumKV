// Package telemetry exports bounded-cardinality observations without key/value,
// certificate, error-message or job-content labels. Collector outage cannot
// block the consensus/storage path: all request counters are in-memory atomics.
package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pavann19/quorumkv/internal/raftnode"
	"github.com/pavann19/quorumkv/internal/requestlimits"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Metrics struct {
	counts  [5][17]atomic.Uint64
	nanos   [5]atomic.Uint64
	buckets [5][8]atomic.Uint64
}

var names = [5]string{"get", "put", "delete", "raft", "other"}
var bounds = [8]float64{.001, .005, .01, .05, .1, .5, 1, 5}

func method(s string) int {
	switch s {
	case "/quorumkv.v1.KV/Get":
		return 0
	case "/quorumkv.v1.KV/Put":
		return 1
	case "/quorumkv.v1.KV/Delete":
		return 2
	}
	if strings.HasPrefix(s, "/RaftTransport/") {
		return 3
	}
	return 4
}
func (m *Metrics) Unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	start := time.Now()
	response, err := next(ctx, req)
	idx := method(info.FullMethod)
	code := int(status.Code(err))
	if code > 16 {
		code = 2
	}
	m.counts[idx][code].Add(1)
	elapsed := time.Since(start)
	m.nanos[idx].Add(uint64(elapsed))
	for b, limit := range bounds {
		if elapsed.Seconds() <= limit {
			m.buckets[idx][b].Add(1)
		}
	}
	return response, err
}
func (m *Metrics) Handler(n *raftnode.Node, configHash string, suppliedConfig ...any) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "live") })
	mux.HandleFunc("/startup", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintln(w, "storage open; transport registered") })
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if !n.IsLeader() {
			http.Error(w, "not leader", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if _, err := raftnode.NewKVServer(n).Get(ctx, &pb.GetRequest{Key: []byte("__readiness__")}); err != nil {
			http.Error(w, "quorum read unavailable", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "leader")
	})
	mux.HandleFunc("/effective-config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		limits := n.Store.Limits()
		requests := n.RequestLimits()
		report := map[string]any{
			"schema": 1, "configuration_sha256": configHash,
			"write_read_grace_ms": requests.WriteReadGraceMS,
			"max_key_bytes":       requests.KeyBytes, "max_value_bytes": requests.ValueBytes, "max_message_bytes": requestlimits.MaxMessageBytes,
			"max_inflight_kv": requests.Inflight, "max_streams": requests.Streams, "rpc_deadline_ms": requests.TimeoutMS, "rpc_deadline_seconds": float64(requests.TimeoutMS) / 1000,
			"max_store_bytes": limits.MaxBytes, "max_store_entries": limits.MaxEntries,
			"disk_watermark_percent": n.DiskPolicy().MaxUsedPercent, "min_disk_free_bytes": n.DiskPolicy().MinFreeBytes,
			"read_mode":               n.ReadMode(),
			"snapshot_format_version": n.SnapshotFormatVersion(),
			"wal_format_version":      n.WALFormatVersion(), "command_format_version": n.CommandFormatVersion(),
			"raft": n.EffectiveRaftSettings(), "cluster_id": n.ClusterID(), "peer_protocol_version": n.PeerProtocolVersion(),
		}
		if len(suppliedConfig) == 1 {
			report["node_configuration"] = suppliedConfig[0]
		}
		json.NewEncoder(w).Encode(report)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for idx, name := range names {
			var total uint64
			for code := 0; code < 17; code++ {
				count := m.counts[idx][code].Load()
				total += count
				fmt.Fprintf(w, "quorumkv_rpc_total{method=%q,code=%q} %d\n", name, strconv.Itoa(code), count)
			}
			for b, limit := range bounds {
				fmt.Fprintf(w, "quorumkv_rpc_seconds_bucket{method=%q,le=%q} %d\n", name, strconv.FormatFloat(limit, 'f', -1, 64), m.buckets[idx][b].Load())
			}
			fmt.Fprintf(w, "quorumkv_rpc_seconds_bucket{method=%q,le=\"+Inf\"} %d\nquorumkv_rpc_seconds_count{method=%q} %d\nquorumkv_rpc_seconds_sum{method=%q} %f\n", name, total, name, total, name, float64(m.nanos[idx].Load())/1e9)
		}
		leader := 0
		if n.IsLeader() {
			leader = 1
		}
		fmt.Fprintf(w, "quorumkv_leader %d\n", leader)
		stats := n.Raft.Stats()
		for _, key := range []string{"commit_index", "applied_index", "last_log_index", "last_snapshot_index", "num_peers", "term"} {
			value, _ := strconv.ParseUint(stats[key], 10, 64)
			fmt.Fprintf(w, "quorumkv_raft_%s %d\n", key, value)
		}
		disk, _ := n.DiskStatus()
		known, degraded := 0, 0
		if disk.Known {
			known = 1
		}
		if disk.Degraded {
			degraded = 1
		}
		fmt.Fprintf(w, "# HELP quorumkv_disk_status_known Whether local disk capacity could be observed.\n# TYPE quorumkv_disk_status_known gauge\nquorumkv_disk_status_known %d\n# HELP quorumkv_disk_degraded Whether new local client writes are refused for disk capacity.\n# TYPE quorumkv_disk_degraded gauge\nquorumkv_disk_degraded %d\nquorumkv_disk_available_bytes %d\nquorumkv_disk_total_bytes %d\n", known, degraded, disk.Available, disk.Total)
		usage := n.Store.Usage()
		fmt.Fprintf(w, "# HELP quorumkv_store_bytes Logical key and value bytes.\n# TYPE quorumkv_store_bytes gauge\nquorumkv_store_bytes %d\n# HELP quorumkv_store_keys Distinct stored keys.\n# TYPE quorumkv_store_keys gauge\nquorumkv_store_keys %d\nquorumkv_store_reserved_bytes %d\nquorumkv_store_reserved_entries %d\n", usage.Bytes, usage.Entries, usage.ReservedBytes, usage.ReservedEntries)
		wal := n.Store.WALStats()
		fmt.Fprintf(w, "quorumkv_wal_appends_total %d\nquorumkv_wal_bytes_total %d\nquorumkv_wal_syncs_total %d\nquorumkv_wal_failures_total %d\nquorumkv_wal_sync_seconds_sum %f\n", wal.Appends, wal.Bytes, wal.Syncs, wal.Failures, float64(wal.SyncNanos)/1e9)
	})
	return mux
}
