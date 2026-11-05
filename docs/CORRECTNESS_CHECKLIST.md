# Failure-correctness checkpoint assessment

Assessment: **6 of 8 tasks complete within the tested failure model; 2 partial.**
Evidence: the reliability implementation at `76260e1` passed all seven hosted
jobs in [CI](https://github.com/pavann19/QuorumKV/actions/runs/41578897087) and
[Linux/Windows diagnostics](https://github.com/pavann19/QuorumKV/actions/runs/41578897183).
Native Windows validation also passed 64 Go tests with race instrumentation,
22 Python tests and vet. Later naming/documentation commits need their own CI.

| Task | Status | Evidence and boundary |
| --- | --- | --- |
| Validate Raft leader election and term handling | Partial | [Process tests](../test/cluster/cluster_test.go) demonstrate election, leader replacement and continued writes. Direct assertions for stale-term RPC rejection, monotonic/persisted terms and higher-term step-down are missing from this project's tests. Upstream Raft implementation is not a substitute for those project-level assertions. |
| Test network partitions and node failures | Complete | [Fault scenarios](../test/fault/scenarios_test.go) exercise leader/minority isolation, attempted split brain and rolling restarts; process tests kill the leader. These are bounded local scenarios, not a host-HA claim. |
| Test log replication and consistency | Complete | Shared-key Put/Get/Delete histories retain retries, acknowledgements and uncertain outcomes. The [checker](../test/fault/check.go) validates observed histories; failover verifies retained acknowledged state and a fresh write. This does not enumerate every possible execution. |
| Validate WAL recovery after crashes | Complete | [100 process-kill trials](../test/crashrecovery/crash_test.go) require populated acknowledged state before termination and verify recovery. Complete corruption also fails closed. Process termination is not host power loss. |
| Test snapshots and log compaction | Complete | [Snapshot regression](../internal/raftnode/snapshot_test.go) forces compaction past a lagging member, checks snapshot installation, retained values/deletions/stale-key removal, restarts and a fresh replicated write. |
| Add deterministic fault-injection tests | Complete | [Channel-gated proxy boundaries](../internal/faultproxy/boundary_test.go), [applied-write/lost-reply fixtures](../test/fault/recording_test.go) and [write/fsync failure injection](../internal/wal/disk_pressure_test.go) control specific fault boundaries. This is not deterministic replay of the entire distributed scheduler. |
| Measure read/write throughput and tail latency | Partial | [Sequential](../bench/throughput_test.go) and [concurrent write measurements](../bench/concurrent_test.go) retain write latency quantiles, errors and readback. Mixed lab windows include reads, but separate Get throughput and read-tail distributions under a declared workload are missing. Readback is not a read benchmark. |
| Document consistency guarantees and limitations | Complete | [Transport/recovery](TRANSPORT_AND_RECOVERY.md) and [reliability review](RELIABILITY_REVIEW.md) document leader-fenced reads, quorum commitment, uncertain mutations, format/restore behavior and unverified boundaries. |

## Reproduce the evidence families

Use the Go version required by `go.mod` and disposable local fixtures:

```sh
go test ./test/cluster ./test/fault ./test/crashrecovery ./internal/raftnode ./internal/wal ./internal/faultproxy -p=1 -count=1 -timeout=300s
go test -tags=bench ./bench -count=1 -timeout=300s
```

The [source-pinned runner](../scripts/run_checks.py) can retain functional, vet,
diagnostics and optional race/measurement receipts. Inspect raw results and test
failures; do not relabel an uncertain write as definitely uncommitted.

## Remaining work

Add direct term-handling regressions and a separate read throughput/tail-latency
benchmark before marking all eight tasks complete. Reproducible failure tests,
recovery demonstrations and documented consistency behavior already exist for
the modeled scenarios; the full requested task set is still incomplete.

The historical failed 24-hour run, intermittent delayed-network availability
finding and target-platform/power-loss qualification remain separate open gates.
These results are finite empirical evidence, not a universal correctness proof
or a production-readiness certification.
