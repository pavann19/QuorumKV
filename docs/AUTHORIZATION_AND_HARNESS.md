# Bounded authorization verification and fault-harness cleanup

## Behavior

The identity layer previously rebuilt and cryptographically verified a certificate
chain on every RPC and stream send/receive. Each Identity now caches at most 256
successful chain proofs. The key covers the complete presented DER chain and
required extended key usage. The cached interval is the intersection of every
certificate's validity window in a verified path, including intermediates and root.

Every invocation still checks its exact URI, configured membership, method role
and current time. An expired proof or time before its validity interval forces
normal verification; failed verification is never cached. Cache entries cannot
cross Identity instances or trust stores. Roots and allowlists remain immutable
for the process lifetime, and leaf rotation is still loaded on new handshakes.
This introduces no live certificate revocation or automatic trust-root rotation.

The fault harness also reuses one gRPC ClientConn per member instead of retaining
a new client on every leader-discovery poll. Connections belong to the cluster's
test lifetime; gRPC reconnects them after process restart. A concurrent regression
checks that 257 polls retain one client for the target member, while distinct
members retain distinct clients. This counts client objects, not physical sockets.

Closing a delayed TCP proxy now interrupts its timer wait and repeated Close is
safe. The regression closes a pump configured with a two-second delay and requires
it to exit within 250 ms. This is a bounded regression, not a shutdown benchmark.
Per-chunk delay semantics are unchanged.

No RPC, storage or peer protocol changes are introduced. Raft quorum checks,
completed-FSM read fences, fsync-before-acknowledgement and uncertain-write
classification are unchanged. There is no data migration.

## Reproduce

Use the Go version in `go.mod`, disposable loopback ports and a compatible C
compiler for race instrumentation. Run from the repository root:

```sh
go test ./internal/identity ./internal/faultproxy ./test/fault -run 'TestVerificationCache|TestMTLSAuthorizationAndRotation|TestClusterClientConnectionsBoundedPerMember|TestDelayedPumpStopsOnClose|TestCutRejectsDialThatFinishesAfterRestore|TestRelayClosesOtherPumpOnEOF' -race -count=5 -timeout=120s
go test ./internal/identity -run '^$' -bench '^BenchmarkAuthorize$' -benchmem -benchtime=1s -count=5
```

The authorization benchmark excludes certificate issuance, initial verification
and connection setup from timing; each iteration still checks method permission.
Ordinary tests cover rogue roots, wrong peer URIs, client/server usage separation,
read-only roles, leaf/intermediate/root expiry, time before validity, certificate
rotation, bounded eviction and concurrent access.

Optional finite measurements use a fresh output directory each time:

```sh
export QUORUMKV_RESULTS_DIR="$(mktemp -d)"
go test -tags=measure ./internal/labcluster -run '^TestReadFencePairedMeasurement$' -count=1 -timeout=180s -v
# Use another fresh directory for the mixed workload and exact follower catch-up.
export QUORUMKV_RESULTS_DIR="$(mktemp -d)"
go test -tags=measure ./internal/labcluster -run '^TestMTLSHotPathMeasurement$' -count=1 -timeout=360s -v
```

These commands do not start an endurance run. Preserve results before repeating.
Each helper execution compares its own barrier/quorum paths or observes its mixed
workload; comparing source versions requires separately built before/after binaries.

## Recorded local observations — 22 November 2026

[Selected samples and source hashes](evidence/authorization-read-20261122.json)
come from an isolated local Windows development-workspace experiment using
Go 1.27.4. They are historical measurements of that workspace, not a benchmark
of this publication commit or hosted CI. The identity/proxy/harness baseline
sources match the public base, and the selected fix sources match this change;
the development workspace also contains excluded, disabled experiments. Exact
publication-candidate checks are reported separately by its PR and CI.

| Measurement | Before | After | Scope |
|---|---:|---:|---|
| Median warm authorization | 194.104 us | 1.263 us | Five one-second repetitions; 99.35% lower time/call |
| Authorization allocations | 38 | 2 | Allocations/call; 1,896 to 96 bytes/call |
| Four-client mTLS read throughput | 1,219.43/s | 1,979.96/s | Median of six local repetitions; 62.4% higher |
| Four-client mTLS per-run read p99 | 10.007 ms | 6.156 ms | Median of six per-run p99 values; 38.5% lower |

Read measurements used before/candidate/candidate/before binary order, three
repetitions per execution, 128 timed reads per repetition and 1 KiB values.
Each variant/concurrency profile has 768 timed reads. Exact values, unchanged
leadership and zero read-only Raft-log growth/store-WAL syncs were checked on
all three replicas. This is not a pooled p99 or a sustained capacity test.
The candidate's 29.881 ms per-run p99 outlier is retained in the selected samples.

The separate mixed workload is **not an accepted overall performance gain**:
median write throughput increased from 128.33 to 144.97 writes/s, but read p99
increased from 12.180 to 18.157 ms and write p99 from 153.667 to 169.427 ms;
follower catch-up also varied adversely. Each of three repetitions per version
retained 2,048 writes, 1,024 reads, exact catch-up and accepted attempt histories.
Those results remain visible alongside the read-only improvement.

These finite observations establish no production capacity, multi-host
availability, power-loss durability, exactly-once writes or successful 24-hour
endurance run. Existing failed endurance and batching/read-tail gates remain open.
