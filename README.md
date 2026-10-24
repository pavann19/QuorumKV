# QuorumKV

A Go key-value store with Raft replication, a local write-ahead log, and a
process-based fault-injection harness. The project focuses on recovery behavior,
read consistency, and tests that expose failures across replica boundaries.

HashiCorp Raft provides consensus. The implementation here covers the WAL, store,
FSM adapter, client-facing gRPC service, fault proxies, history recording, and
test workloads. See the [ownership boundary](docs/RAFT_BOUNDARY.md) for the split
between project code and dependencies.

## Verification

[CI passed on main](https://github.com/pavann19/QuorumKV/actions/runs/40066244588)
for commit `9ca520e` on 2026-10-24. Build, vet, race checks, cluster/fault tests,
vulnerability scanning, and the separate benchmark job passed. A fresh local
clone also passed the documented correctness run. The
[verification report](docs/CORRECTNESS_CHECKS.md) records the tested commits,
environment, reproduced bugs, and limits of those checks.

| Area | Implemented behavior | Evidence |
| --- | --- | --- |
| Local durability | CRC-framed WAL, fsync before acknowledgment, repaired tails on reopen | Unit tests and 100 subprocess kill/recovery trials |
| Replication | Static cluster configuration, replicated Put/Delete, snapshot/restore adapter | Three-process election, replication, and leader-kill tests |
| Reads | Commit and await a Raft barrier before reading local state | Healthy-read, cancellation, and isolated-leader regression tests |
| Storage failures | Reject further WAL writes after I/O failure; stop a replica if a committed mutation cannot be applied | WAL and FSM regression tests |
| Fault testing | Directed-edge partitions, rolling restarts, delayed traffic, and a double-leader attempt | Five scenarios checked with Porcupine; known-invalid histories test the checker |

The crash test checks that acknowledged writes survive a process kill and allows
one additional record whose fsync may finish before acknowledgment is observed.
It does not test machine power loss. Passing fault scenarios means the recorded
histories were accepted by the model, not that every possible execution has been
proved linearizable.

## Recorded Measurements

The JSON under [bench/results](bench/results/) records localhost runs from
2026-09-18, before the October hardening changes:

| Workload | Samples | Recorded result |
| --- | ---: | --- |
| Three-node leader-kill failover | 10 | p50 2.106 s; p90 2.458 s |
| Three-node sequential Put latency | 100 | p50 4.295 ms |
| Five-node sequential Put latency | 100 | p50 6.586 ms |

Write measurements use one client with one request in flight. The reported
operations per second are inverse sequential latency, not a concurrent throughput
limit. These files are historical local measurements; the successful hosted
benchmark run has its own artifacts in CI. Neither is a production capacity claim.

## Run Locally

Use Go 1.27.3 or newer and free localhost ports for the test clusters.

```bash
go test ./... -count=1
```

The suite starts real child processes for crash recovery, cluster tests, and fault
scenarios. Use `-short` to skip those slower checks. The
[demo guide](docs/DEMO.md) includes the fault-only run and race-check setup.

Measurements are opt-in and excluded from ordinary tests:

```bash
go test -tags=bench ./bench -run TestMeasure -count=1
```

## Scope and Limits

This is a local demonstration and test project. The gRPC endpoints have no
authentication or TLS; the command-line servers can bind to non-loopback addresses,
so keep them on a trusted local machine. Cluster membership is static. Testing
covers Windows locally and Linux in CI; macOS has not been verified.

The project has not been operated in production. Arbitrary disk corruption,
power-loss durability, large-scale load, and sustained multi-host operation are
outside the verified scope. Read barriers add quorum traffic and log entries.
Committed storage failures stop the affected process and require working storage
before restart.

Feature scope is frozen. Further changes should address a reproduced defect,
a failing check, a dependency vulnerability, or a specific role requirement.

## Code and Design

- `internal/wal`, `internal/store`: local durability and state management.
- `internal/kv`, `internal/raftnode`: standalone and replicated gRPC services.
- `internal/faultproxy`, `test/fault`: directed-edge proxies, workloads, and history checking.
- `test/cluster`, `test/crashrecovery`: process-level replication and recovery tests.
- `bench`: separate failover and sequential write measurements.
- `cmd`: standalone server, replicated server, and crash-test writer.

[Design decisions](docs/DECISIONS.md) describe the implementation trade-offs.
[Portfolio notes](docs/PORTFOLIO.md) summarize the work and the technical questions
it supports in a backend or platform interview.
