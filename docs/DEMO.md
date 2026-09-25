# Reproducible Demo

This demo is intentionally local-only: no Docker, Kubernetes, cloud services, or
external dependencies beyond Go modules.

## Prerequisites

- Go 1.22 or newer.
- A Windows, macOS, or Linux shell with free localhost ports in the ranges used
  by the tests.

## Quick Correctness Run

Run the normal test suite without benchmarks:

```bash
go test ./...
```

This includes unit tests, WAL crash recovery, the real 3-process cluster test,
and the fault-injection/linearizability scenarios. It excludes the `bench`
package because benchmark-style measurements are behind the `bench` build tag.

For a faster local smoke check:

```bash
go test -short ./...
```

## Fault-Injection Demo

Run only the fault scenarios:

```bash
go test ./test/fault -run Test
```

Expected shape:

- Five scenarios run: leader partition, minority partition, rolling restarts,
  network delay, and double leader attempt.
- Each scenario records a client history.
- Porcupine reports the committed histories as linearizable.
- JSON artifacts are written under `test/fault/results/`.

## Measurement Run

Benchmarks are separate from tests and must be requested explicitly:

```bash
go test -tags=bench ./bench -run TestMeasure -count=1
```

This writes raw JSON under `bench/results/`:

- `failover_time.json`: 10 leader-kill failover trials.
- `throughput_3nodes.json`: 100 sequential single-client `Put`s on 3 nodes.
- `throughput_5nodes.json`: 100 sequential single-client `Put`s on 5 nodes.

The throughput files report `sequential_ops_per_sec`. That number is inverse
latency for one client with one operation in flight, not a concurrent throughput
ceiling.

## Reproducibility Notes

- Tests and measurements spawn real `quorumkv-node` OS processes on localhost.
- Results depend on local scheduler, filesystem, antivirus, and loopback
  behavior; quote the committed JSON as a recorded local run, not a universal
  benchmark.
- Re-run with `-count=1` to avoid Go test caching when regenerating artifacts.
- If a port is busy, wait for stale child processes to exit or change the base
  ports in the relevant test/benchmark file.
