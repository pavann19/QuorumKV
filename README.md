# QuorumKV

A Raft-replicated key-value store, checked against real client histories with a linearizability
checker rather than just asserted to be consistent — see the sibling
[ModelGate](https://github.com/pavann19/modelgate) project for the same "no number until it's
measured" discipline applied to a different domain.

See [docs/DECISIONS.md](docs/DECISIONS.md) for design trade-offs and an honest status of what's
verified so far.

## Status

**The entire build plan is complete: MVP through M3.**

- **MVP** — the write-ahead log (`internal/wal`) and its crash-recovery proof: 100 trials of
  starting a real subprocess, killing it abruptly at a random point, and verifying no committed
  write disappears and no uncommitted write appears
  ([test/crashrecovery/crash_test.go](test/crashrecovery/crash_test.go)). The single-node
  `Get`/`Put`/`Delete` gRPC service (`internal/kv`, `internal/store`) is built on top of it.
- **M1** — Raft-replicated log replication (`internal/raftnode`, adopting `hashicorp/raft` and
  `Jille/raft-grpc-transport` rather than reimplementing either — see
  [docs/DECISIONS.md](docs/DECISIONS.md)): a real 3-process cluster proving leader election, and a
  real leader-kill test proving a new leader is elected and the cluster keeps accepting writes
  ([test/cluster/cluster_test.go](test/cluster/cluster_test.go)).
- **M2** — the fault-injection harness, "the actual point of the project" per the plan's own words:
  a Toxiproxy-style per-edge proxy (`internal/faultproxy`) between every pair of nodes, and 5 real
  fault scenarios (leader partition, minority partition, rolling restarts, network delay, double
  leader attempt) each producing a committed history and a real Porcupine linearizability **PASS**
  — [test/fault](test/fault), results committed at
  [test/fault/results/](test/fault/results/).
- **M3** — measured failover time (10 independent trials: p50 2.0s, p90 2.6s) and
  throughput/latency at cluster sizes 3 and 5 (230 ops/sec vs. 215 ops/sec), committed as raw JSON,
  not hand-typed — [bench](bench), results at [bench/results/](bench/results/).

See [docs/DECISIONS.md](docs/DECISIONS.md) for every trade-off write-up, including the real bugs
found along the way (a `Store.Restore` durability bug caught by M1's own work, and why the
per-node-perspective Raft addressing trick that makes M2's fault injection possible is safe only
because the cluster is statically bootstrapped).

## Layout

- `internal/wal` — the write-ahead log: append-only, fsync'd, CRC-checked records.
- `internal/store` — the in-memory KV store: every mutation goes through the WAL first.
- `internal/kv` — the single-node gRPC service layer (`proto/kv.proto`), a thin translation over
  `internal/store`. Used by `cmd/quorumkv`, the standalone (non-replicated) server.
- `internal/raftnode` — the Raft-replicated node: an `FSM` applying committed entries to
  `internal/store`, and a `Node`/`KVServer` bootstrapping `hashicorp/raft` over a gRPC transport.
  Used by `cmd/quorumkv-node`, the replicated cluster server.
- `internal/faultproxy` — the Toxiproxy-style per-edge TCP relay M2's fault injection is built on.
- `cmd/quorumkv` — the single-node (MVP) server binary.
- `cmd/quorumkv-node` — the Raft-replicated (M1+) cluster server binary.
- `cmd/walwriter` — a test-only helper binary the crash-recovery test drives as a real subprocess.
- `test/crashrecovery` — the MVP's exit criterion: 100 real kill-and-recover trials.
- `test/cluster` — M1's exit criterion: a real 3-process cluster, leader election, and a real
  leader-kill test.
- `test/fault` — M2's exit criterion: the fault-injection harness, 5 real scenarios, and the
  Porcupine linearizability model/check.
- `bench` — M3's exit criterion: measured failover time and throughput/latency benchmarks.

## Running tests

```bash
go test ./...
```

`test/cluster`, `test/fault`, and `bench` each spawn real multi-process clusters and take real wall
time (tens of seconds to a couple of minutes); pass `-short` to skip them for a quick check of
everything else.
