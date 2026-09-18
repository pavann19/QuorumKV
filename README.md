# QuorumKV

A Raft-replicated key-value store, checked against real client histories with a linearizability
checker rather than just asserted to be consistent — see the sibling
[ModelGate](https://github.com/pavann19/modelgate) project for the same "no number until it's
measured" discipline applied to a different domain.

See [docs/DECISIONS.md](docs/DECISIONS.md) for design trade-offs and an honest status of what's
verified so far.

## Status

**MVP and M1 done.** The write-ahead log (`internal/wal`) and its crash-recovery proof came first:
100 trials of starting a real subprocess, killing it abruptly at a random point, and verifying no
committed write disappears and no uncommitted write appears — see
[test/crashrecovery/crash_test.go](test/crashrecovery/crash_test.go). The single-node
`Get`/`Put`/`Delete` gRPC service (`internal/kv`, `internal/store`) is built on top of it. M1 adds
Raft-replicated log replication (`internal/raftnode`, adopting `hashicorp/raft` and
`Jille/raft-grpc-transport` rather than reimplementing either — see
[docs/DECISIONS.md](docs/DECISIONS.md)): a real 3-process cluster proving leader election, and a
real leader-kill test proving a new leader is elected and the cluster keeps accepting writes —
[test/cluster/cluster_test.go](test/cluster/cluster_test.go). M2's fault-injection harness and
linearizability checking — the plan's stated "actual point" of this project — is next.

## Layout

- `internal/wal` — the write-ahead log: append-only, fsync'd, CRC-checked records.
- `internal/store` — the in-memory KV store: every mutation goes through the WAL first.
- `internal/kv` — the single-node gRPC service layer (`proto/kv.proto`), a thin translation over
  `internal/store`. Used by `cmd/quorumkv`, the standalone (non-replicated) server.
- `internal/raftnode` — the Raft-replicated node: an `FSM` applying committed entries to
  `internal/store`, and a `Node`/`KVServer` bootstrapping `hashicorp/raft` over a gRPC transport.
  Used by `cmd/quorumkv-node`, the replicated cluster server.
- `cmd/quorumkv` — the single-node (MVP) server binary.
- `cmd/quorumkv-node` — the Raft-replicated (M1) cluster server binary.
- `cmd/walwriter` — a test-only helper binary the crash-recovery test drives as a real subprocess.
- `test/crashrecovery` — the MVP's exit criterion: 100 real kill-and-recover trials.
- `test/cluster` — M1's exit criterion: a real 3-process cluster, leader election, and a real
  leader-kill test.

## Running tests

```bash
go test ./...
```
