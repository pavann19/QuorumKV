# QuorumKV

A Raft-replicated key-value store, checked against real client histories with a linearizability
checker rather than just asserted to be consistent — see the sibling
[ModelGate](https://github.com/pavann19/modelgate) project for the same "no number until it's
measured" discipline applied to a different domain.

See [docs/DECISIONS.md](docs/DECISIONS.md) for design trade-offs and an honest status of what's
verified so far.

## Status

**MVP in progress.** The write-ahead log (`internal/wal`) and its crash-recovery proof are done:
100 trials of starting a real subprocess, killing it abruptly at a random point, and verifying no
committed write disappears and no uncommitted write appears — see
[test/crashrecovery/crash_test.go](test/crashrecovery/crash_test.go). The gRPC `Get`/`Put`/`Delete`
service the MVP also calls for is not yet built. Raft, replication, and fault injection are M1/M2,
not started.

## Layout

- `internal/wal` — the write-ahead log: append-only, fsync'd, CRC-checked records.
- `cmd/walwriter` — a test-only helper binary the crash-recovery test drives as a real subprocess.
- `test/crashrecovery` — the MVP's exit criterion: 100 real kill-and-recover trials.

## Running tests

```bash
go test ./...
```
