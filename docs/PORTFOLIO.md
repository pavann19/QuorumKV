# QuorumKV: Backend and Platform Notes

## Project Summary

Built a Go key-value store around HashiCorp Raft, with a custom WAL and state
machine adapter. Added process-level crash tests, directed network partitions,
and Porcupine history checks. Reproduced and fixed recovery and read-consistency
bugs, then verified the changes locally and in Linux CI with race instrumentation.

This project supports backend and platform work involving durable state, service
failure handling, dependency maintenance, and reproducible testing. The consensus
protocol and peer transport are adopted libraries; the
[ownership boundary](RAFT_BOUNDARY.md) lists their responsibilities.

## Technical Discussion

| Topic | Implementation and trade-off |
| --- | --- |
| WAL recovery | A torn suffix used to hide writes appended after restart. Reopen now truncates and syncs the validated prefix before accepting writes. |
| Read consistency | A local leader flag can remain true during a partition. Get commits a Raft barrier and waits for the FSM before reading, at the cost of quorum traffic and another log entry. |
| Committed storage failures | Returning an FSM error does not undo a committed log entry. The replica stops on storage failure rather than continuing with divergent state. |
| Snapshot ownership | Store values are copied at API and snapshot boundaries so callers cannot mutate state outside the WAL path. |
| Fault injection | Per-directed-edge TCP proxies control which peers can communicate. Tests use real child processes on localhost. This does not establish multi-host operating experience. |
| Checker coverage | Known-invalid histories verify that the Porcupine model rejects specific violations. Passing sampled workloads remains bounded evidence. |
| Race testing | The harness builds its own server binaries. Inheriting `GOFLAGS=-race` instruments those children; CI also fails on their race reports. |

## Evidence to Show

- [Main CI](https://github.com/pavann19/QuorumKV/actions/runs/40066244588): passing checks for merged commit `9ca520e`.
- [Verification report](CORRECTNESS_CHECKS.md): reproduced failures, test commands, clean-clone result, and platform coverage.
- [Demo guide](DEMO.md): correctness and fault workloads, with separate opt-in measurements.
- [Regression tests](../internal/raftnode/correctness_test.go): isolated-leader reads and committed storage failures.

## Boundaries

Describe this as a tested local distributed-storage project. It has no client
authentication, TLS, dynamic membership, or production operating history.
Historical benchmark results measure sequential localhost workloads. The
replication algorithm comes from HashiCorp Raft; the work here is storage
integration, client behavior, failure testing, and verification.

Keep the current scope for general backend and platform applications. Additional
distributed-systems features should follow a concrete requirement from a target
role or a reproduced failure in the existing implementation.
