# Source-to-evidence claim map

The [case study evidence summary](PORTFOLIO_EVIDENCE.md) records the freshly run
short demo and its source boundaries. Other rows identify implementation and
regression coverage, not a claim that every check ran in that selection.
Historical measurements retain their original source and environment scope.

| Bounded claim | Implementation | Regression / raw evidence | Limitation |
|---|---|---|---|
| Authenticated replicated KV with distinct client/peer permissions | `internal/identity/identity.go`, `cmd/quorumkv-node/main.go` | `TestMTLSAuthorizationAndRotation`, `TestProcessMTLSKillLeaderAndRotation` | Private CA/URI allowlist; no Byzantine peer tolerance, live revocation or deployed trust system |
| Rejects untrusted/expired/wrong-target identities and read-only writes; reloads matched leaves | `internal/identity/identity.go` | Real TLS/gRPC handshakes and rotation test; authenticated process restart | Existing connections retain negotiated leaf until reconnect; operator-owned root/allowlist changes require restart |
| Bounded RPC waits preserve uncertain submitted mutations | `internal/raftnode/kv_server.go` | `TestBoundsAndCancellationKeepSubmittedEffect`, recorder lost-reply controls | Timeout is not rollback; pending Raft futures hold admission slots until resolved |
| Acknowledged WAL prefix survives abrupt process termination | `internal/wal/wal.go`, `internal/store/store.go` | `TestCrashRecovery_100Trials`, raw per-trial acknowledgements | Local process kill, not hardware power loss; fsync-before-reply race can retain an unacknowledged record |
| Snapshot installation/restart and offline full-member backup preserve exact state | `internal/raftnode/fsm.go`, `internal/store/store.go` | `TestSnapshotInstallAndRestart`, `TestOfflineBackupRestoreExactState` | Synthetic graceful backups/restores; not an operational disaster-recovery RTO/RPO guarantee |
| Disk-full write/sync errors prevent acknowledgement and poison subsequent writes | `internal/wal/wal.go` | `TestDiskPressureNeverAcknowledgesOrContinues` | Deterministic ENOSPC fixture, not filling a shared/physical volume |
| Checker retains RPC attempts, retries and unknown write outcomes | `test/fault/recording.go`, `model.go`, `validate.go` | Known-bad histories, lost reply, five fault scenarios, authenticated leader kill | Observed finite histories only; an accepted history does not establish all executions |
| Bounded-cardinality request, Raft-index and WAL metrics expose actual effects | `internal/telemetry/metrics.go`, `internal/wal/metrics.go` | `TestBoundedMetricsAndHealth` and lab samples | Process-local counters; no cross-host replication-latency tracing or external monitoring service |
| Independent persistent diagnostics jobs recover leases and fence stale results | `diagnostics/worker.py` | SQLite restart, actual worker subprocess restart, expiry/duplicate/collision tests | Single local worker; safe read/isolated experiment retries, not exactly-once external effects |
| Hostile model/log proposals cannot mutate consensus, policy or trust | `diagnostics/worker.py` | Scope/tenant/path/size/hash denial, inert malicious proposals, protected-file assertions | Default deterministic offline mode; real model inference quality remains unverified |
| Supervised mixed-key mTLS lab with explicit resource/duration gates | `cmd/quorumkv-lab/main.go`, `scripts/run_lab.py` | Unique smoke/soak manifests, raw samples/histories, queue/resource reports | Smoke is not 24 hours; running is not PASS; one host is not host HA |

The adopted HashiCorp Raft implementation and Jille gRPC transport are library
components. The original engineering here is the storage adapter/WAL, observed
fault machinery, identity boundary, diagnostics isolation and evidence workflow.
Do not describe consensus as implemented from scratch or describe short local
throughput results as sustained production capacity. Do not infer monthly
availability, a speedup, production readiness or hosted CI from these runs.

## Current case study boundary

Default reads use a fresh quorum and a completed-FSM fence; they do not append
Raft log entries. Barrier mode is an explicit comparison option. Receipt-aware
journal/command/snapshot experiments are preserved outside this candidate and
excluded from its resume claims. Supported format-0/1 copy migration
must not be confused with a receipt-preserving rollback for the new experiment.
P4/P5 performance, maximum-state and endurance gates remain incomplete.
