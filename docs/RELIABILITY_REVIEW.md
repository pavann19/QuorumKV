# Correctness and reliability review — 3 November 2026

## Assessment

QuorumKV has a credible local correctness foundation, but has not passed the
acceptance gates for a production release. There is no single certification
called “industry standard” for a replicated KV service. This review uses explicit
invariants, failure behavior and operational evidence to assess the implementation.
It is a source review and local test exercise, not an independent security audit.

HashiCorp Raft supplies consensus; this review concentrates on the integration,
custom WAL/store, authentication, resource bounds and retained execution history.

## Reproduced defects and changes

| Severity | Trigger and prior behavior | Change and regression |
| --- | --- | --- |
| High | A complete WAL record had a bad CRC or invalid body. Replay accepted only its prefix; reopening could truncate the bad record and every later acknowledged write. | Complete corruption now returns `wal.ErrCorrupt`. Both replay and reopen fail, leaving all original bytes intact. Tests cover middle/last-record damage, unknown operations and invalid internal lengths. |
| Medium | HTTP readiness constructed a new KV handler on each probe, resetting its outstanding-future counter and bypassing the node's 64-request ceiling. | Each node constructs its KV handler once. Readiness and gRPC share admission, including pending operations whose RPCs timed out. Tests cover saturation, independent nodes and concurrent construction. |
| Medium | This working checkout's CI did not run the Python diagnostics/checkpoint tests. Other publication receipts describe a different candidate. | Added Linux/Windows Python CI with an explicit job timeout. Its hosted result remains unverified until this exact candidate runs. |
| Medium | Workflow dependencies used movable action tags. | Both workflows now use full upstream commit references, resolved from the existing major-version tags during this review. Updating those references requires review. |
| Medium | Recursive local validation discovered old Go source under preserved evidence, compiling mixed historical/current packages and failing outside the candidate. | The runner now records candidate and excluded artifact packages explicitly. Original artifacts remain untouched. Workflow and service files are included in source/config drift hashes. |
| Medium | This checkout had the repaired checkpoint publisher but no checkpoint regression tests, despite historical publication receipts describing such tests elsewhere. | Added six regressions, including a real Windows handle denying file replacement, bounded transient/exhausted denial, concurrent publication/read integrity, Unicode and invalid/oversized data. Linux explicitly skips the Windows handle fixture. |

The original failing regressions are retained separately from the fixed-candidate
results. The old checksum test now requires an error instead of silent data loss;
this strengthens recovery acceptance. No checker, workload deadline, fsync policy
or uncertainty rule was relaxed. WAL encoding remains unchanged.

Review the [WAL change](../internal/wal/wal.go),
[corruption regressions](../internal/wal/corruption_test.go),
[shared handler](../internal/raftnode/kv_server.go),
[admission regressions](../internal/raftnode/admission_test.go),
[checkpoint regressions](../diagnostics/test_checkpoint.py),
[Python workflow](../.github/workflows/diagnostics.yml), and
[candidate check runner](../scripts/run_checks.py).

## Correctness contract

| Area | Implementation and evidence boundary |
| --- | --- |
| Quorum and reads | Put/Delete use Raft commitment and FSM application. Get commits and awaits a barrier before reading. Local `Node.Get` alone is explicitly a potentially stale lookup. Isolation regressions must keep rejecting old-leader reads/writes. |
| Write outcomes | A timeout after submission is uncertain. Request IDs in histories are observations, not server deduplication. Retrying an old Put after another writer's Put can overwrite that writer; no exactly-once, transaction or compare-and-swap interface is promised. |
| Local persistence | Acknowledgement follows WAL append and fsync. I/O failure poisons further writes; a failed committed FSM mutation stops the process. Process termination tests do not prove host power-loss durability. |
| Recovery | Incomplete append tails are repaired before new writes. Complete checksum/structural corruption now fails closed. Snapshot tests verify values, deletions, stale-key removal and post-restore writes. |
| Identity | TLS 1.3, CA validation, URI-to-peer binding, separate client roles and method authorization. Peers are trusted cluster members; this is not Byzantine consensus. Leaf rotation is tested; live revocation/trust-root rollover is not implemented. |
| Overload | Replicated requests have key/value/message bounds and a shared 64-future admission ceiling. This is not a total connection, keyspace, disk or tenant quota. Operators still need OS/network/storage limits. |
| Diagnostics | Persistent leased jobs, bounded retries and review states; worker/model failure cannot bypass consensus or authorization. Live model quality is unverified. |
| Operations | Loopback admin health/metrics and a service template exist. Only the leader is ready. Do not use leader readiness as liveness or restart healthy followers because they return 503. |

## Corruption response and compatibility

On `wal: corrupt record`, stop the affected member and preserve its entire data
directory. Investigate disk/filesystem health and use the existing isolated
[backup/restore procedure](OPERATIONS.md). Do not edit checksums, truncate the
file, delete a member directory, or force quorum as an automatic repair.

Availability is deliberately sacrificed when acknowledged state is uncertain.
A previously silently “recoverable” complete corrupt file now blocks startup.
Healthy files and genuinely incomplete append tails retain their existing format
and recovery behavior. Downgrading to a build that silently truncates corruption
is unsafe for affected data.

The existing length field is not itself checksummed. Arbitrary length corruption
that resembles an incomplete final append cannot always be distinguished from a
crash using this format. Fail-closed complete-record detection is not a general
disk-corruption proof. A versioned/checksummed framing format and migration plan
would be a separate storage-format change.

## Release gates still open

1. **Endurance:** the original 24-hour run failed after roughly eight hours at a
   supervisor checkpoint. Preserve that failure. A fresh authorized 86,400-second
   run from zero, with the declared workload and resource gates, is still required.
2. **Intermittent failures:** delayed-network retry exhaustion and a Windows
   diagnostics experiment failure were recorded previously. Passing a later run
   does not establish their root causes or resolve them.
3. **Storage/host qualification:** parent-directory fsync on WAL creation/reset
   is not implemented. Power-loss, target filesystem behavior and full-volume
   recovery need qualification before any machine-crash durability claim.
4. **Deployment:** distinct host/failure domains, externally protected backups,
   measured restore RTO/RPO, upgrade/rollback and monitored operation remain
   unverified. No monthly availability or production capacity claim is supported.
5. **Resource/scale qualification:** keyspace/snapshot memory, local WAL disk
   growth, total connections and operator quotas need workload-specific limits.
   Maximum request size and short benchmarks do not establish safe total capacity.
6. **Exact candidate CI:** historical published CI cannot certify this dirty
   local tree. Run the new workflows on the reviewed candidate before release.

These gates remain explicit rather than being replaced by a readiness badge or
test count. No paid infrastructure, public change or long soak is part of this
bounded review.

## Reproduction and review references

Run `python -B scripts/run_checks.py` for source-pinned functional,
vet, Python, targeted race and measurement receipts. Child-process race reports
are checked separately. See the [current case study evidence](PORTFOLIO_EVIDENCE.md)
for the exact review receipt and outcome; old receipts remain historical.

Useful primary references for the review criteria:

- [HashiCorp Raft API](https://github.com/hashicorp/raft/blob/v1.7.1/api.go):
  commitment, application barriers and enqueue-timeout semantics.
- [etcd corruption handling](https://etcd.io/docs/v3.6/op-guide/data_corruption/):
  a mature KV service detects corruption and prevents divergent state from
  silently serving. QuorumKV does not implement etcd's cross-member hash checks.

## Current local validation

The final source-pinned local check manifest passed on 3 November 2026:

| Check | Result | Boundary |
| --- | --- | --- |
| Functional | PASS: 64 top-level Go tests; zero failed/skipped tests | 18 candidate packages; 14 artifact packages explicitly excluded |
| Static checks | PASS: `go vet` | Current candidate packages |
| Diagnostics/checkpoint | PASS: 20 tests | Native Windows; includes the actual held-reader fixture |
| Race | PASS: 29 top-level tests; no race report files | Targeted identity/Raft/lab/storage/telemetry/secure-process packages; child node instrumented |
| Measurements | PASS: four top-level checks | Short local samples; no baseline improvement or capacity claim |
| Workflow lint | PASS: actionlint v1.7.7 | Shellcheck unavailable; hosted execution unverified |
| Source/config drift | PASS | Exact hashes unchanged across the final checks |
| Authenticated smoke | PASS: 60.006 seconds of workload; 384 recorded RPC attempts, six diagnostics jobs and one maintenance/recovery cycle | Three in-process members, separate stores on one host; supervisor elapsed 61.532 seconds; deterministic offline diagnostics |

The first broad run remains FAIL: it included archived partial source and the
runner was subsequently corrected. Its functional candidate results are not used
to substitute for the final source-stable rerun. The original pre-fix regression
failures are also retained. Exact receipts are referenced by the continuation
state. Hosted CI, Linux execution of the new checkpoint tests, the complete
24-hour gate and production operation remain unverified for this candidate.
