# Correctness and reliability review — November 2026

QuorumKV has a tested local correctness foundation. Production release gates
remain open. This source review and its bounded tests do not establish a
production deployment, host resilience or a service availability guarantee.

## Runtime fixes

| Finding | Previous behavior | Resulting behavior and regressions |
| --- | --- | --- |
| Complete WAL corruption could silently discard acknowledged state | Replay stopped at an invalid CRC/body; reopening could truncate that record and every later valid record. | [Recovery](../internal/wal/wal.go) now returns `wal.ErrCorrupt` without changing the file. [Regressions](../internal/wal/corruption_test.go) cover middle/last-record checksums, unknown operations, malformed structure and byte-for-byte evidence preservation. |
| Readiness bypassed the node's outstanding-request limit | Each readiness probe constructed a new KV handler and a fresh admission counter. | [Handler construction](../internal/raftnode/kv_server.go) is now once per node. gRPC and readiness share the 64-future ceiling, including submitted operations whose RPC timed out. [Regressions](../internal/raftnode/admission_test.go) cover saturation, independent nodes and concurrent first construction. |

The prior checksum test now requires an error rather than successful recovery of
only a prefix. This strengthens acceptance. The WAL wire format, fsync policy,
Raft quorum, read barriers and uncertain-write semantics remain unchanged.

## Validation and provenance

The fixes were first reproduced and tested in an older dirty local checkout.
Before publication, current `main` was reconciled: it already contained pinned
actions, Python CI, newer Windows checkpoint handling and eight checkpoint tests.
Those changes are preserved; older local versions are not replacements for them.

The portable [check runner](../scripts/run_checks.py) records the current
candidate packages, command results, source/workflow/service hashes and source
drift. It excludes archived Go source under results directories, preserving that
evidence without executing mixed historical/current packages. Race and finite
measurements are explicit options; the runner never starts an endurance test.
See [testing](TESTING.md) for commands and [verification](VERIFICATION.md) for
the dated historical baseline. Exact new hosted results belong to this branch's
GitHub Actions revision; historical green receipts do not certify a later commit.

Before publication, `python -B scripts/run_checks.py --race` passed all 64
top-level Go tests (including instrumented child node builds), vet and all 22
Python regressions on native Windows, with no source/config drift or race reports.
Workflow linting and staged-tree/published-ancestry secret scans also passed.
Hosted CI remains a separate check of the pushed revision.

## Failure and compatibility contract

Complete checksum or structural corruption now blocks startup. Preserve the
entire member directory, investigate the storage fault and follow the isolated
[restore procedure](TRANSPORT_AND_RECOVERY.md). Do not truncate data, rewrite
checksums or force quorum. Downgrading an affected file to an implementation that
silently truncates corruption is unsafe.

Healthy files and incomplete append tails retain their existing recovery format.
The length field itself is not checksummed: length corruption resembling an
incomplete append cannot always be distinguished from a crash by this format.
This fix is not proof of arbitrary disk-corruption recovery.

A mutation timeout is still uncertain. History IDs are observations rather than
server deduplication; no exactly-once or transaction API is introduced. Raft peers
remain trusted cluster members, not Byzantine participants. The shared admission
ceiling is not a total connection, keyspace, memory or disk quota.

## Open production gates

- The original 24-hour attempt failed after about eight hours at supervisor
  checkpoint publication. A fresh authorized 86,400-second run from zero is
  required; passing finite tests does not complete that gate.
- Prior delayed-network retry exhaustion and an intermittent Windows worker
  experiment failure remain retained findings. A passing rerun does not explain
  their causes.
- WAL creation/reset does not fsync the parent directory. Target filesystem,
  power-loss and full-volume recovery need qualification.
- Distinct host/failure domains, protected backups, measured restore RTO/RPO,
  upgrade/rollback and monitored operation remain unverified.
- Workload-specific connection, keyspace, snapshot-memory and disk limits need
  qualification. Short measurements do not establish production capacity.
- Live model quality, trust-root rollover and live revocation are outside the
  verified implementation scope. Diagnostics cannot change quorum or trust.
