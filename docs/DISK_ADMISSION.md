# Disk admission contract

Production defaults reject new client mutations at **90% used** or when available
space is below **64 MiB**. Configure `-disk-watermark-percent` (1..99) and
`-min-disk-free-bytes` (positive) for the member's data filesystem. These local
admission settings are independent of deterministic keyspace capacity limits.

Windows uses GetDiskFreeSpaceEx and Unix targets use Statfs, with available
space for the calling account. Probe failures or invalid observations fail
closed with Unavailable/DISK_STATUS_UNKNOWN. Observed pressure returns
ResourceExhausted/DISK_PRESSURE. Both carry `outcome=not_submitted`; the check runs
before reservations and Raft append. Put and Delete are both WAL mutations and
are both refused. Reads remain available under the existing read contract.

This is a local client-admission hint, not a replicated decision. A follower
never skips committed work because its disk observation differs from the leader.
Replication/apply still follows the ordered log; actual storage failure retains
the fail-stop/FATAL behavior. A preflight cannot reserve physical blocks or
prevent another process from filling the disk after observation. Compaction,
Raft log, snapshots, startup reconstruction and backups still require separate
space budgets; this check does not prove ENOSPC prevention.

Metrics include disk status-known/degraded flags and available/total bytes.
Unknown observations export known=0 and degraded=1, so zero byte gauges must not
be read as a measured empty disk. Effective configuration includes the actual
watermark and free-byte reserve. The existing readiness semantics are unchanged
and remain a separate review finding.

## This host and disposable test profile

Read-only preflight on 4 November 2026 found C: approximately 6.6% free and D:
approximately 1.6% free. Both exceed the production used-space watermark, which
correctly rejected early integration writes. No files were deleted or disk
capacity changed to hide this result.

Disposable fixture constructors/process harnesses now explicitly use a **95%
lab watermark** and the same 64 MiB minimum. Their temporary member directories
are on C:. This lets bounded functional tests run with measured available space;
it is not a qualification of the 90% production profile. The general watermark
boundary tests still verify rejection at 90%, below-reserve rejection, overflow
safety, invalid observation refusal and the real filesystem probe. The injected
low-space node test verifies zero new Raft entries/reservations, no forbidden
mutation, explicit error outcomes and recovery when headroom returns.

Do not start the production-profile soak on the current constrained filesystems.
An operator must separately provide a suitable target filesystem before that
qualification gate can pass. Choosing a lab profile does not close the disk-full,
power-loss, resource-ceiling or 24-hour acceptance requirements.
