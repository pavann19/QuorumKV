# Durable write batching

Status: H3/D5 foundation implemented and locally verified; end-to-end batching
performance acceptance is open. The recorded foundation checkpoint used a networked append ceiling of one.
The working H4 candidate supports byte-bounded groups up to eight; its performance
acceptance is still open (see REPLICATION_BATCHING.md). Enabling a buffering flag alone is
not evidence of networked write aggregation or higher throughput.

## Persistence and publication

WAL.AppendBatch validates an entire bounded input before file effects, writes
legacy CRC-framed records in one buffer and fsyncs once. Limits are 16 records
and 32 MiB of framed bytes, with the existing 16 MiB record-body ceiling.
Append delegates to the same path for one record. Empty accepted groups do not
sync; rejected input does not poison an otherwise healthy WAL.

No record is acknowledged before the shared sync succeeds. Short writes and
sync errors poison the WAL; no later append can be acknowledged. A failed group
may leave a complete unacknowledged prefix. Recovery retains valid uncertain
records and repairs only incomplete tails. This is not a new atomic multi-key
transaction format or a guarantee that failed operations are absent.

The store uses a mutation mutex to serialize append, reset, compaction and close.
Its map lock covers bounded validation/copying and publication, not disk I/O.
Get/Dump can observe the previous durable state while a write/reset is pending.
A batch's map changes publish only after sync. Snapshot compaction still captures
and resets under mutation serialization, preserving later acknowledged appends.

Capacity is evaluated in command order using a bounded overlay, including
repeated keys, overwrites, deletes and capacity no-ops. Every input has a matching
response; only accepted mutations enter the WAL. Temporary publication accounting
and key fences prevent reservations borrowing unfinished overwrite/deletion
credit. These conservative counters can overlap existing RPC reservations;
they are admission bounds, not measured allocated memory or an RSS guarantee.

## Raft integration

FSM implements the adopted raft.BatchingFSM contract. Configuration entries
receive nil responses without KV mutation; command responses retain input order.
Large library groups split at WAL count/byte bounds, and all chunks finish before
Raft receives the result slice and acknowledges its futures. Completed-command
notifications publish only after durable store publication, preserving the quorum
read fence. Committed decode/storage/limit failures retain fatal-stop behavior.

Default Raft configuration enables BatchApplyCh; JSON `raft.batch_apply` controls
this startup buffer and effective configuration reports its actual setting.
MaxAppendEntries was one in the recorded foundation network profile; current
H4 work and its retained failed performance gate are described separately in
REPLICATION_BATCHING.md. The explicit eight-entry
in-memory test profile exercises genuine adopted Raft buffering/commit/apply
aggregation without claiming gRPC message-size qualification.

The per-write store WAL is retained. Removing it would change the independently
verified WAL durability/corruption/recovery contract and requires a separate
architecture and recovery comparison. No sync has been disabled to improve a
measurement. Both the adopted Raft persistence and custom WAL remain durable
before acknowledgement.

## Local evidence

The source-matched checkpoint is
retained local checkpoint (not distributed with source).
Native Linux/ext4 execution passes all 12 selected package scopes (143 top-level
pass entries, no failed/skipped tests), including original fault/history oracles,
mTLS leader-kill/rotation, snapshot install/restart, corruption restore and CLI
migration/configuration. Scoped Windows race/config/compile/vet checks pass too.

A full Windows regression with race instrumentation passes 150 top-level tests.
New owned temporary fixtures use D: with observed headroom below the unchanged
95-percent lab watermark; production remains 90 percent. Five packages have no
test files and are recorded separately from actual test skips (zero). Process
child executable builds retain their original ordinary build behavior; this is
not a claim that those separate executables were race-instrumented.

New checks prove one write/one sync before any batch acknowledgement, invalid
input with no file effect, short-write/ENOSPC uncertainty and poisoned continuation,
ordered capacity results and legacy replay, owned batch buffers, nonblocking old
state reads while append/reset is gated, bounded byte/count splitting, fatal
prevalidation and a real in-memory Raft fixture with nine acknowledged commands
recovered after two WAL syncs. Ten fresh subprocess trials retain every member
of an acknowledged batch and its deletion after abrupt process termination.
These are process-kill tests, not physical power-loss tests.

H3/D5 remains PARTIAL pending networked aggregation, paired write throughput and
mixed-read tail measurements with H4. H9 WAL/command format migration, broader
M6 memory sizing, real endurance and target-filesystem physical durability gates
remain open. No production, hosted CI, multi-host, deployment or publication claim
follows from these local tests.
