# Byte-bounded replication groups

Status: implementation and maximum-size correctness checks PASS_LOCAL; paired
performance acceptance is NOT PASSED. This is an unreleased working candidate.
See the retained measurement failure below; do not infer P4/P5 completion.

## Transport contract

Default logical MaxAppendEntries is one; eight-entry groups remain opt-in; startup JSON `raft.max_append_entries`
and explicit `-max-append-entries` accept 1..8. The existing 1 MiB value, 4 KiB key
and 4 MiB gRPC send/receive limits remain. Instead of increasing shared ingress
allocation limits or shrinking the client value contract, the sender measures the
actual adopted protobuf size and splits oversized logical append groups into
physical requests below the existing limit. Small groups remain a single RPC.

Every physical request preserves the adopted header, term, leader and commit
index, and links its preceding index/term to the prior piece's last entry. The
entire split plan is validated before any send. Unknown request-schema fields,
unsupported group sizes, invalid split sequences and individual oversized entries
fail closed. No entry, caller buffer or original protobuf message is mutated.
The consensus implementation remains the adopted Raft library.

Unary calls return only after all required pieces succeed, stopping on a
rejection or higher term. Pipelined calls consume every response in their logical
group, preserve the highest term/first rejection and never turn partial success
or a lost tail into a successful logical acknowledgement. Response alignment for
the next logical request is preserved. I/O failure cancels that owned stream;
Raft retains responsibility for retry, quorum and uncertain outcomes.

A per-stream 32 MiB budget bounds credited logical payload waiting for responses.
One sender may additionally hold a preconstructed blocked logical group (at most
32 MiB) and serialization has its own existing physical frame allowance. The
budget is not an RSS cap, a global membership budget or a claim that batching
uses no additional memory. Cancellation wakes blocked senders; queues contain
bounded response-plan metadata rather than duplicate log payloads.

## Correctness evidence

retained local checkpoint (not distributed with source)
records native Linux/ext4 raftnode/labcluster/telemetry checks. Actual plaintext
and mTLS three-node fixtures acknowledge sixteen maximum-size values/keys, keep
one member offline, recover exact values/deletion through log catch-up and verify
linearizable readback without snapshot fallback. The largest received canonical
protobuf append is 3158555 bytes, below the unchanged 4194304-byte limit.

Valid-peer-certificate raw oversized unary and pipeline messages are rejected
with ResourceExhausted without changing target term/log. Unit/race checks cover
complete byte/link preservation, validation before send, partial errors, first
rejection/highest-term handling, lost-tail refusal, next-response alignment and
byte-budget backpressure/cancellation. Full final-candidate regression is pending.

## Performance finding retained

The initial paired measurement fixture was rejected by the original history
coverage oracle because its deleted seed lacked a recorded client read. That
read was added; the oracle was not changed. Both failed receipts are preserved.

`readiness-write-measure-linux-20261105-02` completes all 24 three-node paired
runs with original attempt-level linearizability/coverage checks and exact
replica catch-up. Throughput, write-tail and catch-up gates improve, but the
mTLS/1 KiB profile fails the unchanged mixed-read p99 gate (ratio 1.7252).
The aggregate remains FAIL; this is not an accepted performance result.

Read-phase counters were added to locate target/quorum/application/lookup delay.
They are fixed-cardinality, atomic, payload-free observations, never an acceptance
or consistency oracle. A focused observation-only run attributes most server
read delay to quorum confirmation and does not reproduce the earlier read-tail
regression. It does not override the failed acceptance run.

Further diagnosis isolates each profile in a fresh helper process, fsyncs retained
history/measurement artifacts outside timing before starting the next profile,
and observes both cgroup v1/v2 CPU statistics plus GC/resource counters. This
strengthens run independence; it does not relax performance gates or discard
prior results. No chain-verification, authorization, durability or fence check
was weakened in response to a noisy result.

## Diagnostic outcome and default decision

Fresh-process diagnostic 05 uses 2048 writes and 1024 timed reads per run with
25 ms read-aware write grace. It retains exact histories/state, but mTLS/1 KiB
read p99 medians are 14.093 ms for one-entry groups and 19.661 ms for eight-entry
groups. Its runner PASS denotes functional checks only; the result is explicitly
OBSERVATIONS_ONLY and does not close the performance gate. Earlier diagnostics
and failed acceptance receipts remain intact.

The read grace experiment did not establish a read-tail improvement. Default
MaxAppendEntries is restored to one and default write_read_grace_ms to zero.
The bounded byte-splitting implementation and optional 1..8 setting remain for
further qualification; grace may be explicitly set to 0..25 ms. No performance
gate, quorum check, sync, timeout classification or oracle was relaxed. Full
combined-profile performance acceptance remains open.

The strengthened acceptance run 03 hit a whole-profile fixture timeout during
control-member replay. The 60 s fixture budget was too short for the expanded
2048-write workload; a 120 s helper budget now records incomplete catch-up
progress explicitly. Per-RPC deadlines and performance thresholds are unchanged.
That failed run remains incomplete evidence, not a data-loss finding or a PASS.
