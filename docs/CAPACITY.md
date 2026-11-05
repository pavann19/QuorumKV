# Keyspace capacity and recovery contract

Every member uses identical positive `-max-store-bytes` and
`-max-store-entries` settings. Defaults are 268435456 logical bytes and 1000000
keys. Logical bytes count `len(key)+len(value)` for current entries; this is not
an RSS limit. Go map overhead, snapshots, decoding, reservations and Raft buffers
need separate memory budgets. No eviction is performed.

The limits are persisted in `cluster.json`; changing them on restart is refused.
New mutation commands also carry the effective limits. A replica with different
limits fails stop before deciding a mutation; it cannot silently accept a
different capacity decision. Legacy commands without these fields mean the
default profile. Format migration/rollback and snapshot-envelope compatibility
remain separate uncompleted gates.

Leader-side reservations account for positive growth until the Raft future
resolves. RPC cancellation does not release the reservation early. Concurrent
updates to the same key reserve conservatively rather than sharing old-value
credit. Reservations can therefore reject bursts before the final keyspace is
full; they do not predict that an earlier operation will delete or shrink data.
ResourceExhausted/KEYSPACE_FULL reports `outcome=not_applied`, which describes the
current invocation only. Ordinary capacity overload is rejected before append.
Across replay or leader-transition races the deterministic FSM check is the
final guard: a committed over-limit command is an ordered no-op, returns the
capacity error and never appends to the store WAL. It is not a storage fault and
does not terminate the process. Store I/O faults still fail stop.

Startup first checks fatal markers, manifest and store-WAL integrity. Complete
WAL corruption remains fatal and original corrupt bytes are preserved. After
Raft state is established, a healthy redundant store WAL is reset to empty and
the FSM is rebuilt from Raft snapshot plus committed log. This is required:
preloading the latest store WAL and then replaying older commands would evaluate
capacity against the wrong historical state. Raft's durable snapshot/log are the
authority for a member's replay order; store WAL acknowledgements still follow
fsync during normal operation. A nonempty store with no Raft state is refused.

Snapshot restore validates capacity before replacing the WAL/map. Snapshot
capture compacts the complete current state. Tiny-limit tests exercise log-only
recovery, snapshot recovery, overwritten values, capacity rejection, subsequent
delete/write, and equivalent results in two independent FSMs.

Production Store.Open now streams WAL records once with running byte/key
accounting, using OpenReplay; visitor failure preserves the original file. The
legacy Replay API still materializes records for inspection/test consumers.
Persisted Raft membership is checked before healthy WAL reconstruction using an
offline configuration inspection; a drift refusal preserves the original WAL.

Open boundaries: measured startup memory/replay-time budgets, disk watermark,
total process memory limits, reserved
space for compaction, snapshot-format limit binding, explicit format migration,
fault qualification
and actual 24-hour storage measurements. This checkpoint closes none of those
by inference. Never change limits on one member or manually edit its manifest.
