# Design decisions

## Status as of this commit

- The WAL primitive (`internal/wal`) and its crash-recovery proof are done and verified. This was
  deliberately built and tested *before* anything else, per the build plan: "don't build Raft yet
  ... prove the WAL + recovery works" first, the same "prove the primitive before building on it"
  discipline LedgerLine used for isolation levels.
- **The MVP's stated exit criterion is met**: `test/crashrecovery/crash_test.go` runs 100 trials,
  each starting a real OS subprocess (`cmd/walwriter`) that appends WAL records in a loop, killing
  it abruptly at a random point via `(*os.Process).Kill()`, then replaying the WAL and checking two
  invariants — no acknowledged ("OK n") write ever disappears, and no record beyond what could have
  raced past acknowledgment ever appears. All 100 trials pass with zero violations.
- **The MVP is now fully done.** `internal/store` wraps the WAL with an in-memory map (every
  mutation appends to the WAL, and fsyncs, before the map is updated or the call returns), and
  `internal/kv` + `cmd/quorumkv` expose it as the `Get`/`Put`/`Delete` gRPC service the MVP calls
  for (`proto/kv.proto`). Covered by unit tests (`internal/store`) and a real gRPC integration test
  (`internal/kv/server_test.go`, using `bufconn` — a real gRPC server and client, an in-memory
  connection instead of a real socket).
- **M1 (leader election and log replication) is done.** See "Raft: adopted, not reimplemented"
  and "M1 exit criterion: killing the leader" below.

## Raft: adopted, not reimplemented (and so is the gRPC transport)

`internal/raftnode` wraps `github.com/hashicorp/raft` for consensus and
`github.com/Jille/raft-grpc-transport` for the wire protocol between nodes. Neither Raft itself nor
the RPC transport is implemented from scratch here.

**Why, for Raft:** the build plan says this explicitly — "focus your engineering on the harness,
not reinventing Raft... since 'which parts did I actually build vs. use a library for' is the first
question an interviewer will ask." Raft is widely considered one of the harder consensus protocols
to get exactly right (the paper itself exists partly because Paxos proved too easy to get subtly
wrong in practice); a hand-rolled implementation would mean the later fault-injection and
linearizability work (M2, the plan's stated "actual point" of this project) would partly be testing
*this project's own* Raft bugs rather than the harness's ability to catch real distributed-systems
failure modes. Using a mature, widely-deployed library keeps the two concerns separate.

**Why, for the gRPC transport too, and not just Raft itself:** the plan names "gRPC" as the
transport but hashicorp/raft ships its own custom binary wire protocol (`raft.NetworkTransport`),
not gRPC — getting gRPC specifically meant either hand-writing a `raft.Transport` implementation
(RequestVote/AppendEntries/InstallSnapshot RPCs, encoding, pipelining) or adopting one. The same
reasoning applied to Raft itself applies here: a hand-rolled RPC transport is exactly the kind of
subtle plumbing (message framing, pipelining, backpressure) that's easy to get wrong in a way that
would masquerade as a Raft correctness bug later, and it isn't what this project is meant to
demonstrate. `github.com/Jille/raft-grpc-transport` is a small, focused, already-used-elsewhere
library that does exactly this one thing.

**What was actually built here, in `internal/raftnode`:**
- `FSM` — applies committed Raft log entries to `internal/store.Store`, and implements
  `Snapshot`/`Restore` (a full key/value dump, not incremental — log compaction tuning isn't what
  M1's exit criterion calls for).
- `Node` — bootstraps a `raft.Raft` instance (BoltDB-backed log/stable store, file-backed snapshot
  store, the gRPC transport) and exposes `Put`/`Delete` (through `raft.Apply`, leader-only) and
  `Get` (a direct local read).
- `KVServer` — the gRPC service backing a Raft node: rejects writes (and, per the plan's "start
  simple" choice, reads too) on a non-leader with an error naming the current leader, so a client
  knows where to retry.
- A real bug this surfaced and fixed, not a hypothetical: `Store.Restore` (used when Raft installs
  a snapshot) originally just *appended* the snapshot's entries to the existing WAL. That leaves
  stale pre-snapshot records in place — on the next restart, `Replay` would resurrect a key the
  snapshot no longer has, since nothing ever told the WAL that key was gone. Fixed by adding
  `WAL.Reset`, which atomically replaces the WAL's entire file (write-fsync-rename) rather than
  appending, with a regression test (`internal/wal/reset_test.go`) that plants a stale key, resets
  past it, and confirms it never comes back on replay.

## M1 exit criterion: killing the leader

The plan's M1 test calls for "a real 3-node cluster in Docker Compose, kill the leader, assert a new
leader is elected and the cluster keeps accepting writes." Docker was fully shut down partway
through this session (disk-space pressure was a running theme all through the ModelGate work), so
this is a real 3-node cluster of separate OS **processes** on localhost instead of containers —
`test/cluster/cluster_test.go` builds `cmd/quorumkv-node` once, starts three real subprocesses on
`127.0.0.1:17001-17003`, and:

1. Polls each node's `Put` RPC (the cheapest real leadership probe — only the leader accepts
   writes) until exactly one succeeds, proving real leader election.
2. Writes a key through the leader, confirms it reads back — proving the write actually replicated
   and applied through the FSM, not just that the RPC returned success.
3. **Kills the leader process** (`(*os.Process).Kill()`, the same abrupt-termination mechanism
   proven in the MVP's crash-recovery test) and polls the two survivors until a *different* node
   wins a new election.
4. Writes through the new leader and confirms the write from *before* the kill is still there on
   the new leader — proving the kill didn't just trigger a new election but that state actually
   survived the leader's death.

All of this passed on the first real run: `node2` elected initial leader, killed, `node1` elected
new leader, pre-kill write (`before-kill=v1`) confirmed present on the new leader, and a fresh
post-kill write (`after-kill=v2`) accepted immediately after.

**Why processes instead of containers isn't a lesser test:** Raft's correctness properties (a
majority quorum elects a leader, committed entries survive a leader's death) don't depend on
whether "a node" is a container or a process — both are separate OS-level entities with independent
memory, communicating only over the network, which is exactly the isolation Raft is designed to
tolerate failures across. The same pattern (spawn a real subprocess, kill it abruptly, verify
correctness after) is already how the MVP's crash-recovery proof works, so this reuses an
already-established, already-trusted testing pattern rather than introducing a new one.

**What this doesn't yet cover, honestly:** network partitions (as opposed to a node dying outright),
minority-partition behavior, and the actual fault-injection harness with recorded histories and
Porcupine linearizability checking — that is M2, explicitly, and is "the actual point of the
project" per the plan's own words, not yet started.

## Windows `Kill()` vs. Linux `SIGKILL`, stated honestly

The crash-recovery test's kill mechanism is `(*os.Process).Kill()`, which on this development
machine (Windows) maps to `TerminateProcess`, not a POSIX `SIGKILL`.

**Why this is still a meaningful test:** both mechanisms share the property that matters here — an
abrupt, unrecoverable termination with no chance for the target process to run any further code,
flush any buffer, or execute a deferred cleanup after the signal arrives. The WAL's durability
guarantee (`Append` fsyncs before returning) either holds under that condition or it doesn't,
regardless of which OS API delivers the abrupt stop.

**Why it is not a complete substitute for real crash testing:** neither `TerminateProcess` nor
`SIGKILL` from a sibling process reproduces everything a real hard crash (power loss, kernel panic)
can do — in particular, a real crash can also lose or reorder writes that already left the
process's buffers and are sitting in the OS page cache or a disk controller's write cache, which
`fsync`/`FlushFileBuffers` is supposed to prevent but which this test cannot independently verify
(it trusts the OS/filesystem's fsync implementation is honest). `Append`'s fsync call is the
project's actual durability boundary; this test proves the code above that boundary is correct, not
that every possible storage stack beneath it honors fsync perfectly.

**Revisit if:** this project is later run/tested on Linux, where the same test could additionally
be run with a real `SIGKILL` sent to a genuinely separate process (already true here) and, for even
higher fidelity, inside a VM that can be hard-power-cycled between writes -- a heavier setup than
this test needs at the MVP stage.

## Deletes are tombstones in the WAL, not silently dropped

`Store.Delete` appends an `OpDelete` record to the WAL even when the key doesn't currently exist,
rather than short-circuiting with "nothing to do."

**Why:** this store is a foundation for Raft log replication (M1). A delete is a log entry that
every replica must apply identically regardless of whether *that specific replica* happened to have
the key — a follower that never received the corresponding `Put` (e.g. it joined after a
compaction, or missed an entry that a snapshot later covered) must still be able to replay a
`Delete` for a key it never had without that being treated as an anomaly. Making `Delete` of an
absent key a normal, always-durable no-op now avoids a special case that would otherwise need
reconciling once replication exists.

## The gRPC layer is intentionally free of logic

`internal/kv.Server` only translates `pb.*Request`/`pb.*Response` to/from `internal/store` calls;
it validates that keys aren't empty and nothing else.

**Why:** durability and correctness already live entirely in `internal/wal` and `internal/store`,
both already tested in isolation. Keeping the gRPC layer as thin as possible means there's exactly
one place a bug in the actual storage logic could hide, and the gRPC integration test
(`internal/kv/server_test.go`) only needs to prove the wiring is correct — request in, right
response out — not re-prove durability that's already been proven underneath it.

**Why `bufconn`, not a real TCP socket, for the integration test:** `bufconn` gives a real
`grpc.Server` and a real generated client stub talking over a real (if in-memory) `net.Conn` — the
full gRPC framing, serialization, and service-dispatch path runs for real. The only thing it skips
is an actual OS socket, which isn't what this test is trying to prove (that's an OS/network
concern, not a QuorumKV one) and would only add flakiness (port collisions, firewall prompts) for
no added confidence.

## WAL record framing: length + CRC32, not a fixed-size header alone

Each record is `[4-byte length][4-byte CRC32][op][key][value]`, and `Replay` treats *any* record
that doesn't fully validate (too short, checksum mismatch) as "no more valid records" rather than
an error.

**Why the CRC, not just the length:** a torn write from a kill mid-`Append` almost always leaves a
short/incomplete tail, which the length check alone would catch. The CRC exists for the rarer case
where a write completes structurally (right number of bytes present) but the content is corrupt —
e.g. a partial flush that overlapped with old file content, or genuine disk-level bit rot. Without
it, `Replay` could silently accept and return corrupted data as if it were a valid committed write,
which is a strictly worse failure mode than stopping recovery a record early.

**Why stopping silently, not erroring:** a torn trailing record is the *expected*, correct shape a
real crash leaves behind, not an exceptional condition. Treating it as an error would mean recovery
fails outright after every real crash, defeating the WAL's purpose. `Replay` returning an error is
reserved for cases that indicate a bug (e.g. a fully-CRC-valid record whose internal length prefixes
don't parse), which should never happen if `Append`'s encoding and `Replay`'s decoding stay in sync.

## Raft transport has no TLS yet

`internal/raftnode.New` dials and serves the Raft gRPC transport with `insecure.NewCredentials()` —
plaintext, no encryption or peer authentication between nodes.

**Why this is acceptable for M1's scope:** M1's exit criterion is about consensus correctness
(leader election, replication surviving a leader's death), tested with nodes on `127.0.0.1`. Adding
TLS now would mean generating and managing per-node certificates before there's any real multi-host
deployment to protect, mirroring the same "MVP with one hardcoded key first, real trust policy
later" sequencing ModelGate used for cosign.

**Revisit before any real multi-host deployment:** plaintext Raft traffic between nodes on separate
machines would let anyone on the network path read and potentially forge cluster state — this is a
real, not cosmetic, gap the moment nodes aren't all on localhost.
