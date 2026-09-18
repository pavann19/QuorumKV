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
- Raft, replication, and everything from M1 onward are out of scope for this commit, per the plan's
  own sequencing (Raft comes in M1, after MVP is proven).

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
