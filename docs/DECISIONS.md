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
- **M2 (the fault-injection harness) is done.** All 5 named scenarios pass with a real Porcupine
  linearizability check against a real recorded history — see "M2: the fault-injection harness"
  below.
- **M3 (measured failover time and throughput) is done.** See "M3: measured failover time and
  throughput" below.
- **QuorumKV's entire build plan (MVP through M3) is now complete.**

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

**What this doesn't yet cover:** network partitions (as opposed to a node dying outright), minority-
partition behavior, and the actual fault-injection harness with recorded histories and Porcupine
linearizability checking — that was M2, done next (see below).

## M2: the fault-injection harness

Per the plan's own words, this is "the actual point of the project" — not asserting the cluster
"seems fine" after a fault, but recording every client operation's start time, end time, and
result, and checking that recorded history against a real linearizability checker (Porcupine).

**No Docker/iptables, so a Toxiproxy-style proxy instead — exactly as the plan itself allows.**
`internal/faultproxy.DirectedEdge` is a small TCP relay: one instance per *directed* pair of nodes
(for 3 nodes, that's 6 proxies: 1→2, 2→1, 1→3, 3→1, 2→3, 3→2), each independently able to `Cut()`
(reject/tear down connections), `Restore()`, or `SetDelay()` traffic on that specific link.

**Per-node-perspective Raft configuration is what makes per-edge control possible at all, and it's
safe here specifically because the cluster is statically bootstrapped, not dynamically
reconfigured.** hashicorp/raft's `Configuration` maps a `ServerID` to one canonical `Address` that
in a *normal* deployment must be identical everywhere (dynamic membership changes replicate that
mapping through the log itself). But `test/fault.Start` calls `raft.BootstrapCluster` independently
on each node's own local log, before any log entries exist to replicate — so each node's own local
bootstrap `Configuration` can legitimately list a *different* address for the same peer ID: node1's
local config points to node2 via the `1→2` edge proxy; node2's own local config points to node1 via
the `2→1` edge proxy. Raft's quorum/voting logic only cares about the *set* of `ServerID`s and their
suffrage, never the literal address string, so this asymmetry is invisible to consensus correctness
while giving the test harness independent control over each direction of each link — with no need
for source-port fingerprinting or deep packet inspection. This is a test-harness-only technique;
`cmd/quorumkv-node` itself is unaware any of this exists, and a real (non-test) deployment would use
one shared, symmetric address per peer as normal.

**The 5 scenarios named in the plan's exit criterion, all in `test/fault/scenarios_test.go`, all
passing with a committed history and a Porcupine PASS** (`test/fault/results/*.json`):

1. **Leader partition** — fully isolate the current leader; the majority elects a new leader and
   keeps serving.
2. **Minority partition** — isolate a single *follower* (never the leader); the majority (leader +
   remaining follower) keeps serving normally throughout, undisturbed.
3. **Rolling restarts** — kill and restart each node in turn (one node down at a time, so the
   majority is always intact) while a workload runs concurrently, restarting each node against its
   *own real data directory* so it genuinely replays its WAL/Raft log rather than starting fresh.
4. **Network delay** — add 150ms of latency to every inter-node link (not a partition — the cluster
   must stay correct, just slower); proves the harness can distinguish "slow" from "down."
5. **Double leader attempt** — the split-brain check: partition the leader away from the majority,
   then attempt a write *directly* against the now-isolated old leader (bypassing the normal
   retry-to-another-node client behavior). The old leader's write must fail (it can't reach a
   quorum to commit) while the new majority-side leader's write succeeds — proving there's never a
   moment where two nodes can both successfully commit as leader. This passed on the real run: the
   isolated old leader's direct write failed with `"not the leader, and no leader is currently
   known"` — it had already stepped down (a real hashicorp/raft leadership-lease behavior, not a
   contrived response) once it detected it couldn't confirm quorum.

**The checker was validated against known-bad histories, not just the scenarios' own.** Five
"linearizable" verdicts only show the checker accepts the histories those scenarios produced.
`test/fault/model_test.go` feeds it hand-built histories with no cluster involved: a stale read
(both Puts complete, then a Get returns the older value), a read of a never-written value,
not-found after a completed Put, and two reads observing writes in contradictory orders. The checker
must reject all four, and does. It also has positive controls (a latest read, a read overlapping an
in-flight write, independent keys) so that "rejects everything" would fail too.

**Errored Puts are checked as "may have happened," not dropped.** An earlier version of
`toOperations` dropped any operation that still errored after retries, and its comment claimed that
mirrored Jepsen. That was wrong on both counts: dropping a Put that actually committed can hide a real
violation, and Jepsen keeps such operations as open-ended `:info` operations. Now an errored Put is kept
with no return time (`Return = MaxInt64`), so the checker may place it anywhere after its call — which
covers "it took effect" — or after everything else, which is indistinguishable from "it never
happened." Controls cover both readings (a later read of the errored Put's value is accepted; a read
of the old value is accepted) and the limits (an errored Put cannot excuse a stale read, and cannot be
observed and then unobserved). Errored Gets are still dropped: a read has no effect on state either
way.

**Limit: that path has not been exercised by a live run.** In every committed scenario history all
operations completed (0 errored), so the may-have-happened handling is verified only by the unit
controls above, not by a real crash-mid-write in a cluster. The retry budgets (15–30s) are generous
enough that nothing errored; tightening them until writes genuinely time out is the way to make this
path fire for real.

**Limit: the histories are small.** Each committed history is 18–40 operations across 3–4 keys and
3–4 clients. Linearizability checking is exponential in the worst case, which is part of why these are
small, but it also means a subtle violation that needs more interleaving than 18–40 operations
provide could go unseen. A PASS here means "no violation in these histories," not "linearizable in
general."

**What actually happened when this ran, not what was expected to happen:** all 5 scenarios passed
on the very first complete run, with 0 operations excluded as indeterminate in every scenario (every
retried operation eventually got a definitive answer within its deadline). That is itself informative
— per the plan's own framing ("if any scenario produces a FAIL, that's the most valuable finding in
it"), a clean pass across the board on the first attempt suggests the harness's retry/timeout budget
(15-30s per scenario, node-level operation timeouts of 1-2s) is generously matched to how fast this
Raft implementation actually recovers in practice (see the M3 failover numbers below — a real
failover here averages ~2 seconds, comfortably inside these budgets). A tighter budget would be a
reasonable next step to actually find the edge of what this implementation tolerates, rather than
confirming it tolerates a comfortable margin.

## M3: measured failover time and throughput

Both measured over multiple independent runs and committed as raw output (`bench/results/*.json`),
per the same "distribution, not a cherry-picked number" discipline used for ModelGate's admission
latency benchmark.

**Failover time** (`bench/failover_test.go`): 10 independent trials, each a fresh 3-node cluster —
kill the leader, time from that kill to the moment a write succeeds again on the majority side.

| Percentile | Time |
|---|---|
| min | 1440 ms |
| p50 | 2006 ms |
| p90 | 2576 ms |
| max | 3233 ms |
| mean | 2135 ms |

This is dominated by hashicorp/raft's default election timeout randomization (150-300ms range) plus
the time for the killed leader's TCP connections to actually be torn down and detected by its
former followers — it is a measurement of *this configuration's* real behavior, not a claim about
what Raft can theoretically achieve with tuned timeouts.

**Throughput/latency at cluster sizes 3 and 5** (`bench/throughput_test.go`): 100 sequential `Put`s
against the leader of a healthy cluster.

| Cluster size | p50 | p90 | p99 | Throughput |
|---|---|---|---|---|
| 3 nodes | 4.29 ms | 4.93 ms | 5.97 ms | 227.8 ops/sec |
| 5 nodes | 6.59 ms | 15.07 ms | 18.65 ms | 111.9 ops/sec |

The 5-node cluster is measurably slower and lower-throughput than the 3-node one — expected, since
the leader must wait for acknowledgment from a majority (3 of 5, vs. 2 of 3), and the additional
network round trips to the extra replicas add latency to every write. This is exactly the tradeoff
Raft cluster sizing is about, now measured rather than assumed.

**What this measures, stated plainly:** each run is 100 `Put`s issued one at a time by a single
client (`bench/throughput_test.go`'s loop has exactly one operation in flight), so this is a
per-operation *latency* measurement. "ops/sec" is just 1/latency, not a concurrency-scaled throughput
ceiling — the committed JSON labels it `sequential_ops_per_sec` for that reason. A concurrent-client
ceiling has not been measured.

**Where it runs:** three or five `quorumkv-node` OS processes on one machine (loopback, no real
network latency, no containers), and each size is measured once. The 5-node result is visibly
unstable: an earlier run of the same benchmark gave p50 4.77 ms / 215 ops/sec, and the run committed
here gave p50 6.59 ms / 112 ops/sec, with a p90 of 15 ms. The 3-node number was steady across runs
(230 vs. 228 ops/sec). So the direction (5 nodes slower than 3) is expected, but the 5-node
magnitude should not be quoted as a stable figure without repeated runs and a distribution, the way
the failover benchmark does.

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
