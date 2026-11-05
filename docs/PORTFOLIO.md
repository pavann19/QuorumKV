# QuorumKV: Backend / Platform case study

## Problem and result

A replicated service must distinguish a rejected write from a committed write
whose reply disappeared. QuorumKV makes that distinction observable and tests
recovery against what clients actually saw. The result is a Go KV service with
custom local durability, Raft integration, authenticated RPC boundaries and a
fault harness that preserves uncertainty rather than reporting false success.

## My contribution

I built the WAL/store, FSM and service adapters, identity boundaries, read fences,
fault proxies, attempt recorder and validation workloads. HashiCorp Raft provides
leader election, log replication and quorum commitment; Jille provides the peer
transport and BoltDB its durable log/stable storage. I integrated these libraries
rather than implementing consensus from scratch.

## Three resume bullets

- Preserved acknowledged state through 100 process-crash recovery trials by
  building a CRC-framed, sync-before-acknowledgement WAL and verifying each
  recovered prefix against acknowledgements observed before termination.
- Verified exact replica recovery after forced snapshot installation by checking
  48 retained keys, 16 deletions and stale-key removal, then restarting the
  replica again and confirming a new replicated write.
- Made timeout and failover behavior auditable by recording individual RPC
  attempts, retaining uncertain mutations in Porcupine histories, and testing
  an applied write with a lost reply alongside authenticated leader-kill recovery.

These are bounded local validation claims. The fresh run and source references
are in [PORTFOLIO_EVIDENCE.md](PORTFOLIO_EVIDENCE.md). The 100 trials are iterations
within one test. Avoid substituting total test counts for engineering impact.

## Interview walkthrough

**Start with the lost reply.** A server can commit and apply a Put while the
client receives an error. The recorder keeps that attempt uncertain; a retry is
another attempt, not proof of exactly-once processing. The checker considers
whether the uncertain mutation took effect. Known-bad histories ensure that
uncertainty cannot excuse a stale read after a later observation.

**Explain the durability boundary.** WAL sync completes before acknowledgement.
An incomplete trailing record is repaired before new appends; complete checksum
corruption refuses recovery. A process termination can leave an extra durable
record whose acknowledgement the parent did not observe. The test allows that
window without inventing an acknowledged result. Hardware power loss remains
outside this experiment.

**Explain reads and fail-stop behavior.** Quorum verification must be followed by
a completed-application fence, not merely Raft's dispatched/applied counter. The
current default avoids adding a log entry for each read. If storage fails while
applying a committed mutation, returning an error cannot roll the commit back;
the member stops rather than continuing with divergent state.

**Explain snapshots through exact state.** The test isolates a lagging member,
forces leader compaction, installs a snapshot and compares values and deletions.
A planted stale key must disappear. Another restart tests persisted restoration,
then a fresh write proves that the recovered member still replicates.

## A useful negative result

Larger replication groups improved some throughput observations but failed the
read-tail acceptance gate. The default remains one entry and no write-read grace.
This is an example of rejecting an optimization with a real trade-off; it is not
a resume speedup claim. Historical short localhost throughput measurements do
not establish sustained capacity or a causal performance improvement.

## Scope to present

Lead with durable state, failure semantics and reproducible validation. Identity,
bounded admission, conservative retries and format-0/1 migration provide further
discussion depth. Keep the separate diagnostics worker secondary; runtime model
output is outside consensus and policy authority.

Receipt-aware command/snapshot experiments remain in the development workspace
and are excluded from this candidate and its resume claims. Static membership, no verified host HA,
no successful 24-hour run, no physical power-loss validation and no production
operation remain explicit limitations. Historical hosted CI is not current-candidate CI.

Run the [short walkthrough](PORTFOLIO_DEMO.md) before an interview. Use the
[ownership boundary](RAFT_BOUNDARY.md) and [claim map](CLAIM_MAP.md) to answer
which behavior comes from project code, dependencies and direct observation.
