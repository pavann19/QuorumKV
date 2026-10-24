# Raft Ownership Boundary

QuorumKV uses HashiCorp Raft as a consensus library. It does not implement the
Raft protocol from scratch.

## Provided by HashiCorp Raft

`github.com/hashicorp/raft` provides:

- Leader election and term management.
- Log replication and quorum commit rules.
- The `raft.Apply` API used by the leader to append commands and wait for
  commitment.
- Follower, candidate, and leader state transitions.
- Snapshot installation and log compaction hooks through the `raft.FSM`
  interface.
- Cluster membership/configuration types, including `ServerID`,
  `ServerAddress`, and `Configuration`.
- The core `raft.Raft` runtime used by every replicated node.

`github.com/hashicorp/raft-boltdb/v2` provides:

- The durable BoltDB-backed Raft log store.
- The durable BoltDB-backed Raft stable store.

## Provided by Jille Raft gRPC Transport

`github.com/Jille/raft-grpc-transport` provides:

- The `raft.Transport` implementation used for Raft peer-to-peer RPCs.
- gRPC service registration for Raft's internal consensus traffic.
- Dialing/framing behavior for RequestVote, AppendEntries, and InstallSnapshot
  traffic between Raft nodes.

QuorumKV configures this transport with plaintext localhost gRPC for the current
demo/test scope. TLS and multi-host trust policy are intentionally not built yet.

## Written in QuorumKV

QuorumKV provides:

- `internal/wal`: append-only, fsync'd, CRC-checked write-ahead log records.
- `internal/store`: the key/value state machine backed by the WAL.
- `internal/kv`: the single-node gRPC service over `internal/store`.
- `internal/raftnode.FSM`: the adapter that applies committed Raft log entries
  to `internal/store`, plus snapshot/restore behavior.
- `internal/raftnode.Node`: node bootstrap code that wires HashiCorp Raft,
  BoltDB stores, snapshots, and the gRPC transport together.
- `internal/raftnode.KVServer`: the client-facing replicated KV gRPC service,
  including leader-only writes, quorum-committed barriers before reads, and
  retry hints. HashiCorp Raft provides the barrier mechanism; QuorumKV chooses
  to invoke it and waits before accessing its local store.
- `internal/faultproxy`: per-directed-edge TCP proxy used by the fault-injection
  harness.
- `test/cluster`: real multi-process leader-election and leader-kill tests.
- `test/fault`: fault scenarios, operation history recording, and Porcupine
  linearizability checking.
- `bench`: opt-in measurement code for failover time and sequential single-client
  write latency.

## Interview-Safe Summary

HashiCorp Raft decides who the leader is, replicates log entries, commits them
by quorum, and drives snapshot hooks. QuorumKV supplies the state machine, the
durability layer underneath it, the client-facing service, the process/fault
harness, the linearizability checks, and the measurement scripts.

## Failure Semantics

On reopen, the WAL truncates an incomplete suffix to its last validated record
and syncs that repair before allowing new appends. Records are limited to 16 MiB;
oversized on-disk lengths fail recovery rather than allocating arbitrary memory.
An I/O error during append or reset makes that WAL instance reject later writes.
The existing checksum recovery policy treats a bad suffix as a torn append;
it does not promise recovery from arbitrary disk corruption or power loss.

A replicated Get commits a Raft barrier and waits for the local FSM before
reading. This adds quorum traffic and log entries to reads. An isolated leader
cannot acknowledge a read merely because its local role still says leader.

If applying an already committed command fails in local storage, the FSM panics,
stopping the process. Raft's FSM response is a caller result, not a rollback or
replica-repair mechanism. Restart requires working storage; persistent failures
require operator intervention. This favors consistency over availability.
