# Authenticated transport and observed recovery

## Checkpoint repair status

The reviewed candidate repairs Windows checkpoint publication when readers hold files open and includes regression checks. The earlier WinError 5 soak remains a failed historical result; there is no replacement 24-hour PASS. See [verification status](VERIFICATION.md) for the repaired source revision and hosted runs. Preserve old checkpoint evidence when performing a separately authorized rerun.

HashiCorp Raft and Jille's gRPC transport provide consensus/transport machinery.
This project supplies its KV FSM, WAL/store, identity policy, observation harness
and recovery controls. An accepted observed history is not a proof of all executions.

The node supports -identity with operator-owned JSON: certificate/key/CA paths,
self URI, exact peer-address-to-URI bindings and read/write client allowlists.
See internal/identity.Config. TLS 1.3 verifies certificate chains, EKUs and exactly
one configured URI SAN. Peer methods require peer identity; read-only clients cannot
mutate KV state or call Raft. Unauthenticated mode accepts literal loopback peer
addresses only. The legacy single-node default is now 127.0.0.1:7070; non-loopback
plaintext and hostname aliases are rejected. Existing remote plaintext users must
provision identities before upgrading. The protobuf schema is unchanged.

Leaf files reload on new handshakes. Use a matching key/certificate pair with the
same URI under the pinned CA; mismatched pairs fail closed. Existing connections
revalidate certificate validity at authorization, but do not reload roots or
allowlists. Live CA rollover, CRL/OCSP and dynamic membership are not implemented.
Keep separate member data directories, protect private keys and retain a majority
during upgrade. Roll back one member at a time with a previously tested binary,
identity configuration and compatible complete member data; never force quorum.

Keys are 1..4096 bytes, values at most 1 MiB, messages at most 4 MiB, and a KV server
has at most 64 outstanding futures. KV wait/enqueue budgets are five seconds. A caller
timeout does not cancel an already submitted mutation: retain it as uncertain.
Request slots remain occupied until futures resolve. Append batches are capped at
one entry to keep maximum values within transport bounds; throughput tradeoffs
require separate measurements. Fsync policy and existing WAL format are retained.

The optional -admin-addr listener accepts literal loopback only. /live reports process
health; /ready requires leadership and a quorum barrier, so followers return 503.
/metrics uses fixed labels and in-memory counters, including WAL sync/failure counts.
It is not an external collector dependency or a durability oracle. /effective-config
reports configuration identity and ceilings without private material.

Histories now retain each RPC attempt, logical operation/retry IDs, acknowledgements,
pre-submission rejections, failed reads and uncertain mutations. JSON schema is v2;
logical event ordering retains the legacy CallNS/ReturnNS field names but is not a
wall-clock latency measurement. Checker consumers must understand attempt semantics.
Known-bad controls reject disjoint write/read workloads, lost acknowledgements and
invented values. Five original proxy scenarios use plaintext; authenticated process
failover/rotation tests are separate evidence, not a claim all faults ran under TLS.

Run bounded checks in a copied checkout with Go from go.mod:

```sh
go build ./...
go vet ./...
go test ./... -p=1 -count=1 -timeout=300s
```

Tests allocate loopback fixtures and temporary stores; never target existing member
directories. QUORUMKV_RESULTS_DIR can select a new synthetic evidence directory.
Generated histories, keys, queues, WALs and captures remain private. Benchmark-tagged
measurements are opt-in and do not run as ordinary tests.

Back up a stopped, caught-up member's entire directory, including Raft Bolt state,
snapshots and KV WAL. Restore into an isolated network with original IDs and explicit
identities; verify retained values, deletions, exact keyspace and a fresh quorum write
before using the backup. A WAL-only copy is not a replicated backup protocol. Existing
WAL reset does not fsync the parent directory: power-loss rename durability is unproven.
The systemd unit is a reviewable template, not a deployed service or host-HA result.

The retained 24h attempt failed after about 8h05m at supervisor checkpoint publication
with WinError 5. Its precise Windows contention cause is unproven. No cumulative
duration credit, completed endurance, production capacity or host resilience is claimed.
The earlier intermittent delayed-network retry-exhaustion failure remains undiagnosed.
