# Peer cluster binding

All peer RPCs now require exactly one `quorumkv-cluster-id` matching the local
persisted operator cluster ID and one `quorumkv-peer-protocol` with value `2`, plus exactly one `quorumkv-snapshot-format` matching
the local persisted snapshot format (`0` or `1`).
Missing, duplicate, mismatched or unsupported values return PermissionDenied
before the adopted transport handles a message. Cluster IDs must contain 1..128
ASCII letters/digits/dot/underscore/hyphen; invalid IDs refuse before data-directory
creation.

The Node's outgoing transport interceptors replace any caller-provided binding
with its own configured identity. Node.RegisterOn wraps the adopted service
registration itself, covering all unary methods plus AppendEntriesPipeline and
InstallSnapshot. A caller cannot accidentally omit this gate by forgetting to
supply a separate server option. The global adopted service descriptor is copied,
not mutated; existing TLS identity and method-authorization interceptors continue
to run.

Cluster metadata is an additional compatibility/accidental-cross-cluster fence,
not an independent authentication credential. Transport encryption, the issuing
CA, exact member URI/address bindings and authorization still define peer trust.
A deliberately compromised authorized peer can lie about metadata and disrupt
Raft; this service is not Byzantine tolerant. Plaintext remains disposable
literal-loopback lab scope only.

This wire protocol intentionally refuses peers that omit the binding, including
older unmodified node executables. There is no silent legacy/plaintext fallback.
A rolling mixed-version upgrade from an unbound release is not qualified by this
change; it requires its own compatibility/upgrade runbook and isolated drill.
Never assume an existing deployment can be upgraded one node at a time merely
because a local all-new-candidate cluster passes.

Tests use a valid peer certificate against a real mTLS three-node cluster, then
send missing/wrong/duplicate/unsupported binding to all four unary peer methods
and both streaming methods. Higher-term inputs must be refused without changing
the target's term, leadership or log index. A matching authenticated pre-vote
control reaches the library, and later legitimate write/readback succeeds.
The ordinary snapshot-install/restart, process mTLS and full fault suite validate
matching-cluster transport behavior for the current candidate. These are local
observations; multi-host deployment/upgrade and independent security review stay
open production gates.

## Command profile extension

Legacy command profiles retain protocol 2. Opt-in command format 1 uses protocol
3 and exactly one command-format header with value 1, alongside matching cluster
and snapshot format. Both directions reject incompatible profiles before
consensus input. WAL format remains a local persisted member setting; command
format is a peer compatibility boundary. See COMMAND_FORMAT.md.
