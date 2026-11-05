# Versioned committed commands

Status: opt-in command format 1 implemented; final matching-source Linux qualification passes 175 top-level checks;
Windows race passes 182, plus the separately tagged actual-executable proof. Current schemas now have a verified local offline copy roundtrip with an actual
previous executable; release and larger-topology qualification remain separate. No existing member is converted on startup.

`command_format_version` / `-command-format-version` accepts 0 (legacy gob,
default) or 1 (bounded protobuf). Format 1 requires explicit WAL format 1 and
snapshot format 1. Manifest version 4 records the command selection and refuses
incompatible restarts before store open. The effective configuration reports
the actual command format and peer protocol. Legacy profiles retain their
existing encoding and peer protocol 2.

Format 1 is `QKCMD\0\0\1` followed by the canonical protobuf `CommandEnvelope`
defined in `proto/storage.proto`. It carries schema version 1, cluster ID,
operation (1 put, 2 delete), key/value and positive store byte/key limits. The
entire input is bounded before decoding at 4096 key bytes + 1 MiB value bytes +
4096 bytes of envelope allowance. Decoder recursion is bounded. It refuses a
wrong cluster, unknown schema/operation/fields, overflowing or absent limits,
invalid payload bounds, delete values, duplicate/reordered fields, alternate
varints and explicit-default encodings. There is no permissive gob fallback.
The encoder emits the single canonical representation. Field numbers are
append-only; incompatible interpretation requires an explicit profile upgrade.

Admission, committed command order, per-command capacity outcomes, WAL sync
before acknowledgement, completed-application publication and fatal handling
are unchanged. Limits in each command must still equal the member's persisted
limits. A malformed committed command fails stop; it cannot be skipped as a
recoverable rejection. The new encoding alone implements no idempotency ledger.

New command profiles use peer protocol 3 and require exactly one
`quorumkv-command-format: 1`, in addition to the cluster and snapshot binding.
Every native unary/streaming transport entry point checks this before handling
consensus input. Legacy protocol 2 and new protocol 3 reject each other. New
outbound interceptors replace untrusted metadata; legacy outbound calls remove
spoofed command-format fields. These are compatibility fences; existing mTLS
identity and authorization remain required and are not replaced by headers.

Synthetic qualification covers maximum-size round trips, malformed/canonical
refusal, profile checks before storage, metadata ownership and redacted fatal
exit 70 for a committed cross-cluster command. A three-member mTLS fixture uses
the new profile to transfer a real snapshot to a lagging member, verify retained
values/deletion, refuse old/missing/duplicate peer bindings without term/log
changes, then acknowledge a write after leader kill. Existing fault oracles are
retained. Test status is recorded in the current checkpoint receipts.

Current limitations: format 0 remains gob-based; a mixed-profile rolling upgrade
is deliberately refused; offline copy conversion and writable rollback for current schemas are now
verified locally (see FORMAT_MIGRATION.md). Fenced incompatible startup is a safety property,
not proof that a migration or rollback works. P4 physical/endurance and P5
performance, sizing and durable-client-identity gates remain separate.

## Retained executable compatibility

A separate `compat` build-tag fixture requires a previous node executable from
a successful retained run. The Linux runner validates its manifest, binary SHA,
source inventory and distinctness from the current executable before copying
it to this run's read-only binary mount. It never substitutes today's binary.

`readiness-command-compat-linux-20261105-01` passes actual previous -> current ->
previous read/write checks in explicit legacy format, including retained values
and deletions. It also writes the new profile, proves previous-executable startup
refusal without changes to cluster.json, the Raft log or WAL, then restarts the
current executable and acknowledges another write. Both binary identities and
member-file fingerprints are retained in evidence/format-compat/compatibility.json.

This proves legacy bidirectional executable compatibility and safe refusal of
the new profile. `format_conversion` and `new_profile_writable_downgrade` remain
NOT_IMPLEMENTED in the receipt. It does not close the offline conversion gate.
The source inventory now includes protobuf definitions as well as generated Go.

Joint current checkpoint: retained local checkpoint (not distributed with source). It records compiler/source identities, exact proof scopes and uncompleted gates.

The later format-migration checkpoint implements and tests the previously open conversion gate for current KV schemas 0/1. Earlier NOT_IMPLEMENTED receipt fields remain historical for their exact candidate; they were not rewritten. Future receipt-bearing schemas need new compatibility work.
