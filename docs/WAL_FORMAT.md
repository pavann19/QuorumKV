# Cluster-bound WAL format

Status: opt-in format 1 implemented; current-candidate native Linux qualification
passes 159 top-level tests across nine affected packages. Matching-source Windows race
qualification passes 177 top-level tests, zero actual test skips. This is not a completed H9 migration/rollback or P4 durability
gate. Legacy format 0 remains the default. No existing file is converted on open.

Startup JSON `wal_format_version` and explicit `-wal-format-version` accept 0 or
1. Selecting 1 for a fresh member writes manifest version 3 with the selected WAL
format. Future starts must match that persisted setting before opening the store.
The effective-config endpoint reports the actual selected WAL format. The
identity-only legacy migration refuses to authorize this format change.

Format 1 starts with this bounded, little-endian header:

| Field | Bytes |
| --- | --- |
| Magic `QKWAL\0\0\1` | 8 |
| Format version, currently 1 | 4 |
| Cluster ID byte length, 1..128 | 2 |
| Per-file record CRC seed | 4 |
| ASCII cluster ID | declared length |
| IEEE CRC32 of all preceding header bytes | 4 |

The header occupies at most 150 bytes. Magic, version and declared length are
checked before allocating the remaining bounded header. The entire header CRC
and exact configured cluster identity are checked before visiting records or
repairing an incomplete record tail. An existing empty or partial-header file
refuses startup rather than being treated as a new file. Exclusive creation
distinguishes a new WAL from an existing file. File and parent-directory sync
retain their current platform guarantees; Windows directory fsync is unverified.

Record length/op/key/value framing remains unchanged. Each record checksum uses
IEEE CRC32 initialized with the header's seed. Reset/restore/compaction preserves
the validated header and seed when atomically replacing the file. Complete
record corruption refuses recovery; only an incomplete record suffix can be
repaired after successful header and prefix validation. Visitor failure never
repairs the source. CRCs detect accidental damage, not malicious modification;
they provide no authentication or protection against an attacker with disk write
access who can recompute them.

The legacy reader rejects the magic as an invalid record length before modifying
the file. The new format-1 reader refuses legacy files instead of guessing their
format. This verifies refusal with the unchanged legacy API; it is not yet a
cross-release executable rollback test. Manifest versions also prevent an older
member profile from opening a format-1 directory. Conversion must eventually run
offline on a separate copy with fingerprints, full validation and explicit
manifest handling; that offline copy converter is now implemented and locally verified; see
FORMAT_MIGRATION.md.

WAL versioning is local to each member and does not change consensus or the
client API. Raft command payloads remain gob-based in legacy profiles; opt-in command
format 1 now carries a bounded protobuf envelope (see COMMAND_FORMAT.md). Snapshot-format selection remains separate. Durable operation
deduplication must preserve its state through committed log replay and snapshots;
the header alone does not implement it or make uncertain retries safe.

Synthetic checks cover byte-preserving format/header/cluster refusal, malformed
bounded lengths, seeded record corruption, read-only prefix inspection, visitor
failure, tail repair, reset/compaction, store capacity and real node
snapshot/restart. Ten fresh-file abrupt-exit trials kill only owned helper
processes after the batch acknowledgement and validate every acknowledged record
and tombstone. They are process-termination evidence, not physical power-loss or
production filesystem evidence.

Retained native receipt: retained local checkpoint (not distributed with source). Previous run 01 remains FAIL because it exposed a client stale-hint discovery bug; WAL/store/node/recovery checks passed in that run but do not override its aggregate status.

Joint checkpoint: retained local checkpoint (not distributed with source). Scope and remaining migration/physical gates are explicit in that receipt.
