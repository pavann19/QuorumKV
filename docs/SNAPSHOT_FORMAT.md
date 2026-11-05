# Snapshot format compatibility

Snapshot format 1 is an explicit fresh-cluster opt-in: `-snapshot-format-version=1`
or JSON `snapshot_format_version: 1`. The default remains legacy format 0.
Do not enable it on an existing member: startup refuses a manifest mismatch.
Format 0 retains the historical gob decoder and is not allocation-hardened.

Format 1 uses the eight-byte magic QKSNAP/0/1, a little-endian uint32 frame length,
a protobuf header, and one framed protobuf message per key/value. The header binds
schema version, operator cluster ID, store limits, logical size, entry count and
last applied command index. Header and entry frames are bounded before allocation;
unknown fields/versions, duplicate keys, invalid sizes and trailing bytes refuse.
All decoding and validation precede Store.Restore, preserving the existing WAL
and keyspace on malformed input. The adopted Raft snapshot store supplies its
outer checksum; this encoding is not an independent authenticated container.

This is bounded decoding, not constant-memory recovery: the complete decoded
live keyspace, duplicate-key set and replacement store coexist during restore.
Snapshot creation still copies the live keyspace. Capacity sizing must account
for these overheads; a logical byte limit is not an RSS ceiling.

Format 1 persists cluster manifest version 2. Legacy-mode startup refuses it,
and format 1 refuses legacy manifests rather than converting them silently.
Peer protocol 2 additionally requires exact snapshot-format agreement on every
peer RPC/stream. Older peers and mixed-format peers are refused; rolling mixed
release upgrades are not qualified. Do not run historical unguarded binaries on
these directories. The offline legacy identity tool cannot authorize a format
upgrade and refuses unmanifested versioned snapshots.

WAL framing and Raft command gob encoding are still unversioned. Offline format
conversion, old-executable rollback and cross-release migration testing remain
open H9 gates. This opt-in implementation does not close H9 or P5. No production
upgrade or rollback safety claim follows from same-candidate fixture recovery.
