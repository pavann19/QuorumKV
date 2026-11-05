# Offline legacy identity migration

Use this only after stopping the member/supervisor and preserving a verified
copy of its entire data directory. Work on an isolated recovery copy first.
This command is an explicit operator maintenance path; diagnostics jobs and
model output have no authority to invoke it. It cannot bootstrap, clear a fatal
incident, change an existing cluster identity or alter voting membership.

Inspect a manifest-free member with its **persisted** peer IDs and addresses:

```text
quorumkv-node migrate-identity -id=node1 -peers=node1=127.0.0.1:7070,node2=127.0.0.1:7080,node3=127.0.0.1:7090 -cluster-id=OPERATOR_ASSIGNED_CLUSTER_ID -data-dir=ISOLATED_MEMBER_COPY
```

Inspection is the default. Review/save its `INSPECTED_NOT_APPLIED` JSON report
outside the member directory. It lists SHA-256/size for every ordinary evidence
file except `member.lock`, plus the desired identity/membership/default storage
limits and a fingerprint binding those bytes to the requested manifest. All
members of the existing cluster must be assigned the same operator-selected
cluster ID. A local fingerprint is evidence of these bytes, not remote proof of
cluster identity or an authorization credential.

Only after reviewing the exact report, repeat the same command with
`-approve-sha256=THE_REVIEWED_SOURCE_SHA256`. It re-inspects under exclusive member
ownership, verifies the approval and unchanged source, then publishes only
`cluster.json`. Success reports `APPLIED_IDENTITY_ONLY`. It does not rewrite the
WAL, Bolt file or snapshots. After migration, normal startup uses the same node,
cluster ID, default legacy store limits and membership, **without bootstrap**.
Verify retained values, deletions and a new write/readback before considering
that recovered member healthy. Never migrate a second member until the first
has been verified and the cluster can safely support the next maintenance step.

## Checks and preservation boundary

- Acquire the member's OS-managed ownership lock; a live owner is refused.
  Inspection may create the persistent 0600 `member.lock` file if absent. It is
  excluded from the source fingerprint and never unlinked for lock release.
- Refuse an existing `cluster.json`, any `FATAL` marker, symlink/reparse links,
  nonregular evidence, >10000 files or >64 GiB of file content.
- Stream the WAL read-only and require a complete valid prefix equal to file
  size. Corruption or an incomplete tail is refused, never repaired by migration.
- Open Bolt read-only with a one-second ownership timeout; require the adopted
  log/stable buckets and existing Raft state. The library's offline reader calls
  initialization code that writes a current term; an in-memory stable overlay
  isolates those writes. The underlying read-only handle remains a backstop.
- Read snapshot metadata and stream its state to verify size and adopted CRC64.
  No ordinary FileSnapshotStore constructor is used: that constructor writes a
  permissions-test file. Incomplete/unsupported/mismatched snapshots are refused.
- Read persisted membership with no Raft background loop, network connection or
  FSM restoration. Damaged-layout library panics at this offline boundary become
  a refusal; committed runtime failures still fail stop.
- Re-hash before approval publication. Original data bytes remain unchanged.
  Directory fsync has the existing Windows limitation. If publication returns
  an I/O error, inspect the copy and manifest before retrying; do not infer that
  no directory entry was created.

The command does not validate arbitrary legacy application schemas, repair
corruption, prove acknowledged-state completeness, assign custom historical
capacity limits or perform a format upgrade. Generated v1.7.1 legacy-compatible
log/snapshot fixtures have been migrated and restarted with exact retained and
deleted state verified. Historical deployed versions need their own fixture
qualification. Peer-handshake cluster binding and a full multi-member migration
and corruption/restore drill remain separate unfinished acceptance work.
