# Offline member-copy format conversion

Status: supported KV format 0/1 local copy conversion verified. No current production, physical
durability or writable rollback claim follows from this document. The tool never
starts a node or changes which directory a running service uses.

`quorumkv-node migrate-format` accepts `-source`, `-destination`, and
`-to-profile=legacy|versioned`. Legacy means snapshot/WAL/command format 0;
versioned means all three format 1. Source format and identity come from its
existing supported manifest. Cluster ID, member ID, peers and store limits stay
bound to that manifest; this command cannot reassign identity or membership.

Omitting `-approve-sha256` performs inspection and prints a plan without creating
the destination. Applying requires the exact plan's approval fingerprint, binding
source/destination paths, all source file hashes, formats, manifest and semantic
inspection proofs. Wrong or stale approval refuses. Source and destination must
be disjoint; destination must not exist, and its parent must already exist.
Both member directories are locked during conversion. The only source-side
operational file the tool may create is the ordinary member lock; data and
evidence files remain unchanged. Symlinks and unsupported file types refuse.

An approved operation reserves a fresh destination, writes MIGRATION_INCOMPLETE
and an unsupported sentinel manifest, then copies/transcodes there. It never
resumes or overwrites a previous target. A failed target is preserved for review.
The sentinel also prevents an older executable that lacks the new incomplete
marker check from opening a partial copy. Current startup independently checks
the incomplete marker before store open.

WAL records retain operation/key/value order, including tombstones; only file
header/CRC format changes. Raft commands are decoded and re-encoded for the target
profile while indices, terms, log kinds, extensions, append times and stable
state are preserved. Membership inspection uses a read-only Bolt handle and a
private stable-store overlay, never a running consensus worker. Snapshot payloads
are transcoded and their official metadata size/CRC updated without changing
index, term or configuration. A legacy snapshot has no command cursor: its new
header records zero (unknown), while Raft's snapshot index still fences the
completed prefix. The converter never invents a command index or commitment.

Header/CRC failures, partial WAL tails, incomplete snapshots, unknown schemas,
future metadata and unsupported commands refuse. No tail repair, corruption
repair, force bootstrap, fatal clearance or acceptance-oracle change is offered.
Committed capacity limits must match the manifest. Legacy zero/absent command
limits retain their existing default-limit interpretation. Ambiguous legacy
delete payloads require review rather than silent reinterpretation.

Inspection checks source WAL capacity, canonical command meanings, snapshot
values/deletions and persisted membership. After conversion, the tool repeats
read-only checks on the target and compares semantic digests, then rehashes the
source. Files are synced before the ready report and final manifest publication;
the incomplete marker is removed last. The ready inventory excludes its own
report and the member lock. It describes the copy before its first startup;
normal subsequent consensus/storage writes legitimately change file hashes.
Older migration reports are retained as hashed history files in later copies.

If final publication returns an error, preserve the target and inspect both the
marker and ready inventory. A report alone is not activation permission. Directory
sync has the existing Unix guarantee and known Windows portability limitation;
none of these checks is a physical power-loss test.

The implemented migration ceiling is one million live entries / 256 MiB logical
state. Source inventories are bounded at 10000 files / 64 GiB; the default time
budget is two minutes, configurable up to thirty minutes. Context cancellation is checked between I/O
operations; a blocking filesystem call can exceed that budget. Preserve an
interrupted target for review. Snapshot encoded size
is bounded at 256 MiB + 32 bytes per allowed entry + 2 MiB. Before gob decoding,
legacy message framing, type declaration sizes/count and declared outer entry
count are checked without copying the whole value message. Protobuf snapshot
counts/bytes are checked before entry allocation. Legacy decoding still uses the
adopted gob field semantics inside that bounded envelope. Larger sizing remains
unqualified and refuses rather than silently changing limits. Run conversion in
a declared isolated resource budget; current synthetic Linux evidence uses the
existing 2 CPU / 1 GiB offline container profile.

For a real upgrade or downgrade, quiesce client traffic and stop every member,
retain verified original directories/backups, create and validate all new copies,
then explicitly select one complete set for activation. Do not mix peer protocols
or run old and converted sets simultaneously. Normal restart uses the persisted
identity and explicit matching format flags, without bootstrap. This repository
work authorizes only owned synthetic fixtures; operating existing infrastructure
or real user data is a separate operator action.

## Current executable proof

The retained previous executable SHA is
2829641ea5a7200a79f338a7c707b011977a3755dd055d4e9e7d7989a385e8ac.
`readiness-format-migration-compat-linux-20261105-01` exercises the real CLI on
previous-produced data: three gob snapshots and two retained commands convert
forward; the current binary reads values/deletion and acknowledges a new write;
three protobuf snapshots and three commands convert back; the actual previous
binary reads the converted copy and acknowledges another write. Original source
file fingerprints remain unchanged in both directions. Plan/applied reports,
semantic hashes, both executable identities and child logs are retained.

This is local one-member executable/format qualification, not a coordinated
multi-host upgrade, physical durability test or maximum-size migration benchmark.
The 256 MiB / million-entry ceiling is a bound, not a measured capacity claim.
Full matching-source native Linux regression passes 181 top-level checks;
Windows race verification passes 188 top-level checks, zero actual test skips.

The approval plan also reports a conservative initial disk-headroom floor:
three times source bytes plus 1 KiB per retained command plus a 64 MiB reserve.
Unknown/insufficient target capacity refuses before directory creation. This is
not a reservation or a guarantee against concurrent disk consumption; later I/O
failure still leaves an incomplete copy. It does not alter node disk watermarks.

Joint checkpoint: retained local checkpoint (not distributed with source). It binds full native/race source identity to the actual previous-executable proof and records remaining release/physical/sizing boundaries. Future receipt-bearing schemas are not supported by this converter yet.
