# Synthetic corruption and verified restore drill

This runbook reproduces the D7 local recovery procedure on disposable generated
fixtures. It never opens a user member directory. The broader production restore,
backup automation, filesystem/power-loss and endurance gates remain separate.

From the repository root, set `QUORUMKV_RESULTS_DIR` to a new evidence directory,
then run:

```text
go test ./internal/labcluster -run '^TestCorruptMemberVerifiedBackupRestoreDrill$' -count=1 -timeout 90s -v
```

The selected profile is a three-member, authenticated loopback cluster with
synthetic certificates and independent temporary member paths. Its explicit lab
disk watermark is 95%; production remains 90%, which currently rejects writes
on this host. The command runs to completion and stops its owned cluster; it is
not a supervisor, deployment or long soak.

The procedure:

1. Acknowledge 16 puts and four deletes. Require all three local FSM maps to
   converge to the exact 12 retained values before backup. Take snapshots and
   stop every owned member gracefully.
2. Copy each complete stopped member directory into a new backup directory,
   using exclusive file creation and file fsync. Verify every relative file's
   size and SHA-256 against the stopped source. Preserve the verified backup
   separately in the evidence artifact.
3. Flip a byte in the stopped synthetic leader's populated WAL. Retain the
   damaged bytes and full incident-file fingerprint before restart. Attempt to
   restart only that owned member; require fatal corruption refusal, no serving
   node, unchanged damaged WAL and unchanged other original data files. A second
   restart must refuse the newly persisted FATAL marker.
4. Copy the verified backup into another new member root. Verify its complete
   pre-start file hashes against the backup. Keep the corrupted member and the
   backup untouched. Start the restored members with the original identities,
   membership and cluster ID, without bootstrap.
5. Verify every retained value through leader RPC reads, verify every deletion,
   and require exact map equality (no stale/unexpected keys). Acknowledge a new
   write and check its exact readback. Re-hash the backup and original damaged
   WAL to confirm recovery did not modify the preserved evidence.

Each `corruption-restore-*/drill.json` records stage results, acknowledged count,
backup and incident fingerprints, exact-state checks and elapsed seconds from
starting restore-copy materialization through verified new write/readback. The
artifact also retains the checksum-verified backup, corrupt WAL and fatal marker.
FAILED/INCOMPLETE stages remain failures; absence of a PASS report is not success.
The enclosing runner binds reports to candidate source hashes and test output.

This is a bounded graceful offline backup plus corruption drill. It retains
synthetic PKI outside member data and reuses it on restore, so it does not prove
recovery after loss of credentials/the entire host. No production RTO/RPO number,
online backup guarantee, arbitrary corruption coverage or power-loss durability
follows from its measured local interval. A production incident must preserve
actual evidence first and follow FATAL_RECOVERY.md; do not run this synthetic
fault procedure against a real member directory.
