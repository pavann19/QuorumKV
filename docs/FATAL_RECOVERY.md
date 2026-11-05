# Fatal member recovery

A committed-state failure exits with status 70. The member attempts to create a
0600 `FATAL` marker containing a version, UTC timestamp, fixed reason identifier
and log index. No key/value or raw command bytes are included. Failure to persist
the marker (for example, a full/unavailable disk) is logged as
`marker_persisted=false`; it never permits continued service. Do not assume a
marker exists after a storage outage.

On startup any `FATAL` path, including a malformed partial marker, blocks storage
open. Complete WAL corruption also records a fatal incident without truncating
the original WAL. The example systemd service prevents automatic restart on 70;
this is an unverified Linux template, not proof of supervision behavior.

1. Stop the member supervisor. Quarantine that member without forcing quorum or
   deleting membership. Confirm the remaining cluster can safely serve.
2. Preserve the entire member directory and logs, including `FATAL`, WAL,
   snapshots, cluster manifest and Raft Bolt files. Hash the evidence copy.
   Never test repairs on the original directory.
3. Diagnose on an isolated copy. Restore verified state through the documented
   recovery procedure. Verify retained keys, deletions and a new write/readback;
   an incident-marker clearance is not a repair or a durability check.
4. Only after recovery has been reviewed, invoke the member once with
   `-clear-fatal` and its unchanged cluster identity/membership. Under exclusive
   directory ownership this moves the incident into a unique
   `FATAL.resolved.*` archive and syncs the directory on supported Unix systems.
   Never retain `-clear-fatal` in a service definition or automatic retry loop.
5. If the underlying error persists, the member must fail again; preserve the
   additional incident. Do not loop on clearance or remove the marker manually.

The clearance flag is an explicit local operator action. Runtime diagnostics/AI
workers have no authority to clear incidents, repair state or modify trust or
membership. The live restore/corruption drill and Linux target-filesystem
qualification remain open acceptance gates.
