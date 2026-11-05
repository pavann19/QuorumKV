# Node configuration

`quorumkv-node -config=node.json` loads one JSON object, bounded to 64 KiB.
Omitted fields retain shared defaults. Explicit CLI flags override the matching
file fields; unspecified CLI defaults do not overwrite file values. Unknown,
duplicate (case-insensitive), null, oversized, excessively nested and trailing
configuration data are rejected. Semantic validation runs before member data is
opened: required identity/topology fields, peer IDs/ports, literal-loopback admin
listener, distinct peer/admin listener strings, positive keyspace/disk limits
and bounded request settings.

Bootstrap, expected bootstrap voter count and fatal clearance remain explicit
CLI-only one-time operations. They are not accepted in a persistent config file.
Secrets are referenced through the existing identity JSON path; inline private
keys are unsupported. No environment override or automatic credential lookup is
performed by this loader.

```json
{
  "id": "node1",
  "cluster_id": "operator-assigned-cluster-id",
  "peers": "node1=127.0.0.1:7070,node2=127.0.0.1:7080,node3=127.0.0.1:7090",
  "data_dir": "REPLACE_WITH_MEMBER_DIRECTORY",
  "identity": "REPLACE_WITH_OPERATOR_IDENTITY_JSON",
  "listen_addr": "127.0.0.1:7070",
  "admin_addr": "127.0.0.1:7071",
  "store": {"max_bytes": 268435456, "max_entries": 1000000},
  "disk": {"max_used_percent": 90, "min_free_bytes": 67108864},
  "raft": {
    "heartbeat_timeout_ms": 1000,
    "election_timeout_ms": 1000,
    "leader_lease_timeout_ms": 500,
    "commit_timeout_ms": 50,
    "snapshot_interval_ms": 120000,
    "snapshot_threshold": 8192,
    "trailing_logs": 10240,
    "max_append_entries": 1
  },
  "requests": {
    "max_key_bytes": 4096,
    "max_value_bytes": 1048576,
    "max_inflight": 64,
    "max_streams": 128,
    "timeout_ms": 5000
  }
}
```

Review/replace the paths and per-member topology before use. This is a schema
example, not a running or validated deployment. New-cluster initialization adds
`-bootstrap -bootstrap-expect=3` once; ordinary restart omits those flags.
Fatal clearance requires the separate recovery runbook. On this host the
production 90% disk admission policy rejects writes; lab qualification uses an
explicit 95% profile, never a silent fallback.

Request settings may only tighten established safety ceilings: keys <=4096,
values <=1 MiB, pending KV operations <=64, gRPC streams <=128 and RPC wait <=5 s.
The common transport message ceiling remains 4 MiB. Raising it and changing
replication batch size require the separate H4 wire-size/catch-up checks. Limits
are exposed through `/effective-config`; handlers use node-owned settings and
share admission across wrappers. A canceled RPC still retains its submitted
future and reservation until resolution.

Startup prints `configuration_sha256`, computed from the merged normalized
struct plus the startup identity-config digest. It changes when effective node,
request, disk or store settings change. This is a configuration identity, not a
binary/source hash, not a private-key digest, and not proof of runtime health.
Bootstrap/clear-fatal actions are not persistent settings and are outside this
hash. Leaf certificate rotation does not change this startup config hash.
The admin endpoint reports implemented limits plus this hash; it does not yet
expose the full deployment topology or unimplemented settings.

Acceptance evidence includes a real child executable launched from a file with
explicit flag overrides, checking the effective hash/settings, configured-size
rejection and actual successful write/readback. Invalid config refuses before
creating its member directory. Unit tests cover malformed data and semantic
bounds; request tests verify configured admission, size and timeout behavior.

The `raft` settings above are now applied to the actual startup configuration.
Durations are integer milliseconds. Lease timeout cannot exceed heartbeat,
election cannot be shorter than heartbeat, standard Raft validation also runs,
and intervals/counts have explicit positive/overflow bounds. Max append entries
is deliberately restricted to the established value 1 until H4 wire-size and
catch-up qualification. Valid tuning values are not performance/availability or
storage-sizing guarantees. Startup settings are immutable; there is no reload API.

`peers` contains advertised endpoints and must include this member's ID exactly
once. Wildcard/empty/malformed advertised addresses are refused. `listen_addr`
defaults to the self advertised address and may be explicitly different for an
operator-managed routing/NAT setup. Plaintext listen must still be a literal
loopback IP; a wildcard listener requires the separately validated mTLS identity.
Admin remains literal loopback. Wildcard, IPv4-mapped and numeric-port aliases
are considered when refusing overlap with the node listener. DNS resolution and
actual bind availability remain runtime checks, not promises of this validator.

The canonical hash sorts peer IDs and fills the inferred listen default, so an
ordering-only peer change or equivalent explicit listen default does not change
configuration identity. It does not collapse distinct DNS/dial authorities or
certificate bindings. `/effective-config` retains its existing fields and adds
actual startup Raft settings, cluster ID, peer protocol version and the merged
`node_configuration` supplied by the executable. That structure contains paths,
not file contents or certificate/private-key bytes. Treat the loopback admin
boundary as part of the current security scope; authenticated admin is unfinished.

Tests use nondefault timers to cause an automatic snapshot, restart and verify
all 16 retained keys. The executable test binds a distinct loopback listen
endpoint, checks its advertised/listen report and hash, performs write/readback,
and rejects invalid timing relationships before member-directory creation. The
internal constructor clones caller-supplied Raft configuration rather than
mutating it during local-ID/batch-limit setup or offline inspection.

Open M2/section 7.6 scope: a typed peer-map schema (the present schema still uses
the compatible peer string), separate peer/client listeners, full TLS/logging/
tracing/lifecycle configuration, authenticated admin/probe policy and validated
upgrade/reload semantics. The current strict schema refuses proposed fields
that are not implemented. Capacity, resource and latency sizing remain separate
measured qualification work. See the current progress receipts before making
any full-completion or deployment claim.

## Read path selection

`read_mode` accepts `quorum` (default) or `barrier`; explicit `-read-mode` overrides
the file. `/effective-config` reports the actual mode as well as the merged
configuration hash. Quorum mode requires fresh quorum verification and completed
application; barrier mode preserves the original replicated-log read fence for
controlled comparison. Neither permits stale follower-local reads. See
[READ_FENCING.md](READ_FENCING.md) for failure/cancellation and startup behavior.

## Write buffering

`raft.batch_apply` controls the adopted Raft apply-channel buffer (default true).
It does not by itself increase the verified replication append ceiling, which
remains one pending H4 qualification. The FSM supports bounded shared WAL syncs;
see WRITE_BATCHING.md for the verified in-memory profile and remaining network/
performance gates. No setting bypasses committed storage sync or fail-stop rules.

## Candidate replication and read-pressure settings

The unreleased H4 candidate accepts `raft.max_append_entries` and explicit
`-max-append-entries` from 1 through 8 (default 1; larger groups are opt-in). Actual protobuf
splitting preserves the existing 4 MiB physical frame and 1 MiB value ceilings.
Its paired performance acceptance remains open; see REPLICATION_BATCHING.md.

`requests.write_read_grace_ms` accepts 0..25 (default 0). When a read is
pending, a new mutation may coalesce until pending reads drain or that requested
grace ends; its context still bounds waiting. Zero disables this scheduling aid.
It does not delay committed application, weaken a read fence or alter fsync. The
effective configuration reports it; no hot reload is implied. This candidate
setting is still being measured against the original performance gates.

## WAL format selection

`wal_format_version` / `-wal-format-version` accepts 0 (legacy default) or 1
(cluster-bound header and seeded record CRCs). This is a persisted member-format
choice, not a tuning switch: incompatible restarts refuse before opening the
store and existing WAL files never convert on open. Format 1 uses manifest
version 3. See WAL_FORMAT.md for the exact encoding, limitations and remaining
offline conversion / cross-release rollback gates.

## Command format selection

`command_format_version` / `-command-format-version` accepts 0 (legacy gob
default) or 1 (bounded protobuf). Format 1 requires both WAL 1 and snapshot 1,
uses member manifest 4 and peer protocol 3, and refuses mixed-profile restarts
and transport traffic. It does not convert existing data. See COMMAND_FORMAT.md
for encoding, strict validation and the open migration/rollback gate.
