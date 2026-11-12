# QuorumKV local operation and deployment review

The continuation implementation is a local lab candidate. Deployment, hosted CI,
host-level HA, power-loss durability and a completed 24-hour run require their
own evidence. Consult `docs/PORTFOLIO_EVIDENCE.md` for current candidate outcomes.

## Preflight and effective configuration

1. Preserve the dirty checkout. Read `AI_CONTINUE_HERE.md` and the phase evidence.
   The original baseline is a preservation snapshot, not a reset target.
2. Use Go 1.27.4 and Python 3.11+ standard library. Run
   `python -B scripts/run_checks.py`. It records the exact toolchain,
   source hashes, reconciled edits, commands, raw outcomes and drift checks.
3. Run `python -B scripts/run_lab.py --profile smoke --seconds 60`.
   Each execution owns new temporary credentials, loopback listeners, member
   stores and diagnostics SQLite files under a new ignored evidence directory.
4. Review its `manifest.json`, `cluster/preflight.json`, `cluster/summary.json`,
   `resources.jsonl`, `cluster/samples.jsonl`, raw histories and job metrics.
   The three lab members are in one process with separate stores; process-kill
   tests are separate. This is not a multi-host deployment.

The node defaults to a literal-loopback-only plaintext lab when `-identity` is
absent. Every peer address must be a literal loopback IP in that mode. The legacy
single-node command is also loopback-only. Non-loopback replicated operation
requires an operator-owned identity configuration; no runtime agent can edit it.

Example identity configuration (replace fixture addresses with actual approved
members, provision keys out of band, and keep this file/key readable only by the
service/operator):

```json
{
  "certificate": "/etc/quorumkv/node1.crt",
  "key": "/etc/quorumkv/node1.key",
  "ca": "/etc/quorumkv/cluster-ca.pem",
  "self": "spiffe://quorumkv/peer/node1",
  "peers": {
    "10.20.0.11:7070": "spiffe://quorumkv/peer/node1",
    "10.20.0.12:7070": "spiffe://quorumkv/peer/node2",
    "10.20.0.13:7070": "spiffe://quorumkv/peer/node3"
  },
  "clients": {
    "spiffe://quorumkv/client/operator": "write",
    "spiffe://quorumkv/client/observer": "read"
  }
}
```

Certificates need exactly one matching URI SAN, validity covering the planned
run, and the appropriate client/server EKUs. The lab PKI is disposable and must
never become a production trust root. Peer membership is trusted, not Byzantine
fault tolerant. An authorized peer must not be treated as an untrusted tenant.

Run a member with its own data directory and identity file:

```text
quorumkv-node -id=node1 -peers=node1=10.20.0.11:7070,node2=10.20.0.12:7070,node3=10.20.0.13:7070 -data-dir=/var/lib/quorumkv/node1 -identity=/etc/quorumkv/node1.json -admin-addr=127.0.0.1:7071
```

The admin listener is optional and always literal-loopback-only. `/live` is
process health, `/startup` indicates the constructed service, and `/ready`
requires leadership plus a successful quorum read fence (default log-free quorum
verification; explicit barrier comparison mode remains available). A follower returns
503 for readiness. `/effective-config` reports a configuration-file hash and
request ceilings without key material. `/metrics` exports fixed method/code
labels, latency buckets, leader/term/log/apply/snapshot indices and local WAL
append/sync/failure counters. These counters reset on process restart and are
observations, not recovery metadata or proof of replication latency. No external
collector is required; metrics collection is outside the storage path.

Defaults: keys 1..4096 bytes, values <=1 MiB, messages <=4 MiB, 64 outstanding KV
futures, 128 streams per connection, 5-second KV wait/enqueue limit. A submitted
write can still commit after its caller times out. Keep it uncertain and
reconcile by observed reads; never label cancellation as definite non-commitment.
Transport append batches are capped at one entry so maximum-size values fit the
message limit. Raft/storage/fsync behavior is not delegated to diagnostics.

## Rotation, shutdown, backup and rollback

Complete WAL checksum/structure corruption now fails startup with
`wal: corrupt record`, preserving the original file. Preserve the entire member
directory, investigate the storage fault, and use the isolated restore procedure
below. Never truncate/edit corruption to force startup. Incomplete trailing
appends retain their existing repair behavior. See the
[reliability review](RELIABILITY_REVIEW.md) for format limits and release gates.

Leaf/key files are loaded on new handshakes. Publish a matched replacement pair
with the same allowed URI under the existing CA. A transient mismatched pair
fails closed. Establish a fresh test connection and verify a write/read/delete
with a permitted client. Existing connections do not instantly switch leaves;
authorize checks revalidate certificate validity on each RPC/stream message.
Trust roots and allowlists are pinned in memory until an operator restarts the
member. CRL/OCSP revocation and a live trust-root rollover protocol are not
implemented. Emergency revocation needs an operator-coordinated stop/firewall
and allowlist/trust update; an agent proposal grants no authority.

For an orderly stop send SIGTERM/interrupt. gRPC drains for up to five seconds,
then stops; Raft, its transport, log store and WAL are closed. Keep a majority
available during a rolling upgrade. On Windows the owned process-kill tests use
TerminateProcess; that is abrupt process termination, not a power-loss test.

For a backup, stop one caught-up member gracefully and copy its *entire* member
data directory while stopped (WAL, Raft Bolt log/stable state and snapshots) to
a new backup location. Retain the immutable source/config/certificate hashes,
membership and last observed application acknowledgements separately. Restart
the original and verify catch-up before stopping another member. Copying live
database files or replaying only `store.wal` is not a replicated backup protocol.

Restore first in an isolated network with the original member IDs and static
membership, operator-provided matching identities, copied complete member
directories, and no production peers. Verify retained values, deletions, exact
keyspace, a new quorum write and readback. Keep the original backup untouched.
Record elapsed restore time and the acknowledged-state boundary; only then
define measured RTO/RPO for that procedure. Leader-election timings are not RTO.
Never force a minority to accept writes, change membership to manufacture a
quorum, edit a snapshot/WAL, or rewrite a failed history.

Rollback a code upgrade by stopping one member, preserving its current data and
logs, restoring the previously validated binary/config, and verifying storage
compatibility and catch-up before proceeding. The continuation keeps the
existing WAL wire format. A future format change requires a separate migration
and rollback plan. Do not reset/clean this working tree or erase older results.

## Smoke, performance comparison and soak

Use identical source/hardware, 3 members, 4 clients, 8 synthetic keys, 1 KiB
values and fsync policy for the plaintext/mTLS comparison:

```text
python -B scripts/run_lab.py --profile smoke --seconds 60 --transport plaintext
python -B scripts/run_lab.py --profile smoke --seconds 60 --transport mtls
```

Cold first-ack timing uses a fresh client connection to an elected leader.
Warm batch samples retain all logical-operation latencies and p50/p95/p99.
Percentiles use the lower order statistic at `floor(p*(n-1))`; raw samples are
retained so other conventions and whole-run distributions can be computed.
These are short, rate-limited local comparisons, not capacity measurements or
an optimization claim. Retain individual replies, pre-submission rejections and
uncertain mutations; never subtract failed attempts from the record.

The long-run command is explicitly `python -B scripts/run_lab.py
--profile soak`. It always requests 86,400 real seconds, requires mTLS and cannot
be shortened using `--seconds`. Review the generated preflight before treating
the run as evidence. A background launch is not completion. Sleep/hibernation,
reboots, source drift, nonzero exit, invariant failure, job failure or resource
breach prevent a PASS. Preserve the failed manifest, histories and raw samples.

Stop gates are declared before execution: zero lost acknowledgements/illegal
histories, zero failed logical operations, zero uncertain mutations in healthy
windows, per-window logical p99 <=5 seconds, measured restore/restart probe
<=15 seconds, Go-observed memory <=1 GiB, process RSS <=1.5 GiB, average owned
lab CPU <=2 cores, entire run disk <=1 GiB, log <=64 MiB, queued/running jobs <=8
and oldest queued age <=60 seconds. An observation gap over 60 seconds or a
backward wall-clock discontinuity aborts the continuous-run claim. Windows CPU/RSS use OS process counters.
Supervisor samples and Go samples distinguish RSS from Go allocator statistics.
Resource ceilings stop the owned process; they do not change host settings.

The cluster uses a 5-second batch interval with Put/Get/Delete/Get cycles and
continuous history checks. Snapshot/leaf rotation/member restart exercises run
at startup and every 360 batches. Diagnostics jobs run every 300 seconds in the
soak, include deliberate duplicate delivery, and reopen their SQLite queue every
20 jobs. During a soak, the fixed isolated worker regression suite also runs at
startup and every six hours, including an actual leased-worker crash between
experiment and result acknowledgement, virtual-clock lease expiry, mocked local
model dependency outage, duplicates and hostile outputs. Separate storage tests
inject deterministic ENOSPC at write/fsync.
The ENOSPC fixture is fault injection, not an actual full-volume experiment.

## Hosting review: remaining operator gates

`deploy/quorumkv-node.service` is a reviewable Linux template, not an installed
or validated service. It uses restart backoff/start limits, a dedicated user,
read-only system configuration, bounded resources and SIGTERM shutdown. There is
no pretend watchdog integration. Provision distinct host/failure domains and
persistent disks before making host resilience claims; three laptop members
only demonstrate local behavior. Set disk quotas and alerts, protect backup
encryption/access, independently provision CA/keys, test firewall/client policy,
validate filesystem/rename/fsync behavior on the target OS/storage, and run the
backup/restore/upgrade procedure there before deployment approval. The custom
WAL reset does not fsync the parent directory; power-loss rename durability is
not established. Membership changes, live revocation, automated production
chaos and Kubernetes hosting are outside this candidate's implemented scope.

No cloud resources, paid services, public repository changes or production data
are created by these local commands. Hosted deployment remains a separate human
decision after the evidence and these limitations are reviewed.
