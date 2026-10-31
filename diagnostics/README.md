# Separate bounded diagnostics worker

`worker.py` uses Python 3.11+ standard library and SQLite WAL with FULL synchronous
commits. It has no KV client, Raft admin, membership, process-kill, snapshot-delete,
trust-root, production resource or arbitrary shell adapter. The request/storage
path imports none of this package. A failed/offline worker cannot change quorum.

Each queue is pinned to one tenant, evidence root and source SHA-256. Submit a
bounded JSON envelope with exactly these fields:

```json
{
  "job_id": "sample-1",
  "idempotency_key": "sample-1",
  "goal_version": "quorumkv-diagnostics-v1",
  "source_version": "<64 lowercase hex characters from the source manifest>",
  "schema_version": 1,
  "tenant": "lab",
  "scope": "readonly",
  "tool": "history_summary",
  "evidence_refs": [{"path": "history.json", "sha256": "<exact file SHA256>"}],
  "deadline": 1793300000
}
```

The deadline is a Unix timestamp in the next 24 hours, not the example literal.
Evidence must be schema-2 JSON with `ops` inside the configured root and <=64 KiB
each (at most four files). Paths are resolved before use; symlink/path traversal
outside the root is rejected. Evidence hashes are checked at enqueue, execution
and finish. Only attempt/outcome counts enter generated observations/prompts;
raw keys, values, error strings and claimed checker verdicts are not trusted.
Keep this directory application-owned; no agent can write evidence or oracles.

Optional `metrics` in a history document accepts only nonnegative integer
leader/term/commit/apply/log/snapshot/peer-count and WAL append/sync/failure/byte
fields. Unknown keys, string/injected values and invalid bounds are rejected.
The supervised lab supplies these from actual node observations. A retained
boolean `linearizable` is reported as `recorded_checker_verdict`, explicitly a
retained artifact rather than an oracle rerun or a model-authorized verdict.

Run one bounded job per invocation (no daemon implied):

```text
python -B diagnostics/worker.py --db <new-owned-queue.sqlite> --evidence-root <synthetic-evidence-directory> --source-version <source-sha256> --enqueue <job.json>
```

Omit `--enqueue` to recover/claim existing queued work. The default result uses
`deterministic-offline-v1`, explicitly not an LLM inference. `--model` opts into
an already installed named model at literal `127.0.0.1:11434`; redirects, proxies,
cloud-tagged models and automatic download/fallback are disallowed. Installed
model digest is retained in its version. Requests bound context (2048), output
(512 tokens), threads (2), HTTP timeout (20 seconds), a separate owned helper
with a hard 25-second wall limit, and response bytes (16 KiB).
The external Ollama service/model memory requires operator limits independently;
this worker does not alter drivers or host GPU settings. A missing model is
blocked/needs_review, not success. Live model quality is not established by
mocked outage/adversarial tests.

The result must have exactly `status`, `evidence_refs`, `observation`, `hypothesis`,
`proposed_actions`, `confidence_or_uncertainty`, and `model_version`. Status is
prepared/needs_review/blocked/failed. There is no applied state/effect adapter.
Proposed action strings remain inert even if a model requests deletion or policy
changes. Evidence references must exactly match the submitted job. Output is
<=8 KiB, with per-field and proposal-count limits.

The optional `lab_lost_reply` tool requires disposable_lab scope and a LAB_ONLY
marker. It does not connect to any supplied cluster. It launches the fixed
`TestRecorderPreservesAppliedWriteWithLostReply` test, which creates its own
bufconn server and temporary WAL, checks actual applied-write/lost-reply behavior,
and retains bounded raw checker evidence. Tool ID, test, command, cwd and target
creation are deterministic. A job/model cannot supply a command or port.

Claims/heartbeats/results/retries use immediate SQLite transactions. A random
lease token and owner fence every finish, with an expiry/deadline check. A global
worker slot allows one active job. Expired work retries with bounded exponential
backoff/jitter, then reaches needs_review after three attempts. A crash can
repeat the read-only or isolated synthetic experiment; this is not exactly-once
execution. Idempotency suppresses duplicate logical jobs and rejects changed
payload collisions. Queue metrics include states, oldest age, retries, expired
leases, result states and policy denials, with no job IDs/content as labels.

Inputs <=8 KiB, 1000 retained job rows, 500 new jobs/day and a 64 MiB database
page limit bound growth. Capacity exhaustion rejects ingestion rather than
deleting audit history. Archive a closed owned queue and start an explicitly
versioned replacement when retention/capacity requires it. Checkpoint on close;
the lab supervisor also bounds total disk, RSS, CPU and wall time. Long-running
model/tool operations stay below the 60-second lease and renew before/after;
expired results cannot commit. Keep queue files and sidecars private to the
application user (Windows needs an appropriate directory ACL).

Validate with `python -B -m unittest discover -s diagnostics -v`. Tests use only
temporary synthetic evidence/queues and assert forbidden files and uncertainty
records remain unchanged. The final evidence pack identifies the actual runs.
