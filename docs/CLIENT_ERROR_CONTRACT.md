# KV request identity and error contract

Authenticated KV handlers accept one canonical lowercase UUIDv7 in
`quorumkv-request-id`. Missing IDs receive a server-generated UUIDv7 for backward
compatibility. Valid IDs are echoed in success/error headers and trailers; the
same ID is included in structured `google.rpc.ErrorInfo` details. Invalid,
duplicate, wrong-version, oversized or noncanonical IDs refuse before admission
or Raft append, without reflecting the untrusted value.

These IDs are **correlation only**. They are not stored in consensus/WAL, have no
query-by-ID endpoint, do not deduplicate, grant no authorization and do not prove
exactly-once execution. The wire test deliberately reuses an ID for two distinct
puts and verifies the second value is applied. The first-party client generates
and retains its own attempt ID before sending (see CLIENT_LIBRARY.md); server metadata may never arrive
if the call disconnects or its deadline expires. Authentication errors produced
before the KV handler need not contain a server-generated ID.

Errors use domain `quorumkv`, a stable reason and `metadata.outcome`, describing
only this invocation. Earlier uncertain attempts remain uncertain. Lost replies
and transport failures remain uncertain even if no structured status is received.

| Condition | gRPC code | Reason | Outcome |
| --- | --- | --- | --- |
| Request identity invalid | InvalidArgument | REQUEST_ID_INVALID | not_submitted |
| Request size invalid | InvalidArgument | REQUEST_SIZE_INVALID | not_submitted |
| No leader known before mutation | Unavailable | NO_LEADER | not_submitted |
| Known follower / Raft rejects before append | FailedPrecondition | NOT_LEADER | not_submitted |
| Leadership transfer rejects submission | Unavailable | LEADERSHIP_TRANSFER | not_submitted |
| Raft enqueue timeout | Unavailable | ENQUEUE_TIMEOUT | not_submitted |
| Shared admission full | ResourceExhausted | ADMISSION_FULL | not_submitted |
| Keyspace capacity rejected (admission or committed no-op) | ResourceExhausted | KEYSPACE_FULL | not_applied |
| Disk watermark/reserve rejects submission | ResourceExhausted | DISK_PRESSURE | not_submitted |
| Disk probe/observation unavailable | Unavailable | DISK_STATUS_UNKNOWN | not_submitted |
| Context ends before mutation starts | Canceled / DeadlineExceeded | CONTEXT_ENDED | not_submitted |
| Context ends after mutation starts | Canceled / DeadlineExceeded | CONTEXT_ENDED | uncertain |
| Leadership loss, restore interruption or shutdown | Unavailable | APPLY_INTERRUPTED | uncertain |
| Other apply failure | Internal | APPLY_FAILED | uncertain |
| Unknown unstructured mutation status | original code | RPC_FAILURE | uncertain |
| Read fence failure | Unavailable | READ_FENCE_FAILED | no_mutation |

Unknown unstructured mutation errors must never be upgraded to a known rejection.
Their raw internal message is redacted. Read `no_mutation` means no KV data effect;
default quorum reads do not append Raft entries; explicit barrier mode may still
append a Raft barrier. See READ_FENCING.md for the current read contract.
An FSM keyspace rejection may be a committed no-op; `not_applied` does not mean
no log entry exists. Storage failure is different and retains fail-stop handling.

Fenced/no-leader errors carry an advisory Raft term and, when known, structured
leader address; entry-point redirects also retain the `quorumkv-leader` trailer.
Hints are not authorization or proof of current leadership. Validate configured
destinations and peer certificates; do not trust arbitrary redirect addresses.

`metadata.next_action` is advisory:

- `reconcile` for uncertain mutations, with **no RetryInfo**.
- `review_capacity` for keyspace exhaustion, with **no automatic retry advice**.
- `bounded_retry_if_no_prior_uncertainty` for selected rejected submissions.
- `review_request` otherwise, including intentional pre-submission cancellation.

Rejected admission/enqueue/transfer/no-leader/fenced calls include a RetryInfo
minimum delay of 100 ms; disk-pressure/probe failures use 1 s. These are backoff
hints, not a promise of recovery or permission to replay an earlier uncertain
mutation. The server neither sleeps nor retries. Callers still need bounded
attempts, deadlines, jitter, cancellation and their own uncertainty history.

A timeout stops the RPC wait, never an admitted Raft future. Admission and capacity
reservations remain owned until that future resolves. A linearizable read may
help an application reconcile state but cannot universally prove a particular
write applied or failed when concurrent writers or overwrites exist. Do not
blindly repeat a mutation. Durable operation identity, deduplication/CAS and a
first-party reconciliation client remain separate uncompleted requirements.

Mapping basis: adopted hashicorp/raft v1.7.1 ApplyLog timeout select, follower/
candidate rejection, leader in-flight cleanup and transfer rejection. Shutdown
and post-submission deadline races remain conservative. Existing fault-history
oracles were not changed to classify new metadata as success or discard uncertain
attempts. End-to-end audit/tracing and incident-ID association are still open.
