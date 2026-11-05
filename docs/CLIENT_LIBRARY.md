# First-party Go client foundation

Status: conservative retry implementation; H8 remains PARTIAL. Durable idempotency
keys, operation-status queries, version/capability negotiation and API compatibility
qualification are still open. This client does not promise exactly-once writes.

Import `github.com/pavann19/quorumkv/client`. `New` accepts 1..16 static address /
peer certificate URI bindings, a client URI, CA, certificate and key files. It
uses the same TLS 1.3, certificate-chain, exact URI and client-role policies as the
node transport. There is no plaintext fallback or redirect-driven discovery.
Each connection uses a literal passthrough target and the configured authority;
untrusted hints cannot create connections, change trust or introduce resolvers.
Connections are reused and owned by the client; call `Close` when finished.

Defaults are three attempts, one second per attempt and five seconds for the
whole operation, capped at eight attempts / five seconds per attempt / thirty
seconds overall. An earlier caller deadline wins. Retry sleeps honor cancellation
and use an exponential minimum delay (100 ms up to 1 s) plus positive jitter. A valid server RetryInfo
is a minimum; advice above one second stops retries instead of retrying early.
Built-in configured gRPC retry policies are disabled. Transport-transparent
retries, where gRPC knows an RPC was not delivered, remain gRPC's responsibility.

`Get` returns a protobuf response, receipt and error. `Put` and `Delete` return
a receipt and error. Keys/values are copied after local size validation; callers
must not modify their input concurrently while the method is copying it. The
client supports concurrent calls. Receipts contain operation/attempt UUIDv7s,
configured endpoint, times, status, stable reason when verified and outcome.
They contain no keys/values. `*client.Error` retains the receipt and original
cause. Applications needing durable reconciliation must persist receipts in
their own operation journal; this in-memory library is not that journal.

Read-only unavailable/deadline failures can retry. Mutations retry only exact
known pre-submission rejection reasons/codes with one structured ErrorInfo in
the `quorumkv` domain matching the current attempt ID and `not_submitted` outcome.
Capacity rejections do not retry. Unknown statuses, missing/wrong IDs, conflicting
details, deadlines or lost replies stop mutations as `uncertain`. A later read
never rewrites an uncertain receipt. Confirmed rejections can redirect only to
an exact configured address; other hints are ignored in favor of the next static
endpoint. Per-operation failed-endpoint and visit tracking prevents a stale hint
from trapping discovery at a stopped member or starving configured alternatives. No outcome is inferred from arbitrary message text or trailers.

Every attempt gets a fresh correlation UUIDv7; the operation ID links the local
attempt history. Neither is a deduplication key. Repeating a mutation after an
uncertain receipt, even with identical key/value bytes, can overwrite a concurrent
writer. The client deliberately provides no automatic replay/reconciliation
method until a durable server identity contract exists. Existing server behavior
allowing repeated correlation IDs remains unchanged.

Synthetic coverage includes strict retry classification, bounded attempts,
cancellation during backoff, untrusted redirects and disabled-client behavior.
An isolated three-node mTLS test redirects from a follower, drops a reply after
a real committed write and checks that the client attempts it once and retains
uncertainty, then stops the leader and checks read recovery and a new-leader
delete. A wrong peer-URI test must fail TLS without reaching the FSM. These are
local fixtures, not production HA or host-failure evidence. See retained Linux
checkpoint receipts for current test status.

Current local checkpoint: retained local checkpoint (not distributed with source): Linux 161 and Windows race 168 top-level checks, identical source maps, zero actual test skips. These totals include existing regression checks, not just new client tests.

A later WAL qualification run exposed stale-hint discovery oscillation in the
previous client candidate. That aggregate remains FAIL. Endpoint selection and
minimum backoff were corrected with a three-member regression test; current
combined qualification passes Linux 159 affected and Windows race 177 top-level
checks with matching source (WAL foundation checkpoint). Earlier receipts describe their exact source
and do not establish the current candidate.
