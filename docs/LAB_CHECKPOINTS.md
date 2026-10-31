# Bounded lab supervision and checkpoint recovery

The portable supervisor uses only new local synthetic stores, loopback addresses,
fresh lab PKI and an owned child process. It accepts no existing cluster/data target.
From a copied source checkout with Go from go.mod and Python 3.12:

```sh
go test ./test/fault -run '^$' -timeout=20s
python -B -m unittest discover -s diagnostics -v
python -B scripts/run_lab.py --profile smoke --seconds 15 --transport mtls
```

The smoke runs checks against real local Raft members; it is not shortened soak
evidence. Run artifacts go into a new ignored test/fault/results/v2 directory.
Keep the entire output private: it includes temporary keys, stores and queues.
The supervisor records source hashes, child identity, resource ceilings, histories,
diagnostic outcomes and exit status. It never writes product acceptance state.
A separate explicit soak profile still requires 86,400 real seconds and mTLS;
do not start it without the corresponding human scope and unchanged preflight gates.

Checkpoint publication now uses an exclusively created unique sibling, flush/fsync,
then replace. PermissionError gets at most eight attempts with 25ms retry sleeps;
reads use the same sharing-denial retry policy. The sleep budget is 175ms, not a
real-time bound on filesystem calls or process scheduling. Invalid/corrupt JSON is
never retried as success, and observations are capped at 1 MiB.

If replacement exhausts its budget, the old destination is untouched and the new
uniquely named sibling is retained for explicit diagnosis/recovery. Do not silently
promote an orphan to PASS. A restart writes a new sibling without clobbering it.
Concurrent publishers give last-successful-writer-wins atomic visibility; this is
not transactional merging of independent state edits. Parent-directory/power-loss
durability is not established. Use one owner for acceptance metadata.

Tests cover transient/persistent denial, concurrent reader/publisher visibility,
actual process termination before replace, restart with retained orphan, corrupt
JSON and rejected budgets. The first Windows concurrency attempt failed on reader
sharing denial and is retained privately; bounded reader retry fixed that fixture.
The earlier 8h05m WinError 5 soak failure remains failed and its exact cause is
unproven. These tests do not establish supervisor readiness over 24 hours.

Worker offline analysis remains separate from consensus and cannot authorize
experiments on existing clusters. Model proposals cannot change membership, trust,
acknowledgement histories or acceptance labels. CI checks Python and fixed isolated
Go experiments on Linux and Windows; a proposed workflow is not hosted CI evidence.
