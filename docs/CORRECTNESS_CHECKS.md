# Correctness Hardening Verification

Verified locally on 2026-10-24, Windows amd64, Go 1.27.3, with the installed
MSYS2 UCRT64 GCC compiler targeting x86_64-w64-mingw32.

## Reproduced Failures

The initial regression tests failed against the original implementation:

| Regression | Observed failure | Correction |
| --- | --- | --- |
| Reopen a torn WAL, append, then replay again | The later acknowledged write disappeared behind the unrepaired tail | Truncate and sync the validated prefix before accepting new appends |
| Disconnect a writable leader from both peers, then Get immediately | The isolated node returned local data without quorum confirmation | Commit and await a Raft barrier before reading |
| Apply a committed command with a closed local WAL | The FSM returned an error and could continue past the missing mutation | Panic on committed-command storage failure to stop the process |

Additional regressions cover oversized WAL lengths, rejection of writes after
an I/O failure, and isolation of store values from caller and snapshot buffers.
The read regression also checks successful healthy reads and cancellation.

## Final Local Checks

| Check | Result |
| --- | --- |
| Full suite with inherited `GOFLAGS=-race`, `-count=1 -timeout 300s` | PASS |
| Parent and subprocess race reports | None produced |
| `go build ./...` | PASS |
| `go vet ./...` | PASS |
| `go mod verify` | PASS |
| `govulncheck@v1.8.0 ./...` | No vulnerabilities found |
| `actionlint@v1.7.7 .github/workflows/ci.yml` | PASS |
| `go test -tags=bench ./bench/... -run '^$'` | PASS, compilation only |

The full race run includes real child node processes: setting `GOFLAGS=-race`
instruments the `go build` calls inside the harnesses. Locally, `GORACE` used
`halt_on_error=1 log_path=quorumkv-race-20261024`; the Windows drive-letter form
was rejected by the sanitizer. All package directories were checked for reports
after the suite. CI uses its Linux runner temporary directory and fails if any
subprocess race report exists.

Final fault histories accepted by the existing Porcupine model:

| Scenario | Recorded operations |
| --- | ---: |
| Leader partition | 40 |
| Minority partition | 40 |
| Rolling restarts | 32 |
| Network delay | 18 |
| Double leader attempt | 18 |

These counts describe recorded workload histories, not an exhaustive proof of
linearizability. Workloads and checker semantics were unchanged. Generated JSON
was excluded from this patch; benchmarks were not rerun or remeasured.

## Clean Checkout Demo

On 2026-10-24, a fresh local clone of commit `21d12f3` started with an empty
`git status --porcelain`. The documented correctness run was executed there as
`go test ./... -count=1 -timeout 180s` and passed, including the real cluster,
crash-recovery, and all five fault scenarios. The added flags disable caching
and bound the run; the workload is the documented `go test ./...` workload.
The clone used the existing local Go module cache, so this verifies a clean
source checkout rather than a machine with no previously downloaded modules.

Generated fault JSON remained in the temporary clone and was not added to the
release commits. The optional measurement run was not executed. The final diff
review found no additional blockers in the scoped hardening patch. Hosted CI
and cross-platform verification remain pending an authorized push.

## Dependencies and Limits

The final versions are Go 1.27.3, gRPC 1.83.2, protobuf 1.36.11, and
`golang.org/x/net` 0.60.0, with corresponding module checksum updates. A fresh
scan found additional fixes beyond the earlier audit, so both the Go toolchain
and HTTP/2 dependencies were updated. The final scan passed at the date above.

CI now reads the Go version from `go.mod`, instruments cluster subprocesses,
checks their race reports, and runs a pinned vulnerability scanner. The workflow
has been validated locally; a hosted GitHub run for these changes is pending.

Reads now incur a quorum-committed log barrier. Storage failures intentionally
stop a replicated node, reducing availability until working storage is restored.
See [the ownership boundary](RAFT_BOUNDARY.md) for recovery guarantees and
limitations. This verification does not establish authentication, TLS, arbitrary
disk-corruption recovery, power-loss durability, or production readiness.
