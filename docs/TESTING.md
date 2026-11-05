# Testing and evidence

Run commands from the repository root with the Go version required by `go.mod`. Ordinary Go tests require process execution and available loopback ports, not Docker. Initial module retrieval requires network access or a populated module cache.

| Command | Checks | Boundary |
|---|---|---|
| `go test -short ./...` | Fast selected tests | Skips slower process scenarios |
| `go test ./...` | Unit, storage, cluster and fault tests | Finite workloads; excludes tagged benchmarks |
| `go test -race ./internal/... -count=1` | Instrumented internal packages | Requires a compatible C compiler |
| `go test ./test/fault -run '^$' -timeout=20s` | Compile the fixed fault fixture | Compilation only |
| `python -B -m unittest discover -s diagnostics -v` | Worker/checkpoint regressions | First compile the fixture as CI does |

For process tests, propagate `GOFLAGS=-race` to child builds as the workflow does. Instrumenting only the parent test does not instrument child node binaries. Hosted diagnostics jobs specify their Python version and platform matrix; do not infer macOS coverage.

History checks preserve operations, retries, acknowledgements and uncertain outcomes. Inspect the model and negative controls in [correctness checks](CORRECTNESS_CHECKS.md). Acceptance is scoped to recorded histories and modeled faults, not arbitrary storage or power failures.

Benchmarks can replace files at documented output paths. Preserve artifacts before rerunning. Sequential single-client operations per second are not a concurrent throughput ceiling. Compile-only checks and skipped scenarios are not runtime PASS evidence.

See the [demo](DEMO.md) for artifact locations and [verification status](VERIFICATION.md) for exact hosted revisions and the failed historical soak. No new long soak was run for this documentation refresh.

CI also runs a digest-pinned Gitleaks scan of reachable Git history. Secret scanning complements tests; it is not proof that every possible sensitive value can be detected.

## Source-pinned local receipts

Run `python -B scripts/run_checks.py` for functional, vet and diagnostics receipts.
Use `--race` to instrument Go tests and child builds (a compatible C compiler is
required), and `--measurements` for optional finite benchmarks. On Windows set
`CC` and the compiler directory in the invoking process environment as needed.
The runner pins the effective Go executable, records source/workflow/service
hashes and rejects source drift. It explicitly excludes Go packages under fault
and benchmark results directories so archived source is preserved without being
executed as the current candidate. Inspect `manifest.json` and per-command logs
in the new ignored evidence directory. No long soak is launched by this runner.
