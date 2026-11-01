# Contributing to QuorumKV

Start with the [documentation index](docs/README.md), [verification status](docs/VERIFICATION.md) and [security policy](SECURITY.md). Open an issue for a substantial interface or design change; keep fixes small enough to review and reproduce.

## Local workflow

1. Branch from current `main` using `fix/`, `feat/` or `docs/` and a short descriptive suffix.
2. Preserve unrelated work. Use synthetic fixtures and isolated stores; never reset shared data to make a test pass.
3. Implement one coherent change with the meaningful regression and documentation it needs.
4. Inspect staged content and `git diff --cached --check`. Stage explicit paths. Exclude secrets, personal data, runtime databases, model weights and bulk raw logs.
5. Write a clear commit subject explaining the change. Preserve actual human attribution; do not add generated co-author trailers or claim nonexistent reviewers.
6. Open a PR to `main`, report exact commands/results and wait for required checks. Do not force-push public history or bypass branch protection.

## Verification

Use the Go version in `go.mod`. Start with `go test -short ./...`; run `go test ./...` for process/fault coverage and the documented race checks for runtime changes. Worker/checkpoint changes require the fixture compile and Python suite in [testing](docs/TESTING.md).

Run relevant checks before submission; CI verifies the submitted revision again. Mark checks not run as NOT RUN and explain why. Documentation-only edits need valid relative links, accurate commands and a clean whitespace diff; passing earlier runtime CI does not certify a new revision.

Preserve WAL/fsync behavior, quorum barriers, identity enforcement and recorded RPC uncertainty. Do not relabel timeouts as definite failures or allow diagnostics to change membership, trust or consensus state.

Preserve negative controls and failed artifacts. Historical local measurements, hosted CI and production operation are distinct evidence. Do not introduce a long soak, paid cloud job or external deployment as an implicit part of a contribution.

## Review and compatibility

Explain the triggering problem, resulting behavior, tests and remaining limits. Call out API, configuration, schema and storage changes, including upgrade/restore implications. Do not edit applied database migrations or promise an untested downgrade. Review [maintenance rules](MAINTAINING.md) before merging or releasing.

Contributions retain their real authorship and are reviewed under the repository's [license](LICENSE). Third-party code must retain required notices and upstream terms.
