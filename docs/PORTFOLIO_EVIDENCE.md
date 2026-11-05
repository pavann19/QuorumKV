# Portfolio evidence - 5 November 2026

Status: PASS_LOCAL_LINUX_FULL_RACE for the curated candidate on 5 November 2026.
All 191 top-level Go checks passed with no skips, including the 100 populated
process-crash trials, both supported snapshot profiles, authenticated process
leader failure, lost replies and known-invalid history controls. Child node and
writer processes inherited race instrumentation. The copied source ran offline
inside an owned Linux volume with 2 CPUs, 1 GiB memory, a read-only container root
and no host port bindings. Executed Go source and dependency files match this
candidate. A schema EOF whitespace cleanup produced byte-identical generated Go.

A separate native Linux functional selection passed 181 checks across 13 package
groups. The retained diagnostics/checkpoint implementation passed all 22 Python
checks on Windows. Candidate `go vet` and module integrity checks passed.

The broad Windows race run failed after D: exceeded the lab's 95% disk watermark;
write refusal and failed outputs were preserved. Earlier result postprocessing
also failed and is retained. The successful Linux run does not erase those runs
or establish a passing full Windows candidate. Physical power loss, sustained
capacity, completed 24-hour endurance and production operation remain unverified.

Dependency scanning found no reachable vulnerabilities and none in imported
packages, but reported module-only HTML advisories GO-2026-6866 and GO-2026-6860
in `golang.org/x/net` v0.60.0, fixed upstream in v0.62.0. The affected HTML packages
are not imported by this candidate. These findings are retained, not described
as a vulnerability-free dependency graph. The scanner was govulncheck v1.8.0
against the official Go database; its source-call-graph scope was Windows, not
all OS/build-tag combinations. Existing notices cover all 24 imported external
application/test modules; this is source attribution, not binary-release clearance.

| Resume claim | Authoritative implementation / check | Boundary |
|---|---|---|
| 100 populated acknowledged-prefix recoveries | [WAL](../internal/wal/wal.go), [crash trials](../test/crashrecovery/crash_test.go) | Process termination, not power loss; an unobserved durable record is permitted. |
| Exact snapshot recovery | [FSM](../internal/raftnode/fsm.go), [snapshot test](../internal/raftnode/snapshot_test.go) | Local disk-backed cluster; graceful replica restarts; exact 48 retained/16 deleted/stale-key checks. |
| Lost replies remain uncertain | [recorder](../test/fault/recording.go), [controls](../test/fault/recording_test.go), [model](../test/fault/model_test.go) | Finite observed histories, not universal linearizability or exactly-once writes. |
| Authenticated leader failure | [process test](../test/securecluster/cluster_test.go), [identity](../internal/identity/identity.go) | Disposable localhost PKI/processes; no host failure-domain or deployed CA claim. |

The [demo command](PORTFOLIO_DEMO.md) reproduces this selection. Raw run receipts
remain local/private, including source hashes, commands, exit status and failed
attempts. This portable summary links to source rather than machine-specific
artifact paths. Publication must include these source files to make the commands
usable. A documentation pass is not a secret/history/license release audit.

Earlier measurements and hosted checks retain their historical source scope.
Unfinished receipt-aware experiments are excluded from the candidate and
these claims. Product performance/endurance gates remain incomplete.
