# Portfolio demo: recovery, uncertainty and authenticated failover

Use synthetic local fixtures; no existing service or data directory is needed.
Install Go 1.27.3 or newer and allow loopback traffic. Leave enough free temporary
storage for builds and WAL files. The owned lab refuses writes above its 95%
disk watermark; free space outside this task rather than raising the gate.
A first build can take longer than the demo, depending on dependency/cache state.

From the repository root, run the command in [README](../README.md#reproduce-the-portfolio-demo).
Use a POSIX shell or PowerShell; both accept the quoted test expression. Do not
add `-short`, which skips the process fixtures. Go downloads dependencies/toolchain
when they are not already installed; the demo itself needs no external service.

For a staged explanation, run these separately:

```sh
go test ./test/crashrecovery -run '^TestCrashRecovery_100Trials$' -count=1 -timeout=180s -v
go test ./internal/raftnode -run '^TestSnapshotInstallAndRestart$' -count=1 -timeout=180s -v
go test ./test/fault -run '^(TestRecorderPreservesAppliedWriteWithLostReply|TestChecker_.*)$' -count=1 -timeout=180s -v
go test ./test/securecluster -run '^TestProcessMTLSKillLeaderAndRotation$' -count=1 -timeout=180s -v
```

| Show | Evidence to point at | What it establishes |
|---|---|---|
| Kill a writer after acknowledgements | Per-trial acknowledged/recovered prefix and final 100/100 result | Acknowledged prefix survives these process kills; one unobserved durable record can remain. |
| Restore a lagging replica | Snapshot integration passes exact state and restart checks | 48 retained keys, 16 deletions and stale-key removal, followed by replication after restart. |
| Lose the reply to an applied write | Lost-reply control and checker negatives pass | Unknown mutation outcomes stay in the history; invalid observations are rejected. |
| Kill the authenticated leader | Process mTLS scenario passes | Generated fixture identities, authorization, leader replacement and retained canary in a local process cluster. |

Say what each result means before moving on. No latency/throughput claim comes
from these checks. Passing a known-invalid control means the checker rejected
the bad history, not that the bad execution was accepted.

The original [broader demo](DEMO.md) covers all five partition/restart scenarios
and optional measurements. The short portfolio selection is not the full suite.

## Common setup issues

- Build failure: check the module's Go version and dependency access first.
- Bind failure: these are localhost fixtures; stop only your own conflicting
  test processes, or choose an available environment. Do not kill shared services.
- Disk refusal: use a sufficiently empty owned test filesystem; keep production
  defaults and lab gates unchanged.
- On constrained Windows systems, set `TEMP`, `TMP` and `GOTMPDIR` to a new owned
  directory on a drive with headroom before running. Do not point tests at an
  existing member directory.
- Preserve failed output and report it. A later pass does not erase the failure.

This demo creates no deployment, release or availability guarantee. TLS fixture
keys and raw histories are runtime artifacts, not files to publish with source.
