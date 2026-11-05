# Recovery-memory profile

The opt-in Linux `measure` profile compares two recovery algorithms using the
same current CRC decoder, sync semantics, generated WAL, Go toolchain and declared
runtime controls. It does not compare two historical executables or establish
production startup/capacity guarantees.

```sh
# Linux only; this finite measurement is optional and separate from the demo.
go test -tags=measure ./internal/store -run '^TestMeasureStreamingRecoveryMemory$' -count=1 -timeout=180s -v
```

The input contains 2048 overwrites of one key with 64 KiB values: 134260736 bytes
on disk and 65540 logical live bytes. The generator uses a deterministic payload
and records the full input SHA-256. Each recovery runs in a fresh subprocess;
three paired repetitions alternate which algorithm runs first. Both must return
one exact retained value with the same content hash and leave the WAL unchanged.

The materializing control reconstructs the original algorithm: Replay retains
all decoded records, builds the map, then Open scans the WAL again. The candidate
uses production OpenWithLimits/OpenReplay, applying records directly to the
bounded map in one scan. The control's decode records remain reachable through
the peak-memory observation; no final steady-state heap comparison is claimed.

Predeclared gates require candidate process peak RSS <=64 MiB and <=60% of the
paired materializing control. Linux getrusage(RUSAGE_SELF).Maxrss supplies the
process high-water RSS, including process/runtime startup. Raw Go heap/total
allocation/GC counts and elapsed open seconds are also recorded, but elapsed
results have uncontrolled page-cache state and support no latency claim. Both
processes use GOMAXPROCS=2, GOGC=100 and GOMEMLIMIT=256MiB. The measurement refuses a
race-instrumented build. The Linux runner separately records its effective
2-CPU/1-GiB container limits, immutable image/binaries and ext4 filesystem.

The final 5 November 2026 profile passed all three pairs: streaming peak RSS was
16769024 bytes (about 16 MiB); materializing peaks ranged 235892736Ã¢â‚¬â€œ246669312 bytes
(about 225Ã¢â‚¬â€œ235 MiB). The earlier pre-instrumentation-guard profile is preserved as
a distinct run with its own source hashes and samples. The reported small run-to-
run variations are not hidden or turned into a universal reduction claim.

Evidence: retained local checkpoint (not distributed with source),
its nested `recovery-memory-*/report.json`, `memory-summary.json`, raw package logs
and source/binary hashes. The earlier run remains under
`readiness-recovery-memory-20261104-01`. This is native Linux execution of the
opt-in store profile, not a fresh full nine-package or Linux-race qualification.

Open boundaries include large live keyspaces, maximum-size values, concurrent
load and snapshot/restore copies, replay-time distributions, adverse disk I/O,
full member rather than isolated store startup, production memory sizing and the
actual 24-hour resource gate. Logical keyspace caps are not an RSS promise. This
bounded result supplies measured evidence for the single-pass recovery change;
it does not close those broader requirements.

Historical Linux container receipts are retained privately; this candidate has
not rerun the optional memory measurement. Do not infer current sizing results.
