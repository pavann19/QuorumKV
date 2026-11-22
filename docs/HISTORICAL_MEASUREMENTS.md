# Historical measurement evidence: October 26, 2026

These are bounded local experiments on Windows amd64, Go 1.27.3, using three OS processes on localhost. The recorded server revision was `ec6c8cc72fd1c64d62dd8ba00d28d36095355c54+working-tree`: the commit alone does not reconstruct that dirty source. [Source file hashes](evidence/historical-20261026/source-hashes.json) identify the recorded source snapshot. These receipts do not measure current-source performance, independent-host availability, production operation or power-loss durability.

## Failover: ten trials

The [complete failover receipt](evidence/historical-20261026/failover_time.json) contains all ten samples. The conventional median is `(1969.487 + 2019.568) / 2 = 1994.5275 ms`, rounded to **1.995 s**. The p90 uses sorted index `floor(0.9 * (10 - 1)) = 8`: **2429.314 ms**, rounded to **2.429 s**. The receipt's `p50_ms = 1969.487` uses the lower order statistic; it is not the conventional median.

The measurement runs from leader termination to a successful write on the surviving majority and includes probe/polling overhead. It measures local process failover, not recovery from a physical host or network failure. The older 2.006 s / 2.576 s narrative in DECISIONS did not identify a matching receipt; those values are not the October experiment. The separate committed September receipt under `bench/results` must also remain a separate run.

## Fault histories: 607 recorded RPC attempts

| Scenario | Attempts | Published history |
|---|---:|---|
| Leader partition | 40 | [JSON](evidence/historical-20261026/leader_partition.json) |
| Minority partition | 40 | [JSON](evidence/historical-20261026/minority_partition.json) |
| Rolling restarts | 488 | [JSON](evidence/historical-20261026/rolling_restarts.json) |
| Network delay | 18 | [JSON](evidence/historical-20261026/network_delay.json) |
| Double-leader attempt | 21 | [JSON](evidence/historical-20261026/double_leader_attempt.json) |

All attempts are retained, including 150 acknowledged outcomes, 265 uncertain writes, 180 rejected writes and 12 failed reads. Thus 607 is not a successful-operation count. The five histories use logical event ordering and observed RPC attempts only. Porcupine models uncertain writes as potentially applied and excludes rejected writes and failed reads. Passing these histories does not establish correctness for unobserved executions or prove that two leaders committed conflicting writes. The separate lost-reply experiment is excluded from 607.

## Concurrency: 2,304 acknowledged writes

The [complete concurrent receipt](evidence/historical-20261026/concurrent_candidate.json) retains every operation sample and all nine run summaries: 1, 4 and 16 clients, three repetitions each, 256 measured writes per run, 1 KiB payloads, 32 warmup writes per run, no retries. All 2,304 measured writes were acknowledged with zero recorded errors; each run records 256 verified keys. Readback was outside the timed phase. This is a finite fixed-count experiment, not a sustained throughput ceiling. The receipt records readback totals, not individual readback responses; replaying it cannot independently repeat the original readback.

## Verification and provenance

From the repository root:

```
python scripts/verify_historical_measurements.py
go test ./test/fault -run TestHistoricalOctoberHistories -count=1
```

The first command verifies published SHA256 hashes, recomputes the statistics and counts, and checks all concurrent samples against their summaries. The second replays all five histories through the current validation and Porcupine model, independently of stored `linearizable` verdicts. Neither command reruns the historical executable.

[The manifest](evidence/historical-20261026/manifest.json) records original and published SHA256 hashes and the exact export transformation. Original local receipts remain unchanged. Published synthetic records retain observations and ordering; JSON whitespace is compacted, verbose nonempty RPC error strings become `recorded RPC error`, and hardware CPU count is omitted. The original error classification, outcomes and all modeling fields remain present. These are sanitized projections, not byte-identical original files. No bulk process logs are included.
