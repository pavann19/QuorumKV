"""Verify the published, bounded October 26 historical measurement receipts."""
import hashlib
import json
import statistics
from pathlib import Path


def main():
    base = Path(__file__).resolve().parents[1] / "docs/evidence/historical-20261026"
    read = lambda name: json.loads((base / (name + ".json")).read_text())
    for name, digest in read("manifest")["published_sha256"].items():
        assert hashlib.sha256((base / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest() == digest, name
    failover = read("failover_time")
    samples = sorted(failover["samples_ms"])
    assert len(samples) == failover["sample_size"] == 10
    assert round(statistics.median(samples) / 1000, 3) == 1.995
    assert round(samples[int(.9 * (len(samples) - 1))] / 1000, 3) == 2.429
    counts = {"leader_partition": 40, "minority_partition": 40,
              "rolling_restarts": 488, "network_delay": 18,
              "double_leader_attempt": 21}
    for name, count in counts.items():
        history = read(name)
        assert len(history["ops"]) == history["attempt_count"] == count
        observed = {}
        for op in history["ops"]:
            observed[op["outcome"]] = observed.get(op["outcome"], 0) + 1
        assert observed == history["outcomes"], name
    assert sum(counts.values()) == 607
    runs = read("concurrent_candidate")["runs"]
    assert len(runs) == 9
    assert {(r["clients"], r["repetition"]) for r in runs} == {(c, r) for c in (1, 4, 16) for r in range(3)}
    for run in runs:
        assert len(run["samples"]) == run["operations"] == run["verified_keys"] == 256
        assert run["errors"] == 0
        assert len({s["operation_id"] for s in run["samples"]}) == 256
        assert len({s["key"] for s in run["samples"]}) == 256
        for sample in run["samples"]:
            assert sample["acknowledged"] and sample["error_code"] == "OK"
            assert sample["return_ns"] - sample["call_ns"] == sample["latency_ns"] >= 0
    assert sum(r["operations"] for r in runs) == 2304
    print("PASS: 10 failover samples (median 1.995 s, p90 2.429 s); 607 RPC attempts; 2304 acknowledged writes and recorded readbacks, zero errors")


if __name__ == "__main__":
    main()
