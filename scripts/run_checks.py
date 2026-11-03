"""Record bounded candidate checks without executing archived source artifacts.

Default: functional, vet and diagnostics. --race and --measurements are opt-in.
All test data is synthetic; this runner never starts an endurance profile.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

from run_lab import ROOT, source_identity


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--race", action="store_true", help="instrument Go tests and child node builds; requires a compatible C compiler")
    parser.add_argument("--measurements", action="store_true", help="also run the tagged finite measurement suite")
    args = parser.parse_args()
    out = ROOT / "test/fault/results/v2" / ("checks-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    out.mkdir(parents=True, exist_ok=False)
    env = {k: v for k, v in os.environ.items() if not k.startswith("QUORUMKV_") and k not in ("GOFLAGS", "GORACE")}
    bindir = Path(subprocess.check_output(["go", "env", "GOROOT"], cwd=ROOT, env=env).decode().strip()) / "bin"
    go = str(bindir / ("go.exe" if os.name == "nt" else "go"))
    env.update(PATH=str(bindir) + os.pathsep + env.get("PATH", ""), GOTOOLCHAIN="local", GOFLAGS="-race" if args.race else "")
    if args.race:
        env["GORACE"] = "log_path='" + (out / "race-report").as_posix() + "' halt_on_error=1"
    discovered = subprocess.check_output([go, "list", "./..."], cwd=ROOT, env=env).decode().splitlines()
    excluded = [p for p in discovered if "/test/fault/results/" in p or "/bench/results/" in p]
    packages = [p for p in discovered if p not in excluded]
    manifest = {"status": "running", "source_sha256": source_identity(),
                "go": subprocess.check_output([go, "version"], cwd=ROOT, env=env).decode().strip(),
                "head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip(),
                "candidate_packages": packages, "excluded_artifact_packages": excluded,
                "race_enabled": args.race, "measurements_enabled": args.measurements, "commands": [],
                "scope": "Local synthetic fixtures; no endurance run, deployment or hosted-CI claim."}

    def save():
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    commands = [
        ("functional", [go, "test", *packages, "-p=1", "-count=1", "-timeout=300s", "-json"]),
        ("vet", [go, "vet", *packages]),
        ("compile-worker-fixture", [go, "test", "./test/fault", "-run", "^$", "-timeout=20s"]),
        ("diagnostics", [sys.executable, "-B", "-m", "unittest", "discover", "-s", "diagnostics", "-v"]),
    ]
    if args.measurements:
        commands.append(("measurements", [go, "test", "-tags=bench", "./bench", "-count=1", "-timeout=300s", "-json"]))
    save()
    print("Evidence: " + str(out), flush=True)
    for label, command in commands:
        print("Running " + label, flush=True)
        artifact = out / label
        artifact.mkdir()
        command_env = dict(env, QUORUMKV_RESULTS_DIR=str(artifact))
        # The fixed diagnostics adapter intentionally strips Go instrumentation
        # overrides. Warm its actual build mode before its wall-time budget.
        if label in ("compile-worker-fixture", "diagnostics"):
            command_env["GOFLAGS"] = ""
            command_env.pop("GORACE", None)
        start = datetime.now(timezone.utc)
        with (out / (label + ".log")).open("wb") as log:
            try:
                result = subprocess.run(command, cwd=ROOT, env=command_env, stdout=log, stderr=subprocess.STDOUT, timeout=600)
                code = result.returncode
            except subprocess.TimeoutExpired:
                code = 124
                log.write(b"\nRunner wall-time budget exceeded (600 seconds).\n")
        manifest["commands"].append({"name": label, "argv": command, "exit_code": code,
                                    "status": "PASS" if code == 0 else "FAIL",
                                    "seconds": (datetime.now(timezone.utc) - start).total_seconds(), "log": label + ".log"})
        save()
    manifest["source_unchanged"] = source_identity() == manifest["source_sha256"]
    manifest["race_reports"] = [p.name for p in out.glob("race-report.*")]
    manifest["status"] = "PASS" if manifest["source_unchanged"] and not manifest["race_reports"] and all(c["exit_code"] == 0 for c in manifest["commands"]) else "FAIL"
    manifest["finished_utc"] = datetime.now(timezone.utc).isoformat()
    save()
    print(json.dumps({"status": manifest["status"], "evidence": str(out)}))
    return 0 if manifest["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
