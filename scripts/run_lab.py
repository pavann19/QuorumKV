"""Supervise one owned synthetic lab process with predeclared stop gates.

--profile smoke --seconds 60 exercises the same code as --profile soak, which
always runs 86400 real seconds. No timer scaling or background launch here.
"""
from datetime import datetime, timezone
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "diagnostics"))
from worker import Queue, GOAL, digest, work_once
from checkpoint import save, read, read_bytes

def source_identity():
    result = {}
    for prefix in ("cmd", "internal", "proto", "test", "bench", "diagnostics", "scripts"):
        for folder, dirs, files in os.walk(ROOT / prefix):
            dirs[:] = [d for d in dirs if d not in ("results", "__pycache__", ".git")]
            for name in files:
                path = Path(folder) / name
                if path.suffix in (".go", ".py"):
                    result[path.relative_to(ROOT).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    for name in ("go.mod", "go.sum"):
        result[name] = hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
    return result


def resources(pid):
    if os.name == "nt":
        from ctypes import wintypes
        class Memory(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("faults", wintypes.DWORD)] + [(name, ctypes.c_size_t) for name in ("peak", "rss", "a", "b", "c", "d", "e", "f", "private")]
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(ctypes.c_uint64)] * 4
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
        handle = kernel.OpenProcess(0x1000 | 0x10, False, pid)
        if not handle:
            raise OSError("cannot observe owned process")
        try:
            mem = Memory(); mem.cb = ctypes.sizeof(mem)
            if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(mem), mem.cb):
                raise OSError("process memory observation failed")
            created, ended, system, user = (ctypes.c_uint64() for _ in range(4))
            if not kernel.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(ended), ctypes.byref(system), ctypes.byref(user)):
                raise OSError("process CPU observation failed")
            return {"rss_bytes": mem.rss, "private_bytes": mem.private, "cpu_seconds": (system.value + user.value) / 1e7}
        finally:
            kernel.CloseHandle(handle)
    fields = Path(f"/proc/{pid}/stat").read_text().split()
    return {"rss_bytes": int(fields[23]) * os.sysconf("SC_PAGE_SIZE"), "cpu_seconds": (int(fields[13]) + int(fields[14])) / os.sysconf("SC_CLK_TCK")}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=["smoke", "soak"], default="smoke")
    parser.add_argument("--seconds", type=int, default=60, help="smoke only; ignored for soak")
    parser.add_argument("--transport", choices=["mtls", "plaintext"], default="mtls")
    args = parser.parse_args()
    if args.profile == "soak" and args.transport != "mtls":
        parser.error("soak requires mTLS")
    required = 86400 if args.profile == "soak" else args.seconds
    if args.profile == "smoke" and not 10 <= required <= 600:
        parser.error("smoke must be 10..600 seconds")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    output = ROOT / "test/fault/results/v2" / f"lab-{args.profile}-{args.transport}-{stamp}"
    output.mkdir(parents=True, exist_ok=False)
    evidence = output / "diagnostic-evidence"; evidence.mkdir()
    (evidence / "LAB_ONLY").write_text("synthetic diagnostic fixtures only\n")
    source = source_identity()
    source_hash = digest(json.dumps(source, sort_keys=True).encode())
    env = {k: v for k, v in os.environ.items() if not k.startswith("QUORUMKV_") and k not in ("GOFLAGS", "GORACE")}
    goroot = subprocess.check_output(["go", "env", "GOROOT"], cwd=ROOT, env=env).decode().strip()
    bindir = Path(goroot) / "bin"; go = str(bindir / ("go.exe" if os.name == "nt" else "go"))
    env["PATH"] = str(bindir) + os.pathsep + env.get("PATH", "")
    env["GOTOOLCHAIN"] = "local"; env["GOFLAGS"] = ""
    binary = output / ("lab.exe" if os.name == "nt" else "lab")
    manifest = {"status": "preflight", "profile": args.profile, "transport": args.transport, "required_seconds": required, "source_sha256": source, "source_version": source_hash,
                "go": subprocess.check_output([go, "version"], cwd=ROOT, env=env).decode().strip(), "go_executable": go,
                "hardware": {"os": platform.platform(), "machine": platform.machine(), "logical_cpus": os.cpu_count()},
                "gates": {"lab_rss_bytes": 1536 << 20, "average_cpu_cores": 2, "run_disk_bytes": 1 << 30, "log_bytes": 64 << 20, "queued_jobs": 8, "max_queue_age_seconds": 60, "max_observation_gap_seconds":60, "job_failures": 0, "daily_jobs": 500},
                "scope": "three in-process members with separate persistent stores on one host; no host HA, power loss, production capacity or uptime claim",
                "model_version": "deterministic-offline-v1", "started_utc": datetime.now(timezone.utc).isoformat()}
    save(output / "manifest.json", manifest)
    print("Evidence: " + str(output), flush=True)
    with (output / "build.log").open("wb") as log:
        build = subprocess.run([go, "build", "-o", str(binary), "./cmd/quorumkv-lab"], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    if build.returncode:
        manifest.update(status="FAIL", failure="build failed; see build.log")
        save(output / "manifest.json", manifest)
        raise RuntimeError("lab build failed")
    manifest["binary_sha256"] = digest(binary.read_bytes())
    lab = output / "cluster"
    command = [str(binary), "-profile=" + args.profile, "-transport=" + args.transport, "-duration=" + str(required) + "s", "-out=" + str(lab)]
    manifest["command"] = command
    queue = Queue(output / "diagnostics.sqlite", evidence, source_hash)
    started = time.monotonic(); job_count = 0; last_job = -1000
    last_wall = time.time()
    last_scenarios = -21600
    scenario_runs = 0
    proc = None
    try:
        with (output / "lab.log").open("wb") as log, (output / "resources.jsonl").open("w", encoding="utf-8") as observations:
            proc = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
            manifest.update(status="running", owned_pid=proc.pid, supervisor_pid=os.getpid())
            save(output / "manifest.json", manifest)
            while proc.poll() is None:
                elapsed = time.monotonic() - started
                now_wall=time.time()
                if now_wall-last_wall>60 or now_wall-last_wall < -5:
                    raise RuntimeError("observation gap/clock discontinuity; continuous soak not verified")
                last_wall=now_wall
                try:
                    stats = resources(proc.pid)
                except OSError:
                    if proc.poll() is not None:
                        break
                    raise
                disk = sum(p.stat().st_size for p in output.rglob("*") if p.is_file())
                metrics = queue.metrics()
                pending = sum(metrics["states"].get(s, 0) for s in ("queued", "retry_wait", "running"))
                record = {"elapsed_seconds": elapsed, **stats, "disk_bytes": disk, "diagnostics": metrics}
                observations.write(json.dumps(record) + "\n"); observations.flush()
                manifest["heartbeat_utc"] = datetime.now(timezone.utc).isoformat()
                manifest["elapsed_seconds"] = elapsed
                manifest["last_resources"] = record
                if not manifest.get("initial_public_config_sha256") and (lab / "preflight.json").is_file():
                    configs = [p for p in lab.rglob("*") if p.is_file() and (p.name.endswith("-identity.json") or p.suffix == ".crt" or p.name == "ca.pem")]
                    manifest["initial_public_config_sha256"] = {p.relative_to(output).as_posix(): digest(p.read_bytes()) for p in configs}
                save(output / "manifest.json", manifest)
                if stats["rss_bytes"] > manifest["gates"]["lab_rss_bytes"] or (elapsed > 30 and stats["cpu_seconds"] / elapsed > 2) or disk > 1 << 30 or (output / "lab.log").stat().st_size > 64 << 20 or pending > 8 or metrics["queue_age_seconds"] > 60:
                    raise RuntimeError("resource/queue stop gate exceeded")
                if args.profile == "soak" and elapsed-last_scenarios >= 21600:
                    # Re-exercise actual worker process crash/restart and isolated
                    # fixtures for dependency outage, injection, duplicate/lease
                    # delivery during the soak, never against an existing service.
                    with (output / f"adversarial-scenarios-{scenario_runs:02d}.log").open("wb") as scenarios:
                        checked = subprocess.run([sys.executable,"-B","-m","unittest","discover","-s","diagnostics","-v"],cwd=ROOT,env=env,stdout=scenarios,stderr=subprocess.STDOUT,timeout=45)
                    if checked.returncode:
                        raise RuntimeError("during-soak adversarial worker scenarios failed")
                    scenario_runs += 1
                    last_scenarios = elapsed
                    manifest["adversarial_scenario_runs"] = scenario_runs
                latest = lab / "latest-history.json"
                interval = 300 if args.profile == "soak" else 10
                if latest.is_file() and elapsed - last_job >= interval:
                    raw = read_bytes(latest)
                    if len(raw) > 65536:
                        raise RuntimeError("evidence too large for diagnostic worker")
                    filename = f"history-{job_count:05d}.json"
                    (evidence / filename).write_bytes(raw)
                    job = {"job_id": f"lab-{job_count}", "idempotency_key": f"lab-{job_count}", "goal_version": GOAL, "source_version": source_hash, "schema_version": 1, "tenant": "lab", "scope": "readonly", "tool": "history_summary", "evidence_refs": [{"path": filename, "sha256": digest(raw)}], "deadline": time.time() + 60}
                    queue.enqueue(job); queue.enqueue(job)  # Deliberate duplicate has no second effect.
                    if not work_once(queue):
                        raise RuntimeError("diagnostic claim failed")
                    state = queue.db.execute("SELECT state FROM jobs WHERE job_id=?", (job["job_id"],)).fetchone()[0]
                    if state != "prepared":
                        raise RuntimeError("diagnostic job did not prepare")
                    job_count += 1; last_job = elapsed
                    if job_count % 20 == 0:
                        queue.close(); queue = Queue(output / "diagnostics.sqlite", evidence, source_hash)
                if elapsed > required + 90:
                    raise RuntimeError("wall-time overrun")
                time.sleep(2 if args.profile == "smoke" else 10)
            if proc.returncode:
                raise RuntimeError(f"owned lab exited {proc.returncode}")
            summary = read(lab / "summary.json")
            if summary["status"] != "PASS" or summary["elapsed_seconds"] < required or not job_count:
                raise RuntimeError("duration/invariant/diagnostic completion gate failed")
            if source_identity() != source:
                raise RuntimeError("source changed during run")
            manifest.update(status="PASS", summary=summary, diagnostic_jobs=job_count, diagnostics=queue.metrics())
    except BaseException as exc:
        manifest.update(status="FAIL", failure=type(exc).__name__ + ": " + str(exc))
        if proc and proc.poll() is None:
            proc.terminate(); proc.wait(timeout=20)  # Only this owned process; no external targets.
        raise
    finally:
        queue.close()
        manifest["elapsed_seconds"] = time.monotonic() - started
        manifest["finished_utc"] = datetime.now(timezone.utc).isoformat()
        save(output / "manifest.json", manifest)
    print(json.dumps({"status": manifest["status"], "evidence": str(output), "elapsed_seconds": manifest["elapsed_seconds"]}), flush=True)


if __name__ == "__main__":
    main()
