#!/usr/bin/env python3
"""Run the expanded tmux-wrapper contract against a real SSH/CANN target."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
TRANSPORT = ROOT / "metric" / "baselines" / "tmux_job_ssh.py"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def invoke(base: list[str], *arguments: str, expected: set[int] | None = None) -> dict[str, Any]:
    result = subprocess.run(
        [*base, *arguments], text=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, check=False,
    )
    if result.returncode not in (expected or {0}):
        raise RuntimeError(
            f"transport failed ({result.returncode}): {' '.join(arguments[:3])}: "
            f"{result.stderr.strip()[-1000:]}"
        )
    stream = result.stdout if result.stdout.strip() else result.stderr
    for line in reversed(stream.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise RuntimeError("transport returned no JSON object")


def remote_text(host: str, path: str) -> str:
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", host, "cat", path],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    if result.returncode:
        raise RuntimeError(f"cannot read remote acceptance file: {result.stderr.strip()}")
    return result.stdout


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="zhiyuan")
    parser.add_argument("--project", required=True)
    parser.add_argument("--cann-source", required=True)
    parser.add_argument("--device", default="0")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error(f"refusing to overwrite {args.output}")

    nonce = uuid.uuid4().hex[:12]
    remote_root = f"/tmp/tmux-wrapper-acceptance-{nonce}"
    remote_script = f"{remote_root}/tmux_job.py"
    job_root = f"{remote_root}/jobs"
    events = f"{remote_root}/events.txt"
    socket = f"tmux-wrapper-{nonce}"
    base = [
        sys.executable, str(TRANSPORT), "--host", args.host,
        "--remote-script", remote_script, "--root", job_root,
        "--socket", socket,
    ]
    checks: dict[str, bool] = {}
    started_at = utc_now()
    try:
        install = subprocess.run(
            [*base, "install"], text=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, check=False,
        )
        if install.returncode:
            raise RuntimeError(f"install failed: {install.stderr.strip()}")
        checks["atomic_remote_install"] = True
        invoke(base, "queue-create", "device", "--concurrency", "1")

        first_command = (
            f"printf 'first-start\\n' >> {events}; sleep 0.5; "
            f"printf 'first-end\\n' >> {events}"
        )
        first = invoke(
            base, "submit", "--queue", "device", "--client-request-id", "first",
            "--cwd", args.project, "--", "bash", "-lc", first_command,
        )
        replay = invoke(
            base, "submit", "--queue", "device", "--client-request-id", "first",
            "--cwd", args.project, "--", "bash", "-lc", first_command,
        )
        checks["idempotent_replay"] = (
            replay.get("job_id") == first.get("job_id")
            and replay.get("idempotent_replay") is True
        )
        conflict = invoke(
            base, "submit", "--queue", "device", "--client-request-id", "first",
            "--cwd", args.project, "--", "bash", "-lc", "printf different",
            expected={2},
        )
        checks["idempotent_conflict_rejected"] = "different arguments" in str(conflict.get("error"))

        smoke = f"""set -e
printf 'second-start\\n' >> {events}
source {args.cann_source}
export ASCEND_RT_VISIBLE_DEVICES={args.device}
export LD_LIBRARY_PATH={args.project}/build/smoke-install/lib64:${{ASCEND_HOME_PATH}}/lib64:${{LD_LIBRARY_PATH:-}}
{args.project}/build/smoke-install/bin/smoke_runner
printf 'second-end\\n' >> {events}
"""
        second = invoke(
            base, "submit", "--queue", "device", "--client-request-id", "second",
            "--cwd", args.project, "--", "bash", "-lc", smoke,
        )
        recovery = invoke(
            base, "submit", "--client-request-id", "recovery",
            "--cwd", args.project, "--", "bash", "-lc", "sleep 1; printf recovered",
        )
        interrupted_waiter = subprocess.Popen(
            [*base, "wait", str(recovery["job_id"])],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        time.sleep(0.2)
        interrupted_waiter.terminate()
        interrupted_waiter.wait(timeout=5)
        first_result = invoke(base, "wait", str(first["job_id"]))
        second_result = invoke(base, "wait", str(second["job_id"]))
        recovery_result = invoke(base, "wait", str(recovery["job_id"]))
        checks["both_jobs_succeeded"] = (
            first_result.get("state") == "succeeded"
            and second_result.get("state") == "succeeded"
        )
        checks["cann_smoke"] = "Ascend910B3 smoke kernel passed" in str(second_result.get("stdout_tail"))
        checks["disconnect_recovery"] = (
            interrupted_waiter.returncode != 0
            and recovery_result.get("state") == "succeeded"
            and "recovered" in str(recovery_result.get("stdout_tail"))
        )
        checks["queue_serialized"] = remote_text(args.host, events).splitlines() == [
            "first-start", "first-end", "second-start", "second-end",
        ]

        page = invoke(base, "completions", "--limit", "1")
        replay_page = invoke(base, "completions", "--limit", "1")
        next_page = invoke(
            base, "completions", "--after", str(page["next_cursor"]), "--limit", "1"
        )
        checks["completion_replay"] = page["completions"] == replay_page["completions"]
        checks["completion_cursor"] = (
            len(page["completions"]) == len(next_page["completions"]) == 1
            and page["next_cursor"] != next_page["next_cursor"]
        )
        result = {
            "schema_version": 1,
            "acceptance": "expanded-tmux-wrapper-ssh-cann",
            "status": "passed" if all(checks.values()) else "failed",
            "started_at": started_at,
            "finished_at": utc_now(),
            "target": {"host": args.host, "project": args.project, "device": args.device},
            "checks": checks,
            "jobs": [first["job_id"], second["job_id"], recovery["job_id"]],
        }
    except Exception as exc:
        result = {
            "schema_version": 1,
            "acceptance": "expanded-tmux-wrapper-ssh-cann",
            "status": "failed",
            "started_at": started_at,
            "finished_at": utc_now(),
            "target": {"host": args.host, "project": args.project, "device": args.device},
            "checks": checks,
            "error": f"{type(exc).__name__}: {exc}",
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "output": str(args.output)}))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
