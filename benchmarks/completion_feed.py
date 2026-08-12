#!/usr/bin/env python3
"""Compare per-Job Agent polling with Awaitless's durable completion feed."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "benchmarks" / "results" / "completion-feed.json"
TERMINAL_STATES = {"succeeded", "failed", "cancelled", "timed_out", "lost"}


def invoke(
    arguments: list[str], *, environment: dict[str, str]
) -> dict[str, Any]:
    result = subprocess.run(
        [sys.executable, "-m", "awaitless", *arguments],
        cwd=ROOT,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[-2000:]
        raise RuntimeError(f"Awaitless CLI failed ({result.returncode}): {detail}")
    return json.loads(result.stdout)


def submit_jobs(
    root: Path, durations: list[float], environment: dict[str, str]
) -> list[str]:
    job_ids: list[str] = []
    for index, duration in enumerate(durations, start=1):
        work = root / f"job-{index}"
        work.mkdir(parents=True)
        label = f"job-{index}"
        payload = json.dumps({"correctness": True, "label": label})
        source = (
            "import time; from pathlib import Path; "
            f"time.sleep({duration!r}); "
            f"Path('result.json').write_text({payload!r}, encoding='utf-8'); "
            f"print({label!r})"
        )
        submitted = invoke(
            [
                "submit",
                "--json",
                "--cwd",
                str(work),
                "--artifact",
                "result.json",
                "--",
                sys.executable,
                "-c",
                source,
            ],
            environment=environment,
        )
        job_ids.append(submitted["job_id"])
    return job_ids


def polling_arm(
    root: Path,
    durations: list[float],
    poll_interval: float,
    environment: dict[str, str],
) -> dict[str, Any]:
    started = time.monotonic()
    job_ids = submit_jobs(root, durations, environment)
    active = set(job_ids)
    status_calls = 0
    while active:
        for job_id in job_ids:
            if job_id not in active:
                continue
            current = invoke(["status", job_id, "--json"], environment=environment)
            status_calls += 1
            if current["state"] in TERMINAL_STATES:
                active.remove(job_id)
        if active:
            time.sleep(poll_interval)

    results = [
        invoke(["wait", job_id, "--json"], environment=environment)
        for job_id in job_ids
    ]
    return {
        "submit_calls": len(job_ids),
        "status_calls": status_calls,
        "result_calls": len(job_ids),
        "total_agent_visible_calls": len(job_ids) * 2 + status_calls,
        "wall_seconds": round(time.monotonic() - started, 3),
        "states": [result["state"] for result in results],
        "parsed_results": [result.get("parsed_results") for result in results],
    }


def completion_arm(
    root: Path, durations: list[float], environment: dict[str, str]
) -> dict[str, Any]:
    started = time.monotonic()
    job_ids = submit_jobs(root, durations, environment)
    cursor: str | None = None
    completion_calls = 0
    completions: list[dict[str, Any]] = []
    active = list(job_ids)
    has_more = False
    while active or has_more:
        arguments = ["completions", *job_ids]
        if cursor is not None:
            arguments.extend(["--after", cursor])
        arguments.append("--json")
        batch = invoke(arguments, environment=environment)
        completion_calls += 1
        completions.extend(batch["completions"])
        cursor = batch["next_cursor"]
        active = batch["active_job_ids"]
        has_more = batch["has_more"]

    return {
        "submit_calls": len(job_ids),
        "completion_calls": completion_calls,
        "total_agent_visible_calls": len(job_ids) + completion_calls,
        "wall_seconds": round(time.monotonic() - started, 3),
        "completion_ids": [item["completion_id"] for item in completions],
        "states": [item["state"] for item in completions],
        "parsed_results": [
            item["result"].get("parsed_results") for item in completions
        ],
        "final_cursor": cursor,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--durations",
        default="0.4,0.7,1.0",
        help="comma-separated local sleep durations",
    )
    parser.add_argument("--poll-interval", type=float, default=0.1)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    try:
        durations = [float(value) for value in args.durations.split(",")]
    except ValueError as exc:
        parser.error(f"invalid --durations: {exc}")
    if not durations or any(value <= 0 for value in durations):
        parser.error("--durations must contain positive values")
    if args.poll_interval <= 0:
        parser.error("--poll-interval must be positive")

    with tempfile.TemporaryDirectory(prefix="awaitless-completion-benchmark-") as temp:
        temporary = Path(temp)

        def environment_for(arm: str) -> dict[str, str]:
            environment = os.environ.copy()
            environment["AWAITLESS_DATA_DIR"] = str(temporary / arm / "data")
            environment["PYTHONPATH"] = str(ROOT / "src")
            config = temporary / arm / "config.toml"
            config.parent.mkdir(parents=True)
            config.write_text("[defaults]\npoll_interval = 0.02\n", encoding="utf-8")
            environment["AWAITLESS_CONFIG"] = str(config)
            return environment

        polling = polling_arm(
            temporary / "polling" / "work",
            durations,
            args.poll_interval,
            environment_for("polling"),
        )
        completion = completion_arm(
            temporary / "completion" / "work",
            durations,
            environment_for("completion"),
        )

    expected_labels = {f"job-{index}" for index in range(1, len(durations) + 1)}
    for name, arm in (("per_job_polling", polling), ("completion_feed", completion)):
        labels = {
            item.get("label") for item in arm["parsed_results"] if isinstance(item, dict)
        }
        if arm["states"] != ["succeeded"] * len(durations) or labels != expected_labels:
            raise RuntimeError(f"{name} did not return equivalent correct results: {arm}")
    if len(set(completion["completion_ids"])) != len(durations):
        raise RuntimeError("completion feed returned missing or duplicate completion IDs")

    calls_saved = (
        polling["total_agent_visible_calls"]
        - completion["total_agent_visible_calls"]
    )
    result = {
        "schema_version": 1,
        "recorded_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "measurement_scope": {
            "calls": "separate agent-visible Awaitless CLI invocations",
            "claim_boundary": (
                "deterministic protocol benchmark; no model tokens or reasoning "
                "quality are measured"
            ),
        },
        "environment": {
            "backend": "local",
            "workload": "sleep-only jobs with small JSON Artifacts",
            "durations_seconds": durations,
            "poll_interval_seconds": args.poll_interval,
        },
        "per_job_polling": polling,
        "completion_feed": completion,
        "comparison": {
            "agent_visible_calls_saved": calls_saved,
            "agent_visible_call_reduction_percent": round(
                100
                * calls_saved
                / polling["total_agent_visible_calls"],
                1,
            ),
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
