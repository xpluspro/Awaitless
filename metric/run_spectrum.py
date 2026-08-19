#!/usr/bin/env python3
"""Compare direct shell with an installed Awaitless adaptive run across real commands."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from .provenance import git_state
except ImportError:
    from provenance import git_state  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "metric" / "configs" / "spectrum-v0.8.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def execute(argv: list[str], *, cwd: Path, timeout: float = 1800) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        argv, cwd=cwd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, check=False, timeout=timeout,
    )


def parse_json(result: subprocess.CompletedProcess[bytes], label: str) -> dict[str, Any]:
    if result.returncode not in {0, 1, 2, 124}:
        raise RuntimeError(f"{label} failed with exit {result.returncode}: {result.stderr[-1000:]!r}")
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{label} did not return JSON: {result.stdout[-1000:]!r}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} returned a non-object")
    return value


def installed_identity(binary: str, expected_version: str) -> dict[str, Any]:
    resolved = shutil.which(binary)
    if not resolved:
        raise ValueError(f"Awaitless binary not found: {binary}")
    version_result = execute([resolved, "--version"], cwd=ROOT, timeout=10)
    version_text = version_result.stdout.decode(errors="replace").strip()
    if version_result.returncode != 0 or version_text != f"awaitless {expected_version}":
        raise ValueError(
            f"benchmark requires awaitless {expected_version}; {resolved} reported {version_text!r}"
        )
    first_line = Path(resolved).read_text(encoding="utf-8", errors="replace").splitlines()[0]
    interpreter = first_line[2:] if first_line.startswith("#!") else sys.executable
    package = execute(
        [interpreter, "-c", "import awaitless,inspect; print(awaitless.__version__); print(inspect.getfile(awaitless))"],
        cwd=ROOT, timeout=10,
    )
    lines = package.stdout.decode(errors="replace").splitlines()
    if package.returncode != 0 or len(lines) != 2 or lines[0] != expected_version:
        raise ValueError("Awaitless executable and import package are not the expected installed version")
    return {
        "binary": str(Path(resolved).resolve()),
        "version": expected_version,
        "python": interpreter,
        "package_file": str(Path(lines[1]).resolve()),
    }


def validate_config(config: dict[str, Any]) -> None:
    if config.get("schema_version") != 1 or int(config.get("trials", 0)) <= 0:
        raise ValueError("spectrum config requires schema_version 1 and positive trials")
    commands = config.get("commands")
    if not isinstance(commands, list) or not commands:
        raise ValueError("spectrum config requires commands")
    ids = [item.get("id") for item in commands]
    if any(not isinstance(item, str) for item in ids) or len(ids) != len(set(ids)):
        raise ValueError("command IDs must be unique strings")
    for command in commands:
        if not isinstance(command.get("argv"), list) or not command["argv"]:
            raise ValueError(f"{command.get('id')}: argv must be a non-empty list")


def append(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def run_shell(argv: list[str], expected: int) -> dict[str, Any]:
    started = time.monotonic()
    result = execute(argv, cwd=ROOT)
    elapsed = time.monotonic() - started
    return {
        "arm": "shell", "delivery": "blocking", "agent_tool_calls": 1,
        "time_to_release_seconds": round(elapsed, 6), "wall_time_seconds": round(elapsed, 6),
        "exit_code": result.returncode, "result_correct": result.returncode == expected,
        "visible_bytes": len(result.stdout) + len(result.stderr),
        "stdout_sha256": hashlib.sha256(result.stdout).hexdigest(),
        "stderr_sha256": hashlib.sha256(result.stderr).hexdigest(),
    }


def run_awaitless(binary: str, argv: list[str], expected: int) -> dict[str, Any]:
    started = time.monotonic()
    initial = execute([binary, "run", "--json", "--cwd", str(ROOT), "--", *argv], cwd=ROOT)
    released = time.monotonic() - started
    value = parse_json(initial, "awaitless run")
    delivery = value.get("delivery")
    calls = 1
    if delivery == "detached":
        job_id = value.get("job_id")
        if not isinstance(job_id, str):
            raise RuntimeError("detached run omitted job_id")
        terminal = execute([binary, "wait", job_id, "--json"], cwd=ROOT)
        value = parse_json(terminal, "awaitless wait")
        calls = 2
    elapsed = time.monotonic() - started
    exit_code = value.get("exit_code")
    visible = len(initial.stdout) + len(initial.stderr)
    if calls == 2:
        visible += len(terminal.stdout) + len(terminal.stderr)
    return {
        "arm": "awaitless", "delivery": delivery, "agent_tool_calls": calls,
        "time_to_release_seconds": round(released, 6), "wall_time_seconds": round(elapsed, 6),
        "exit_code": exit_code, "result_correct": exit_code == expected,
        "visible_bytes": visible, "job_id": value.get("job_id"),
        "job_state": value.get("job_state", value.get("state")),
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--awaitless-bin", default="awaitless")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.output.exists():
        print(f"spectrum: refusing to overwrite {args.output}", file=sys.stderr)
        return 2
    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
        validate_config(config)
        identity = installed_identity(args.awaitless_bin, str(config["expected_version"]))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"spectrum: {exc}", file=sys.stderr)
        return 2
    commit, dirty, untracked = git_state(ROOT)
    config_sha = hashlib.sha256(args.config.read_bytes()).hexdigest()
    rng = random.Random(20260819)
    failed = 0
    replacements = {"{python}": sys.executable, "{workspace}": str(ROOT)}
    for trial in range(int(config["trials"])):
        commands = list(config["commands"])
        rng.shuffle(commands)
        for command in commands:
            command_argv = [replacements.get(str(item), str(item)) for item in command["argv"]]
            arms = ["shell", "awaitless"]
            rng.shuffle(arms)
            for arm in arms:
                observation = (
                    run_shell(command_argv, int(command["expected_exit_code"]))
                    if arm == "shell"
                    else run_awaitless(identity["binary"], command_argv, int(command["expected_exit_code"]))
                )
                failed += int(not observation["result_correct"])
                append(args.output, {
                    "schema_version": 1, "record_type": "spectrum_trial", "recorded_at": utc_now(),
                    "suite": config["name"], "trial": trial, "command_id": command["id"],
                    "argv": command_argv, "expected_exit_code": command["expected_exit_code"],
                    "observation": observation, "awaitless_install": identity,
                    "git_commit": commit, "git_dirty": dirty,
                    "git_untracked_files": untracked, "config_sha256": config_sha,
                    "platform": platform.platform(),
                })
    print(json.dumps({"records": int(config["trials"]) * len(config["commands"]) * 2, "failed": failed, "awaitless_install": identity}, separators=(",", ":")))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
