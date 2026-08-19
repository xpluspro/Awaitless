#!/usr/bin/env python3
"""Deterministic workloads shared by every metric arm."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def _fixed_line(index: int, line_bytes: int) -> bytes:
    prefix = f"sample={index:06d} ".encode()
    if line_bytes <= len(prefix):
        raise ValueError("line_bytes is too small for the sample prefix and newline")
    return prefix + (b"x" * (line_bytes - len(prefix) - 1)) + b"\n"


def _write_logs(line_count: int, line_bytes: int, duration: float) -> None:
    started = time.monotonic()
    batch_size = max(1, line_count // 100)
    for offset in range(0, line_count, batch_size):
        stop = min(line_count, offset + batch_size)
        payload = b"".join(_fixed_line(index, line_bytes) for index in range(offset, stop))
        sys.stdout.buffer.write(payload)
        sys.stdout.buffer.flush()
        target = started + duration * stop / max(1, line_count)
        time.sleep(max(0.0, target - time.monotonic()))


def _write_artifact(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _tree_child(parent_pid: int, pid_file: Path, duration: float) -> int:
    grandchild = subprocess.Popen(["sleep", f"{duration:g}"])
    _write_artifact(
        pid_file,
        {
            "parent": parent_pid,
            "child": os.getpid(),
            "grandchild": grandchild.pid,
        },
    )
    return grandchild.wait()


def _cancel_tree(pid_file: Path, duration: float) -> int:
    child = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--tree-child",
            "--parent-pid",
            str(os.getpid()),
            "--pid-file",
            str(pid_file),
            "--duration-seconds",
            f"{duration:g}",
        ]
    )
    print(f"TREE_READY parent={os.getpid()} child={child.pid}", flush=True)
    return child.wait()


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--scenario", default="normal")
    result.add_argument("--trial-id", default="trial")
    result.add_argument("--duration-seconds", type=float, default=0.1)
    result.add_argument("--line-count", type=int, default=1)
    result.add_argument("--line-bytes", type=int, default=128)
    result.add_argument("--exit-code", type=int, default=0)
    result.add_argument("--artifact", type=Path)
    result.add_argument("--marker", default="marker")
    result.add_argument("--score", type=float, default=0.0)
    result.add_argument("--pid-file", type=Path)
    result.add_argument("--tree-child", action="store_true", help=argparse.SUPPRESS)
    result.add_argument("--parent-pid", type=int, help=argparse.SUPPRESS)
    result.add_argument("--pre-command-json")
    result.add_argument("--pre-cwd", type=Path)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.duration_seconds < 0 or args.line_count < 0 or args.line_bytes < 32:
        raise SystemExit("duration/line count must be non-negative and line bytes >= 32")
    if args.tree_child:
        if args.parent_pid is None or args.pid_file is None:
            raise SystemExit("tree child requires --parent-pid and --pid-file")
        return _tree_child(args.parent_pid, args.pid_file, args.duration_seconds)
    if args.scenario == "cancel_tree":
        if args.pid_file is None:
            raise SystemExit("cancel_tree requires --pid-file")
        return _cancel_tree(args.pid_file, args.duration_seconds)

    actual_exit_code = args.exit_code
    if args.pre_command_json:
        command = json.loads(args.pre_command_json)
        if not isinstance(command, list) or not command or not all(
            isinstance(item, str) for item in command
        ):
            raise SystemExit("pre-command must be a non-empty JSON string array")
        completed = subprocess.run(
            command,
            cwd=args.pre_cwd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        sys.stdout.buffer.write(completed.stdout)
        sys.stdout.buffer.flush()
        sys.stderr.buffer.write(completed.stderr)
        sys.stderr.buffer.flush()
        actual_exit_code = completed.returncode
    _write_logs(args.line_count, args.line_bytes, args.duration_seconds)
    stdout_marker = f"FINAL_MARKER={args.marker}\n"
    stderr_marker = f"STDERR_MARKER={args.marker}\n"
    sys.stdout.write(stdout_marker)
    sys.stdout.flush()
    sys.stderr.write(stderr_marker)
    sys.stderr.flush()
    if args.artifact:
        _write_artifact(
            args.artifact,
            {
                "ok": actual_exit_code == 0,
                "scenario": args.scenario,
                "trial_id": args.trial_id,
                "score": args.score,
            },
        )
    return actual_exit_code


if __name__ == "__main__":
    raise SystemExit(main())
