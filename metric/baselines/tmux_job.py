#!/usr/bin/env python3
"""A deliberately strong, standalone tmux job wrapper used as a baseline.

This is consumer-owned glue, not part of Awaitless. It persists status and exit
code, bounds returned logs, parses one JSON artifact, uses tmux wait-for for a
blocking client call, and validates the process group before cancellation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SAFE_ID = re.compile(r"^[A-Za-z0-9_-]+$")
TERMINAL_STATES = {"succeeded", "failed", "cancelled", "lost"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def process_start_ticks(pid: int | None) -> int | None:
    if not pid or os.uname().sysname != "Linux":
        return None
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        return int(fields[19])
    except (OSError, ValueError, IndexError):
        return None


def process_matches(pid: int | None, ticks: int | None) -> bool:
    if not pid:
        return False
    if ticks is not None and os.uname().sysname == "Linux":
        return process_start_ticks(pid) == ticks
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def trace_tmux(arguments: tuple[str, ...]) -> None:
    trace_path = os.environ.get("TMUX_METRIC_TRACE")
    if trace_path:
        with Path(trace_path).open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"at": utc_now(), "arguments": list(arguments)}) + "\n")


def tmux(socket: str, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    trace_tmux(arguments)
    result = subprocess.run(
        ["tmux", "-L", socket, *arguments],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"tmux {' '.join(arguments[:2])} failed: {detail}")
    return result


def job_dir(root: Path, job_id: str) -> Path:
    if not SAFE_ID.fullmatch(job_id):
        raise ValueError(f"unsafe job id: {job_id!r}")
    return root / job_id


def _tail(path: Path, lines: int, max_bytes: int) -> tuple[str, bool]:
    if not path.exists():
        return "", False
    size = path.stat().st_size
    if lines == 0:
        return "", size > 0
    with path.open("rb") as handle:
        handle.seek(max(0, size - max_bytes))
        raw = handle.read(max_bytes)
    all_lines = raw.splitlines(keepends=True)
    selected = b"".join(all_lines[-lines:])
    truncated = size > len(raw) or len(all_lines) > lines
    return selected.decode("utf-8", errors="replace"), truncated


def _artifacts(metadata: dict[str, Any], max_bytes: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    cwd = Path(metadata["cwd"])
    for declared in metadata["artifacts"]:
        path = Path(declared)
        resolved = path if path.is_absolute() else cwd / path
        item: dict[str, Any] = {"path": declared, "exists": resolved.is_file()}
        if resolved.is_file():
            item["size_bytes"] = resolved.stat().st_size
            if resolved.suffix.lower() == ".json" and resolved.stat().st_size <= max_bytes:
                try:
                    item["content"] = json.loads(resolved.read_text(encoding="utf-8"))
                except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                    item["parse_error"] = str(exc)
        items.append(item)
    return items


def _summary(root: Path, job_id: str, lines: int, max_bytes: int) -> dict[str, Any]:
    directory = job_dir(root, job_id)
    metadata = read_json(directory / "metadata.json")
    status_path = directory / "result.json"
    status = read_json(status_path) if status_path.exists() else {"state": "running", "exit_code": None}
    each = max(1, max_bytes // 2)
    stdout, stdout_truncated = _tail(directory / "stdout.log", lines, each)
    stderr, stderr_truncated = _tail(directory / "stderr.log", lines, each)
    artifacts = _artifacts(metadata, max_bytes)
    parsed = [item["content"] for item in artifacts if "content" in item]
    result = {
        "job_id": job_id,
        "state": status.get("state"),
        "exit_code": status.get("exit_code"),
        "duration_seconds": status.get("duration_seconds"),
        "stdout_tail": stdout,
        "stderr_tail": stderr,
        "truncated": stdout_truncated or stderr_truncated,
        "artifacts": artifacts,
    }
    if status.get("error"):
        result["error"] = status["error"]
    if len(parsed) == 1:
        result["parsed_results"] = parsed[0]
    return result


def submit(args: argparse.Namespace) -> int:
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        raise ValueError("submit requires a command after --")
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    job_id = args.job_id or f"tmj_{uuid.uuid4().hex[:16]}"
    directory = job_dir(root, job_id)
    directory.mkdir(mode=0o700)
    session = f"job_{hashlib.sha256(job_id.encode()).hexdigest()[:16]}"
    channel = f"done_{hashlib.sha256(job_id.encode()).hexdigest()[:20]}"
    cwd = str(Path(args.cwd or os.getcwd()).resolve())
    metadata = {
        "job_id": job_id,
        "session": session,
        "channel": channel,
        "socket": args.socket,
        "cwd": cwd,
        "command": command,
        "artifacts": args.artifact,
        "created_at": utc_now(),
    }
    atomic_json(directory / "metadata.json", metadata)

    keeper = f"keeper_{uuid.uuid4().hex[:8]}"
    tmux(args.socket, "-f", "/dev/null", "new-session", "-d", "-s", keeper, "sleep 86400")
    try:
        tmux(args.socket, "set-option", "-g", "remain-on-exit", "on")
        worker = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--root",
            str(root),
            "--socket",
            args.socket,
            "_worker",
            job_id,
        ]
        tmux(
            args.socket,
            "new-session",
            "-d",
            "-s",
            session,
            "-c",
            cwd,
            shlex.join(worker),
        )
    finally:
        tmux(args.socket, "kill-session", "-t", keeper, check=False)
    print(json.dumps({"job_id": job_id, "state": "running", "backend": "tmux_wrapped"}, separators=(",", ":")))
    return 0


def worker(args: argparse.Namespace) -> int:
    directory = job_dir(args.root.resolve(), args.job_id)
    metadata = read_json(directory / "metadata.json")
    started = time.monotonic()
    try:
        with (directory / "stdout.log").open("ab", buffering=0) as stdout, (directory / "stderr.log").open("ab", buffering=0) as stderr:
            process = subprocess.Popen(
                metadata["command"],
                cwd=metadata["cwd"],
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
                close_fds=True,
            )
            atomic_json(
                directory / "process.json",
                {"pid": process.pid, "start_ticks": process_start_ticks(process.pid)},
            )
            exit_code = process.wait()
        cancelled = (directory / "cancelled").exists()
        atomic_json(
            directory / "result.json",
            {
                "state": "cancelled" if cancelled else ("succeeded" if exit_code == 0 else "failed"),
                "exit_code": None if cancelled else exit_code,
                "duration_seconds": round(time.monotonic() - started, 6),
                "finished_at": utc_now(),
            },
        )
    except Exception as exc:
        atomic_json(
            directory / "result.json",
            {
                "state": "failed",
                "exit_code": None,
                "duration_seconds": round(time.monotonic() - started, 6),
                "finished_at": utc_now(),
                "error": str(exc),
            },
        )
    finally:
        tmux(metadata["socket"], "wait-for", "-S", metadata["channel"], check=False)
    return 0


def wait_for_job(args: argparse.Namespace) -> int:
    directory = job_dir(args.root.resolve(), args.job_id)
    metadata = read_json(directory / "metadata.json")
    if not (directory / "result.json").exists():
        arguments = ("wait-for", metadata["channel"])
        trace_tmux(arguments)
        waiter = subprocess.Popen(
            ["tmux", "-L", args.socket, *arguments],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        while waiter.poll() is None:
            # The result-file check closes the race where the completion signal
            # occurs just before this client is registered with the tmux server.
            if (directory / "result.json").exists():
                tmux(args.socket, "wait-for", "-S", metadata["channel"], check=False)
            time.sleep(0.02)
        if waiter.returncode:
            raise RuntimeError("tmux wait-for client exited before completion")
    result = _summary(args.root.resolve(), args.job_id, args.tail, args.max_bytes)
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return {"succeeded": 0, "failed": 3, "cancelled": 5}.get(str(result["state"]), 1)


def cancel(args: argparse.Namespace) -> int:
    directory = job_dir(args.root.resolve(), args.job_id)
    if (directory / "result.json").exists():
        print(json.dumps(_summary(args.root.resolve(), args.job_id, 0, args.max_bytes), separators=(",", ":")))
        return 0
    (directory / "cancelled").write_text(utc_now(), encoding="utf-8")
    process_path = directory / "process.json"
    process = read_json(process_path) if process_path.exists() else {}
    pid = process.get("pid")
    ticks = process.get("start_ticks")
    if process_matches(pid, ticks):
        try:
            os.killpg(int(pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + args.grace_seconds
        while process_matches(pid, ticks) and time.monotonic() < deadline:
            time.sleep(0.02)
        if process_matches(pid, ticks):
            try:
                os.killpg(int(pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
    deadline = time.monotonic() + 2.0
    while not (directory / "result.json").exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    if not (directory / "result.json").exists():
        atomic_json(
            directory / "result.json",
            {"state": "cancelled", "exit_code": None, "duration_seconds": None, "finished_at": utc_now()},
        )
    print(json.dumps(_summary(args.root.resolve(), args.job_id, 0, args.max_bytes), separators=(",", ":")))
    return 0


def status(args: argparse.Namespace) -> int:
    print(json.dumps(_summary(args.root.resolve(), args.job_id, 0, args.max_bytes), separators=(",", ":")))
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--root", required=True, type=Path)
    result.add_argument("--socket", required=True)
    commands = result.add_subparsers(dest="action", required=True)

    submit_parser = commands.add_parser("submit")
    submit_parser.add_argument("--job-id")
    submit_parser.add_argument("--cwd")
    submit_parser.add_argument("--artifact", action="append", default=[])
    submit_parser.add_argument("command", nargs=argparse.REMAINDER)

    wait_parser = commands.add_parser("wait")
    wait_parser.add_argument("job_id")
    wait_parser.add_argument("--tail", type=int, default=50)
    wait_parser.add_argument("--max-bytes", type=int, default=65536)

    cancel_parser = commands.add_parser("cancel")
    cancel_parser.add_argument("job_id")
    cancel_parser.add_argument("--grace-seconds", type=float, default=1.0)
    cancel_parser.add_argument("--max-bytes", type=int, default=65536)

    status_parser = commands.add_parser("status")
    status_parser.add_argument("job_id")
    status_parser.add_argument("--max-bytes", type=int, default=65536)

    worker_parser = commands.add_parser("_worker")
    worker_parser.add_argument("job_id")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.action == "submit":
            return submit(args)
        if args.action == "wait":
            return wait_for_job(args)
        if args.action == "cancel":
            return cancel(args)
        if args.action == "status":
            return status(args)
        if args.action == "_worker":
            return worker(args)
        return 2
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, separators=(",", ":")), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
