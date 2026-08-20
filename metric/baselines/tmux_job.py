#!/usr/bin/env python3
"""A deliberately strong, standalone tmux job wrapper used as a baseline.

This is consumer-owned glue, not part of Awaitless. It persists status and exit
code, bounds returned logs, parses one JSON artifact, uses tmux wait-for for a
blocking client call, and validates the process group before cancellation.
"""

from __future__ import annotations

import argparse
import fcntl
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
from contextlib import contextmanager
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


@contextmanager
def exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield


def request_path(root: Path, request_id: str) -> Path:
    digest = hashlib.sha256(request_id.encode()).hexdigest()
    return root / "requests" / f"{digest}.json"


def submission_fingerprint(
    command: list[str], cwd: str, artifacts: list[str], queue: str | None
) -> str:
    value = {"command": command, "cwd": cwd, "artifacts": artifacts, "queue": queue}
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def queue_config(root: Path, name: str) -> dict[str, Any]:
    if not SAFE_ID.fullmatch(name):
        raise ValueError(f"unsafe queue name: {name!r}")
    path = root / "queues" / name / "config.json"
    if not path.is_file():
        raise ValueError(f"queue does not exist: {name}")
    return read_json(path)


@contextmanager
def queue_slot(root: Path, name: str | None):
    if name is None:
        yield None
        return
    concurrency = int(queue_config(root, name)["concurrency"])
    handle = None
    selected = None
    try:
        while selected is None:
            for index in range(concurrency):
                path = root / "queues" / name / f"slot-{index}.lock"
                candidate = path.open("a+", encoding="utf-8")
                try:
                    fcntl.flock(candidate, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    candidate.close()
                    continue
                handle = candidate
                selected = index
                break
            if selected is None:
                time.sleep(0.02)
        yield selected
    finally:
        if handle is not None:
            fcntl.flock(handle, fcntl.LOCK_UN)
            handle.close()


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


def ensure_worker_started(root: Path, socket: str, metadata: dict[str, Any]) -> None:
    session = str(metadata["session"])
    if tmux(socket, "has-session", "-t", session, check=False).returncode == 0:
        return
    keeper = f"keeper_{uuid.uuid4().hex[:8]}"
    tmux(socket, "-f", "/dev/null", "new-session", "-d", "-s", keeper, "sleep 86400")
    try:
        tmux(socket, "set-option", "-g", "remain-on-exit", "on")
        worker = [
            sys.executable, str(Path(__file__).resolve()),
            "--root", str(root), "--socket", socket, "_worker", metadata["job_id"],
        ]
        result = tmux(
            socket, "new-session", "-d", "-s", session,
            "-c", metadata["cwd"], shlex.join(worker), check=False,
        )
        if result.returncode and tmux(
            socket, "has-session", "-t", session, check=False
        ).returncode:
            raise RuntimeError(f"cannot launch tmux worker: {result.stderr.strip()}")
    finally:
        tmux(socket, "kill-session", "-t", keeper, check=False)


def submit(args: argparse.Namespace) -> int:
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        raise ValueError("submit requires a command after --")
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    cwd = str(Path(args.cwd or os.getcwd()).resolve())
    if args.queue:
        queue_config(root, args.queue)
    fingerprint = submission_fingerprint(command, cwd, args.artifact, args.queue)
    with exclusive_lock(root / ".submission.lock"):
        if args.client_request_id:
            index = request_path(root, args.client_request_id)
            if index.exists():
                existing = read_json(index)
                if existing["fingerprint"] != fingerprint:
                    raise ValueError("client request ID was reused with different arguments")
                metadata = read_json(job_dir(root, existing["job_id"]) / "metadata.json")
                ensure_worker_started(root, args.socket, metadata)
                print(json.dumps({
                    "job_id": existing["job_id"], "state": "running",
                    "backend": "tmux_wrapped", "idempotent_replay": True,
                }, separators=(",", ":")))
                return 0
        job_id = args.job_id or f"tmj_{uuid.uuid4().hex[:16]}"
        directory = job_dir(root, job_id)
        directory.mkdir(mode=0o700)
        session = f"job_{hashlib.sha256(job_id.encode()).hexdigest()[:16]}"
        channel = f"done_{hashlib.sha256(job_id.encode()).hexdigest()[:20]}"
        metadata = {
            "job_id": job_id, "session": session, "channel": channel,
            "socket": args.socket, "cwd": cwd, "command": command,
            "artifacts": args.artifact, "client_request_id": args.client_request_id,
            "queue": args.queue, "created_at": utc_now(),
        }
        atomic_json(directory / "metadata.json", metadata)
        if args.client_request_id:
            atomic_json(
                request_path(root, args.client_request_id),
                {"client_request_id": args.client_request_id, "job_id": job_id,
                 "fingerprint": fingerprint},
            )
        ensure_worker_started(root, args.socket, metadata)
    print(json.dumps({"job_id": job_id, "state": "running", "backend": "tmux_wrapped"}, separators=(",", ":")))
    return 0


def worker(args: argparse.Namespace) -> int:
    directory = job_dir(args.root.resolve(), args.job_id)
    metadata = read_json(directory / "metadata.json")
    started = time.monotonic()
    try:
        (directory / "queue_state").write_text("queued\n", encoding="utf-8")
        with queue_slot(args.root.resolve(), metadata.get("queue")) as slot:
            (directory / "queue_state").write_text("running\n", encoding="utf-8")
            if slot is not None:
                (directory / "queue_slot").write_text(str(slot), encoding="utf-8")
            cancelled = (directory / "cancelled").exists()
            exit_code = 0
            if not cancelled:
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


def create_queue(args: argparse.Namespace) -> int:
    if not SAFE_ID.fullmatch(args.name):
        raise ValueError(f"unsafe queue name: {args.name!r}")
    if args.concurrency <= 0:
        raise ValueError("queue concurrency must be positive")
    root = args.root.resolve()
    path = root / "queues" / args.name / "config.json"
    with exclusive_lock(root / ".queue.lock"):
        if path.exists():
            existing = read_json(path)
            if existing["concurrency"] != args.concurrency:
                raise ValueError("queue already exists with different concurrency")
            created = False
        else:
            atomic_json(path, {"name": args.name, "concurrency": args.concurrency})
            created = True
    print(json.dumps({"name": args.name, "concurrency": args.concurrency, "created": created}))
    return 0


def register_completion_events(root: Path) -> None:
    event_root = root / "completion-events"
    with exclusive_lock(root / ".completion.lock"):
        registered = {
            str(value["job_id"])
            for path in event_root.glob("cmp_*.json")
            for value in [read_json(path)]
        }
        counter_path = event_root / "counter"
        counter = int(counter_path.read_text(encoding="utf-8")) if counter_path.exists() else 0
        for result_path in sorted(root.glob("tmj_*/result.json")):
            job_id = result_path.parent.name
            if job_id in registered:
                continue
            counter += 1
            completion_id = f"cmp_{counter:020d}"
            result = read_json(result_path)
            atomic_json(event_root / f"{completion_id}.json", {
                "completion_id": completion_id, "job_id": job_id,
                "state": result.get("state"),
                "finished_at": result.get("finished_at"),
            })
        counter_path.parent.mkdir(parents=True, exist_ok=True)
        counter_path.write_text(str(counter), encoding="utf-8")


def completion_rows(root: Path) -> list[dict[str, Any]]:
    register_completion_events(root)
    rows = []
    for path in sorted((root / "completion-events").glob("cmp_*.json")):
        event = read_json(path)
        event["result"] = _summary(root, str(event["job_id"]), 50, 65536)
        rows.append(event)
    return rows


def completions(args: argparse.Namespace) -> int:
    rows = completion_rows(args.root.resolve())
    start = 0
    if args.after:
        matches = [index for index, row in enumerate(rows) if row["completion_id"] == args.after]
        if not matches:
            raise ValueError("unknown completion cursor")
        start = matches[0] + 1
    page = rows[start:start + args.limit]
    next_cursor = page[-1]["completion_id"] if page else args.after
    print(json.dumps({
        "completions": page, "next_cursor": next_cursor,
        "has_more": start + len(page) < len(rows),
    }, ensure_ascii=False, separators=(",", ":")))
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
    submit_parser.add_argument("--client-request-id")
    submit_parser.add_argument("--queue")
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

    queue_parser = commands.add_parser("queue-create")
    queue_parser.add_argument("name")
    queue_parser.add_argument("--concurrency", required=True, type=int)

    completions_parser = commands.add_parser("completions")
    completions_parser.add_argument("--after")
    completions_parser.add_argument("--limit", type=int, default=50)

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
        if args.action == "queue-create":
            return create_queue(args)
        if args.action == "completions":
            return completions(args)
        if args.action == "_worker":
            return worker(args)
        return 2
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, separators=(",", ":")), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
