from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .backends import LocalBackend, SSHBackend
from .backends.ssh import SSHError
from .config import Settings
from .constants import TERMINAL_STATES
from .db import Store
from .util import atomic_json, parse_time, utc_now


ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SENSITIVE_NAME = re.compile(r"TOKEN|SECRET|PASSWORD|PASSWD|PRIVATE|CREDENTIAL|API_KEY", re.I)


class AwaitlessError(RuntimeError):
    pass


class JobNotFound(AwaitlessError):
    pass


class Service:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = Store(settings.db_path)
        self.backends = {
            "local": LocalBackend(self.store),
            "ssh": SSHBackend(self.store, settings),
        }

    def close(self) -> None:
        self.store.close()

    def submit(
        self,
        *,
        job_id: str,
        command: list[str],
        backend: str,
        host: str | None,
        cwd: str | None,
        env: dict[str, str],
        timeout_seconds: float | None,
        stall_timeout_seconds: float | None,
        name: str | None,
        artifacts: list[str],
        log_dir: str | None = None,
    ) -> dict[str, Any]:
        if not command:
            raise AwaitlessError("a command is required after --")
        if backend not in self.backends:
            raise AwaitlessError(f"unsupported backend: {backend}")
        if backend == "ssh" and not host:
            raise AwaitlessError("SSH backend requires --host")
        if backend == "local" and host:
            raise AwaitlessError("--host can only be used with the SSH backend")
        if backend == "local" and cwd and not Path(cwd).expanduser().is_dir():
            raise AwaitlessError(f"working directory does not exist: {cwd}")
        for key in env:
            if not ENV_NAME.fullmatch(key):
                raise AwaitlessError(f"invalid environment variable name: {key!r}")

        job_dir = self.settings.jobs_dir / job_id
        job_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
        logs = Path(log_dir).expanduser().resolve() if log_dir else job_dir
        logs.mkdir(mode=0o700, parents=True, exist_ok=True)
        stdout_path = logs / "stdout.log"
        stderr_path = logs / "stderr.log"
        stdout_path.touch(mode=0o600)
        stderr_path.touch(mode=0o600)
        resolved_cwd = str(Path(cwd).expanduser().resolve()) if cwd and backend == "local" else cwd
        redacted_env = {key: ("<redacted>" if SENSITIVE_NAME.search(key) else value) for key, value in env.items()}
        metadata = {
            "job_id": job_id,
            "name": name,
            "backend": backend,
            "host": host,
            "command": command,
            "cwd": resolved_cwd,
            "env": redacted_env,
            "artifacts": artifacts,
            "created_at": utc_now(),
        }
        atomic_json(job_dir / "metadata.json", metadata)
        spec = {
            "command": command,
            "cwd": resolved_cwd,
            "env": env,
            "timeout_seconds": timeout_seconds,
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
        }
        spec_path = job_dir / "run-spec.json"
        atomic_json(spec_path, spec)
        os.chmod(spec_path, 0o600)
        self.store.create({
            "job_id": job_id,
            "name": name,
            "backend": backend,
            "host": host,
            "command_json": json.dumps(command),
            "cwd": resolved_cwd,
            "env_json": json.dumps(redacted_env),
            "state": "starting",
            "timeout_seconds": timeout_seconds,
            "stall_timeout_seconds": stall_timeout_seconds,
            "job_dir": str(job_dir),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "artifacts_json": json.dumps(artifacts),
        })
        job = self.require(job_id)
        try:
            if backend == "local":
                job = self.backends[backend].submit(job, spec_path)  # type: ignore[arg-type]
            else:
                job = self.backends[backend].submit(job, spec)  # type: ignore[arg-type]
        except Exception as exc:
            self.store.update_if_active(
                job_id, state="failed", finished_at=utc_now(), error=f"backend start failed: {exc}"
            )
            raise
        if (job.get("error") or "").startswith("failed to start command:"):
            raise AwaitlessError(job["error"])
        result = self.summary(job)
        # The submit contract reports a successfully launched command as running even when a
        # very short command reaches its terminal state before the client receives the reply.
        if result["state"] in TERMINAL_STATES and job.get("started_at") and job.get("pid"):
            result["state"] = "running"
            result["exit_code"] = None
            result["finished_at"] = None
        return result

    def require(self, job_id: str) -> dict[str, Any]:
        job = self.store.get(job_id)
        if not job:
            raise JobNotFound(f"unknown job ID: {job_id}")
        return job

    def status(self, job_id: str) -> dict[str, Any]:
        job = self.require(job_id)
        job = self.backends[job["backend"]].refresh(job)  # type: ignore[attr-defined]
        return self.summary(self._apply_stall(job))

    def wait(self, job_id: str, wait_timeout: float | None = None) -> tuple[dict[str, Any], bool]:
        started = time.monotonic()
        while True:
            try:
                result = self.status(job_id)
            except SSHError:
                # A dropped waiter must not affect the remote process. Retry transient SSH
                # failures internally so the Agent still needs only one wait invocation.
                if wait_timeout is not None and time.monotonic() - started >= wait_timeout:
                    result = self.summary(self.require(job_id))
                    result["backend_connected"] = False
                    result["wait_timed_out"] = True
                    return result, True
                remaining = None if wait_timeout is None else wait_timeout - (time.monotonic() - started)
                time.sleep(max(0.05, min(self.settings.poll_interval, remaining or self.settings.poll_interval)))
                continue
            if result["state"] in TERMINAL_STATES:
                result.update(self.logs(job_id, self.settings.log_tail_lines, self.settings.max_return_bytes))
                result["artifacts"] = self.artifacts(self.require(job_id))
                parsed = [item.get("content") for item in result["artifacts"] if "content" in item]
                if len(parsed) == 1:
                    result["parsed_results"] = parsed[0]
                return result, False
            if wait_timeout is not None and time.monotonic() - started >= wait_timeout:
                result["wait_timed_out"] = True
                return result, True
            remaining = None if wait_timeout is None else wait_timeout - (time.monotonic() - started)
            time.sleep(max(0.05, min(self.settings.poll_interval, remaining or self.settings.poll_interval)))

    def cancel(self, job_id: str, grace_seconds: float) -> dict[str, Any]:
        job = self.require(job_id)
        job = self.backends[job["backend"]].cancel(job, grace_seconds)  # type: ignore[attr-defined]
        return self.summary(job)

    def list(self, state: str | None = None, host: str | None = None) -> list[dict[str, Any]]:
        results = []
        for job in self.store.list(state=state, host=host):
            if job["state"] not in TERMINAL_STATES:
                try:
                    job = self.backends[job["backend"]].refresh(job)  # type: ignore[attr-defined]
                except Exception:
                    pass
            results.append(self.summary(job))
        return results

    def logs(self, job_id: str, tail: int, max_bytes: int) -> dict[str, Any]:
        job = self.require(job_id)
        if job["backend"] == "ssh":
            return self.backends["ssh"].read_logs(job, tail, max_bytes)  # type: ignore[attr-defined]
        each = max(1, max_bytes // 2)
        result: dict[str, Any] = {"truncated": False, "stdout_tail": "", "stderr_tail": ""}
        for stream in ("stdout", "stderr"):
            path = Path(job[f"{stream}_path"])
            data, truncated = _tail_file(path, tail, each)
            result[f"{stream}_tail"] = data
            result["truncated"] = result["truncated"] or truncated
        return result

    def inspect(self, job_id: str) -> dict[str, Any]:
        job = self.require(job_id)
        return {**self.summary(job), "command": job["command"], "cwd": job["cwd"], "env": job["env"], "events": self.store.events(job_id), "error": job["error"]}

    def artifacts(self, job: dict[str, Any]) -> list[dict[str, Any]]:
        if job["backend"] == "ssh":
            return self.backends["ssh"].artifacts(job, self.settings.max_return_bytes)  # type: ignore[attr-defined]
        cwd = Path(job["cwd"] or ".")
        items: list[dict[str, Any]] = []
        for declared in job["artifact_paths"]:
            path = Path(declared)
            resolved = path if path.is_absolute() else cwd / path
            item: dict[str, Any] = {"path": declared, "exists": resolved.is_file()}
            if resolved.is_file():
                stat = resolved.stat()
                item.update(size_bytes=stat.st_size, modified_at=datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat().replace("+00:00", "Z"))
                if resolved.suffix.lower() == ".json" and stat.st_size <= self.settings.max_return_bytes:
                    try:
                        item["content"] = json.loads(resolved.read_text(encoding="utf-8"))
                    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                        item["parse_error"] = str(exc)
            items.append(item)
        return items

    def _apply_stall(self, job: dict[str, Any]) -> dict[str, Any]:
        timeout = job.get("stall_timeout_seconds")
        if not timeout or job["state"] not in {"running", "stalled"}:
            return job
        candidates = [parse_time(job["started_at"]).timestamp() if parse_time(job["started_at"]) else 0]
        if job["backend"] == "ssh":
            candidates.append(parse_time(job.get("last_output_at")).timestamp() if parse_time(job.get("last_output_at")) else 0)
        else:
            candidates.extend(
                Path(job[key]).stat().st_mtime if Path(job[key]).exists() else 0
                for key in ("stdout_path", "stderr_path")
            )
        latest = max(candidates)
        desired = "stalled" if time.time() - latest >= timeout else "running"
        return self.store.update(job["job_id"], state=desired) if desired != job["state"] else job

    @staticmethod
    def summary(job: dict[str, Any]) -> dict[str, Any]:
        start = parse_time(job.get("started_at")) or parse_time(job.get("created_at"))
        finish = parse_time(job.get("finished_at")) or datetime.now(timezone.utc)
        elapsed = max(0.0, (finish - start).total_seconds()) if start else 0.0
        stdout = Path(job["stdout_path"])
        stderr = Path(job["stderr_path"])
        mtimes = [path.stat().st_mtime for path in (stdout, stderr) if path.exists() and path.stat().st_size]
        local_last_output = datetime.fromtimestamp(max(mtimes), timezone.utc).isoformat().replace("+00:00", "Z") if mtimes else None
        return {
            "job_id": job["job_id"], "name": job["name"], "backend": job["backend"], "host": job["host"],
            "state": job["state"], "pid": job["pid"], "backend_id": job["backend_id"],
            "created_at": job["created_at"], "started_at": job["started_at"], "finished_at": job["finished_at"],
            "elapsed_seconds": round(elapsed, 3), "duration_seconds": round(elapsed, 3) if job.get("finished_at") else None,
            "exit_code": job["exit_code"],
            "last_output_at": local_last_output or job.get("last_output_at"),
            "stdout_bytes": stdout.stat().st_size if stdout.exists() and job["backend"] == "local" else job.get("stdout_bytes", 0),
            "stderr_bytes": stderr.stat().st_size if stderr.exists() and job["backend"] == "local" else job.get("stderr_bytes", 0),
            "backend_connected": True,
            "error": job["error"],
        }


def _tail_file(path: Path, lines: int, max_bytes: int) -> tuple[str, bool]:
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
