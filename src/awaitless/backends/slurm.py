from __future__ import annotations

import hashlib
import json
import posixpath
import shlex
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from ..config import Settings, ssh_target_and_options
from ..constants import TERMINAL_STATES
from ..db import Store
from ..util import parse_time, utc_now
from .ssh import SSHError


SCHEDULER_COMMANDS = frozenset({"sbatch", "squeue", "sacct", "scancel"})
PENDING_STATES = frozenset(
    {
        "PENDING",
        "REQUEUED",
        "REQUEUE_FED",
        "REQUEUE_HOLD",
        "RESV_DEL_HOLD",
        "SPECIAL_EXIT",
    }
)
RUNNING_STATES = frozenset(
    {
        "RUNNING",
        "COMPLETING",
        "CONFIGURING",
        "EXPEDITING",
        "POWER_UP_NODE",
        "RESIZING",
        "SIGNALING",
        "STAGE_OUT",
        "SUSPENDED",
    }
)
FAILURE_STATES = frozenset(
    {
        "BOOT_FAIL",
        "FAILED",
        "LAUNCH_FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "RECONFIG_FAIL",
        "REVOKED",
    }
)
SLURM_OPTION_FLAGS = {
    "account": "--account",
    "constraint": "--constraint",
    "cpus_per_task": "--cpus-per-task",
    "gres": "--gres",
    "mem": "--mem",
    "nodes": "--nodes",
    "ntasks": "--ntasks",
    "partition": "--partition",
    "qos": "--qos",
    "time": "--time",
}


@dataclass(frozen=True)
class RemoteFile:
    exists: bool
    size: int = 0
    data: bytes = b""
    modified_at: str | None = None


def _sftp_quote(value: str) -> str:
    if "\n" in value or "\r" in value or "\0" in value:
        raise ValueError("remote paths cannot contain newlines or NUL bytes")
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    for character in "*?[]":
        escaped = escaped.replace(character, "\\" + character)
    return f'"{escaped}"'


def _sftp_path(value: str) -> str:
    if value == "~":
        return "."
    if value.startswith("~/"):
        return value[2:]
    if value.startswith("~"):
        raise ValueError("~user paths are not supported; use ~ or an absolute path")
    return value


def _state_name(value: str) -> str:
    return value.strip().upper().split(maxsplit=1)[0].rstrip("+")


def _exit_code(value: str) -> int | None:
    code, separator, signal = value.strip().partition(":")
    if not code.isdigit() or (separator and not signal.isdigit()):
        return None
    status = int(code)
    signal_number = int(signal) if separator else 0
    return status if status or not signal_number else 128 + signal_number


class SlurmBackend:
    """Submit compute work to Slurm while keeping the SSH login node control-only."""

    name = "slurm"

    def __init__(self, store: Store, settings: Settings):
        self.store = store
        self.settings = settings

    def _operation_timeout(self, host: str, default: float = 10) -> float:
        configured = self.settings.hosts.get(host, {}).get("operation_timeout")
        if configured is None:
            return default
        if isinstance(configured, bool):
            raise ValueError(f"hosts.{host}.operation_timeout must be positive")
        try:
            value = float(configured)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"hosts.{host}.operation_timeout must be positive"
            ) from exc
        if value <= 0:
            raise ValueError(f"hosts.{host}.operation_timeout must be positive")
        return max(default, value)

    def _invoke(
        self,
        host: str,
        command: list[str],
        *,
        stdin: str | None = None,
        timeout: float = 10,
    ) -> str:
        if not command or command[0] not in SCHEDULER_COMMANDS:
            raise ValueError("Slurm SSH control command is not allowlisted")
        target, options, _ = ssh_target_and_options(self.settings, host)
        try:
            result = subprocess.run(
                ["ssh", *options, target, shlex.join(command)],
                input=stdin,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=self._operation_timeout(host, timeout),
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SSHError(f"Slurm SSH connection to {host!r} failed: {exc}") from exc
        if result.returncode != 0:
            detail = result.stderr.strip()[-1000:]
            raise SSHError(
                f"Slurm command {command[0]!r} on {host!r} failed "
                f"({result.returncode}): {detail}"
            )
        return result.stdout

    def _sftp_options(self, host: str) -> tuple[str, list[str]]:
        target, ssh_options, _ = ssh_target_and_options(self.settings, host)
        options: list[str] = []
        index = 0
        while index < len(ssh_options):
            option = ssh_options[index]
            if option == "-p":
                options.extend(["-P", ssh_options[index + 1]])
                index += 2
            elif option in {"-i", "-o"}:
                options.extend([option, ssh_options[index + 1]])
                index += 2
            else:
                options.append(option)
                index += 1
        return target, options

    def _sftp(self, host: str, commands: list[str], *, timeout: float = 10) -> str:
        target, options = self._sftp_options(host)
        batch = "\n".join(commands + ["@quit"]) + "\n"
        try:
            result = subprocess.run(
                ["sftp", "-q", "-b", "-", *options, target],
                input=batch,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=self._operation_timeout(host, timeout),
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SSHError(f"Slurm SFTP connection to {host!r} failed: {exc}") from exc
        if result.returncode != 0:
            detail = result.stderr.strip()[-1000:]
            raise SSHError(f"Slurm SFTP operation on {host!r} failed: {detail}")
        return result.stdout

    def _job_root(self, host: str, job_id: str) -> str:
        configured = self.settings.hosts.get(host, {}).get(
            "slurm_job_dir", ".awaitless/slurm/jobs"
        )
        root = _sftp_path(str(configured)).rstrip("/") or "."
        return posixpath.join(root, job_id)

    def _ensure_job_directory(self, host: str, job_id: str) -> str:
        path = self._job_root(host, job_id)
        pure = PurePosixPath(path)
        current = "/" if pure.is_absolute() else ""
        commands: list[str] = []
        for part in pure.parts:
            if part in {"/", "."}:
                continue
            current = posixpath.join(current, part) if current else part
            commands.append(f"-@mkdir {_sftp_quote(current)}")
        commands.extend([f"@cd {_sftp_quote(path)}", "@pwd"])
        output = self._sftp(host, commands)
        prefix = "Remote working directory: "
        for line in reversed(output.splitlines()):
            if line.startswith(prefix):
                return line.removeprefix(prefix)
        raise SSHError(f"Slurm SFTP on {host!r} returned no working directory")

    def _remote_stat(self, host: str, path: str) -> tuple[bool, int]:
        output = self._sftp(host, [f"-@ls -ln {_sftp_quote(_sftp_path(path))}"])
        for line in output.splitlines():
            fields = line.split()
            if len(fields) >= 5 and fields[0][:1] in {"-", "l", "d"}:
                try:
                    return fields[0].startswith("-"), int(fields[4])
                except ValueError:
                    continue
        return False, 0

    def _read_remote_file(self, host: str, path: str, max_bytes: int) -> RemoteFile:
        remote_path = _sftp_path(path)
        is_file, size = self._remote_stat(host, remote_path)
        if not is_file:
            return RemoteFile(exists=False)
        offset = max(0, size - max_bytes)
        with tempfile.TemporaryDirectory(prefix="awaitless-sftp-") as temp:
            local_path = Path(temp) / "download"
            with local_path.open("wb") as handle:
                handle.truncate(offset)
            operation = "reget" if offset else "get"
            self._sftp(
                host,
                [
                    f"@{operation} -p {_sftp_quote(remote_path)} "
                    f"{_sftp_quote(str(local_path))}"
                ],
            )
            with local_path.open("rb") as handle:
                handle.seek(max(0, local_path.stat().st_size - max_bytes))
                data = handle.read(max_bytes)
            modified_at = datetime.fromtimestamp(
                local_path.stat().st_mtime, timezone.utc
            ).isoformat().replace("+00:00", "Z")
        return RemoteFile(
            exists=True,
            size=max(size, offset + len(data)),
            data=data,
            modified_at=modified_at,
        )

    def _remote_sha256(self, host: str, path: str) -> str:
        """Hash a complete Artifact through the SFTP data channel."""
        with tempfile.TemporaryDirectory(prefix="awaitless-sftp-artifact-") as temp:
            local_path = Path(temp) / "artifact"
            self._sftp(
                host,
                [
                    f"@get -p {_sftp_quote(_sftp_path(path))} "
                    f"{_sftp_quote(str(local_path))}"
                ],
            )
            digest = hashlib.sha256()
            with local_path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            return digest.hexdigest()

    def _slurm_options(self, host: str, spec: dict[str, Any]) -> list[str]:
        configured = self.settings.hosts.get(host, {}).get("slurm", {})
        if not isinstance(configured, dict):
            raise ValueError(f"hosts.{host}.slurm must be a table")
        requested = spec.get("backend_options") or {}
        if not isinstance(requested, dict):
            raise ValueError("Slurm options must be an object")
        unknown = (set(configured) | set(requested)) - set(SLURM_OPTION_FLAGS)
        if unknown:
            names = ", ".join(sorted(unknown))
            raise ValueError(f"unsupported Slurm option(s): {names}")
        merged = {**configured, **requested}
        options: list[str] = []
        for name, flag in SLURM_OPTION_FLAGS.items():
            if name not in merged or merged[name] is None:
                continue
            value = merged[name]
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                raise ValueError(f"Slurm option {name!r} must be a string or number")
            options.append(f"{flag}={value}")
        return options

    def submit(self, job: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
        assert job["host"]
        absolute_job_dir = self._ensure_job_directory(job["host"], job["job_id"])
        stdout_path = posixpath.join(absolute_job_dir, "stdout.log")
        stderr_path = posixpath.join(absolute_job_dir, "stderr.log")
        command = shlex.join(spec["command"])
        timeout = spec.get("timeout_seconds")
        if timeout:
            command = (
                "timeout --signal=TERM --kill-after=2s "
                f"{float(timeout):g}s {command}"
            )
        exports = "\n".join(
            f"export {key}={shlex.quote(value)}"
            for key, value in spec.get("env", {}).items()
        )
        script = f"""#!/usr/bin/env bash
set +e
{exports}
{command}
exit $?
"""
        slurm_name = str(job.get("name") or f"awaitless-{job['job_id'][-10:]}")
        submit = [
            "sbatch",
            "--parsable",
            "--open-mode=truncate",
            f"--job-name={slurm_name}",
            f"--output={stdout_path}",
            f"--error={stderr_path}",
            *self._slurm_options(job["host"], spec),
        ]
        if spec.get("cwd"):
            submit.append(f"--chdir={spec['cwd']}")
        output = self._invoke(job["host"], submit, stdin=script)
        slurm_id = output.strip().splitlines()[-1].split(";", 1)[0]
        if not slurm_id.isdigit():
            raise SSHError(f"sbatch returned an invalid job ID: {output.strip()!r}")
        return self.store.update_if_active(
            job["job_id"], state="queued", backend_id=slurm_id
        )

    def _mapped_update(
        self,
        job: dict[str, Any],
        slurm_state: str,
        exit_value: str | None = None,
        elapsed_value: str | None = None,
    ) -> dict[str, Any]:
        state = _state_name(slurm_state)
        exit_code = _exit_code(exit_value or "")
        elapsed = (
            int(elapsed_value)
            if elapsed_value and elapsed_value.strip().isdigit()
            else None
        )
        observed_finish = datetime.now(timezone.utc)
        finished_at = observed_finish.isoformat().replace("+00:00", "Z")
        started_at = (
            (observed_finish - timedelta(seconds=elapsed)).isoformat().replace(
                "+00:00", "Z"
            )
            if elapsed is not None
            else job.get("started_at") or finished_at
        )
        updates: dict[str, Any] = {}
        if state in PENDING_STATES:
            updates["state"] = "queued"
        elif state in RUNNING_STATES:
            updates["state"] = "running"
            updates["started_at"] = job.get("started_at") or utc_now()
        elif state == "COMPLETED":
            updates.update(
                state="succeeded" if exit_code in {None, 0} else "failed",
                exit_code=0 if exit_code is None else exit_code,
                started_at=started_at,
                finished_at=finished_at,
            )
            if exit_code not in {None, 0}:
                updates["error"] = f"Slurm job exited with code {exit_code}"
        elif state == "CANCELLED":
            updates.update(
                state="cancelled",
                exit_code=exit_code,
                started_at=started_at,
                finished_at=finished_at,
            )
        elif state in {"TIMEOUT", "DEADLINE"} or (
            exit_code == 124 and job.get("timeout_seconds") is not None
        ):
            updates.update(
                state="timed_out",
                exit_code=exit_code,
                started_at=started_at,
                finished_at=finished_at,
                error=f"Slurm job ended in {state}",
            )
        elif state in FAILURE_STATES:
            updates.update(
                state="failed",
                exit_code=exit_code,
                started_at=started_at,
                finished_at=finished_at,
                error=f"Slurm job ended in {state}",
            )
        else:
            return job
        return self.store.update_if_active(job["job_id"], **updates)

    def refresh(self, job: dict[str, Any]) -> dict[str, Any]:
        if job["state"] in TERMINAL_STATES:
            return job
        assert job["host"]
        if not job.get("backend_id"):
            return job
        slurm_id = str(job["backend_id"])
        queued = self._invoke(
            job["host"],
            ["squeue", "--noheader", f"--jobs={slurm_id}", "--format=%T"],
        ).strip()
        if queued:
            state = queued.splitlines()[0]
            if _state_name(state) in PENDING_STATES | RUNNING_STATES:
                return self._mapped_update(job, state)
        accounting = self._invoke(
            job["host"],
            [
                "sacct",
                "--noheader",
                "--parsable2",
                "--allocations",
                f"--jobs={slurm_id}",
                "--format=JobIDRaw,State,ExitCode,ElapsedRaw",
            ],
        )
        for line in accounting.splitlines():
            fields = line.strip().split("|")
            if len(fields) >= 4 and fields[0] == slurm_id:
                return self._mapped_update(job, fields[1], fields[2], fields[3])
        grace = self.settings.hosts.get(job["host"], {}).get(
            "slurm_accounting_grace", 120
        )
        if isinstance(grace, bool):
            raise ValueError(
                f"hosts.{job['host']}.slurm_accounting_grace must be non-negative"
            )
        grace_seconds = float(grace)
        updated_at = parse_time(job.get("updated_at"))
        if grace_seconds < 0:
            raise ValueError(
                f"hosts.{job['host']}.slurm_accounting_grace must be non-negative"
            )
        if updated_at and (datetime.now(timezone.utc) - updated_at).total_seconds() >= grace_seconds:
            return self.store.update_if_active(
                job["job_id"],
                state="lost",
                finished_at=utc_now(),
                error="Slurm job disappeared from both squeue and sacct",
            )
        return job

    def cancel(self, job: dict[str, Any], grace_seconds: float) -> dict[str, Any]:
        del grace_seconds
        if job["state"] in TERMINAL_STATES:
            return job
        assert job["host"]
        if not job.get("backend_id"):
            return self.store.update_if_active(
                job["job_id"],
                state="lost",
                finished_at=utc_now(),
                error="Slurm job has no scheduler ID",
            )
        self._invoke(job["host"], ["scancel", str(job["backend_id"])])
        return self.store.update_if_active(
            job["job_id"], state="cancelled", finished_at=utc_now()
        )

    def read_logs(
        self, job: dict[str, Any], tail: int, max_bytes: int
    ) -> dict[str, Any]:
        assert job["host"]
        each = max(1, max_bytes // 2)
        result: dict[str, Any] = {
            "truncated": False,
            "stdout_tail": "",
            "stderr_tail": "",
        }
        updates: dict[str, Any] = {}
        modified: list[str] = []
        for stream in ("stdout", "stderr"):
            path = posixpath.join(self._job_root(job["host"], job["job_id"]), f"{stream}.log")
            remote = self._read_remote_file(job["host"], path, each)
            data = remote.data
            lines = data.splitlines(keepends=True)
            selected = b"" if tail == 0 else b"".join(lines[-tail:])
            result[f"{stream}_tail"] = selected.decode("utf-8", errors="replace")
            result["truncated"] = (
                result["truncated"]
                or remote.size > len(data)
                or (tail >= 0 and len(lines) > tail)
            )
            result[f"{stream}_bytes"] = remote.size
            updates[f"{stream}_bytes"] = remote.size
            if remote.modified_at:
                modified.append(remote.modified_at)
        if modified:
            updates["last_output_at"] = max(modified)
            result["last_output_at"] = max(modified)
        if updates:
            self.store.update(job["job_id"], **updates)
        return result

    def _artifact_path(self, job: dict[str, Any], declared: str) -> str:
        if declared.startswith("/"):
            return declared
        if job.get("cwd"):
            return posixpath.join(_sftp_path(str(job["cwd"])), declared)
        return _sftp_path(declared)

    def artifacts(self, job: dict[str, Any], max_bytes: int) -> list[dict[str, Any]]:
        assert job["host"]
        items: list[dict[str, Any]] = []
        for declared in job["artifact_paths"]:
            remote_path = self._artifact_path(job, declared)
            is_file, size = self._remote_stat(job["host"], remote_path)
            item: dict[str, Any] = {
                "path": declared,
                "remote": True,
                "exists": is_file,
            }
            if is_file:
                item["size_bytes"] = size
                item["sha256"] = self._remote_sha256(job["host"], remote_path)
                if declared.lower().endswith(".json") and size <= max_bytes:
                    remote = self._read_remote_file(job["host"], remote_path, max_bytes)
                    item["modified_at"] = remote.modified_at
                    try:
                        item["content"] = json.loads(remote.data)
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        item["parse_error"] = str(exc)
            items.append(item)
        return items
