from __future__ import annotations

import hashlib
import base64
import json
import os
import re
import shutil
import time
import glob
import shlex
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .backends import LocalBackend, SSHBackend, SlurmBackend
from .backends.ssh import SSHError
from .config import Settings
from .constants import TERMINAL_STATES
from .db import Store
from .util import atomic_json, new_job_id, parse_time, utc_now


ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
CLIENT_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$")
QUEUE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
COMPLETION_CURSOR = re.compile(r"^cmp_([0-9]{1,19})$")
SENSITIVE_NAME = re.compile(r"TOKEN|SECRET|PASSWORD|PASSWD|PRIVATE|CREDENTIAL|API_KEY", re.I)
MAX_COMPLETION_JOBS = 500
MAX_COMPLETION_LIMIT = 500
MAX_COMPLETION_EVENT_ID = (1 << 63) - 1


def _redirected_paths(command: list[str]) -> list[str]:
    text = " ".join(command)
    found = re.findall(r"(?:^|\s)(?:\d?>|\d?>>)\s*([^\s;&|]+)", text)
    return [path.strip("'\"") for path in found if path not in {"/dev/null", "-"} and not path.startswith("&")]


class AwaitlessError(RuntimeError):
    pass


class JobNotFound(AwaitlessError):
    pass


class PreflightError(AwaitlessError):
    def __init__(self, result: dict[str, Any]):
        super().__init__(str(result.get("reason") or "preflight failed"))
        self.result = result


class Service:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = Store(settings.db_path)
        self.backends = {
            "local": LocalBackend(self.store),
            "ssh": SSHBackend(self.store, settings),
            "slurm": SlurmBackend(self.store, settings),
        }

    def close(self) -> None:
        self.store.close()

    def submit_group(self, *, group_id: str, devices: list[str], build: str | None = None, run_commands: dict[str, str] | None = None, **kwargs: Any) -> dict[str, Any]:
        if not QUEUE_NAME.fullmatch(group_id):
            raise AwaitlessError("group ID must use letters, digits, dot, underscore, or hyphen")
        if not devices:
            raise AwaitlessError("at least one device is required for a group")
        build_result = None
        if build:
            build_kwargs = dict(kwargs)
            build_kwargs.pop("command", None)
            build_kwargs["queue_name"] = None
            build_kwargs["device_mode"] = "physical"
            build_result = self.submit(job_id=new_job_id(), command=shlex.split(build), device=None, **build_kwargs)
            build_result, _ = self.wait(build_result["job_id"])
            if build_result.get("state") != "succeeded":
                raise AwaitlessError(f"build failed before group fan-out: {build_result.get('job_id')}")
        jobs = []
        for device in devices:
            job_kwargs = dict(kwargs)
            command = job_kwargs.pop("command")
            if run_commands and device in run_commands:
                command = shlex.split(run_commands[device])
            jobs.append(self.submit(job_id=new_job_id(), device=device, command=command, **job_kwargs))
        payload = {"group_id": group_id, "created_at": utc_now(), "job_ids": [item["job_id"] for item in jobs], "devices": devices}
        if build_result:
            payload["build_job_id"] = build_result["job_id"]
        atomic_json(self.settings.data_dir / "groups" / f"{group_id}.json", payload)
        return {**payload, "jobs": jobs}

    def group(self, group_id: str) -> dict[str, Any]:
        path = self.settings.data_dir / "groups" / f"{group_id}.json"
        if not path.is_file():
            raise AwaitlessError(f"unknown experiment group: {group_id}")
        return json.loads(path.read_text(encoding="utf-8"))

    def wait_group(self, group_id: str, wait_timeout: float | None = None) -> dict[str, Any]:
        group = self.group(group_id)
        started = time.monotonic()
        cursor = None
        completions: list[dict[str, Any]] = []
        active = list(group["job_ids"])
        while active:
            remaining = None if wait_timeout is None else max(0.0, wait_timeout - (time.monotonic() - started))
            batch = self.completions(active, after_cursor=cursor, wait_timeout=remaining, limit=500)
            completions.extend(batch["completions"])
            cursor = batch["next_cursor"]
            active = batch["active_job_ids"]
            if batch["wait_timed_out"]:
                break
        by_job = {item["job_id"]: item for item in completions}
        rows = []
        for job_id, device in zip(group["job_ids"], group["devices"]):
            item = by_job.get(job_id)
            result = (item or {}).get("result", {})
            rows.append({"job_id": job_id, "device": device, "state": (item or {}).get("state", result.get("state", "active")), "exit_code": result.get("exit_code"), "duration_seconds": result.get("duration_seconds"), "stage": result.get("stage"), "reason": result.get("reason"), "last_log": (result.get("stderr_tail") or result.get("stdout_tail", ""))[-1000:]})
        return {"group_id": group_id, "rows": rows, "completions": completions, "next_cursor": cursor, "active_job_ids": active, "wait_timed_out": bool(active)}

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
        backend_options: dict[str, Any] | None = None,
        client_request_id: str | None = None,
        client_session: str | None = None,
        mcp_task_ttl_ms: int | None = None,
        queue_name: str | None = None,
        device: str | None = None,
        device_mode: str = "physical",
        capture_logs: list[str] | None = None,
        resources: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not command:
            raise AwaitlessError("a command is required after --")
        if backend not in self.backends:
            raise AwaitlessError(f"unsupported backend: {backend}")
        capture_logs = list(dict.fromkeys((capture_logs or []) + _redirected_paths(command)))
        resources = dict(resources or {})
        if resources and queue_name is None and backend != "slurm":
            identity = "-".join(f"{key}-{value}" for key, value in sorted(resources.items()))
            identity = re.sub(r"[^A-Za-z0-9._-]+", "-", identity).strip("-")[:50]
            queue_name = f"resource-{identity}"
            try:
                self.store.create_queue(queue_name, 1)
            except ValueError as exc:
                raise AwaitlessError(str(exc)) from exc
        if backend in {"ssh", "slurm"} and not host:
            raise AwaitlessError(f"{backend.upper()} backend requires --host")
        if backend == "local" and host:
            raise AwaitlessError("--host can only be used with an SSH-based backend")
        if device is not None:
            if not str(device).isdigit():
                raise AwaitlessError("device must be a numeric device ID")
            device = str(device)
            env = dict(env)
            if device_mode not in {"physical", "native"}:
                raise AwaitlessError("device-mode must be physical or native")
            env.setdefault("AWAITLESS_PHYSICAL_DEVICE", device)
            env.setdefault("ASCEND_RT_VISIBLE_DEVICES", device)
            env.setdefault("ASCEND_DEVICE_ID", "0" if device_mode == "physical" else device)
            env.setdefault("RANK_ID", "0")
            if queue_name is None:
                queue_name = f"device-{device}"
                if host:
                    safe_host = re.sub(r"[^A-Za-z0-9._-]+", "-", host).strip("-")
                    queue_name = f"{safe_host}-{queue_name}"[:64]
                try:
                    self.store.create_queue(queue_name, 1)
                except ValueError as exc:
                    raise AwaitlessError(str(exc)) from exc
        queue: dict[str, Any] | None = None
        if queue_name is not None:
            if not QUEUE_NAME.fullmatch(queue_name):
                raise AwaitlessError(
                    "queue name must be 1-64 characters using letters, digits, "
                    "dot, underscore, or hyphen"
                )
            if backend == "slurm":
                raise AwaitlessError(
                    "--queue is not supported for Slurm; submit directly and let "
                    "Slurm schedule the requested resources"
                )
            queue = self.store.get_queue(queue_name)
            if not queue:
                raise AwaitlessError(
                    f"unknown queue {queue_name!r}; create it with "
                    f"awaitless queue create {queue_name} --concurrency N"
                )
            if backend == "local":
                self._reconcile_local_queue(queue_name)
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise AwaitlessError("--timeout must be positive")
        if stall_timeout_seconds is not None and stall_timeout_seconds <= 0:
            raise AwaitlessError("--stall-timeout must be positive")
        if client_request_id is not None and not CLIENT_REQUEST_ID.fullmatch(
            client_request_id
        ):
            raise AwaitlessError(
                "client_request_id must be 1-200 characters using letters, digits, "
                "dot, underscore, colon, slash, or hyphen"
            )
        if mcp_task_ttl_ms is not None and (
            isinstance(mcp_task_ttl_ms, bool)
            or not isinstance(mcp_task_ttl_ms, int)
            or mcp_task_ttl_ms <= 0
        ):
            raise AwaitlessError("MCP task TTL must be a positive integer")
        for key in env:
            if not ENV_NAME.fullmatch(key):
                raise AwaitlessError(f"invalid environment variable name: {key!r}")

        if backend == "local":
            local_cwd = Path(cwd).expanduser() if cwd else Path.cwd()
            if not local_cwd.is_dir():
                raise AwaitlessError(f"working directory does not exist: {cwd}")
            resolved_cwd = str(local_cwd.resolve())
        else:
            resolved_cwd = cwd

        job_dir = self.settings.jobs_dir / job_id
        log_root: Path | None = None
        if log_dir:
            log_root = Path(log_dir).expanduser().resolve()
            logs = log_root / job_id
        else:
            logs = job_dir
        stdout_path = logs / "stdout.log"
        stderr_path = logs / "stderr.log"
        redacted_env = {key: ("<redacted>" if SENSITIVE_NAME.search(key) else value) for key, value in env.items()}
        metadata = {
            "job_id": job_id,
            "client_request_id": client_request_id,
            "client_session": client_session,
            "name": name,
            "backend": backend,
            "host": host,
            "command": command,
            "cwd": resolved_cwd,
            "env": redacted_env,
            "artifacts": artifacts,
            "backend_options": backend_options or {},
            "queue": queue_name,
            "device": device,
            "device_mode": device_mode if device is not None else None,
            "capture_logs": capture_logs,
            "resources": resources,
            "environment": self._environment_snapshot(env),
            "created_at": utc_now(),
        }
        spec = {
            "command": command,
            "cwd": resolved_cwd,
            "env": env,
            "timeout_seconds": timeout_seconds,
            "backend_options": backend_options or {},
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "queue": (
                {"name": queue_name, "concurrency": queue["concurrency"]}
                if queue_name and queue
                else None
            ),
            "device": device,
            "device_mode": device_mode,
            "capture_logs": capture_logs,
            "resources": resources,
            "environment": self._environment_snapshot(env),
        }
        values = {
            "job_id": job_id,
            "client_session": client_session,
            "name": name,
            "backend": backend,
            "host": host,
            "command_json": json.dumps(command),
            "cwd": resolved_cwd,
            "env_json": json.dumps(redacted_env),
            "state": "queued" if queue_name else "starting",
            "timeout_seconds": timeout_seconds,
            "stall_timeout_seconds": stall_timeout_seconds,
            "job_dir": str(job_dir),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "artifacts_json": json.dumps(artifacts),
            "mcp_task_ttl_ms": mcp_task_ttl_ms,
            "queue_name": queue_name,
            "device": device,
            "device_mode": device_mode if device is not None else None,
            "capture_logs_json": json.dumps(capture_logs),
            "resources_json": json.dumps(resources),
            "environment_json": json.dumps(self._environment_snapshot(env)),
            # Phase is reserved for command-reported AWAITLESS_PHASE markers.
            # Lifecycle and queue admission are exposed separately.
            "phase": None,
        }
        if client_request_id is not None:
            fingerprint = _submission_fingerprint(
                command=command,
                backend=backend,
                host=host,
                cwd=resolved_cwd,
                env=env,
                timeout_seconds=timeout_seconds,
                stall_timeout_seconds=stall_timeout_seconds,
                name=name,
                client_session=client_session,
                artifacts=artifacts,
                log_dir=str(log_root) if log_root else None,
                backend_options=backend_options or {},
                queue_name=queue_name,
                device=device,
                capture_logs=capture_logs,
                resources=resources,
                mcp_task=mcp_task_ttl_ms is not None,
            )
            try:
                job, created = self.store.reserve_submission(
                    values,
                    client_request_id=client_request_id,
                    fingerprint=fingerprint,
                )
            except ValueError as exc:
                raise AwaitlessError(str(exc)) from exc
            if not created:
                result = self.summary(job)
                result["idempotent_replay"] = True
                return result
        else:
            self.store.create(values)
            job = self.require(job_id)

        try:
            job_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
            if log_root:
                log_root.mkdir(mode=0o700, parents=True, exist_ok=True)
                logs.mkdir(mode=0o700, exist_ok=False)
            stdout_path.touch(mode=0o600)
            stderr_path.touch(mode=0o600)
            atomic_json(job_dir / "metadata.json", metadata)
            spec_path = job_dir / "run-spec.json"
            atomic_json(spec_path, spec)
            os.chmod(spec_path, 0o600)
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
        result["idempotent_replay"] = False
        # The submit contract reports a successfully launched command as running even when a
        # very short command reaches its terminal state before the client receives the reply.
        if (
            not queue_name
            and result["state"] in TERMINAL_STATES
            and job.get("started_at")
            and job.get("pid")
        ):
            result["state"] = "running"
            result["exit_code"] = None
            result["finished_at"] = None
        return result

    def doctor_remote(
        self,
        host: str,
        *,
        cwd: str | None = None,
        devices: list[str] | None = None,
        env: dict[str, str] | None = None,
        sources: list[str] | None = None,
        user_group: str | None = None,
        require_flock: bool = False,
    ) -> dict[str, Any]:
        """Run a bounded, machine-readable prerequisite check on an SSH target."""
        backend = self.backends["ssh"]
        checks: list[dict[str, Any]] = []
        checks.append({"name": "cwd", "ok": True, "required": False})
        env = dict(env or {})
        sources = list(sources or [])
        for key in env:
            if not ENV_NAME.fullmatch(key):
                raise AwaitlessError(f"invalid environment variable name: {key!r}")
        if user_group and not re.fullmatch(r"[A-Za-z0-9_.-]+", user_group):
            raise AwaitlessError("user group contains unsupported characters")
        body = "set +e\n"
        if cwd:
            body += f"cd -- {shlex.quote(cwd)} 2>/dev/null; echo CHECK_cwd=$?\n"
        for source in sources:
            body += f". {shlex.quote(str(source))} 2>/dev/null; echo CHECK_source_{len(checks)}=$?\n"
            checks.append({"name": f"source:{source}", "ok": True, "required": True, "value_key": f"CHECK_source_{len(checks)}"})
        for key, value in env.items():
            body += f"export {key}={shlex.quote(str(value))}\n"
        body += "printf 'PATH=%s\\n' \"$PATH\"\n"
        for name, command in (("bash", "bash"), ("cmake", "cmake"), ("npu_smi", "npu-smi"), ("python", "python3")):
            body += f"command -v {command} >/dev/null 2>&1; echo CHECK_{name}=$?\n"
        body += "command -v flock >/dev/null 2>&1; echo CHECK_flock=$?\n"
        if cwd:
            body += "test -r . -a -x .; echo CHECK_permissions=$?\n"
            body += "df -Pk . 2>/dev/null | awk 'NR==2 {print \"DISK_KB=\" $4}'\n"
        body += "if [ -n \"${ASCEND_INSTALL_ROOT:-${ASCEND_HOME_PATH:-}}\" ] || [ -d /usr/local/Ascend ]; then echo CHECK_cann=0; else echo CHECK_cann=1; fi\n"
        if devices:
            body += "npu_tmp=/tmp/awaitless-npu-smi.$$; if command -v npu-smi >/dev/null 2>&1; then npu-smi info >\"$npu_tmp\" 2>&1; echo NPU_RC=$?; else : >\"$npu_tmp\"; echo NPU_RC=127; fi\n"
            for device in devices:
                if not device.isdigit():
                    raise AwaitlessError("devices must be comma-separated numeric IDs")
                body += f"grep -Eq '(^|[^0-9]){device}([^0-9]|$)' \"$npu_tmp\"; echo CHECK_device_{device}=$?\n"
            body += "rm -f \"$npu_tmp\"\n"
        script = body
        if user_group:
            encoded = base64.b64encode(body.encode()).decode()
            script = (
                "set +e\n"
                f"command -v sg >/dev/null 2>&1; echo CHECK_sg=$?\n"
                f"id -Gn | tr ' ' '\\n' | grep -Fx {shlex.quote(user_group)} >/dev/null; echo CHECK_group=$?\n"
                f"printf %s {shlex.quote(encoded)} | base64 -d | sg {shlex.quote(user_group)} -c 'bash -s'\n"
                "echo SG_RC=$?\n"
            )
        try:
            output = backend._invoke(host, script, timeout=15)  # type: ignore[attr-defined]
        except SSHError as exc:
            return {"ok": False, "host": host, "stage": "connection", "reason": "ssh_unreachable", "suggestion": "Check SSH credentials, hostname and network.", "error": str(exc), "checks": checks}
        values = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
        for item in checks:
            if item.get("value_key"):
                item["ok"] = values.get(item.pop("value_key")) == "0"
        for name in ("bash", "cmake", "npu_smi", "python"):
            value = values.get(f"CHECK_{name}")
            required = name in {"bash", "cmake", "python"} or bool(devices) and name == "npu_smi"
            checks.append({"name": "npu-smi" if name == "npu_smi" else name, "ok": value == "0" or not required, "required": required})
        checks.append({"name": "CANN", "ok": values.get("CHECK_cann") == "0", "required": bool(devices)})
        checks.append({"name": "flock", "ok": values.get("CHECK_flock") == "0" or not require_flock, "required": require_flock})
        if user_group:
            checks.extend((
                {"name": "sg", "ok": values.get("CHECK_sg") == "0", "required": True},
                {"name": f"group:{user_group}", "ok": values.get("CHECK_group") == "0" and values.get("SG_RC") == "0", "required": True},
            ))
        if cwd and "CHECK_cwd" in values:
            checks.append({"name": "cwd_exists", "ok": values["CHECK_cwd"] == "0", "required": True})
            checks.append({"name": "cwd_permissions", "ok": values.get("CHECK_permissions") == "0", "required": True})
        for device in devices or []:
            checks.append({"name": f"device_{device}_visible", "ok": values.get(f"CHECK_device_{device}") == "0", "required": True})
        ok = all(item["ok"] for item in checks if item.get("required", True))
        failed = next((item["name"] for item in checks if not item["ok"]), None)
        return {"ok": ok, "host": host, "cwd": cwd, "stage": None if ok else "preflight_failed", "reason": None if ok else f"missing_or_invalid_{failed}", "suggestion": None if ok else "Fix the reported tool, profile, group, cwd, or device check, then retry.", "checks": checks, "execution_profile": {"sources": sources, "user_group": user_group, "env_names": sorted(env)}, "path": values.get("PATH"), "disk_available_kb": int(values["DISK_KB"]) if values.get("DISK_KB", "").isdigit() else None}

    def require(self, job_id: str) -> dict[str, Any]:
        job = self.store.get(job_id)
        if not job:
            raise JobNotFound(f"unknown job ID: {job_id}")
        return job

    def status(self, job_id: str) -> dict[str, Any]:
        job = self.require(job_id)
        if job["backend"] == "local" and job.get("queue_name"):
            self._reconcile_local_queue(job["queue_name"], exclude=job_id)
        job = self.backends[job["backend"]].refresh(job)  # type: ignore[attr-defined]
        job = self._apply_stall(job)
        try:
            observed = self.logs(job_id, 50, 8192)
            text = f"{observed.get('stdout_tail', '')}\n{observed.get('stderr_tail', '')}"
            phases = re.findall(r"(?:^|\n)AWAITLESS_PHASE=([A-Za-z0-9._-]{1,64})(?:\n|$)", text)
            if phases and phases[-1] != job.get("phase"):
                job = self.store.update(job_id, phase=phases[-1])
        except (OSError, SSHError):
            pass
        return self.summary(job)

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
                    result.update(job_state=result.get("state"), wait_state="client_timeout", delivery_state="pending")
                    return result, True
                remaining = None if wait_timeout is None else wait_timeout - (time.monotonic() - started)
                time.sleep(max(0.05, min(self.settings.poll_interval, remaining or self.settings.poll_interval)))
                continue
            if result["state"] in TERMINAL_STATES:
                result = self._terminal_result(job_id, result)
                result.update(job_state=result.get("state"), wait_state="complete", delivery_state="delivered")
                return result, False
            if wait_timeout is not None and time.monotonic() - started >= wait_timeout:
                result["wait_timed_out"] = True
                result.update(job_state=result.get("state"), wait_state="client_timeout", delivery_state="pending")
                return result, True
            remaining = None if wait_timeout is None else wait_timeout - (time.monotonic() - started)
            time.sleep(max(0.05, min(self.settings.poll_interval, remaining or self.settings.poll_interval)))

    def adaptive_wait(
        self,
        job_id: str,
        inline_timeout_seconds: float,
        *,
        detach_immediately: bool = False,
    ) -> dict[str, Any]:
        """Deliver a quick result inline or detach without changing the Job lifecycle."""
        if inline_timeout_seconds < 0:
            raise AwaitlessError("inline timeout must be non-negative")
        wait_timeout = 0.0 if detach_immediately else inline_timeout_seconds
        result, detached = self.wait(job_id, wait_timeout)
        result.pop("wait_timed_out", None)
        if detached and result.get("backend_connected", True):
            try:
                result.update(
                    self.logs(
                        job_id,
                        self.settings.log_tail_lines,
                        self.settings.max_return_bytes,
                    )
                )
            except SSHError:
                result["backend_connected"] = False
        result.update(
            delivery="detached" if detached else "inline",
            detached=detached,
            detach_reason=(
                "queued" if detach_immediately else "inline_timeout"
            )
            if detached
            else None,
            inline_timeout_seconds=inline_timeout_seconds,
        )
        if detached:
            result.update(
                job_state=result.get("state"),
                wait_state="client_timeout",
                delivery_state="pending",
                next_command=f"awaitless wait {job_id} --json",
            )
        if detached:
            self.record_recent_job(result)
        return result

    def record_recent_job(self, result: dict[str, Any]) -> None:
        recent = self.settings.data_dir / "recent-jobs.json"
        entries = []
        if recent.is_file():
            try:
                entries = json.loads(recent.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                entries = []
        entries = [result] + [item for item in entries if item.get("job_id") != result.get("job_id")]
        atomic_json(recent, entries[:50])

    def recover_last(self) -> dict[str, Any]:
        return self.recover_last_filtered()

    def recover_last_filtered(self, **filters: str | None) -> dict[str, Any]:
        path = self.settings.data_dir / "recent-jobs.json"
        if not path.is_file():
            raise AwaitlessError("no detached jobs have been recorded")
        entries = json.loads(path.read_text(encoding="utf-8"))
        normalized = {key: value for key, value in filters.items() if value is not None}
        matches: list[tuple[int, dict[str, Any]]] = []
        for index, entry in enumerate(entries):
            job = self.store.get(entry.get("job_id")) if entry.get("job_id") else None
            if job and all(str(job.get(key) or "") == value for key, value in normalized.items()):
                matches.append((index, job))
        if not matches:
            criteria = ", ".join(f"{key}={value!r}" for key, value in normalized.items()) or "the recent-job list"
            raise AwaitlessError(f"no detached job matches {criteria}")
        index, job = matches[0]
        result = self.status(job["job_id"])
        result["selected_by"] = {
            "source": "recent_jobs",
            "recent_index": index,
            "filters": normalized,
            "candidate_count": len(matches),
        }
        return result

    def completions(
        self,
        job_ids: list[str],
        *,
        after_cursor: str | None = None,
        wait_timeout: float | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        if not job_ids:
            raise AwaitlessError("at least one job ID is required")
        selected = list(dict.fromkeys(job_ids))
        if len(selected) > MAX_COMPLETION_JOBS:
            raise AwaitlessError(
                f"at most {MAX_COMPLETION_JOBS} job IDs may be watched at once"
            )
        if any(not isinstance(job_id, str) or not job_id for job_id in selected):
            raise AwaitlessError("job IDs must be non-empty strings")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit <= 0
            or limit > MAX_COMPLETION_LIMIT
        ):
            raise AwaitlessError(
                f"limit must be between 1 and {MAX_COMPLETION_LIMIT}"
            )
        if wait_timeout is not None and wait_timeout < 0:
            raise AwaitlessError("completion wait timeout must be non-negative")

        after_id = _parse_completion_cursor(after_cursor)
        if after_id > self.store.max_event_id():
            raise AwaitlessError(
                f"completion cursor {after_cursor!r} is ahead of this Awaitless store"
            )
        for job_id in selected:
            self.require(job_id)

        started = time.monotonic()
        while True:
            unreachable: list[str] = []
            for job_id in selected:
                job = self.require(job_id)
                if job["state"] in TERMINAL_STATES:
                    continue
                try:
                    self.status(job_id)
                except SSHError:
                    unreachable.append(job_id)

            events = self.store.completion_events(
                selected, after_id=after_id, limit=limit + 1
            )
            has_more = len(events) > limit
            events = events[:limit]
            current = {job_id: self.require(job_id) for job_id in selected}
            active_job_ids = [
                job_id
                for job_id in selected
                if current[job_id]["state"] not in TERMINAL_STATES
            ]
            if events:
                items: list[dict[str, Any]] = []
                for event in events:
                    job_id = event["job_id"]
                    try:
                        result = self._terminal_result(
                            job_id, self.summary(current[job_id])
                        )
                    except SSHError:
                        if job_id not in unreachable:
                            unreachable.append(job_id)
                        # Cursor order is a delivery guarantee. Never return a
                        # later event while an earlier remote result is unavailable.
                        has_more = True
                        break
                    items.append(
                        {
                            "completion_id": _format_completion_cursor(event["id"]),
                            "job_id": job_id,
                            "state": event["state"],
                            "finished_at": event["finished_at"],
                            "observed_at": event["observed_at"],
                            "result": result,
                        }
                    )
                if items:
                    return {
                        "completions": items,
                        "next_cursor": items[-1]["completion_id"],
                        "active_job_ids": active_job_ids,
                        "unreachable_job_ids": unreachable,
                        "has_more": has_more,
                        "wait_timed_out": False,
                    }

            response = {
                "completions": [],
                "next_cursor": _format_completion_cursor(after_id),
                "active_job_ids": active_job_ids,
                "unreachable_job_ids": unreachable,
                "has_more": bool(events),
                "wait_timed_out": False,
            }
            # A fully terminal and reachable selection cannot produce another
            # completion after this cursor, so return a drained feed immediately.
            if not active_job_ids and not unreachable and not events:
                return response
            if wait_timeout is not None and time.monotonic() - started >= wait_timeout:
                response["wait_timed_out"] = True
                return response
            remaining = (
                None
                if wait_timeout is None
                else wait_timeout - (time.monotonic() - started)
            )
            time.sleep(
                max(
                    0.05,
                    min(
                        self.settings.poll_interval,
                        remaining or self.settings.poll_interval,
                    ),
                )
            )

    def _terminal_result(
        self, job_id: str, summary: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        snapshot = self.store.get_snapshot(job_id)
        if snapshot:
            metadata = {key: snapshot[key] for key in ("captured_at", "sha256", "size_bytes")}
            return {**snapshot["result"], "snapshot": metadata}
        result = dict(summary or self.summary(self.require(job_id)))
        result.update(
            self.logs(
                job_id,
                self.settings.log_tail_lines,
                self.settings.max_return_bytes,
            )
        )
        result["artifacts"] = self.artifacts(self.require(job_id))
        job = self.require(job_id)
        if job["backend"] in {"ssh", "slurm"}:
            reader = getattr(self.backends[job["backend"]], "environment", None)
            if reader:
                result["environment_snapshot"] = reader(job)
        else:
            result["environment_snapshot"] = job.get("environment_snapshot", {})
        parsed = [
            item.get("content")
            for item in result["artifacts"]
            if "content" in item
        ]
        if len(parsed) == 1:
            result["parsed_results"] = parsed[0]
        result["captured_logs"] = self.captured_logs(self.require(job_id))
        self._diagnose_result(result)
        payload = json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(payload.encode()).hexdigest()
        saved = self.store.create_snapshot(job_id, payload, digest, len(payload.encode()))
        metadata = {key: saved[key] for key in ("captured_at", "sha256", "size_bytes")}
        return {**saved["result"], "snapshot": metadata}

    @staticmethod
    def _environment_snapshot(env: dict[str, str]) -> dict[str, Any]:
        merged = dict(os.environ)
        merged.update(env)
        path = merged.get("PATH")
        return {
            "shell": merged.get("SHELL"),
            "path": path,
            "python": shutil.which("python3", path=path),
            "bash": shutil.which("bash", path=path),
            "gpu_visible": merged.get("CUDA_VISIBLE_DEVICES"),
            "npu_visible": merged.get("ASCEND_RT_VISIBLE_DEVICES"),
            "non_interactive": True,
        }

    @staticmethod
    def _diagnose_result(result: dict[str, Any]) -> None:
        if result.get("state") == "succeeded":
            result.setdefault("stage", "completed")
            return
        stderr = (result.get("stderr_tail") or "").lower()
        error = (result.get("error") or "").lower()
        text = f"{stderr}\n{error}"
        if result.get("exit_code") == 21 or "npu-smi" in text or "device" in text and ("unavailable" in text or "busy" in text):
            stage, retryable = "device_unavailable", True
            reason = "device_driver_call_failed" if "device_driver_call_failed" in text else ("device_not_visible" if "device_not_visible" in text else ("device_busy" if "busy" in text else "device_unavailable"))
            suggestion = "Wait for the device or submit with another --device; check ASCEND_RT_VISIBLE_DEVICES."
        elif any(marker in text for marker in ("cmake error", "ninja: build stopped", "make: ***")):
            stage, reason, retryable = "build_failed", "build_failed", False
            suggestion = "Inspect the build error above and verify CMake and compiler paths."
        elif "working directory" in text or "no such file" in text:
            stage, reason, retryable = "preflight_failed", "cwd_unavailable", False
            suggestion = "Verify --cwd exists on the remote host."
        elif "no module named" in text or "command not found" in text or "not found" in text:
            stage, reason, retryable = "environment_failed", "dependency_missing", False
            suggestion = "Install the missing executable or package in the non-interactive execution environment."
        elif "failed" in text and ("pytest" in text or "tests/" in text):
            stage, reason, retryable = "test_failed", "test_assertion_failed", False
            suggestion = "Inspect the bounded test failure and fix the failing assertion or implementation."
        elif any(marker in text for marker in ("validation failed", "threshold", "gate failed")):
            stage, reason, retryable = "validation_failed", "quality_gate_failed", False
            suggestion = "Inspect the reported validation gate and its measured value."
        elif any(marker in text for marker in ("connection reset", "temporarily unavailable", "timed out connecting")):
            stage, reason, retryable = "infrastructure_failed", "transient_connection", True
            suggestion = "Retry the same idempotent submission after connectivity recovers."
        elif result.get("exit_code") in {137, 143}:
            stage, reason, retryable = "runtime_failed", "terminated_by_signal", True
            suggestion = "Check memory/resource limits and scheduler cancellation history before retrying."
        elif result.get("state") == "timed_out":
            stage, reason, retryable = "timed_out", "timeout", True
            suggestion = "Increase --timeout or inspect the bounded logs."
        else:
            stage, reason, retryable = "command_failed", "command_failed", False
            suggestion = "Inspect stderr_tail and verify the remote toolchain and environment."
        result.update(stage=stage, reason=reason, retryable=retryable, suggestion=suggestion)

    def cancel(self, job_id: str, grace_seconds: float) -> dict[str, Any]:
        job = self.require(job_id)
        if job["state"] in TERMINAL_STATES:
            return {**self.summary(job), "cancel_applied": False, "cancel_outcome": "already_terminal"}
        job = self.backends[job["backend"]].cancel(job, grace_seconds)  # type: ignore[attr-defined]
        return {**self.summary(job), "cancel_applied": job["state"] == "cancelled", "cancel_outcome": "cancelled" if job["state"] == "cancelled" else "already_terminal"}

    def list(
        self,
        state: str | None = None,
        host: str | None = None,
        queue_name: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        results = []
        for job in self.store.list(
            state=state, host=host, queue_name=queue_name, limit=limit
        ):
            if job["state"] not in TERMINAL_STATES:
                try:
                    job = self.backends[job["backend"]].refresh(job)  # type: ignore[attr-defined]
                except Exception:
                    pass
            results.append(self.summary(job))
        return results

    def create_queue(self, name: str, concurrency: int) -> dict[str, Any]:
        if not QUEUE_NAME.fullmatch(name):
            raise AwaitlessError(
                "queue name must be 1-64 characters using letters, digits, "
                "dot, underscore, or hyphen"
            )
        if (
            isinstance(concurrency, bool)
            or not isinstance(concurrency, int)
            or concurrency <= 0
        ):
            raise AwaitlessError("queue concurrency must be a positive integer")
        try:
            queue, created = self.store.create_queue(name, concurrency)
        except ValueError as exc:
            raise AwaitlessError(str(exc)) from exc
        summary = next(
            item for item in self.store.list_queues() if item["name"] == name
        )
        return {**summary, "created": created}

    def list_queues(self) -> list[dict[str, Any]]:
        # Local runners commit their own terminal state. SSH wrappers persist it
        # remotely, so reconcile those rows before reporting queue utilization.
        for job in self.store.list():
            if not job.get("queue_name") or job["state"] in TERMINAL_STATES:
                continue
            try:
                self.backends[job["backend"]].refresh(job)  # type: ignore[attr-defined]
            except Exception:
                pass
        summaries = self.store.list_queues()
        for summary in summaries:
            jobs = self.store.list(queue_name=summary["name"])
            queued = sorted((job for job in jobs if job["state"] == "queued"), key=lambda item: (item.get("queue_order") or 0, item["created_at"], item["job_id"]))
            durations = [
                (parse_time(job["finished_at"]) - parse_time(job["started_at"])).total_seconds()
                for job in jobs
                if parse_time(job.get("started_at")) and parse_time(job.get("finished_at"))
            ]
            average = sum(durations) / len(durations) if durations else None
            concurrency = int(summary["concurrency"])
            summary["running_jobs"] = summary.get("active_jobs", 0)
            free_slots = max(0, concurrency - int(summary["running_jobs"]))
            summary["average_runtime_seconds"] = round(average, 3) if average is not None else None
            summary["waiting_jobs"] = [
                {
                    "job_id": job["job_id"],
                    "name": job.get("name"),
                    "position": position,
                    "estimated_wait_seconds": round(
                        average * max(0, (position - free_slots + concurrency - 1) // concurrency),
                        3,
                    ) if average is not None else None,
                }
                for position, job in enumerate(queued, start=1)
            ]
            summary["estimated_wait_seconds"] = summary["waiting_jobs"][0]["estimated_wait_seconds"] if summary["waiting_jobs"] else 0.0
        return summaries

    def _reconcile_local_queue(
        self, queue_name: str, *, exclude: str | None = None
    ) -> None:
        """Release capacity held by local processes that disappeared."""
        for state in ("starting", "running", "stalled"):
            for job in self.store.list(state=state, queue_name=queue_name):
                if job["job_id"] == exclude or job["backend"] != "local":
                    continue
                try:
                    self.backends["local"].refresh(job)
                except Exception:
                    pass

    def logs(self, job_id: str, tail: int, max_bytes: int) -> dict[str, Any]:
        job = self.require(job_id)
        if job["backend"] in {"ssh", "slurm"}:
            return self.backends[job["backend"]].read_logs(job, tail, max_bytes)  # type: ignore[attr-defined]
        each = max(1, max_bytes // 2)
        result: dict[str, Any] = {"truncated": False, "stdout_tail": "", "stderr_tail": ""}
        for stream in ("stdout", "stderr"):
            path = Path(job[f"{stream}_path"])
            data, truncated = _tail_file(path, tail, each)
            result[f"{stream}_tail"] = data
            result["truncated"] = result["truncated"] or truncated
        return result

    def captured_logs(self, job: dict[str, Any]) -> list[dict[str, Any]]:
        paths = job.get("capture_logs") or []
        if not paths:
            return []
        if job["backend"] in {"ssh", "slurm"}:
            backend = self.backends[job["backend"]]
            reader = getattr(backend, "read_capture_logs", None)
            return reader(job, paths, self.settings.log_tail_lines, self.settings.max_return_bytes) if reader else []
        cwd = Path(job["cwd"] or ".")
        each = max(1, self.settings.max_return_bytes // max(1, len(paths)))
        items = []
        for declared in paths:
            resolved = Path(declared)
            if not resolved.is_absolute():
                resolved = cwd / resolved
            data, truncated = _tail_file(resolved, self.settings.log_tail_lines, each)
            items.append({"path": declared, "exists": resolved.is_file(), "tail": data, "truncated": truncated})
        return items

    def inspect(self, job_id: str) -> dict[str, Any]:
        job = self.require(job_id)
        return {
            **self.summary(job),
            "command": job["command"],
            "cwd": job["cwd"],
            "env": job["env"],
            "job_dir": job["job_dir"],
            "stdout_path": job["stdout_path"],
            "stderr_path": job["stderr_path"],
            "artifact_paths": job["artifact_paths"],
            "capture_logs": job.get("capture_logs", []),
            "resources": job.get("resources", {}),
            "environment_snapshot": job.get("environment_snapshot", {}),
            "raw_phase": job.get("phase"),
            "queue_order": job.get("queue_order"),
            "events": self.store.events(job_id),
            "error": job["error"],
        }

    def artifacts(self, job: dict[str, Any]) -> list[dict[str, Any]]:
        if job["backend"] in {"ssh", "slurm"}:
            return self.backends[job["backend"]].artifacts(job, self.settings.max_return_bytes)  # type: ignore[attr-defined]
        cwd = Path(job["cwd"] or ".")
        items: list[dict[str, Any]] = []
        declarations: list[tuple[str, Path, str | None]] = []
        for declared in job["artifact_paths"]:
            if any(ch in declared for ch in "*?["):
                matches = glob.glob(str(cwd / declared), recursive=True)
                if matches:
                    declarations.extend((match, Path(match), declared) for match in matches)
                else:
                    declarations.append((declared, cwd / declared, None))
            else:
                path = Path(declared)
                resolved = path if path.is_absolute() else cwd / path
                if resolved.is_dir():
                    declarations.extend(
                        (str(match), match, declared)
                        for match in sorted(resolved.rglob("*"))
                        if match.is_file()
                    )
                else:
                    declarations.append((declared, resolved, None))
        for declared, resolved, pattern in declarations:
            item: dict[str, Any] = {"path": declared, "exists": resolved.is_file()}
            if pattern:
                item["declared_path"] = pattern
            if resolved.is_file():
                stat = resolved.stat()
                item.update(size_bytes=stat.st_size, sha256=_sha256_file(resolved), modified_at=datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat().replace("+00:00", "Z"))
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
        if job["backend"] in {"ssh", "slurm"}:
            candidates.extend(
                parse_time(job.get(key)).timestamp() if parse_time(job.get(key)) else 0
                for key in ("last_output_at", "last_heartbeat_at")
            )
        else:
            candidates.extend(
                Path(job[key]).stat().st_mtime if Path(job[key]).exists() else 0
                for key in ("stdout_path", "stderr_path")
            )
        latest = max(candidates)
        desired = "stalled" if time.time() - latest >= timeout else "running"
        return (
            self.store.update_if_active(job["job_id"], state=desired)
            if desired != job["state"]
            else job
        )

    @staticmethod
    def summary(job: dict[str, Any]) -> dict[str, Any]:
        created = parse_time(job.get("created_at"))
        started = parse_time(job.get("started_at"))
        finished = parse_time(job.get("finished_at"))
        if created and started and started < created:
            created = started
        if started and finished and finished < started:
            finished = started
        now = datetime.now(timezone.utc)
        elapsed_start = started or created
        elapsed_end = finished or now
        elapsed = (
            max(0.0, (elapsed_end - elapsed_start).total_seconds())
            if elapsed_start
            else 0.0
        )
        duration = (
            max(0.0, (finished - started).total_seconds())
            if started and finished
            else None
        )
        queue_wait = None
        if created and (job.get("queue_name") or job.get("backend") == "slurm"):
            queue_wait_end = started or finished or now
            queue_wait = max(0.0, (queue_wait_end - created).total_seconds())
        stdout = Path(job["stdout_path"])
        stderr = Path(job["stderr_path"])
        mtimes = [path.stat().st_mtime for path in (stdout, stderr) if path.exists() and path.stat().st_size]
        local_last_output = datetime.fromtimestamp(max(mtimes), timezone.utc).isoformat().replace("+00:00", "Z") if mtimes else None
        return {
            "job_id": job["job_id"], "client_request_id": job.get("client_request_id"), "client_session": job.get("client_session"),
            "name": job["name"], "backend": job["backend"], "host": job["host"],
            "state": job["state"], "pid": job["pid"], "backend_id": job["backend_id"],
            "created_at": created.isoformat().replace("+00:00", "Z") if created else job["created_at"], "updated_at": job["updated_at"],
            "started_at": started.isoformat().replace("+00:00", "Z") if started else None, "finished_at": finished.isoformat().replace("+00:00", "Z") if finished else None,
            "mcp_task_ttl_ms": job.get("mcp_task_ttl_ms"),
            "queue": job.get("queue_name"),
            "device": job.get("device"),
            "device_mode": job.get("device_mode"),
            "resources": job.get("resources", {}),
            "phase": (
                job.get("phase")
                if job.get("phase") not in {None, "queued", "starting", "running", "completed"}
                else "unknown"
            ),
            "queue_state": "running" if job.get("started_at") else ("queued" if job.get("queue_name") else "running"),
            "last_heartbeat_at": job.get("last_heartbeat_at"),
            # Compatibility alias. Unlike earlier releases this is never inferred
            # from output or generic database updates.
            "heartbeat_at": job.get("last_heartbeat_at"),
            "queue_wait_seconds": round(queue_wait, 3) if queue_wait is not None else None,
            "elapsed_seconds": round(elapsed, 3),
            "duration_seconds": round(duration, 3) if duration is not None else None,
            "exit_code": job["exit_code"],
            "last_output_at": local_last_output or job.get("last_output_at"),
            "stdout_bytes": stdout.stat().st_size if stdout.exists() and job["backend"] == "local" else job.get("stdout_bytes", 0),
            "stderr_bytes": stderr.stat().st_size if stderr.exists() and job["backend"] == "local" else job.get("stderr_bytes", 0),
            "backend_connected": True,
            "error": job["error"],
        }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _submission_fingerprint(**values: Any) -> str:
    try:
        payload = json.dumps(
            values, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise AwaitlessError(f"submission parameters are not JSON serializable: {exc}") from exc
    return hashlib.sha256(payload).hexdigest()


def _format_completion_cursor(event_id: int) -> str:
    if event_id < 0:
        raise ValueError("completion event ID must be non-negative")
    return f"cmp_{event_id:016d}"


def _parse_completion_cursor(value: str | None) -> int:
    if value is None:
        return 0
    match = COMPLETION_CURSOR.fullmatch(value)
    if not match:
        raise AwaitlessError(
            "completion cursor must use the form cmp_<decimal-event-id>"
        )
    event_id = int(match.group(1))
    if event_id > MAX_COMPLETION_EVENT_ID:
        raise AwaitlessError("completion cursor event ID is out of range")
    return event_id


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
