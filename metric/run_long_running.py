#!/usr/bin/env python3
"""Run Blocking versus Awaitless long-running command benchmarks."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from .provenance import git_state
except ImportError:
    from provenance import git_state  # type: ignore[no-redef]

try:
    from . import long_workload, run_local
except ImportError:
    import long_workload  # type: ignore[no-redef]
    import run_local  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parents[1]
METRIC_ROOT = Path(__file__).resolve().parent
WORKLOAD_SCRIPT = METRIC_ROOT / "long_workload.py"
ARMS = {"blocking", "blocking_parallel", "awaitless"}
SCENARIOS = {"single", "batch", "disconnect"}
TERMINAL_STATES = {"succeeded", "failed", "cancelled", "timed_out", "lost"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def execute(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: float = 300.0,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        command,
        cwd=cwd or ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=timeout,
    )


def json_stdout(result: subprocess.CompletedProcess[bytes]) -> dict[str, Any]:
    try:
        value = json.loads(result.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        detail = (result.stderr or result.stdout)[-2000:].decode("utf-8", errors="replace")
        raise RuntimeError(f"expected JSON output (exit {result.returncode}): {detail}") from exc
    if not isinstance(value, dict):
        raise RuntimeError("expected a JSON object")
    return value


def bounded_response(value: bytes, limit: int | None) -> bytes:
    if limit is None or len(value) <= limit:
        return value
    if limit <= 0:
        return b""
    marker = b"[long benchmark: response truncated]\n"
    if limit <= len(marker):
        return marker[:limit]
    remaining = max(0, limit - len(marker))
    return marker + (value[-remaining:] if remaining else b"")


@dataclass
class EventRecorder:
    epoch: float
    events: list[dict[str, Any]] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def _append(
        self,
        *,
        operation: str,
        actor: str,
        agent_call: bool,
        started_text: str,
        started: float,
        ended: float,
        visible: bytes,
        return_code: int | None,
        interrupted: bool,
        error: str | None = None,
    ) -> None:
        event = {
            "operation": operation,
            "actor": actor,
            "started_at": started_text,
            "started_offset_seconds": round(started - self.epoch, 6),
            "ended_offset_seconds": round(ended - self.epoch, 6),
            "duration_seconds": round(ended - started, 6),
            "agent_call": agent_call,
            "response_bytes": len(visible),
            "response_sha256": hashlib.sha256(visible).hexdigest(),
            "return_code": return_code,
            "system_command_invocations": 1,
            "interrupted": interrupted,
        }
        if error:
            event["error"] = error
        with self._lock:
            self.events.append(event)

    def command(
        self,
        operation: str,
        command: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout: float = 300.0,
        actor: str = "agent",
        agent_call: bool = True,
        max_response_bytes: int | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        started_text = utc_now()
        started = time.monotonic()
        try:
            result = execute(command, cwd=cwd, env=env, timeout=timeout)
        except Exception as exc:
            ended = time.monotonic()
            self._append(
                operation=operation,
                actor=actor,
                agent_call=agent_call,
                started_text=started_text,
                started=started,
                ended=ended,
                visible=str(exc).encode("utf-8", errors="replace"),
                return_code=None,
                interrupted=False,
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        ended = time.monotonic()
        self._append(
            operation=operation,
            actor=actor,
            agent_call=agent_call,
            started_text=started_text,
            started=started,
            ended=ended,
            visible=bounded_response(result.stdout + result.stderr, max_response_bytes),
            return_code=result.returncode,
            interrupted=False,
        )
        return result

    def interrupted_command(
        self,
        operation: str,
        command: list[str],
        *,
        after_seconds: float,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        actor: str = "agent",
        agent_call: bool = True,
        max_response_bytes: int | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        started_text = utc_now()
        started = time.monotonic()
        process = subprocess.Popen(
            command,
            cwd=cwd or ROOT,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        time.sleep(after_seconds)
        interrupted = process.poll() is None
        if interrupted:
            os.killpg(process.pid, signal.SIGTERM)
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            stdout, stderr = process.communicate(timeout=5)
        ended = time.monotonic()
        result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
        self._append(
            operation=operation,
            actor=actor,
            agent_call=agent_call,
            started_text=started_text,
            started=started,
            ended=ended,
            visible=bounded_response(stdout + stderr, max_response_bytes),
            return_code=process.returncode,
            interrupted=interrupted,
        )
        return result

    @property
    def agent_calls(self) -> int:
        return sum(bool(event["agent_call"]) for event in self.events)

    @property
    def visible_bytes(self) -> int:
        return sum(int(event["response_bytes"]) for event in self.events if event["agent_call"])

    @property
    def system_commands(self) -> int:
        return sum(int(event["system_command_invocations"]) for event in self.events)

    @property
    def agent_blocked_seconds(self) -> float:
        intervals = sorted(
            (
                float(event["started_offset_seconds"]),
                float(event["ended_offset_seconds"]),
            )
            for event in self.events
            if event["agent_call"]
        )
        if not intervals:
            return 0.0
        total = 0.0
        current_start, current_end = intervals[0]
        for start, end in intervals[1:]:
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                total += current_end - current_start
                current_start, current_end = start, end
        return max(0.0, total + current_end - current_start)


@dataclass(frozen=True)
class TaskSpec:
    task_id: str
    marker: str
    duration_seconds: float
    index: int

    def task_dir(self, work: Path) -> Path:
        return work / f"task-{self.index:03d}"

    def artifact(self, work: Path) -> Path:
        return self.task_dir(work) / "result.json"

    def command(self, work: Path, workload: dict[str, Any], env_file: Path) -> list[str]:
        task_dir = self.task_dir(work)
        return [
            sys.executable,
            str(WORKLOAD_SCRIPT),
            "run",
            "--workload-json",
            json.dumps(workload, ensure_ascii=False, separators=(",", ":")),
            "--task-id",
            self.task_id,
            "--task-dir",
            str(task_dir),
            "--artifact",
            str(self.artifact(work)),
            "--marker",
            self.marker,
            "--duration-seconds",
            f"{self.duration_seconds:g}",
            "--env-file",
            str(env_file.resolve()),
        ]


def valid_artifact(value: Any, task: TaskSpec, workload: dict[str, Any]) -> bool:
    return (
        isinstance(value, dict)
        and value.get("schema_version") == 1
        and value.get("task_id") == task.task_id
        and value.get("workload") == workload["id"]
        and value.get("adapter") == workload["adapter"]
        and value.get("ok") is True
        and value.get("exit_code") == 0
        and value.get("marker") == task.marker
    )


def read_artifact(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


@dataclass
class ArmResult:
    tasks: list[dict[str, Any]]
    time_to_agent_release_seconds: float
    recovery_injected: bool | None
    recovery_success: bool | None


def observed_task(
    *,
    task: TaskSpec,
    workload: dict[str, Any],
    state: str,
    exit_code: int | None,
    artifact: dict[str, Any] | None,
    log_text: str,
) -> dict[str, Any]:
    artifact_correct = valid_artifact(artifact, task, workload)
    marker_seen = task.marker in log_text
    return {
        "task_id": task.task_id,
        "state": state,
        "exit_code": exit_code,
        "artifact": artifact,
        "artifact_correct": artifact_correct,
        "marker_seen": marker_seen,
        "result_correct": state == "succeeded" and exit_code == 0 and artifact_correct and marker_seen,
    }


def run_blocking(
    *,
    arm: str,
    scenario: str,
    tasks: list[TaskSpec],
    workload: dict[str, Any],
    work: Path,
    recorder: EventRecorder,
    env_file: Path,
    config: dict[str, Any],
) -> ArmResult:
    started = recorder.epoch

    def one(task: TaskSpec) -> dict[str, Any]:
        task_dir = task.task_dir(work)
        task_dir.mkdir(parents=True, exist_ok=True)
        result = recorder.command(
            "execute",
            task.command(work, workload, env_file),
            cwd=task_dir,
            timeout=float(workload.get("timeout_seconds", 600.0)) + 30,
            max_response_bytes=int(config["max_return_bytes"]),
        )
        artifact = read_artifact(task.artifact(work))
        return observed_task(
            task=task,
            workload=workload,
            state="succeeded" if result.returncode == 0 else "failed",
            exit_code=result.returncode,
            artifact=artifact,
            log_text=bounded_response(
                result.stdout + result.stderr, int(config["max_return_bytes"])
            ).decode("utf-8", errors="replace"),
        )

    if scenario == "disconnect":
        task = tasks[0]
        task_dir = task.task_dir(work)
        task_dir.mkdir(parents=True, exist_ok=True)
        result = recorder.interrupted_command(
            "execute_interrupted",
            task.command(work, workload, env_file),
            cwd=task_dir,
            after_seconds=float(config["disconnect_after_seconds"]),
            max_response_bytes=int(config["max_return_bytes"]),
        )
        artifact = read_artifact(task.artifact(work))
        item = observed_task(
            task=task,
            workload=workload,
            state="interrupted",
            exit_code=result.returncode,
            artifact=artifact,
            log_text=bounded_response(
                result.stdout + result.stderr, int(config["max_return_bytes"])
            ).decode("utf-8", errors="replace"),
        )
        return ArmResult(
            tasks=[item],
            time_to_agent_release_seconds=round(time.monotonic() - started, 6),
            recovery_injected=True,
            recovery_success=False,
        )

    if arm == "blocking_parallel" and len(tasks) > 1:
        with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
            observed = list(pool.map(one, tasks))
    else:
        observed = [one(task) for task in tasks]
    return ArmResult(
        tasks=observed,
        time_to_agent_release_seconds=round(time.monotonic() - started, 6),
        recovery_injected=None,
        recovery_success=None,
    )


class AwaitlessHarness:
    def __init__(self, work: Path, config: dict[str, Any]):
        self.work = work
        self.config = config
        self.data = work / "awaitless-data"
        self.config_path = work / "awaitless.toml"
        self.config_path.write_text(
            "\n".join(
                [
                    "[defaults]",
                    f"poll_interval = {float(config['poll_interval_seconds']):g}",
                    f"log_tail_lines = {int(config['log_tail_lines'])}",
                    f"max_return_bytes = {int(config['max_return_bytes'])}",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        self.env = {
            **os.environ,
            "AWAITLESS_DATA_DIR": str(self.data),
            "PYTHONPATH": str(ROOT / "src"),
        }
        self.job_ids: list[str] = []

    def base(self) -> list[str]:
        return [sys.executable, "-m", "awaitless", "--config", str(self.config_path)]

    def submit_command(
        self, task: TaskSpec, workload: dict[str, Any], env_file: Path
    ) -> list[str]:
        task_dir = task.task_dir(self.work)
        return self.base() + [
            "submit",
            "--backend",
            "local",
            "--cwd",
            str(task_dir),
            "--artifact",
            "result.json",
            "--json",
            "--",
            *task.command(self.work, workload, env_file),
        ]

    def wait_command(self, job_id: str) -> list[str]:
        return self.base() + ["wait", job_id, "--json"]

    def cleanup(self) -> None:
        for job_id in self.job_ids:
            execute(
                self.base() + ["cancel", job_id, "--grace-period", "0s", "--json"],
                env=self.env,
                timeout=10,
            )


def run_awaitless(
    *,
    harness: AwaitlessHarness,
    scenario: str,
    tasks: list[TaskSpec],
    workload: dict[str, Any],
    work: Path,
    recorder: EventRecorder,
    env_file: Path,
    config: dict[str, Any],
) -> ArmResult:
    submitted: list[tuple[TaskSpec, str]] = []
    for task in tasks:
        task.task_dir(work).mkdir(parents=True, exist_ok=True)
        result = recorder.command(
            "submit",
            harness.submit_command(task, workload, env_file),
            env=harness.env,
            timeout=30,
            max_response_bytes=int(config["max_return_bytes"]),
        )
        value = json_stdout(result)
        if result.returncode or not isinstance(value.get("job_id"), str):
            raise RuntimeError(value.get("error", "Awaitless submit failed"))
        job_id = value["job_id"]
        harness.job_ids.append(job_id)
        submitted.append((task, job_id))
    release = round(time.monotonic() - recorder.epoch, 6)
    recovery_injected: bool | None = None
    if scenario == "disconnect":
        interrupted = recorder.interrupted_command(
            "wait_interrupted",
            harness.wait_command(submitted[0][1]),
            env=harness.env,
            after_seconds=float(config["disconnect_after_seconds"]),
            max_response_bytes=int(config["max_return_bytes"]),
        )
        recovery_injected = interrupted.returncode is not None and interrupted.returncode < 0
    else:
        maximum_duration = max(task.duration_seconds for task in tasks)
        defer = float(config.get("defer_before_collect_seconds", 0.0)) + (
            float(config.get("defer_before_collect_ratio", 0.0)) * maximum_duration
        )
        if workload["adapter"] == "model_inference":
            defer = float(config.get("model_inference_defer_seconds", 0.0))
        if defer > 0:
            time.sleep(defer)
    observed: list[dict[str, Any]] = []
    for task, job_id in submitted:
        result = recorder.command(
            "wait",
            harness.wait_command(job_id),
            env=harness.env,
            timeout=float(workload.get("timeout_seconds", 600.0)) + 30,
            max_response_bytes=int(config["max_return_bytes"]),
        )
        value = json_stdout(result)
        state = str(value.get("state", "unknown"))
        exit_code = value.get("exit_code")
        artifact = value.get("parsed_results")
        log_text = str(value.get("stdout_tail", "")) + str(value.get("stderr_tail", ""))
        observed.append(
            observed_task(
                task=task,
                workload=workload,
                state=state,
                exit_code=exit_code if isinstance(exit_code, int) else None,
                artifact=artifact if isinstance(artifact, dict) else None,
                log_text=log_text,
            )
        )
    all_correct = all(item["result_correct"] for item in observed)
    return ArmResult(
        tasks=observed,
        time_to_agent_release_seconds=release,
        recovery_injected=recovery_injected,
        recovery_success=(recovery_injected is True and all_correct)
        if scenario == "disconnect"
        else None,
    )


def sample_value(value: Any, rng: random.Random) -> float:
    if isinstance(value, list):
        if len(value) != 2:
            raise ValueError(f"duration range must contain two values: {value!r}")
        return rng.uniform(float(value[0]), float(value[1]))
    return float(value)


def tasks_for_case(
    *,
    case_id: str,
    scenario: str,
    workload: dict[str, Any],
    config: dict[str, Any],
    rng: random.Random,
) -> list[TaskSpec]:
    count = int(config["batch_size"]) if scenario == "batch" else 1
    tasks: list[TaskSpec] = []
    for index in range(count):
        duration = sample_value(workload["duration_seconds"], rng)
        marker = hashlib.sha256(f"{case_id}:{index}:{rng.random()}".encode()).hexdigest()[:20]
        tasks.append(
            TaskSpec(
                task_id=f"{case_id}:task:{index:03d}",
                marker=marker,
                duration_seconds=duration,
                index=index,
            )
        )
    return tasks


def task_duration_sum(observed: list[dict[str, Any]]) -> float:
    result = 0.0
    for item in observed:
        artifact = item.get("artifact")
        if isinstance(artifact, dict) and isinstance(artifact.get("duration_seconds"), (int, float)):
            result += float(artifact["duration_seconds"])
    return result


def build_record(
    *,
    experiment_id: str,
    case_id: str,
    arm: str,
    scenario: str,
    workload: dict[str, Any],
    tasks: list[TaskSpec],
    recorder: EventRecorder,
    result: ArmResult,
    elapsed: float,
    seed: int,
    environment: dict[str, Any],
    trial_root: Path,
    error: str | None,
) -> dict[str, Any]:
    all_correct = bool(result.tasks) and all(item["result_correct"] for item in result.tasks)
    if error:
        all_correct = False
    blocked = recorder.agent_blocked_seconds
    available = max(0.0, elapsed - blocked)
    duration_sum = task_duration_sum(result.tasks)
    return {
        "schema_version": 1,
        "record_type": "trial",
        "experiment_id": experiment_id,
        "case_id": case_id,
        "trial_id": f"{case_id}:{arm}",
        "recorded_at": utc_now(),
        "arm": arm,
        "scenario": scenario,
        "workload": str(workload["id"]),
        "adapter": str(workload["adapter"]),
        "seed": seed,
        "environment": environment,
        "expected": {
            "task_count": len(tasks),
            "exit_code": 0,
            "recovery_required": scenario == "disconnect",
            "tasks": [
                {
                    "task_id": task.task_id,
                    "marker": task.marker,
                    "duration_seconds": task.duration_seconds,
                }
                for task in tasks
            ],
        },
        "observed": {
            "tasks": result.tasks,
            "tasks_completed": sum(item["result_correct"] for item in result.tasks),
            "recovery_injected": result.recovery_injected,
        },
        "metrics": {
            "result_correct": all_correct,
            "recovery_success": result.recovery_success,
            "agent_tool_calls": recorder.agent_calls,
            "agent_visible_bytes": recorder.visible_bytes,
            "agent_blocked_seconds": round(blocked, 6),
            "agent_available_seconds": round(available, 6),
            "agent_occupancy_ratio": round(blocked / elapsed, 6) if elapsed else None,
            "time_to_agent_release_seconds": result.time_to_agent_release_seconds,
            "wall_time_seconds": round(elapsed, 6),
            "task_duration_sum_seconds": round(duration_sum, 6),
            "parallelism_factor": round(duration_sum / elapsed, 6) if elapsed else None,
            "system_command_invocations": recorder.system_commands,
            "reasoning_idle_seconds": None,
            "input_tokens": None,
            "output_tokens": None,
            "disk_bytes": run_local.disk_bytes(trial_root),
            "manual_interventions": 0,
        },
        "events": sorted(recorder.events, key=lambda event: event["started_offset_seconds"]),
        "error": error,
    }


def validate_config(config: dict[str, Any]) -> None:
    if config.get("schema_version") != 1:
        raise ValueError("config schema_version must be 1")
    if not isinstance(config.get("trials"), int) or config["trials"] <= 0:
        raise ValueError("trials must be a positive integer")
    if not config.get("arms") or not set(config["arms"]).issubset(ARMS):
        raise ValueError(f"arms must be selected from {sorted(ARMS)}")
    if not config.get("scenarios") or not set(config["scenarios"]).issubset(SCENARIOS):
        raise ValueError(f"scenarios must be selected from {sorted(SCENARIOS)}")
    if int(config.get("batch_size", 0)) < 2:
        raise ValueError("batch_size must be at least 2")
    for key in (
        "disconnect_after_seconds",
        "poll_interval_seconds",
        "log_tail_lines",
        "max_return_bytes",
    ):
        if float(config.get(key, 0)) <= 0:
            raise ValueError(f"{key} must be positive")
    for key in ("defer_before_collect_seconds", "defer_before_collect_ratio"):
        if float(config.get(key, 0)) < 0:
            raise ValueError(f"{key} cannot be negative")
    workloads = config.get("workloads")
    if not isinstance(workloads, list) or not workloads:
        raise ValueError("workloads must be a non-empty list")
    identifiers: set[str] = set()
    for workload in workloads:
        if not isinstance(workload, dict) or not isinstance(workload.get("id"), str):
            raise ValueError("each workload requires a string id")
        if workload["id"] in identifiers:
            raise ValueError(f"duplicate workload id {workload['id']!r}")
        identifiers.add(workload["id"])
        if workload.get("adapter") not in long_workload.ADAPTERS:
            raise ValueError(f"invalid adapter for workload {workload['id']!r}")
        duration = workload.get("duration_seconds")
        values = duration if isinstance(duration, list) else [duration]
        if not values or any(not isinstance(item, (int, float)) or item < 0 for item in values):
            raise ValueError(f"invalid duration_seconds for workload {workload['id']!r}")
        if isinstance(duration, list) and len(duration) != 2:
            raise ValueError(f"duration range for workload {workload['id']!r} needs two values")


def base_environment(config_path: Path, config: dict[str, Any]) -> dict[str, Any]:
    commit, dirty, untracked = git_state(ROOT)
    effective = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
    version_text = (ROOT / "src" / "awaitless" / "__init__.py").read_text(encoding="utf-8")
    version_match = re.search(r'__version__\s*=\s*"([^"]+)"', version_text)
    return {
        "profile": config["name"],
        "config_path": str(config_path.resolve()),
        "config_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
        "effective_config_sha256": hashlib.sha256(effective).hexdigest(),
        "git_commit": commit,
        "git_dirty": dirty,
        "git_untracked_files": untracked,
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "awaitless_version": version_match.group(1) if version_match else "unknown",
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "workload_sha256": hashlib.sha256(WORKLOAD_SCRIPT.read_bytes()).hexdigest(),
        "agent_blocked_definition": "union of wall-clock intervals occupied by agent-visible synchronous calls",
        "reasoning_idle_definition": "not measured; blocked wall time is not model reasoning time",
    }


def selected_workloads(config: dict[str, Any], filters: list[str]) -> list[dict[str, Any]]:
    workloads = [item for item in config["workloads"] if item.get("enabled", True)]
    if filters:
        requested = set(filters)
        known = {item["id"] for item in workloads}
        missing = requested - known
        if missing:
            raise ValueError("unknown or disabled workload(s): " + ", ".join(sorted(missing)))
        workloads = [item for item in workloads if item["id"] in requested]
    return workloads


def append_record(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def skip_record(
    *,
    experiment_id: str,
    workload: dict[str, Any],
    probe: dict[str, Any],
    environment: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "record_type": "skip",
        "experiment_id": experiment_id,
        "recorded_at": utc_now(),
        "workload": workload["id"],
        "adapter": workload["adapter"],
        "reason": probe["reason"],
        "probe": probe,
        "environment": environment,
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--config", type=Path, default=METRIC_ROOT / "configs" / "long-running-smoke.json"
    )
    result.add_argument("--output", type=Path)
    result.add_argument("--append", action="store_true")
    result.add_argument("--trials", type=int)
    result.add_argument("--workload", action="append", default=[])
    result.add_argument("--env-file", type=Path, default=ROOT / ".env")
    result.add_argument("--probe-only", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
        if args.trials is not None:
            config["trials"] = args.trials
        validate_config(config)
        workloads = selected_workloads(config, args.workload)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"long benchmark: {exc}", file=sys.stderr)
        return 2
    probes = {
        workload["id"]: long_workload.probe_workload(workload, env_file=args.env_file)
        for workload in workloads
    }
    if args.probe_only:
        print(json.dumps(probes, ensure_ascii=False, indent=2))
        return 0
    if args.output is None:
        print("long benchmark: --output is required unless --probe-only is used", file=sys.stderr)
        return 2
    if args.output.exists() and not args.append:
        print(f"long benchmark: refusing to overwrite {args.output}", file=sys.stderr)
        return 2

    environment = base_environment(args.config, config)
    config_hash = hashlib.sha256(
        json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:10]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    experiment_id = f"{config['name']}-{timestamp}-{config_hash}"
    for workload in workloads:
        probe = probes[workload["id"]]
        if not probe["available"]:
            append_record(
                args.output,
                skip_record(
                    experiment_id=experiment_id,
                    workload=workload,
                    probe=probe,
                    environment=environment,
                ),
            )

    records = 0
    incorrect = 0
    errors = 0
    order_rng = random.Random(int(config["seed"]))
    for workload in workloads:
        if not probes[workload["id"]]["available"]:
            continue
        for scenario in config["scenarios"]:
            for trial_index in range(int(config["trials"])):
                case_id = (
                    f"{experiment_id}:{workload['id']}:{scenario}:{trial_index:03d}"
                )
                seed_bytes = hashlib.sha256(
                    f"{config['seed']}:{workload['id']}:{scenario}:{trial_index}".encode()
                ).digest()[:8]
                case_seed = int.from_bytes(seed_bytes, "big")
                tasks = tasks_for_case(
                    case_id=case_id,
                    scenario=scenario,
                    workload=workload,
                    config=config,
                    rng=random.Random(case_seed),
                )
                arms = list(config["arms"])
                order_rng.shuffle(arms)
                for arm in arms:
                    print(
                        f"[long-benchmark] workload={workload['id']} scenario={scenario} "
                        f"trial={trial_index + 1}/{config['trials']} arm={arm}",
                        file=sys.stderr,
                        flush=True,
                    )
                    with tempfile.TemporaryDirectory(
                        prefix=f"awaitless-long-{workload['id']}-{arm}-"
                    ) as temporary:
                        trial_root = Path(temporary)
                        work = trial_root / "work"
                        work.mkdir()
                        epoch = time.monotonic()
                        recorder = EventRecorder(epoch=epoch)
                        arm_result = ArmResult([], 0.0, None, None)
                        awaitless_harness: AwaitlessHarness | None = None
                        trial_error: str | None = None
                        try:
                            if arm == "awaitless":
                                awaitless_harness = AwaitlessHarness(work, config)
                                arm_result = run_awaitless(
                                    harness=awaitless_harness,
                                    scenario=scenario,
                                    tasks=tasks,
                                    workload=workload,
                                    work=work,
                                    recorder=recorder,
                                    env_file=args.env_file,
                                    config=config,
                                )
                            else:
                                arm_result = run_blocking(
                                    arm=arm,
                                    scenario=scenario,
                                    tasks=tasks,
                                    workload=workload,
                                    work=work,
                                    recorder=recorder,
                                    env_file=args.env_file,
                                    config=config,
                                )
                        except Exception as exc:
                            trial_error = f"{type(exc).__name__}: {exc}"
                        try:
                            elapsed = time.monotonic() - epoch
                            record = build_record(
                                experiment_id=experiment_id,
                                case_id=case_id,
                                arm=arm,
                                scenario=scenario,
                                workload=workload,
                                tasks=tasks,
                                recorder=recorder,
                                result=arm_result,
                                elapsed=elapsed,
                                seed=case_seed,
                                environment=environment,
                                trial_root=trial_root,
                                error=trial_error,
                            )
                            append_record(args.output, record)
                        finally:
                            if awaitless_harness is not None:
                                awaitless_harness.cleanup()
                    records += 1
                    incorrect += int(not record["metrics"]["result_correct"])
                    errors += int(record["error"] is not None)
    print(
        json.dumps(
            {
                "experiment_id": experiment_id,
                "records": records,
                "incorrect_records": incorrect,
                "error_records": errors,
                "skipped_workloads": [
                    identifier for identifier, probe in probes.items() if not probe["available"]
                ],
                "output": str(args.output),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
