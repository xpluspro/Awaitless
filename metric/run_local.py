#!/usr/bin/env python3
"""Run a randomized shell, tmux, wrapped tmux, and Awaitless comparison."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
METRIC_ROOT = Path(__file__).resolve().parent
WORKLOAD = METRIC_ROOT / "workload.py"
TMUX_WRAPPER = METRIC_ROOT / "baselines" / "tmux_job.py"
ARMS = {"shell", "tmux_plain", "tmux_wrapped", "awaitless"}
SCENARIOS = {"normal", "failure", "large_log", "recovery", "cancel_tree"}


def source_version() -> str:
    version_text = (ROOT / "src" / "awaitless" / "__init__.py").read_text(encoding="utf-8")
    match = re.search(r'__version__\s*=\s*"([^"]+)"', version_text)
    return match.group(1) if match else "unknown"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def execute(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: float = 180.0,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        command,
        cwd=cwd or ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )


def json_stdout(result: subprocess.CompletedProcess[bytes]) -> dict[str, Any]:
    try:
        return json.loads(result.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        detail = (result.stderr or result.stdout)[-2000:].decode("utf-8", errors="replace")
        raise RuntimeError(f"expected JSON output (exit {result.returncode}): {detail}") from exc


@dataclass
class CallResult:
    visible: bytes
    return_code: int | None
    system_commands: int
    value: Any = None
    interrupted: bool = False


@dataclass
class Recorder:
    events: list[dict[str, Any]] = field(default_factory=list)

    def call(self, operation: str, action: Callable[[], CallResult]) -> CallResult:
        started_text = utc_now()
        started = time.monotonic()
        try:
            result = action()
        except Exception as exc:
            visible = str(exc).encode("utf-8", errors="replace")
            self.events.append(
                {
                    "operation": operation,
                    "started_at": started_text,
                    "duration_seconds": round(time.monotonic() - started, 6),
                    "agent_call": True,
                    "response_bytes": len(visible),
                    "response_sha256": hashlib.sha256(visible).hexdigest(),
                    "return_code": None,
                    "system_command_invocations": 0,
                    "interrupted": False,
                    "error": str(exc),
                }
            )
            raise
        self.events.append(
            {
                "operation": operation,
                "started_at": started_text,
                "duration_seconds": round(time.monotonic() - started, 6),
                "agent_call": True,
                "response_bytes": len(result.visible),
                "response_sha256": hashlib.sha256(result.visible).hexdigest(),
                "return_code": result.return_code,
                "system_command_invocations": result.system_commands,
                "interrupted": result.interrupted,
            }
        )
        return result

    def command(
        self,
        operation: str,
        command: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout: float = 180.0,
    ) -> subprocess.CompletedProcess[bytes]:
        def action() -> CallResult:
            result = execute(command, cwd=cwd, env=env, timeout=timeout)
            return CallResult(
                visible=result.stdout + result.stderr,
                return_code=result.returncode,
                system_commands=1,
                value=result,
            )

        return self.call(operation, action).value

    def interrupted_command(
        self,
        operation: str,
        command: list[str],
        *,
        after_seconds: float,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
    ) -> bool:
        def action() -> CallResult:
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
                stdout, stderr = process.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate(timeout=3)
            return CallResult(
                visible=stdout + stderr,
                return_code=process.returncode,
                system_commands=1,
                interrupted=interrupted,
            )

        return self.call(operation, action).interrupted

    @property
    def agent_calls(self) -> int:
        return sum(1 for event in self.events if event["agent_call"])

    @property
    def visible_bytes(self) -> int:
        return sum(int(event["response_bytes"]) for event in self.events)

    @property
    def system_commands(self) -> int:
        return sum(int(event["system_command_invocations"]) for event in self.events)


@dataclass(frozen=True)
class ScenarioSpec:
    name: str
    duration_seconds: float
    line_count: int
    line_bytes: int
    exit_code: int
    cancel_after_seconds: float
    marker: str
    score: float
    case_id: str
    pre_command: tuple[str, ...] = ()
    pre_cwd: str | None = None

    @property
    def expected_state(self) -> str:
        if self.name == "cancel_tree":
            return "cancelled"
        return "succeeded" if self.exit_code == 0 else "failed"

    @property
    def expected_artifact(self) -> dict[str, Any] | None:
        if self.name == "cancel_tree":
            return None
        return {
            "ok": self.exit_code == 0,
            "scenario": self.name,
            "trial_id": self.case_id,
            "score": self.score,
        }

    @property
    def full_log_bytes(self) -> int:
        if self.name == "cancel_tree":
            return 0
        return self.stdout_log_bytes + self.stderr_log_bytes

    @property
    def stdout_log_bytes(self) -> int:
        if self.name == "cancel_tree":
            return 0
        return self.line_count * self.line_bytes + len(f"FINAL_MARKER={self.marker}\n".encode())

    @property
    def stderr_log_bytes(self) -> int:
        if self.name == "cancel_tree":
            return 0
        return len(f"STDERR_MARKER={self.marker}\n".encode())

    def command(self, work: Path) -> list[str]:
        command = [
            sys.executable,
            str(WORKLOAD),
            "--scenario",
            self.name,
            "--trial-id",
            self.case_id,
            "--duration-seconds",
            f"{self.duration_seconds:g}",
            "--line-count",
            str(self.line_count),
            "--line-bytes",
            str(self.line_bytes),
            "--exit-code",
            str(self.exit_code),
            "--marker",
            self.marker,
            "--score",
            str(self.score),
        ]
        if self.name == "cancel_tree":
            command += ["--pid-file", str(work / "tree-pids.json")]
        else:
            command += ["--artifact", str(work / "result.json")]
        if self.pre_command:
            command += ["--pre-command-json", json.dumps(list(self.pre_command))]
        if self.pre_cwd:
            command += ["--pre-cwd", self.pre_cwd]
        return command


@dataclass
class ArmObservation:
    state: str | None = None
    exit_code: int | None = None
    artifact: dict[str, Any] | None = None
    log_text: str = ""
    truncated: bool | None = None
    orphan_processes: int | None = None
    recovery_injected: bool | None = None
    duplicated_log_bytes: int = 0
    system_command_adjustment: int = 0


def tmux_command(socket: str, *arguments: str) -> list[str]:
    return ["tmux", "-L", socket, *arguments]


def process_snapshot(pid_file: Path, timeout: float = 5.0) -> dict[int, int | None]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pid_file.is_file():
            value = json.loads(pid_file.read_text(encoding="utf-8"))
            return {int(pid): process_start_ticks(int(pid)) for pid in value.values()}
        time.sleep(0.02)
    raise RuntimeError(f"process tree did not become ready: {pid_file}")


def process_start_ticks(pid: int) -> int | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        return int(fields[19])
    except (OSError, ValueError, IndexError):
        return None


def process_running(pid: int, ticks: int | None) -> bool:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        if ticks is not None and int(fields[19]) != ticks:
            return False
        return fields[0] != "Z"
    except (OSError, ValueError, IndexError):
        return False


def remaining_processes(snapshot: dict[int, int | None], timeout: float = 3.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = sum(process_running(pid, ticks) for pid, ticks in snapshot.items())
        if remaining == 0:
            return 0
        time.sleep(0.05)
    return sum(process_running(pid, ticks) for pid, ticks in snapshot.items())


def source_sloc(path: Path) -> int:
    return sum(
        1
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


class ShellArm:
    metadata = {
        "custom_glue_sloc": 0,
        "custom_glue_files": 0,
        "supported_backends": ["local"],
    }

    def __init__(
        self,
        recorder: Recorder,
        spec: ScenarioSpec,
        work: Path,
        config: dict[str, Any],
    ):
        self.recorder = recorder
        self.spec = spec
        self.work = work
        self.config = config

    def run(self) -> ArmObservation:
        command = self.spec.command(self.work)
        if self.spec.name == "recovery":
            interrupted = self.recorder.interrupted_command(
                "run_interrupted",
                command,
                after_seconds=float(self.config["wait_interrupt_after_seconds"]),
                cwd=self.work,
            )
            return ArmObservation(
                state="lost" if interrupted else None,
                recovery_injected=interrupted,
            )
        if self.spec.name == "cancel_tree":
            interrupted = self.recorder.interrupted_command(
                "cancel",
                command,
                after_seconds=self.spec.cancel_after_seconds,
                cwd=self.work,
            )
            snapshot = process_snapshot(self.work / "tree-pids.json")
            return ArmObservation(
                state="cancelled" if interrupted else None,
                orphan_processes=remaining_processes(snapshot),
            )
        result = self.recorder.command(
            "run",
            command,
            cwd=self.work,
            timeout=max(180.0, self.spec.duration_seconds + 30),
        )
        artifact_result = self.recorder.command(
            "read_artifact", ["cat", "--", str(self.work / "result.json")]
        )
        return ArmObservation(
            state="succeeded" if result.returncode == 0 else "failed",
            exit_code=result.returncode,
            artifact=json_stdout(artifact_result),
            log_text=(result.stdout + result.stderr).decode("utf-8", errors="replace"),
            truncated=False,
        )

    def cleanup(self) -> None:
        return None


class PlainTmuxArm:
    metadata = {"custom_glue_sloc": 0, "custom_glue_files": 0, "supported_backends": ["local"]}

    def __init__(self, recorder: Recorder, spec: ScenarioSpec, work: Path, config: dict[str, Any]):
        self.recorder = recorder
        self.spec = spec
        self.work = work
        self.config = config
        self.socket = f"awmp_{uuid.uuid4().hex[:12]}"
        self.session = f"job_{uuid.uuid4().hex[:12]}"

    def submit(self) -> None:
        command_text = shlex.join(self.spec.command(self.work))

        def action() -> CallResult:
            launched = execute(
                tmux_command(
                    self.socket,
                    "-f",
                    "/dev/null",
                    "new-session",
                    "-d",
                    "-P",
                    "-F",
                    "#{session_name}",
                    "-s",
                    self.session,
                    "-c",
                    str(self.work),
                    command_text,
                )
            )
            retained = execute(
                tmux_command(self.socket, "set-option", "-t", self.session, "remain-on-exit", "on")
            )
            return CallResult(
                visible=launched.stdout + launched.stderr + retained.stdout + retained.stderr,
                return_code=launched.returncode or retained.returncode,
                system_commands=2,
            )

        result = self.recorder.call("submit", action)
        if result.return_code:
            raise RuntimeError("plain tmux submit failed")

    def poll(self, *, interrupted: bool = False) -> tuple[bool, int | None, str, int]:
        def action() -> CallResult:
            status = execute(
                tmux_command(
                    self.socket,
                    "display-message",
                    "-p",
                    "-t",
                    self.session,
                    "#{pane_dead}\t#{pane_dead_status}",
                )
            )
            captured = execute(
                tmux_command(self.socket, "capture-pane", "-p", "-S", "-", "-t", self.session)
            )
            value = (status, captured)
            return CallResult(
                visible=status.stdout + status.stderr + captured.stdout + captured.stderr,
                return_code=status.returncode or captured.returncode,
                system_commands=2,
                value=value,
                interrupted=interrupted,
            )

        status, captured = self.recorder.call("poll", action).value
        status_text = status.stdout.decode("utf-8", errors="replace").strip()
        fields = status_text.split("\t", 1)
        dead = bool(fields and fields[0] == "1")
        exit_code = None
        if dead and len(fields) == 2 and fields[1].lstrip("-").isdigit():
            exit_code = int(fields[1])
        log = captured.stdout.decode("utf-8", errors="replace")
        return dead, exit_code, log, len(captured.stdout)

    def run(self) -> ArmObservation:
        self.submit()
        if self.spec.name == "cancel_tree":
            snapshot = process_snapshot(self.work / "tree-pids.json")
            time.sleep(self.spec.cancel_after_seconds)
            result = self.recorder.command(
                "cancel", tmux_command(self.socket, "kill-session", "-t", self.session)
            )
            return ArmObservation(
                state="cancelled" if result.returncode == 0 else None,
                orphan_processes=remaining_processes(snapshot),
            )

        snapshots: list[int] = []
        recovery_injected: bool | None = None
        if self.spec.name == "recovery":
            time.sleep(float(self.config["wait_interrupt_after_seconds"]))
            dead, exit_code, log, size = self.poll(interrupted=True)
            snapshots.append(size)
            recovery_injected = True
            if dead:
                return self._finish(exit_code, log, snapshots, recovery_injected)

        deadline = time.monotonic() + self.spec.duration_seconds + 30
        while time.monotonic() < deadline:
            dead, exit_code, log, size = self.poll()
            snapshots.append(size)
            if dead:
                return self._finish(exit_code, log, snapshots, recovery_injected)
            time.sleep(float(self.config["poll_interval_seconds"]))
        raise RuntimeError("plain tmux polling deadline exceeded")

    def _finish(
        self,
        exit_code: int | None,
        log: str,
        snapshots: list[int],
        recovery_injected: bool | None,
    ) -> ArmObservation:
        artifact_result = self.recorder.command("read_artifact", ["cat", "--", str(self.work / "result.json")])
        artifact = json_stdout(artifact_result)
        return ArmObservation(
            state="succeeded" if exit_code == 0 else "failed",
            exit_code=exit_code,
            artifact=artifact,
            log_text=log,
            truncated=None if expected_truncation(self.spec, self.config) else False,
            recovery_injected=recovery_injected,
            duplicated_log_bytes=max(0, sum(snapshots) - (snapshots[-1] if snapshots else 0)),
        )

    def cleanup(self) -> None:
        execute(tmux_command(self.socket, "kill-server"), timeout=5)


class WrappedTmuxArm:
    metadata = {
        "custom_glue_sloc": source_sloc(TMUX_WRAPPER),
        "custom_glue_files": 1,
        "supported_backends": ["local"],
    }

    def __init__(self, recorder: Recorder, spec: ScenarioSpec, work: Path, config: dict[str, Any]):
        self.recorder = recorder
        self.spec = spec
        self.work = work
        self.config = config
        self.root = work / "tmux-wrapper-state"
        self.socket = f"awmw_{uuid.uuid4().hex[:12]}"
        self.trace = work / "tmux-control-trace.jsonl"
        self.env = {**os.environ, "TMUX_METRIC_TRACE": str(self.trace)}
        self.job_id: str | None = None

    def base(self) -> list[str]:
        return [
            sys.executable,
            str(TMUX_WRAPPER),
            "--root",
            str(self.root),
            "--socket",
            self.socket,
        ]

    def submit(self) -> None:
        command = self.base() + ["submit", "--cwd", str(self.work)]
        if self.spec.name != "cancel_tree":
            command += ["--artifact", "result.json"]
        command += ["--", *self.spec.command(self.work)]
        result = self.recorder.command("submit", command, env=self.env)
        value = json_stdout(result)
        if result.returncode:
            raise RuntimeError(value.get("error", "wrapped tmux submit failed"))
        self.job_id = value["job_id"]

    def wait_command(self) -> list[str]:
        assert self.job_id
        return self.base() + [
            "wait",
            self.job_id,
            "--tail",
            str(self.config["log_tail_lines"]),
            "--max-bytes",
            str(self.config["max_return_bytes"]),
        ]

    def run(self) -> ArmObservation:
        self.submit()
        assert self.job_id
        if self.spec.name == "cancel_tree":
            snapshot = process_snapshot(self.work / "tree-pids.json")
            time.sleep(self.spec.cancel_after_seconds)
            result = self.recorder.command(
                "cancel",
                self.base()
                + [
                    "cancel",
                    self.job_id,
                    "--grace-seconds",
                    "0.5",
                    "--max-bytes",
                    str(self.config["max_return_bytes"]),
                ],
                env=self.env,
            )
            value = json_stdout(result)
            observation = ArmObservation(
                state=value.get("state"),
                exit_code=value.get("exit_code"),
                orphan_processes=remaining_processes(snapshot),
            )
        else:
            recovery_injected = None
            if self.spec.name == "recovery":
                recovery_injected = self.recorder.interrupted_command(
                    "wait_interrupted",
                    self.wait_command(),
                    after_seconds=float(self.config["wait_interrupt_after_seconds"]),
                    env=self.env,
                )
            result = self.recorder.command("wait", self.wait_command(), env=self.env)
            value = json_stdout(result)
            observation = ArmObservation(
                state=value.get("state"),
                exit_code=value.get("exit_code"),
                artifact=value.get("parsed_results"),
                log_text=str(value.get("stdout_tail", "")) + str(value.get("stderr_tail", "")),
                truncated=value.get("truncated"),
                recovery_injected=recovery_injected,
            )
        observation.system_command_adjustment = trace_lines(self.trace)
        return observation

    def cleanup(self) -> None:
        if self.job_id and not (self.root / self.job_id / "result.json").exists():
            execute(
                self.base() + ["cancel", self.job_id, "--grace-seconds", "0.1"],
                env=self.env,
                timeout=5,
            )
        execute(tmux_command(self.socket, "kill-server"), timeout=5)


class AwaitlessArm:
    metadata = {
        "custom_glue_sloc": 0,
        "custom_glue_files": 0,
        "supported_backends": ["local", "ssh", "slurm"],
    }

    def __init__(self, recorder: Recorder, spec: ScenarioSpec, work: Path, config: dict[str, Any]):
        self.recorder = recorder
        self.spec = spec
        self.work = work
        self.config = config
        self.data = work / "awaitless-data"
        self.config_path = work / "awaitless.toml"
        self.config_path.write_text(
            "\n".join(
                [
                    "[defaults]",
                    f"poll_interval = {min(0.05, float(config['poll_interval_seconds'])):g}",
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
        self.job_id: str | None = None

    def base(self) -> list[str]:
        return [sys.executable, "-m", "awaitless", "--config", str(self.config_path)]

    def submit(self) -> None:
        command = self.base() + [
            "submit",
            "--backend",
            "local",
            "--cwd",
            str(self.work),
        ]
        if self.spec.name != "cancel_tree":
            command += ["--artifact", "result.json"]
        command += ["--json", "--", *self.spec.command(self.work)]
        result = self.recorder.command("submit", command, env=self.env)
        value = json_stdout(result)
        if result.returncode:
            raise RuntimeError(value.get("error", "Awaitless submit failed"))
        self.job_id = value["job_id"]

    def wait_command(self) -> list[str]:
        assert self.job_id
        return self.base() + ["wait", self.job_id, "--json"]

    def run(self) -> ArmObservation:
        self.submit()
        assert self.job_id
        if self.spec.name == "cancel_tree":
            snapshot = process_snapshot(self.work / "tree-pids.json")
            time.sleep(self.spec.cancel_after_seconds)
            result = self.recorder.command(
                "cancel",
                self.base() + ["cancel", self.job_id, "--grace-period", "0.5s", "--json"],
                env=self.env,
            )
            value = json_stdout(result)
            return ArmObservation(
                state=value.get("state"),
                exit_code=value.get("exit_code"),
                orphan_processes=remaining_processes(snapshot),
            )

        recovery_injected = None
        if self.spec.name == "recovery":
            recovery_injected = self.recorder.interrupted_command(
                "wait_interrupted",
                self.wait_command(),
                after_seconds=float(self.config["wait_interrupt_after_seconds"]),
                env=self.env,
            )
        result = self.recorder.command("wait", self.wait_command(), env=self.env)
        value = json_stdout(result)
        return ArmObservation(
            state=value.get("state"),
            exit_code=value.get("exit_code"),
            artifact=value.get("parsed_results"),
            log_text=str(value.get("stdout_tail", "")) + str(value.get("stderr_tail", "")),
            truncated=value.get("truncated"),
            recovery_injected=recovery_injected,
        )

    def cleanup(self) -> None:
        if self.job_id:
            execute(
                self.base() + ["cancel", self.job_id, "--grace-period", "0s", "--json"],
                env=self.env,
                timeout=5,
            )


def trace_lines(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def expected_truncation(spec: ScenarioSpec, config: dict[str, Any]) -> bool:
    each_stream_budget = max(1, int(config["max_return_bytes"]) // 2)
    return (
        spec.stdout_log_bytes > each_stream_budget
        or spec.stderr_log_bytes > each_stream_budget
        or spec.line_count + 1 > int(config["log_tail_lines"])
        or 1 > int(config["log_tail_lines"])
    )


def disk_bytes(path: Path) -> int:
    total = 0
    for item in path.rglob("*"):
        try:
            if item.is_file():
                total += item.stat().st_size
        except OSError:
            continue
    return total


def sample_value(value: Any, rng: random.Random, *, integer: bool = False) -> float | int:
    if isinstance(value, list):
        if len(value) != 2:
            raise ValueError(f"range must contain two values: {value!r}")
        if integer:
            return rng.randint(int(value[0]), int(value[1]))
        return rng.uniform(float(value[0]), float(value[1]))
    return int(value) if integer else float(value)


def sample_spec(
    scenario: str,
    raw: dict[str, Any],
    *,
    case_id: str,
    rng: random.Random,
) -> ScenarioSpec:
    marker = hashlib.sha256(f"{case_id}:{rng.random()}".encode()).hexdigest()[:20]
    replacements = {"{python}": sys.executable, "{workspace}": str(ROOT)}
    pre_command = tuple(
        replacements.get(str(item), str(item)) for item in raw.get("pre_command", [])
    )
    pre_cwd = raw.get("pre_cwd")
    if pre_cwd in replacements:
        pre_cwd = replacements[pre_cwd]
    return ScenarioSpec(
        name=scenario,
        duration_seconds=float(sample_value(raw["duration_seconds"], rng)),
        line_count=int(sample_value(raw["line_count"], rng, integer=True)),
        line_bytes=int(sample_value(raw["line_bytes"], rng, integer=True)),
        exit_code=int(rng.choice(raw["exit_codes"])),
        cancel_after_seconds=float(sample_value(raw.get("cancel_after_seconds", 0.0), rng)),
        marker=marker,
        score=round(rng.uniform(0, 100), 6),
        case_id=case_id,
        pre_command=pre_command,
        pre_cwd=str(pre_cwd) if pre_cwd else None,
    )


def arm_class(
    name: str,
) -> type[ShellArm] | type[PlainTmuxArm] | type[WrappedTmuxArm] | type[AwaitlessArm]:
    return {
        "shell": ShellArm,
        "tmux_plain": PlainTmuxArm,
        "tmux_wrapped": WrappedTmuxArm,
        "awaitless": AwaitlessArm,
    }[name]


def evaluate(spec: ScenarioSpec, observation: ArmObservation, config: dict[str, Any]) -> dict[str, Any]:
    state_correct = observation.state == spec.expected_state
    exit_correct = None if spec.name == "cancel_tree" else observation.exit_code == spec.exit_code
    artifact_correct = None if spec.name == "cancel_tree" else observation.artifact == spec.expected_artifact
    marker_seen = None if spec.name == "cancel_tree" else spec.marker in observation.log_text
    log_correct = None
    if spec.name != "cancel_tree":
        log_correct = bool(marker_seen) and (
            not expected_truncation(spec, config) or observation.truncated is True
        )
    cancel_correct = None
    if spec.name == "cancel_tree":
        cancel_correct = observation.orphan_processes == 0 and state_correct
    applicable = [state_correct, exit_correct, artifact_correct, log_correct, cancel_correct]
    result_correct = all(value is not False for value in applicable) and any(
        value is not None for value in applicable
    )
    recovery_success = None
    if spec.name == "recovery":
        recovery_success = observation.recovery_injected is True and result_correct
    return {
        "result_correct": result_correct,
        "state_correct": state_correct,
        "exit_code_correct": exit_correct,
        "artifact_correct": artifact_correct,
        "log_contract_correct": log_correct,
        "recovery_success": recovery_success,
        "cancel_cleanup_success": cancel_correct,
        "marker_seen": marker_seen,
    }


def base_environment(config_path: Path, config: dict[str, Any]) -> dict[str, Any]:
    tmux_version = (
        execute(["tmux", "-V"], timeout=5).stdout.decode().strip()
        if shutil.which("tmux")
        else None
    )
    commit = execute(["git", "rev-parse", "HEAD"], timeout=5).stdout.decode().strip()
    git_status = execute(["git", "status", "--porcelain"], timeout=5)
    version = source_version()
    effective_config = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
    return {
        "profile": config["name"],
        "config_path": str(config_path.resolve()),
        "config_sha256": hashlib.sha256(config_path.read_bytes()).hexdigest(),
        "effective_config_sha256": hashlib.sha256(effective_config).hexdigest(),
        "configured_trials": int(config["trials"]),
        "git_commit": commit,
        "git_dirty": bool(git_status.stdout.strip()),
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "workload_sha256": hashlib.sha256(WORKLOAD.read_bytes()).hexdigest(),
        "tmux_wrapper_sha256": hashlib.sha256(TMUX_WRAPPER.read_bytes()).hexdigest(),
        "platform": platform.platform(),
        "kernel": platform.release(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "tmux_version": tmux_version,
        "awaitless_version": version,
        "awaitless_source": str((ROOT / "src" / "awaitless").resolve()),
        "backend": "local",
        "system_invocation_scope": "harness-visible commands plus self-instrumented tmux wrapper control calls",
    }


def build_record(
    *,
    experiment_id: str,
    case_id: str,
    arm: str,
    spec: ScenarioSpec,
    recorder: Recorder,
    observation: ArmObservation,
    config: dict[str, Any],
    environment: dict[str, Any],
    elapsed: float,
    trial_root: Path,
    error: str | None,
    seed: int,
) -> dict[str, Any]:
    assessed = evaluate(spec, observation, config)
    if error:
        assessed.update(
            result_correct=False,
            state_correct=False,
            exit_code_correct=None if spec.name == "cancel_tree" else False,
            artifact_correct=None if spec.name == "cancel_tree" else False,
            log_contract_correct=None if spec.name == "cancel_tree" else False,
            recovery_success=False if spec.name == "recovery" else None,
            cancel_cleanup_success=False if spec.name == "cancel_tree" else None,
        )
    return {
        "schema_version": 1,
        "record_type": "trial",
        "experiment_id": experiment_id,
        "case_id": case_id,
        "trial_id": f"{case_id}:{arm}",
        "recorded_at": utc_now(),
        "arm": arm,
        "scenario": spec.name,
        "seed": seed,
        "environment": environment,
        "expected": {
            "state": spec.expected_state,
            "exit_code": None if spec.name == "cancel_tree" else spec.exit_code,
            "artifact": spec.expected_artifact,
            "final_log_marker": None if spec.name == "cancel_tree" else spec.marker,
            "full_log_bytes": spec.full_log_bytes,
            "workload": {
                "duration_seconds": spec.duration_seconds,
                "line_count": spec.line_count,
                "line_bytes": spec.line_bytes,
                "cancel_after_seconds": spec.cancel_after_seconds,
            },
        },
        "observed": {
            "state": observation.state,
            "exit_code": observation.exit_code,
            "artifact": observation.artifact,
            "final_log_marker_seen": assessed["marker_seen"],
            "truncated": observation.truncated,
            "orphan_processes": observation.orphan_processes,
            "recovery_injected": observation.recovery_injected,
        },
        "metrics": {
            "result_correct": assessed["result_correct"],
            "state_correct": assessed["state_correct"],
            "exit_code_correct": assessed["exit_code_correct"],
            "artifact_correct": assessed["artifact_correct"],
            "log_contract_correct": assessed["log_contract_correct"],
            "recovery_success": assessed["recovery_success"],
            "cancel_cleanup_success": assessed["cancel_cleanup_success"],
            "duplicate_launch": sum(
                event["operation"] in {"run", "run_interrupted", "submit"}
                for event in recorder.events
            ) > 1,
            "agent_tool_calls": recorder.agent_calls,
            "agent_visible_bytes": recorder.visible_bytes,
            "system_command_invocations": recorder.system_commands + observation.system_command_adjustment,
            "duplicated_log_bytes": observation.duplicated_log_bytes,
            "input_tokens": None,
            "output_tokens": None,
            "cached_input_tokens": None,
            "reasoning_tokens": None,
            "wall_time_seconds": round(elapsed, 6),
            "cpu_time_seconds": None,
            "peak_rss_bytes": None,
            "disk_bytes": disk_bytes(trial_root),
            "ssh_request_count": None,
            "manual_interventions": 0,
        },
        "events": recorder.events,
        "arm_metadata": arm_class(arm).metadata,
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
    expected_version = config.get("expected_version")
    if expected_version is not None and expected_version != source_version():
        raise ValueError(
            f"benchmark requires Awaitless {expected_version}; source is {source_version()}"
        )
    for key in ("poll_interval_seconds", "wait_interrupt_after_seconds", "log_tail_lines", "max_return_bytes"):
        if float(config[key]) <= 0:
            raise ValueError(f"{key} must be positive")


def append_record(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--config", type=Path, default=METRIC_ROOT / "configs" / "smoke.json")
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--append", action="store_true", help="append a new experiment to an existing JSONL")
    result.add_argument("--trials", type=int, help="override the config trial count")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    config = json.loads(args.config.read_text(encoding="utf-8"))
    if args.trials is not None:
        config["trials"] = args.trials
    validate_config(config)
    if shutil.which("tmux") is None and set(config["arms"]) & {"tmux_plain", "tmux_wrapped"}:
        raise SystemExit("tmux is required by the selected arms")
    if args.output.exists() and not args.append:
        raise SystemExit(f"refusing to overwrite {args.output}; pass --append or choose a new path")

    environment = base_environment(args.config, config)
    config_hash = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:10]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    experiment_id = f"{config['name']}-{timestamp}-{config_hash}"
    rng = random.Random(int(config["seed"]))
    completed = 0
    failures = 0
    any_error = False

    for trial_index in range(int(config["trials"])):
        scenarios = list(config["scenarios"])
        rng.shuffle(scenarios)
        for scenario in scenarios:
            case_id = f"{experiment_id}:{scenario}:{trial_index:03d}"
            seed_material = f"{config['seed']}:{trial_index}:{scenario}".encode()
            case_seed = int.from_bytes(hashlib.sha256(seed_material).digest()[:8], "big")
            spec = sample_spec(
                scenario,
                config["scenarios"][scenario],
                case_id=case_id,
                rng=random.Random(case_seed),
            )
            arms = list(config["arms"])
            random.Random(case_seed ^ 0xA71E55).shuffle(arms)
            for arm in arms:
                print(f"[metric] {case_id} arm={arm}", file=sys.stderr, flush=True)
                recorder = Recorder()
                observation = ArmObservation()
                error: str | None = None
                with tempfile.TemporaryDirectory(prefix=f"awaitless-metric-{arm}-") as temporary:
                    trial_root = Path(temporary)
                    work = trial_root / "work"
                    work.mkdir()
                    runner = arm_class(arm)(recorder, spec, work, config)
                    started = time.monotonic()
                    try:
                        try:
                            observation = runner.run()
                        except Exception as exc:
                            error = f"{type(exc).__name__}: {exc}"
                    finally:
                        elapsed = time.monotonic() - started
                        try:
                            runner.cleanup()
                        except Exception as cleanup_exc:
                            if error is None:
                                error = f"cleanup {type(cleanup_exc).__name__}: {cleanup_exc}"
                            else:
                                error += f"; cleanup {type(cleanup_exc).__name__}: {cleanup_exc}"
                    record = build_record(
                        experiment_id=experiment_id,
                        case_id=case_id,
                        arm=arm,
                        spec=spec,
                        recorder=recorder,
                        observation=observation,
                        config=config,
                        environment=environment,
                        elapsed=elapsed,
                        trial_root=trial_root,
                        error=error,
                        seed=case_seed,
                    )
                    append_record(args.output, record)
                completed += 1
                failures += int(not record["metrics"]["result_correct"])
                any_error = any_error or record["error"] is not None

    print(
        json.dumps(
            {
                "experiment_id": experiment_id,
                "records": completed,
                "incorrect_records": failures,
                "output": str(args.output),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    # Comparative deficiencies are experimental results, not harness failures.
    # Execution errors still make the command fail for CI/automation callers.
    return 1 if any_error else 0


if __name__ == "__main__":
    raise SystemExit(main())
