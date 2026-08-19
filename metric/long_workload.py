#!/usr/bin/env python3
"""Controlled real-command workloads for the long-running Agent benchmark."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ADAPTERS = {
    "sleep",
    "cargo_build",
    "pytest",
    "docker_build",
    "npm_install",
    "model_inference",
    "command",
}


@dataclass(frozen=True)
class CommandPlan:
    command: list[str]
    cwd: Path
    env: dict[str, str]
    timeout_seconds: float


def _completed(
    command: list[str], *, cwd: Path | None = None, timeout: float = 15.0
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        command,
        cwd=cwd or ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=timeout,
    )


def _version(command: list[str]) -> str | None:
    try:
        result = _completed(command, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return None
    text = (result.stdout or result.stderr).decode("utf-8", errors="replace").strip()
    return text.splitlines()[0][:300] if result.returncode == 0 and text else None


def _required_commands(workload: dict[str, Any]) -> list[str]:
    adapter = str(workload.get("adapter", ""))
    defaults = {
        "sleep": [sys.executable],
        "cargo_build": ["cargo"],
        "pytest": [sys.executable],
        "docker_build": ["docker"],
        "npm_install": ["npm", "node"],
        "model_inference": [sys.executable],
        "command": [],
    }
    configured = workload.get("required_commands", defaults.get(adapter, []))
    if not isinstance(configured, list) or not all(isinstance(item, str) for item in configured):
        raise ValueError("required_commands must be a list of strings")
    return configured


def probe_workload(workload: dict[str, Any], *, env_file: Path) -> dict[str, Any]:
    adapter = str(workload.get("adapter", ""))
    if adapter not in ADAPTERS:
        return {"available": False, "reason": f"unknown adapter {adapter!r}"}
    missing = [item for item in _required_commands(workload) if shutil.which(item) is None]
    if missing:
        return {
            "available": False,
            "reason": "missing command(s): " + ", ".join(sorted(missing)),
        }
    if adapter == "pytest":
        result = _completed([sys.executable, "-c", "import pytest"], timeout=10)
        if result.returncode:
            return {"available": False, "reason": "Python module pytest is not installed"}
        version = _version([sys.executable, "-m", "pytest", "--version"])
    elif adapter == "docker_build":
        try:
            result = _completed(["docker", "info", "--format", "{{json .ServerVersion}}"], timeout=15)
        except (OSError, subprocess.TimeoutExpired) as exc:
            return {"available": False, "reason": f"Docker daemon probe failed: {type(exc).__name__}"}
        if result.returncode:
            detail = (result.stderr or result.stdout).decode("utf-8", errors="replace").strip()
            return {"available": False, "reason": f"Docker daemon unavailable: {detail[:300]}"}
        base_image = str(workload.get("base_image", "busybox:latest"))
        image = _completed(["docker", "image", "inspect", base_image], timeout=15)
        if image.returncode:
            return {
                "available": False,
                "reason": f"local Docker base image is missing: {base_image}",
            }
        version = _version(["docker", "--version"])
    elif adapter == "model_inference":
        try:
            try:
                from .run_agent import LLMConfig
            except ImportError:
                from run_agent import LLMConfig  # type: ignore[no-redef]

            llm = LLMConfig.load(env_file)
        except (OSError, ValueError) as exc:
            return {"available": False, "reason": f"LLM configuration unavailable: {exc}"}
        version = llm.model
    elif adapter == "command":
        command = workload.get("command")
        if not isinstance(command, list) or not command or not all(
            isinstance(item, str) for item in command
        ):
            return {"available": False, "reason": "command adapter requires a non-empty command list"}
        version = None
    else:
        version_commands = {
            "sleep": [sys.executable, "--version"],
            "cargo_build": ["cargo", "--version"],
            "npm_install": ["npm", "--version"],
        }
        version = _version(version_commands[adapter])
    return {
        "available": True,
        "reason": None,
        "adapter": adapter,
        "version": version,
        "controlled_fixture": adapter != "command",
    }


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _expanded(value: str, replacements: dict[str, str]) -> str:
    result = value
    for key, replacement in replacements.items():
        result = result.replace("{" + key + "}", replacement)
    if re.search(r"\{[A-Za-z_][A-Za-z0-9_]*\}", result):
        raise ValueError(f"unresolved command placeholder in {value!r}")
    return result


def prepare_plan(
    workload: dict[str, Any],
    *,
    task_dir: Path,
    duration_seconds: float,
    env_file: Path,
    marker: str,
    artifact: Path,
) -> CommandPlan:
    adapter = str(workload["adapter"])
    fixture = task_dir / "fixture"
    fixture.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "AWAITLESS_BENCH_HOLD_SECONDS": f"{duration_seconds:g}"}
    timeout = float(workload.get("timeout_seconds", max(300.0, duration_seconds + 120.0)))

    if adapter == "sleep":
        command = [
            sys.executable,
            "-c",
            "import os,time; time.sleep(float(os.environ['AWAITLESS_BENCH_HOLD_SECONDS'])); "
            "print('controlled sleep completed')",
        ]
        cwd = fixture
    elif adapter == "pytest":
        _write(
            fixture / "test_long_task.py",
            "import os\nimport time\n\n"
            "def test_controlled_long_task():\n"
            "    time.sleep(float(os.environ['AWAITLESS_BENCH_HOLD_SECONDS']))\n"
            "    assert True\n",
        )
        command = [sys.executable, "-m", "pytest", "-q"]
        cwd = fixture
    elif adapter == "npm_install":
        _write(
            fixture / "package.json",
            json.dumps(
                {
                    "name": "awaitless-long-running-fixture",
                    "version": "1.0.0",
                    "private": True,
                    "scripts": {"install": "node install.js"},
                },
                indent=2,
            )
            + "\n",
        )
        _write(
            fixture / "install.js",
            "const seconds = Number(process.env.AWAITLESS_BENCH_HOLD_SECONDS);\n"
            "setTimeout(() => console.log('controlled npm install completed'), seconds * 1000);\n",
        )
        command = ["npm", "install", "--no-audit", "--no-fund", "--foreground-scripts"]
        cwd = fixture
    elif adapter == "cargo_build":
        _write(
            fixture / "Cargo.toml",
            "[package]\nname = \"awaitless-long-running-fixture\"\nversion = \"0.1.0\"\n"
            "edition = \"2021\"\nbuild = \"build.rs\"\n",
        )
        _write(fixture / "src" / "main.rs", "fn main() { println!(\"fixture\"); }\n")
        _write(
            fixture / "build.rs",
            "use std::{env, thread, time::Duration};\n"
            "fn main() {\n"
            "  let seconds: f64 = env::var(\"AWAITLESS_BENCH_HOLD_SECONDS\").unwrap().parse().unwrap();\n"
            "  thread::sleep(Duration::from_secs_f64(seconds));\n"
            "  println!(\"cargo:warning=controlled cargo build completed\");\n"
            "}\n",
        )
        command = ["cargo", "build", "--offline"]
        cwd = fixture
    elif adapter == "docker_build":
        base_image = str(workload.get("base_image", "busybox:latest"))
        if any(character.isspace() for character in base_image):
            raise ValueError("Docker base_image cannot contain whitespace")
        _write(
            fixture / "Dockerfile",
            f"FROM {base_image}\nARG HOLD_SECONDS\nRUN sleep \"$HOLD_SECONDS\"\n"
            "RUN echo controlled docker build completed\n",
        )
        command = [
            "docker",
            "build",
            "--no-cache",
            "--build-arg",
            f"HOLD_SECONDS={duration_seconds:g}",
            "-t",
            f"awaitless-long-bench:{marker}",
            ".",
        ]
        cwd = fixture
    elif adapter == "model_inference":
        requests = int(workload.get("inference_requests", 3))
        if requests <= 0:
            raise ValueError("inference_requests must be positive")
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "inference",
            "--env-file",
            str(env_file.resolve()),
            "--requests",
            str(requests),
            "--task-marker",
            marker,
        ]
        cwd = fixture
    elif adapter == "command":
        replacements = {
            "workspace": str(ROOT),
            "task_dir": str(task_dir),
            "fixture": str(fixture),
            "duration_seconds": f"{duration_seconds:g}",
            "marker": marker,
            "artifact": str(artifact),
        }
        command = [_expanded(str(item), replacements) for item in workload["command"]]
        raw_cwd = _expanded(str(workload.get("cwd", "{workspace}")), replacements)
        cwd = Path(raw_cwd).resolve()
        if not cwd.is_dir():
            raise ValueError(f"command cwd does not exist: {cwd}")
    else:
        raise ValueError(f"unsupported adapter {adapter!r}")
    return CommandPlan(command=command, cwd=cwd, env=env, timeout_seconds=timeout)


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n")
    temporary.replace(path)


def run_workload(args: argparse.Namespace) -> int:
    workload = json.loads(args.workload_json)
    task_dir = args.task_dir.resolve()
    task_dir.mkdir(parents=True, exist_ok=True)
    plan = prepare_plan(
        workload,
        task_dir=task_dir,
        duration_seconds=args.duration_seconds,
        env_file=args.env_file,
        marker=args.marker,
        artifact=args.artifact,
    )
    started = time.monotonic()
    try:
        result = subprocess.run(
            plan.command,
            cwd=plan.cwd,
            env=plan.env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=plan.timeout_seconds,
        )
        return_code = result.returncode
        stdout = result.stdout
        stderr = result.stderr
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        return_code = 124
        stdout = exc.stdout or b""
        stderr = (exc.stderr or b"") + b"\ncontrolled workload timed out\n"
        timed_out = True
    elapsed = time.monotonic() - started
    if workload["adapter"] == "docker_build" and return_code == 0:
        # Controlled fixtures use a unique tag so cleanup is exact. Cleanup is
        # outside the inner build duration but remains visible in outer wall time.
        _completed(["docker", "image", "rm", "-f", f"awaitless-long-bench:{args.marker}"], timeout=60)
    sys.stdout.buffer.write(stdout)
    sys.stderr.buffer.write(stderr)
    sys.stdout.buffer.write(f"FINAL_MARKER={args.marker}\n".encode())
    sys.stdout.buffer.flush()
    sys.stderr.buffer.flush()
    adapter_result: dict[str, Any] | None = None
    try:
        candidate = json.loads(stdout.decode("utf-8"))
        if isinstance(candidate, dict):
            adapter_result = candidate
    except (UnicodeDecodeError, json.JSONDecodeError):
        pass
    artifact = {
        "schema_version": 1,
        "task_id": args.task_id,
        "workload": str(workload["id"]),
        "adapter": str(workload["adapter"]),
        "ok": return_code == 0,
        "exit_code": return_code,
        "marker": args.marker,
        "duration_seconds": round(elapsed, 6),
        "timed_out": timed_out,
        "command": shlex.join(plan.command),
        "stdout_bytes": len(stdout),
        "stderr_bytes": len(stderr),
        "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
        "stderr_sha256": hashlib.sha256(stderr).hexdigest(),
        "adapter_result": adapter_result,
    }
    atomic_json(args.artifact, artifact)
    return return_code


def inference(args: argparse.Namespace) -> int:
    try:
        try:
            from .run_agent import LLMConfig, ModelClient
        except ImportError:
            from run_agent import LLMConfig, ModelClient  # type: ignore[no-redef]

        config = LLMConfig.load(args.env_file)
        client = ModelClient(config)
        usages: list[dict[str, int]] = []
        hashes: list[str] = []
        for index in range(args.requests):
            response = client.chat(
                {
                    "model": config.model,
                    "messages": [
                        {
                            "role": "user",
                            "content": (
                                "Return one short JSON object with keys ok and index. "
                                f"Benchmark marker {args.task_marker}; index {index}."
                            ),
                        }
                    ],
                    "thinking": {"type": "disabled"},
                    "response_format": {"type": "json_object"},
                    "max_tokens": 128,
                    "stream": False,
                }
            )
            usage = response.value.get("usage", {})
            content = response.value.get("choices", [{}])[0].get("message", {}).get("content", "")
            if not all(isinstance(usage.get(key), int) for key in ("prompt_tokens", "completion_tokens")):
                raise RuntimeError("model inference response omitted token usage")
            usages.append(
                {
                    "prompt_tokens": int(usage["prompt_tokens"]),
                    "completion_tokens": int(usage["completion_tokens"]),
                }
            )
            hashes.append(hashlib.sha256(str(content).encode()).hexdigest())
        print(
            json.dumps(
                {
                    "model": config.model,
                    "requests": args.requests,
                    "prompt_tokens": sum(item["prompt_tokens"] for item in usages),
                    "completion_tokens": sum(item["completion_tokens"] for item in usages),
                    "response_sha256": hashes,
                },
                separators=(",", ":"),
            )
        )
        return 0
    except Exception as exc:
        print(f"model inference fixture failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="action", required=True)
    run = commands.add_parser("run", help="run one controlled workload")
    run.add_argument("--workload-json", required=True)
    run.add_argument("--task-id", required=True)
    run.add_argument("--task-dir", type=Path, required=True)
    run.add_argument("--artifact", type=Path, required=True)
    run.add_argument("--marker", required=True)
    run.add_argument("--duration-seconds", type=float, required=True)
    run.add_argument("--env-file", type=Path, default=ROOT / ".env")
    child = commands.add_parser("inference", help="run the model-inference adapter")
    child.add_argument("--env-file", type=Path, required=True)
    child.add_argument("--requests", type=int, required=True)
    child.add_argument("--task-marker", required=True)
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.action == "run":
        if args.duration_seconds < 0:
            print("duration_seconds must be non-negative", file=sys.stderr)
            return 2
        return run_workload(args)
    if args.requests <= 0:
        print("requests must be positive", file=sys.stderr)
        return 2
    return inference(args)


if __name__ == "__main__":
    raise SystemExit(main())
