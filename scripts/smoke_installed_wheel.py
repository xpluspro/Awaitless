#!/usr/bin/env python3
"""Verify an installed Awaitless wheel through its public CLI contract."""

from __future__ import annotations

import importlib.metadata
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


def run(command: list[str], *, env: dict[str, str]) -> dict[str, Any]:
    result = subprocess.run(
        command,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[-2000:]
        raise RuntimeError(f"command failed ({result.returncode}): {detail}")
    return json.loads(result.stdout)


def main() -> int:
    version = importlib.metadata.version("awaitless-runner")
    executable = Path(sys.executable).with_name("awaitless")
    if not executable.is_file():
        raise RuntimeError(f"awaitless console script is missing: {executable}")
    mcp_executable = Path(sys.executable).with_name("awaitless-mcp")
    if not mcp_executable.is_file():
        raise RuntimeError(f"awaitless-mcp console script is missing: {mcp_executable}")
    registry_executable = Path(sys.executable).with_name("awaitless-runner")
    for server_executable in (mcp_executable, registry_executable):
        if not server_executable.is_file():
            raise RuntimeError(f"MCP console script is missing: {server_executable}")
        mcp_help = subprocess.run(
            [str(server_executable), "--help"],
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        if mcp_help.returncode != 0 or "stdio MCP server" not in mcp_help.stdout:
            raise RuntimeError(
                f"MCP entry point failed ({server_executable}): {mcp_help.stderr}"
            )

    expected_artifact = {"installed_wheel": True, "version": version}
    artifact_source = json.dumps(expected_artifact, separators=(",", ":"))
    command_source = (
        "from pathlib import Path; "
        f"Path('result.json').write_text({artifact_source!r}, encoding='utf-8'); "
        "print('wheel-smoke-ok', end='')"
    )

    with tempfile.TemporaryDirectory(prefix="awaitless-wheel-smoke-") as temp:
        temporary = Path(temp)
        work = temporary / "work"
        work.mkdir()
        environment = os.environ.copy()
        environment["AWAITLESS_DATA_DIR"] = str(temporary / "data")

        doctor = run([str(executable), "doctor", "--json"], env=environment)
        if doctor.get("ok") is not True:
            raise RuntimeError(f"doctor failed: {doctor}")

        adaptive_inline = run(
            [
                str(executable),
                "run",
                "--inline-timeout",
                "2s",
                "--json",
                "--",
                sys.executable,
                "-c",
                "print('adaptive-wheel-inline')",
            ],
            env=environment,
        )
        if (
            adaptive_inline.get("state") != "succeeded"
            or adaptive_inline.get("delivery") != "inline"
            or adaptive_inline.get("detached") is not False
            or adaptive_inline.get("stdout_tail") != "adaptive-wheel-inline\n"
        ):
            raise RuntimeError(
                f"installed-wheel adaptive inline run failed: {adaptive_inline}"
            )

        adaptive_detached = run(
            [
                str(executable),
                "run",
                "--inline-timeout",
                "0.01s",
                "--json",
                "--",
                sys.executable,
                "-c",
                "import time; print('adaptive-wheel-detached', flush=True); time.sleep(.2)",
            ],
            env=environment,
        )
        if (
            adaptive_detached.get("delivery") != "detached"
            or adaptive_detached.get("detached") is not True
            or adaptive_detached.get("detach_reason") != "inline_timeout"
        ):
            raise RuntimeError(
                f"installed-wheel adaptive detach failed: {adaptive_detached}"
            )
        adaptive_final = run(
            [
                str(executable),
                "wait",
                adaptive_detached["job_id"],
                "--json",
            ],
            env=environment,
        )
        if (
            adaptive_final.get("state") != "succeeded"
            or adaptive_final.get("stdout_tail") != "adaptive-wheel-detached\n"
        ):
            raise RuntimeError(
                f"installed-wheel detached recovery failed: {adaptive_final}"
            )

        submitted = run(
            [
                str(executable),
                "submit",
                "--json",
                "--cwd",
                str(work),
                "--artifact",
                "result.json",
                "--",
                sys.executable,
                "-c",
                command_source,
            ],
            env=environment,
        )
        if submitted.get("state") not in {"running", "succeeded"}:
            raise RuntimeError(f"submit returned an invalid state: {submitted}")

        completion_feed = run(
            [
                str(executable),
                "completions",
                submitted["job_id"],
                "--json",
            ],
            env=environment,
        )
        if len(completion_feed.get("completions", [])) != 1:
            raise RuntimeError(
                f"installed-wheel completion was not delivered: {completion_feed}"
            )
        completed = completion_feed["completions"][0]["result"]
        expected = (
            completed.get("state"),
            completed.get("exit_code"),
            completed.get("stdout_tail"),
            completed.get("parsed_results"),
        )
        if expected != ("succeeded", 0, "wheel-smoke-ok", expected_artifact):
            raise RuntimeError(f"installed-wheel smoke test failed: {completed}")
        if not completed.get("artifacts", [{}])[0].get("exists"):
            raise RuntimeError(f"declared Artifact was not returned: {completed}")

        demo = run(
            [
                str(executable),
                "demo",
                "--duration",
                "0.25",
                "--interrupt-after",
                "0.05",
                "--json",
            ],
            env=environment,
        )
        if (
            demo.get("recovered_by_new_client") is not True
            or demo.get("completion_count") != 2
        ):
            raise RuntimeError(f"installed-wheel recovery demo failed: {demo}")

    print(json.dumps({"ok": True, "version": version}, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
