#!/usr/bin/env python3
"""Compare repeated SSH polling with Awaitless on the same sleep-only workload."""

from __future__ import annotations

import argparse
import base64
import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "benchmarks" / "results" / "polling-vs-awaitless.json"


def run(
    command: list[str],
    *,
    input_text: str | None = None,
    env: dict[str, str] | None = None,
    timeout: float = 60,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        input=input_text,
        env=env,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[-2000:]
        raise RuntimeError(f"command failed ({result.returncode}): {detail}")
    return result


def workload(samples: int, interval: float, line_bytes: int, artifact: str | None) -> str:
    prefix_bytes = len("sample=00 ".encode()) + 1  # trailing newline
    payload = "x" * (line_bytes - prefix_bytes)
    lines = [
        "set -eu",
        f"payload={shlex.quote(payload)}",
        f"for i in $(seq 1 {samples}); do",
        "  printf 'sample=%02d %s\\n' \"$i\" \"$payload\"",
        f"  [ \"$i\" -eq {samples} ] || sleep {interval:g}",
        "done",
    ]
    if artifact:
        content = json.dumps(
            {"correctness": True, "samples": samples, "line_bytes": line_bytes},
            separators=(",", ":"),
        )
        lines.append(f"printf %s {shlex.quote(content)} > {shlex.quote(artifact)}")
    return "\n".join(lines)


def ssh_call(
    ssh: list[str], script: str, *, timeout: float = 30, check: bool = True
) -> subprocess.CompletedProcess[str]:
    return run([*ssh, "bash -s"], input_text=script, timeout=timeout, check=check)


def parse_key_values(output: str) -> dict[str, str]:
    return dict(line.split("=", 1) for line in output.splitlines() if "=" in line)


def note(message: str) -> None:
    print(f"[experiment] {message}", file=sys.stderr, flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, help="OpenSSH host or alias")
    parser.add_argument("--display-host", default="ssh-login-node")
    parser.add_argument("--polls", type=int, default=12)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--samples", type=int, default=12)
    parser.add_argument("--sample-interval", type=float, default=4.5)
    parser.add_argument("--line-bytes", type=int, default=1024)
    parser.add_argument("--operation-timeout", type=float, default=30.0)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--keep-remote", action="store_true")
    args = parser.parse_args()

    if args.polls <= 0 or args.samples <= 0:
        parser.error("--polls and --samples must be positive")
    if args.poll_interval <= 0 or args.sample_interval <= 0:
        parser.error("poll and sample intervals must be positive")
    if args.line_bytes < 32:
        parser.error("--line-bytes must be at least 32")

    experiment_id = f"exp_{uuid.uuid4().hex[:12]}"
    artifact_name = f"awaitless-{experiment_id}.json"
    remote_baseline = f"$HOME/.awaitless/experiments/{experiment_id}"
    awaitless_job_id: str | None = None

    with tempfile.TemporaryDirectory(prefix="awaitless-experiment-", dir="/tmp") as temp:
        temporary = Path(temp)
        control_path = temporary / "ssh-control"
        data_dir = temporary / "awaitless-data"
        config_path = temporary / "config.toml"
        config_path.write_text(
            "\n".join(
                [
                    "[defaults]",
                    "poll_interval = 2",
                    "max_return_bytes = 65536",
                    "",
                    "[hosts.experiment]",
                    f"hostname = {json.dumps(args.host)}",
                    "gssapi_authentication = false",
                    "connect_timeout = 10",
                    f"operation_timeout = {args.operation_timeout:g}",
                    'remote_job_dir = "~/.awaitless/jobs"',
                    "",
                ]
            ),
            encoding="utf-8",
        )
        child_env = os.environ.copy()
        child_env["AWAITLESS_DATA_DIR"] = str(data_dir)
        child_env["PYTHONPATH"] = str(ROOT / "src")

        ssh = [
            "ssh",
            "-S",
            str(control_path),
            "-o",
            "ControlMaster=auto",
            "-o",
            "ControlPersist=120",
            "-o",
            "BatchMode=yes",
            "-o",
            "GSSAPIAuthentication=no",
            "-o",
            "ConnectTimeout=10",
            args.host,
        ]

        # Open one control connection so the twelve baseline polls measure the
        # polling pattern rather than repeated authentication latency.
        run(
            [
                "ssh",
                "-M",
                "-S",
                str(control_path),
                "-o",
                "ControlPersist=120",
                "-o",
                "BatchMode=yes",
                "-o",
                "GSSAPIAuthentication=no",
                "-o",
                "ConnectTimeout=10",
                "-fnNT",
                args.host,
            ],
            timeout=args.operation_timeout,
        )
        note("SSH control connection ready")

        try:
            awaitless_workload = workload(
                args.samples, args.sample_interval, args.line_bytes, artifact_name
            )
            submit_command = [
                sys.executable,
                "-m",
                "awaitless",
                "--config",
                str(config_path),
                "submit",
                "--host",
                "experiment",
                "--artifact",
                artifact_name,
                "--json",
                "--",
                "bash",
                "-c",
                awaitless_workload,
            ]
            submitted = run(
                submit_command,
                env=child_env,
                timeout=args.operation_timeout + 5,
            )
            submit_value = json.loads(submitted.stdout)
            awaitless_job_id = submit_value["job_id"]
            note(f"Awaitless submitted {awaitless_job_id}")

            baseline_workload = workload(
                args.samples, args.sample_interval, args.line_bytes, None
            )
            wrapper = "\n".join(
                [
                    "#!/usr/bin/env bash",
                    "set +e",
                    baseline_workload,
                    "rc=$?",
                    'printf "%s\\n" "$rc" > "$job_dir/exit_code"',
                    'date -u +%Y-%m-%dT%H:%M:%S.%NZ > "$job_dir/finished_at"',
                ]
            )
            encoded_wrapper = base64.b64encode(wrapper.encode()).decode()
            start_script = f"""set -eu
job_dir="{remote_baseline}"
umask 077
mkdir -p "$job_dir"
printf %s {shlex.quote(encoded_wrapper)} | base64 -d > "$job_dir/run.sh"
chmod 700 "$job_dir/run.sh"
: > "$job_dir/stdout.log"
: > "$job_dir/stderr.log"
setsid nohup env job_dir="$job_dir" bash "$job_dir/run.sh" >"$job_dir/stdout.log" 2>"$job_dir/stderr.log" </dev/null &
echo "PID=$!"
"""
            baseline_started = time.monotonic()
            ssh_call(ssh, start_script, timeout=10)
            note("traditional SSH workload launched")

            wait_command = [
                sys.executable,
                "-m",
                "awaitless",
                "--config",
                str(config_path),
                "wait",
                awaitless_job_id,
                "--json",
            ]
            wait_started = time.monotonic()
            waiter = subprocess.Popen(
                wait_command,
                cwd=ROOT,
                env=child_env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            poll_log_bytes: list[int] = []
            poll_response_bytes: list[int] = []
            baseline_exit_code: int | None = None
            for index in range(args.polls):
                target = baseline_started + index * args.poll_interval
                time.sleep(max(0.0, target - time.monotonic()))
                poll_script = f"""set -u
job_dir="{remote_baseline}"
printf 'LOG='
base64 < "$job_dir/stdout.log" | tr -d '\\n'
printf '\\n'
[ -f "$job_dir/exit_code" ] && echo "EXIT=$(cat "$job_dir/exit_code")" || echo 'EXIT='
"""
                poll = ssh_call(ssh, poll_script, timeout=10)
                values = parse_key_values(poll.stdout)
                log = base64.b64decode(values.get("LOG", ""), validate=True)
                poll_log_bytes.append(len(log))
                poll_response_bytes.append(len(poll.stdout.encode()))
                note(f"poll {index + 1}/{args.polls}: returned {len(log)} log bytes")
                if values.get("EXIT", "").lstrip("-").isdigit():
                    baseline_exit_code = int(values["EXIT"])

            wait_stdout, wait_stderr = waiter.communicate(timeout=180)
            wait_elapsed = time.monotonic() - wait_started
            if waiter.returncode != 0:
                raise RuntimeError(
                    f"awaitless wait failed ({waiter.returncode}): "
                    f"{(wait_stderr or wait_stdout).strip()[-2000:]}"
                )
            awaitless_value = json.loads(wait_stdout)
            note("Awaitless wait returned terminal state and Artifact")
            if baseline_exit_code is None:
                final_poll = ssh_call(
                    ssh,
                    f'job_dir="{remote_baseline}"\ncat "$job_dir/exit_code"',
                    timeout=10,
                )
                baseline_exit_code = int(final_poll.stdout.strip())

            baseline_log_bytes = sum(poll_log_bytes)
            awaitless_log_bytes = len(awaitless_value["stdout_tail"].encode()) + len(
                awaitless_value["stderr_tail"].encode()
            )
            final_log_bytes = poll_log_bytes[-1]
            call_reduction = 100 * (1 - 2 / (args.polls + 1))
            log_reduction = 100 * (1 - awaitless_log_bytes / baseline_log_bytes)
            recorded_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            result: dict[str, Any] = {
                "schema_version": 1,
                "recorded_at": recorded_at,
                "measurement_scope": {
                    "calls": (
                        "agent-visible CLI invocations; Awaitless internal SSH "
                        "control operations are not counted"
                    ),
                    "log_bytes": (
                        "decoded log-content bytes returned to the caller; "
                        "protocol and network framing are excluded"
                    ),
                },
                "environment": {
                    "backend": "real SSH login node",
                    "host": args.display_host,
                    "workload": "sleep-only; no CPU/GPU-intensive work",
                    "samples": args.samples,
                    "sample_interval_seconds": args.sample_interval,
                    "line_bytes": args.line_bytes,
                },
                "traditional_polling": {
                    "launch_calls": 1,
                    "polling_calls": args.polls,
                    "total_agent_invocations": args.polls + 1,
                    "poll_snapshot_log_bytes": poll_log_bytes,
                    "returned_log_bytes": baseline_log_bytes,
                    "returned_protocol_bytes": sum(poll_response_bytes),
                    "final_log_bytes": final_log_bytes,
                    "duplicated_log_bytes": baseline_log_bytes - final_log_bytes,
                    "exit_code": baseline_exit_code,
                },
                "awaitless": {
                    "submit_calls": 1,
                    "wait_calls": 1,
                    "total_agent_invocations": 2,
                    "returned_log_bytes": awaitless_log_bytes,
                    "returned_wait_response_bytes": len(wait_stdout.encode()),
                    "state": awaitless_value["state"],
                    "exit_code": awaitless_value["exit_code"],
                    "duration_seconds": awaitless_value["duration_seconds"],
                    "wait_wall_seconds": round(wait_elapsed, 3),
                    "parsed_results": awaitless_value.get("parsed_results"),
                },
                "comparison": {
                    "explicit_polling_invocations_replaced": args.polls,
                    "agent_invocation_reduction_percent": round(call_reduction, 1),
                    "returned_log_reduction_percent": round(log_reduction, 1),
                    "returned_log_bytes_saved": baseline_log_bytes - awaitless_log_bytes,
                },
            }
            if baseline_exit_code != 0 or awaitless_value["state"] != "succeeded":
                raise RuntimeError(f"experiment did not succeed: {json.dumps(result)}")
            if awaitless_value.get("parsed_results", {}).get("correctness") is not True:
                raise RuntimeError("Awaitless did not return the declared JSON artifact")

            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(
                json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
            )
            note(f"result written to {args.output}")
            print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
        finally:
            if not args.keep_remote:
                cleanup_parts = [
                    f'rm -rf -- "{remote_baseline}"',
                    f'rm -f -- "$HOME/{artifact_name}"',
                ]
                if awaitless_job_id:
                    cleanup_parts.append(
                        f'rm -rf -- "$HOME/.awaitless/jobs/{awaitless_job_id}"'
                    )
                ssh_call(ssh, "\n".join(cleanup_parts), timeout=10, check=False)
            run(
                ["ssh", "-S", str(control_path), "-O", "exit", args.host],
                timeout=10,
                check=False,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
