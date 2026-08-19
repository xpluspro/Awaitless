from __future__ import annotations

import argparse
import json
import os
import secrets
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from . import __version__
from .backends.ssh import SSHError
from .config import adaptive_queue, load_settings
from .constants import EXIT_CODES
from .service import AwaitlessError, PreflightError, Service
from .util import new_job_id, parse_duration, utc_now


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="awaitless", description="Adaptive durable execution for coding agents"
    )
    root.add_argument("--config", help="configuration TOML path")
    root.add_argument("--json", action="store_true", dest="global_json", help="emit JSON")
    root.add_argument("--verbose", action="store_true")
    root.add_argument("--quiet", action="store_true")
    root.add_argument("--version", action="version", version=f"awaitless {__version__}")
    commands = root.add_subparsers(dest="action", required=True)

    run = commands.add_parser(
        "run", help="run inline when quick and detach automatically when longer"
    )
    run.add_argument("--backend", choices=["local", "ssh", "slurm"])
    run.add_argument("--host")
    run.add_argument("--cwd")
    run.add_argument("--env", action="append", default=[], metavar="NAME=VALUE")
    run.add_argument("--device", help="Ascend device ID; enables device visibility and exclusive scheduling")
    run.add_argument("--device-mode", choices=["physical", "native"], default="physical")
    run.add_argument("--timeout")
    run.add_argument("--stall-timeout")
    run.add_argument("--inline-timeout")
    run.add_argument("--log-dir")
    run.add_argument("--capture-log", action="append", default=[], help="capture a command-created log file")
    run.add_argument("--resource", action="append", default=[], metavar="NAME=VALUE")
    run.add_argument("--script-file", help="read a Bash script locally and transfer it as one command argument")
    run.add_argument("--artifact", action="append", default=[])
    run.add_argument("--result-file", action="append", default=[], dest="artifact")
    run.add_argument(
        "--slurm-option",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="per-job Slurm option (for example partition=gpu or gres=gpu:1)",
    )
    run.add_argument("--name")
    run.add_argument("--queue", help="override the target's default queue")
    run.add_argument(
        "--client-request-id",
        help="idempotency key; a retry with identical parameters returns the original job",
    )
    run.add_argument("--json", action="store_true")
    run.add_argument("command", nargs=argparse.REMAINDER)

    submit = commands.add_parser("submit", help="submit a durable job")
    submit.add_argument("--backend", choices=["local", "ssh", "slurm"])
    submit.add_argument("--host")
    submit.add_argument("--cwd")
    submit.add_argument("--env", action="append", default=[], metavar="NAME=VALUE")
    submit.add_argument("--device", help="Ascend device ID; enables device visibility and exclusive scheduling")
    submit.add_argument("--device-mode", choices=["physical", "native"], default="physical")
    submit.add_argument("--timeout")
    submit.add_argument("--stall-timeout")
    submit.add_argument("--log-dir")
    submit.add_argument("--capture-log", action="append", default=[], help="capture a command-created log file")
    submit.add_argument("--resource", action="append", default=[], metavar="NAME=VALUE")
    submit.add_argument("--script-file", help="read a Bash script locally and transfer it as one command argument")
    submit.add_argument("--artifact", action="append", default=[])
    submit.add_argument("--result-file", action="append", default=[], dest="artifact")
    submit.add_argument(
        "--slurm-option",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="per-job Slurm option (for example partition=gpu or gres=gpu:1)",
    )
    submit.add_argument("--name")
    submit.add_argument("--queue", help="named concurrency queue")
    submit.add_argument(
        "--client-request-id",
        help="idempotency key; a retry with identical parameters returns the original job",
    )
    submit.add_argument("--json", action="store_true")
    submit.add_argument("command", nargs=argparse.REMAINDER)

    submit_group = commands.add_parser("submit-group", help="submit one job per device as an experiment group")
    submit_group.add_argument("--backend", choices=["local", "ssh", "slurm"])
    submit_group.add_argument("--host")
    submit_group.add_argument("--cwd")
    submit_group.add_argument("--env", action="append", default=[], metavar="NAME=VALUE")
    submit_group.add_argument("--devices", help="comma-separated device IDs")
    submit_group.add_argument("--run-device", action="append", nargs=2, default=[], metavar=("DEVICE", "COMMAND"), help="device and its shell-like run command")
    submit_group.add_argument("--device-mode", choices=["physical", "native"], default="physical")
    submit_group.add_argument("--build", help="build command to run once before device fan-out")
    submit_group.add_argument("--timeout")
    submit_group.add_argument("--stall-timeout")
    submit_group.add_argument("--artifact", action="append", default=[])
    submit_group.add_argument("--name")
    submit_group.add_argument("--group", dest="group_id", default=None)
    submit_group.add_argument("--queue")
    submit_group.add_argument("--json", action="store_true")
    submit_group.add_argument("command", nargs=argparse.REMAINDER)

    wait_group = commands.add_parser("wait-group", help="wait for all jobs in an experiment group")
    wait_group.add_argument("group_id")
    wait_group.add_argument("--timeout")
    wait_group.add_argument("--json", action="store_true")

    wait = commands.add_parser("wait", help="block until a job reaches a terminal state")
    wait.add_argument("job_id")
    wait.add_argument("--timeout")
    wait.add_argument("--progress-interval", help="emit structured heartbeat updates to stderr")
    wait.add_argument("--json", action="store_true")

    completions = commands.add_parser(
        "completions", help="wait for durable completions across multiple jobs"
    )
    completions.add_argument("job_ids", nargs="*")
    completions.add_argument("--group", dest="group_id")
    completions.add_argument("--after", dest="after_cursor")
    completions.add_argument("--timeout")
    completions.add_argument("--limit", type=int, default=50)
    completions.add_argument("--drain", action="store_true", help="wait for all selected jobs and manage the cursor internally")
    completions.add_argument("--json", action="store_true")

    status = commands.add_parser("status", help="show current job state")
    status.add_argument("job_id")
    status.add_argument("--json", action="store_true")

    logs = commands.add_parser("logs", help="read bounded job logs")
    logs.add_argument("job_id")
    logs.add_argument("--tail", type=int, default=None)
    logs.add_argument("--max-bytes", type=int, default=None)
    logs.add_argument("--follow", action="store_true")
    logs.add_argument("--json", action="store_true")

    cancel = commands.add_parser("cancel", help="terminate a managed process group")
    cancel.add_argument("job_id")
    cancel.add_argument("--grace-period", default="5s")
    cancel.add_argument("--json", action="store_true")

    listing = commands.add_parser("list", help="list jobs")
    listing.add_argument("--state")
    listing.add_argument("--host")
    listing.add_argument("--queue")
    listing.add_argument("--json", action="store_true")

    queues = commands.add_parser("queue", help="manage named concurrency queues")
    queue_commands = queues.add_subparsers(dest="queue_action", required=True)
    queue_create = queue_commands.add_parser("create", help="create a queue")
    queue_create.add_argument("name")
    queue_create.add_argument("--concurrency", required=True, type=int)
    queue_create.add_argument("--json", action="store_true")
    queue_list = queue_commands.add_parser("list", help="list queues")
    queue_list.add_argument("--json", action="store_true")

    inspect = commands.add_parser("inspect", help="show job metadata and state history")
    inspect.add_argument("job_id")
    inspect.add_argument("--json", action="store_true")

    doctor = commands.add_parser("doctor", help="check local and SSH prerequisites")
    doctor.add_argument("--host")
    doctor.add_argument("--cwd")
    doctor.add_argument("--devices", help="comma-separated device IDs")
    doctor.add_argument("--json", action="store_true")
    recover = commands.add_parser("recover", help="recover the most recently detached job")
    recover.add_argument("--last", action="store_true", required=True)
    recover.add_argument("--json", action="store_true")

    demo = commands.add_parser(
        "demo",
        help="submit two jobs, kill one completion waiter, and recover their results",
    )
    demo.add_argument("--duration", default="1.2s")
    demo.add_argument("--interrupt-after", default="0.15s")
    demo.add_argument("--json", action="store_true")
    return root


def _env(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, sep, item = value.partition("=")
        if not sep:
            raise AwaitlessError(f"invalid --env value {value!r}; expected NAME=VALUE")
        result[key] = item
    return result


def _slurm_options(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key or not item:
            raise AwaitlessError(
                f"invalid --slurm-option value {value!r}; expected NAME=VALUE"
            )
        result[key.replace("-", "_")] = item
    return result


def _print(value: Any, json_mode: bool, *, quiet: bool = False) -> None:
    if quiet:
        return
    if json_mode:
        print(json.dumps(value, ensure_ascii=False, separators=(",", ":")))
    elif isinstance(value, dict) and "job_id" in value:
        print(value["job_id"] if len(value) <= 3 else _human_job(value))
    elif isinstance(value, list):
        for item in value:
            print(_human_job(item) if "job_id" in item else _human_queue(item))
    else:
        print(value)


def _human_job(job: dict[str, Any]) -> str:
    fields = [job["job_id"], job.get("state", ""), job.get("backend", "")]
    if job.get("name"):
        fields.append(job["name"])
    if job.get("queue"):
        fields.append(f"queue={job['queue']}")
    if job.get("exit_code") is not None:
        fields.append(f"exit={job['exit_code']}")
    return "\t".join(str(field) for field in fields if field != "")


def _human_queue(queue: dict[str, Any]) -> str:
    return "\t".join(
        (
            str(queue["name"]),
            f"concurrency={queue['concurrency']}",
            f"queued={queue.get('queued_jobs', 0)}",
            f"active={queue.get('active_jobs', 0)}",
        )
    )


def _human_completion(completion: dict[str, Any]) -> str:
    result = completion.get("result") or {}
    fields = [
        completion["completion_id"],
        completion["job_id"],
        completion["state"],
    ]
    if result.get("exit_code") is not None:
        fields.append(f"exit={result['exit_code']}")
    return "\t".join(str(field) for field in fields)


def _demo(
    service: Service,
    *,
    config_path: str | None,
    duration: float,
    interrupt_after: float,
) -> dict[str, Any]:
    if duration <= 0:
        raise AwaitlessError("--duration must be positive")
    if interrupt_after <= 0 or interrupt_after >= duration:
        raise AwaitlessError("--interrupt-after must be positive and shorter than --duration")

    marker = secrets.token_hex(8)
    work = service.settings.data_dir / "demo-work" / marker
    work.mkdir(mode=0o700, parents=True, exist_ok=False)
    submissions: list[dict[str, Any]] = []
    expected_by_job: dict[str, dict[str, Any]] = {}
    for index, selected_duration in enumerate((duration, duration * 1.25), start=1):
        job_work = work / f"job-{index}"
        job_work.mkdir(mode=0o700)
        expected = {
            "demo_recovered": True,
            "marker": marker,
            "job": index,
        }
        payload = json.dumps(expected, separators=(",", ":"))
        source = (
            "import time; from pathlib import Path; "
            f"time.sleep({selected_duration!r}); "
            f"Path('result.json').write_text({payload!r}, encoding='utf-8'); "
            f"print('AWAITLESS_DEMO_RECOVERED={marker}:{index}')"
        )
        submitted = service.submit(
            job_id=new_job_id(),
            command=[sys.executable, "-c", source],
            backend="local",
            host=None,
            cwd=str(job_work),
            env={},
            timeout_seconds=selected_duration + 10,
            stall_timeout_seconds=None,
            name=f"awaitless-recovery-demo-{index}",
            artifacts=["result.json"],
            client_request_id=f"demo:{marker}:{index}",
        )
        submissions.append(submitted)
        expected_by_job[submitted["job_id"]] = expected

    base_command = [sys.executable, "-m", "awaitless"]
    if config_path:
        base_command.extend(["--config", config_path])
    base_command.extend(
        ["completions", *(item["job_id"] for item in submissions)]
    )
    command = [*base_command, "--json"]
    waiter = subprocess.Popen(
        command,
        cwd=Path.cwd(),
        env=os.environ.copy(),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    time.sleep(interrupt_after)
    first_waiter_terminated = waiter.poll() is None
    if first_waiter_terminated:
        waiter.terminate()
    try:
        waiter.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        waiter.kill()
        waiter.communicate(timeout=5)
    if not first_waiter_terminated:
        raise AwaitlessError(
            "demo work finished before the first client could be interrupted"
        )

    recovered: list[dict[str, Any]] = []
    cursor: str | None = None
    active = [item["job_id"] for item in submissions]
    has_more = False
    deadline = time.monotonic() + duration * 1.25 + 15
    while active or has_more:
        if time.monotonic() >= deadline:
            raise AwaitlessError("demo timed out while recovering completions")
        resume_command = list(base_command)
        if cursor is not None:
            resume_command.extend(["--after", cursor])
        resume_command.append("--json")
        resumed = subprocess.run(
            resume_command,
            cwd=Path.cwd(),
            env=os.environ.copy(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=max(1.0, deadline - time.monotonic()),
        )
        if resumed.returncode != 0:
            detail = (resumed.stderr or resumed.stdout).strip()[-2000:]
            raise AwaitlessError(
                f"new demo client failed to recover completions: {detail}"
            )
        try:
            batch = json.loads(resumed.stdout)
        except json.JSONDecodeError as exc:
            raise AwaitlessError("new demo client returned invalid JSON") from exc
        recovered.extend(batch.get("completions", []))
        cursor = batch.get("next_cursor")
        active = batch.get("active_job_ids", [])
        has_more = bool(batch.get("has_more"))

    recovered_by_job = {item["job_id"]: item for item in recovered}
    if set(recovered_by_job) != set(expected_by_job):
        raise AwaitlessError("new demo client did not recover every completion")
    for job_id, expected in expected_by_job.items():
        completion = recovered_by_job[job_id]
        if (
            completion.get("state") != "succeeded"
            or completion.get("result", {}).get("parsed_results") != expected
        ):
            raise AwaitlessError("new demo client recovered an invalid result")
    first = recovered_by_job[submissions[0]["job_id"]]
    return {
        "ok": True,
        "job_id": submissions[0]["job_id"],
        "job_ids": [item["job_id"] for item in submissions],
        "client_request_id": submissions[0]["client_request_id"],
        "client_request_ids": [item["client_request_id"] for item in submissions],
        "first_waiter_terminated": True,
        "recovered_by_new_client": True,
        "completion_count": len(recovered),
        "next_cursor": cursor,
        "completions": recovered,
        "state": first["state"],
        "exit_code": first["result"]["exit_code"],
        "stdout_tail": first["result"]["stdout_tail"],
        "parsed_results": first["result"]["parsed_results"],
        "work_dir": str(work),
    }


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    json_mode = bool(getattr(args, "json", False) or args.global_json)
    settings = load_settings(args.config)
    service = Service(settings)
    try:
        if args.action == "submit-group":
            command = args.command[1:] if args.command[:1] == ["--"] else args.command
            host = args.host or settings.default_host
            backend = args.backend or (str(settings.hosts.get(host, {}).get("backend", "ssh")) if host else settings.default_backend)
            group_id = args.group_id or new_job_id().lower()
            devices = [item.strip() for item in (args.devices or "").split(",") if item.strip()]
            run_commands = {device: command for device, command in args.run_device}
            devices.extend(device for device in run_commands if device not in devices)
            result = service.submit_group(
                group_id=group_id,
                devices=devices,
                command=command, backend=backend, host=host, cwd=args.cwd, env=_env(args.env),
                timeout_seconds=parse_duration(args.timeout), stall_timeout_seconds=parse_duration(args.stall_timeout),
                name=args.name, artifacts=args.artifact, queue_name=args.queue,
                device_mode=args.device_mode, build=args.build, run_commands=run_commands,
            )
            _print(result, json_mode, quiet=args.quiet)
            return 0
        if args.action == "wait-group":
            result = service.wait_group(args.group_id, parse_duration(args.timeout))
            _print(result, json_mode, quiet=args.quiet)
            return 4 if result["wait_timed_out"] else 0
        if args.action in {"run", "submit"}:
            command = args.command[1:] if args.command[:1] == ["--"] else args.command
            if args.script_file:
                if command:
                    raise AwaitlessError("--script-file cannot be combined with a command after --")
                script_path = Path(args.script_file).expanduser()
                try:
                    command = ["bash", "-c", script_path.read_text(encoding="utf-8")]
                except OSError as exc:
                    raise AwaitlessError(f"cannot read --script-file: {exc}") from exc
            host = args.host or (
                None if args.backend == "local" else settings.default_host
            )
            backend = args.backend or (
                str(settings.hosts.get(host, {}).get("backend", "ssh"))
                if host
                else settings.default_backend
            )
            queue = args.queue
            if args.action == "run":
                queue = adaptive_queue(
                    settings,
                    backend=backend,
                    host=host,
                    explicit_queue=queue,
                )
            result = service.submit(
                job_id=new_job_id(), command=command, backend=backend, host=host, cwd=args.cwd,
                env=_env(args.env), timeout_seconds=parse_duration(args.timeout),
                stall_timeout_seconds=parse_duration(args.stall_timeout), name=args.name,
                artifacts=args.artifact, log_dir=args.log_dir,
                backend_options=_slurm_options(args.slurm_option),
                client_request_id=args.client_request_id,
                queue_name=queue,
                device=args.device,
                device_mode=args.device_mode,
                capture_logs=args.capture_log,
                resources=_slurm_options(args.resource),
            )
            if args.action == "run":
                # Persist identity before entering the inline waiter so a killed
                # client can still recover the managed workload.
                service.record_recent_job({
                    "job_id": result["job_id"], "state": result["state"],
                    "delivery": "pending", "recorded_at": utc_now(),
                })
                inline_timeout = parse_duration(args.inline_timeout)
                if inline_timeout is None:
                    inline_timeout = settings.adaptive_inline_timeout_seconds
                output = service.adaptive_wait(
                    result["job_id"],
                    inline_timeout,
                    detach_immediately=result["state"] == "queued",
                )
                _print(output, json_mode, quiet=args.quiet)
                return (
                    0
                    if output["detached"]
                    else EXIT_CODES.get(output["state"], 1)
                )
            output = {
                key: result[key]
                for key in (
                    "job_id",
                    "state",
                    "backend",
                    "client_request_id",
                    "queue",
                    "idempotent_replay",
                )
                if result.get(key) is not None
            }
            _print(output, json_mode, quiet=args.quiet)
            return 0
        if args.action == "recover":
            _print(service.recover_last(), True, quiet=args.quiet)
            return 0
        if args.action == "wait":
            progress = parse_duration(args.progress_interval)
            deadline = parse_duration(args.timeout)
            if progress is not None and progress <= 0:
                raise AwaitlessError("--progress-interval must be positive")
            started = time.monotonic()
            while progress is not None:
                remaining = None if deadline is None else max(0.0, deadline - (time.monotonic() - started))
                result, tick = service.wait(args.job_id, min(progress, remaining) if remaining is not None else progress)
                if not tick:
                    wait_timed_out = False
                    break
                print(json.dumps({"event": "heartbeat", **result}, ensure_ascii=False, separators=(",", ":")), file=sys.stderr)
                if remaining is not None and remaining <= progress:
                    wait_timed_out = True
                    break
            else:
                result, wait_timed_out = service.wait(args.job_id, deadline)
            _print(result, json_mode, quiet=args.quiet)
            return 4 if wait_timed_out else EXIT_CODES.get(result["state"], 1)
        if args.action == "completions":
            job_ids = args.job_ids
            if args.group_id:
                if job_ids:
                    raise AwaitlessError("job IDs and --group cannot be combined")
                job_ids = service.group(args.group_id)["job_ids"]
            result = service.completions(
                job_ids,
                after_cursor=args.after_cursor,
                wait_timeout=parse_duration(args.timeout),
                limit=args.limit,
            )
            if args.drain:
                collected = list(result["completions"])
                cursor = result["next_cursor"]
                while result["active_job_ids"] or result["has_more"]:
                    result = service.completions(job_ids, after_cursor=cursor, wait_timeout=parse_duration(args.timeout), limit=args.limit)
                    collected.extend(result["completions"])
                    cursor = result["next_cursor"]
                    if result["wait_timed_out"]:
                        break
                result = {**result, "completions": collected, "next_cursor": cursor}
            if json_mode:
                _print(result, True, quiet=args.quiet)
            elif not args.quiet:
                for completion in result["completions"]:
                    print(_human_completion(completion))
                if result["completions"]:
                    print(f"next_cursor={result['next_cursor']}")
                if result["has_more"]:
                    print("has_more=true")
                if result["wait_timed_out"]:
                    active = ",".join(result["active_job_ids"])
                    print(
                        f"awaitless: completion wait timed out; active={active}",
                        file=sys.stderr,
                    )
            return 4 if result["wait_timed_out"] else 0
        if args.action == "status":
            result = service.status(args.job_id)
            _print(result, json_mode, quiet=args.quiet)
            return 0
        if args.action == "logs":
            if args.follow and json_mode:
                raise AwaitlessError("--follow cannot be combined with --json")
            tail = args.tail if args.tail is not None else settings.log_tail_lines
            max_bytes = args.max_bytes if args.max_bytes is not None else settings.max_return_bytes
            if tail < 0 or max_bytes <= 0:
                raise AwaitlessError("--tail must be non-negative and --max-bytes must be positive")
            if args.follow:
                previous = None
                while True:
                    result = service.logs(args.job_id, tail, max_bytes)
                    rendered = result["stdout_tail"] + result["stderr_tail"]
                    if rendered != previous:
                        print(rendered, end="" if rendered.endswith("\n") else "\n")
                        previous = rendered
                    if service.status(args.job_id)["state"] in EXIT_CODES:
                        return 0
                    time.sleep(settings.poll_interval)
            result = service.logs(args.job_id, tail, max_bytes)
            if json_mode:
                _print(result, True, quiet=args.quiet)
            elif not args.quiet:
                if result["truncated"]:
                    print("[awaitless: log output truncated]", file=sys.stderr)
                print(result["stdout_tail"], end="")
                print(result["stderr_tail"], end="", file=sys.stderr)
            return 0
        if args.action == "cancel":
            result = service.cancel(args.job_id, parse_duration(args.grace_period) or 0)
            _print(result, json_mode, quiet=args.quiet)
            return 0
        if args.action == "list":
            _print(
                service.list(args.state, args.host, args.queue),
                json_mode,
                quiet=args.quiet,
            )
            return 0
        if args.action == "queue":
            if args.queue_action == "create":
                result = service.create_queue(args.name, args.concurrency)
                if json_mode:
                    _print(result, True, quiet=args.quiet)
                elif not args.quiet:
                    print(_human_queue(result))
                return 0
            if args.queue_action == "list":
                _print(service.list_queues(), json_mode, quiet=args.quiet)
                return 0
        if args.action == "inspect":
            _print(service.inspect(args.job_id), True if json_mode else True, quiet=args.quiet)
            return 0
        if args.action == "doctor":
            if args.host:
                devices = [item.strip() for item in (args.devices or "").split(",") if item.strip()]
                result = service.doctor_remote(args.host, cwd=args.cwd, devices=devices or None)
                _print(result, json_mode, quiet=args.quiet)
                return 0 if result["ok"] else 1
            configured_backends = {
                str(value.get("backend", "ssh"))
                for value in settings.hosts.values()
                if isinstance(value, dict)
            } | {settings.default_backend}
            needs_ssh = bool(configured_backends & {"ssh", "slurm"})
            needs_sftp = "slurm" in configured_backends
            result = {
                "ok": (
                    os.name == "posix"
                    and shutil.which("bash") is not None
                    and (not needs_ssh or shutil.which("ssh") is not None)
                    and (not needs_sftp or shutil.which("sftp") is not None)
                ),
                "python": sys.version.split()[0], "bash": shutil.which("bash"), "ssh": shutil.which("ssh"),
                "sftp": shutil.which("sftp"),
                "configured_backends": sorted(configured_backends),
                "data_dir": str(settings.data_dir), "database": str(settings.db_path),
            }
            _print(result, json_mode, quiet=args.quiet)
            return 0 if result["ok"] else 1
        if args.action == "demo":
            result = _demo(
                service,
                config_path=args.config,
                duration=parse_duration(args.duration) or 0,
                interrupt_after=parse_duration(args.interrupt_after) or 0,
            )
            if json_mode:
                _print(result, True, quiet=args.quiet)
            elif not args.quiet:
                print(f"submitted {', '.join(result['job_ids'])}")
                print(
                    "terminated the first completion waiter; managed jobs kept running"
                )
                print(
                    "new clients recovered "
                    f"completions={result['completion_count']} "
                    f"cursor={result['next_cursor']}"
                )
            return 0
        return 2
    except PreflightError as exc:
        if json_mode:
            print(json.dumps(exc.result, ensure_ascii=False, separators=(",", ":")), file=sys.stderr)
        else:
            _error(f"preflight failed: {exc}", False)
        return 2
    except SSHError as exc:
        _error(str(exc), json_mode)
        return 7
    except (AwaitlessError, ValueError) as exc:
        _error(str(exc), json_mode)
        return 2
    except KeyboardInterrupt:
        _error("interrupted; the managed job was not cancelled", json_mode)
        return 130
    except Exception as exc:
        _error(str(exc), json_mode)
        return 1
    finally:
        service.close()


def _error(message: str, json_mode: bool) -> None:
    if json_mode:
        print(json.dumps({"error": message}, ensure_ascii=False, separators=(",", ":")), file=sys.stderr)
    else:
        print(f"awaitless: {message}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
