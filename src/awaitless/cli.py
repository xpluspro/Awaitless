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
from .config import load_settings
from .constants import EXIT_CODES
from .service import AwaitlessError, Service
from .util import new_job_id, parse_duration


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="awaitless", description="Durable long-running jobs for AI coding agents")
    root.add_argument("--config", help="configuration TOML path")
    root.add_argument("--json", action="store_true", dest="global_json", help="emit JSON")
    root.add_argument("--verbose", action="store_true")
    root.add_argument("--quiet", action="store_true")
    root.add_argument("--version", action="version", version=f"awaitless {__version__}")
    commands = root.add_subparsers(dest="action", required=True)

    submit = commands.add_parser("submit", help="submit a durable job")
    submit.add_argument("--backend", choices=["local", "ssh", "slurm"])
    submit.add_argument("--host")
    submit.add_argument("--cwd")
    submit.add_argument("--env", action="append", default=[], metavar="NAME=VALUE")
    submit.add_argument("--timeout")
    submit.add_argument("--stall-timeout")
    submit.add_argument("--log-dir")
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

    wait = commands.add_parser("wait", help="block until a job reaches a terminal state")
    wait.add_argument("job_id")
    wait.add_argument("--timeout")
    wait.add_argument("--json", action="store_true")

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
    doctor.add_argument("--json", action="store_true")

    demo = commands.add_parser(
        "demo", help="submit locally, kill one waiter, and recover from a new client"
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
    client_request_id = f"demo:{marker}"
    work = service.settings.data_dir / "demo-work" / marker
    work.mkdir(mode=0o700, parents=True, exist_ok=False)
    payload = json.dumps({"demo_recovered": True, "marker": marker})
    source = (
        "import json,time; from pathlib import Path; "
        f"time.sleep({duration!r}); "
        f"Path('result.json').write_text({payload!r}, encoding='utf-8'); "
        f"print('AWAITLESS_DEMO_RECOVERED={marker}')"
    )
    submitted = service.submit(
        job_id=new_job_id(),
        command=[sys.executable, "-c", source],
        backend="local",
        host=None,
        cwd=str(work),
        env={},
        timeout_seconds=duration + 10,
        stall_timeout_seconds=None,
        name="awaitless-recovery-demo",
        artifacts=["result.json"],
        client_request_id=client_request_id,
    )

    command = [sys.executable, "-m", "awaitless"]
    if config_path:
        command.extend(["--config", config_path])
    command.extend(["wait", submitted["job_id"], "--json"])
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
        raise AwaitlessError("demo task finished before the first client could be interrupted")

    resumed = subprocess.run(
        command,
        cwd=Path.cwd(),
        env=os.environ.copy(),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
        timeout=duration + 15,
    )
    if resumed.returncode != 0:
        detail = (resumed.stderr or resumed.stdout).strip()[-2000:]
        raise AwaitlessError(f"new demo client failed to recover the job: {detail}")
    try:
        final = json.loads(resumed.stdout)
    except json.JSONDecodeError as exc:
        raise AwaitlessError("new demo client returned invalid JSON") from exc
    expected = {"demo_recovered": True, "marker": marker}
    if final.get("state") != "succeeded" or final.get("parsed_results") != expected:
        raise AwaitlessError("new demo client did not recover the expected result")
    return {
        "ok": True,
        "job_id": submitted["job_id"],
        "client_request_id": client_request_id,
        "first_waiter_terminated": True,
        "recovered_by_new_client": True,
        "state": final["state"],
        "exit_code": final["exit_code"],
        "stdout_tail": final["stdout_tail"],
        "parsed_results": final["parsed_results"],
        "work_dir": str(work),
    }


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    json_mode = bool(getattr(args, "json", False) or args.global_json)
    settings = load_settings(args.config)
    service = Service(settings)
    try:
        if args.action == "submit":
            command = args.command[1:] if args.command[:1] == ["--"] else args.command
            host = args.host or (
                None if args.backend == "local" else settings.default_host
            )
            backend = args.backend or (
                str(settings.hosts.get(host, {}).get("backend", "ssh"))
                if host
                else settings.default_backend
            )
            result = service.submit(
                job_id=new_job_id(), command=command, backend=backend, host=host, cwd=args.cwd,
                env=_env(args.env), timeout_seconds=parse_duration(args.timeout),
                stall_timeout_seconds=parse_duration(args.stall_timeout), name=args.name,
                artifacts=args.artifact, log_dir=args.log_dir,
                backend_options=_slurm_options(args.slurm_option),
                client_request_id=args.client_request_id,
                queue_name=args.queue,
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
        if args.action == "wait":
            result, wait_timed_out = service.wait(args.job_id, parse_duration(args.timeout))
            _print(result, json_mode, quiet=args.quiet)
            return 4 if wait_timed_out else EXIT_CODES.get(result["state"], 1)
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
                print(f"submitted {result['job_id']}")
                print("terminated the first waiting client; managed job kept running")
                print(
                    "new client recovered "
                    f"state={result['state']} exit={result['exit_code']} "
                    f"artifact={json.dumps(result['parsed_results'], ensure_ascii=False)}"
                )
            return 0
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
