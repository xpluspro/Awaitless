from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from typing import Any

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
    root.add_argument("--version", action="version", version="awaitless 0.1.0")
    commands = root.add_subparsers(dest="action", required=True)

    submit = commands.add_parser("submit", help="submit a durable job")
    submit.add_argument("--backend", choices=["local", "ssh"])
    submit.add_argument("--host")
    submit.add_argument("--cwd")
    submit.add_argument("--env", action="append", default=[], metavar="NAME=VALUE")
    submit.add_argument("--timeout")
    submit.add_argument("--stall-timeout")
    submit.add_argument("--log-dir")
    submit.add_argument("--artifact", action="append", default=[])
    submit.add_argument("--result-file", action="append", default=[], dest="artifact")
    submit.add_argument("--name")
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
    listing.add_argument("--json", action="store_true")

    inspect = commands.add_parser("inspect", help="show job metadata and state history")
    inspect.add_argument("job_id")
    inspect.add_argument("--json", action="store_true")

    doctor = commands.add_parser("doctor", help="check local and SSH prerequisites")
    doctor.add_argument("--json", action="store_true")
    return root


def _env(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, sep, item = value.partition("=")
        if not sep:
            raise AwaitlessError(f"invalid --env value {value!r}; expected NAME=VALUE")
        result[key] = item
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
            print(_human_job(item))
    else:
        print(value)


def _human_job(job: dict[str, Any]) -> str:
    fields = [job["job_id"], job.get("state", ""), job.get("backend", "")]
    if job.get("name"):
        fields.append(job["name"])
    if job.get("exit_code") is not None:
        fields.append(f"exit={job['exit_code']}")
    return "\t".join(str(field) for field in fields if field != "")


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    json_mode = bool(getattr(args, "json", False) or args.global_json)
    settings = load_settings(args.config)
    service = Service(settings)
    try:
        if args.action == "submit":
            command = args.command[1:] if args.command[:1] == ["--"] else args.command
            backend = args.backend or ("ssh" if args.host else settings.default_backend)
            result = service.submit(
                job_id=new_job_id(), command=command, backend=backend, host=args.host, cwd=args.cwd,
                env=_env(args.env), timeout_seconds=parse_duration(args.timeout),
                stall_timeout_seconds=parse_duration(args.stall_timeout), name=args.name,
                artifacts=args.artifact, log_dir=args.log_dir,
            )
            output = {key: result[key] for key in ("job_id", "state", "backend")}
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
            _print(service.list(args.state, args.host), json_mode, quiet=args.quiet)
            return 0
        if args.action == "inspect":
            _print(service.inspect(args.job_id), True if json_mode else True, quiet=args.quiet)
            return 0
        if args.action == "doctor":
            result = {
                "ok": os.name == "posix" and shutil.which("bash") is not None,
                "python": sys.version.split()[0], "bash": shutil.which("bash"), "ssh": shutil.which("ssh"),
                "data_dir": str(settings.data_dir), "database": str(settings.db_path),
            }
            _print(result, json_mode, quiet=args.quiet)
            return 0 if result["ok"] else 1
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
