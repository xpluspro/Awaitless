#!/usr/bin/env python3
"""Demonstrate MCP submit, client exit, reconnect, and Slurm result recovery."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def call_tool(
    parameters: StdioServerParameters,
    name: str,
    arguments: dict[str, Any],
    *,
    read_timeout: float,
) -> Any:
    async with stdio_client(parameters) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                name, arguments, read_timeout_seconds=read_timeout
            )
            if result.is_error:
                detail = " ".join(
                    getattr(item, "text", repr(item)) for item in result.content
                )
                raise RuntimeError(detail)
            return result.structured_content


def emit(phase: str, value: Any) -> None:
    print(
        json.dumps(
            {"phase": phase, "value": value},
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        flush=True,
    )


async def run(args: argparse.Namespace) -> int:
    parameters = StdioServerParameters(
        command=args.server_command,
        args=["--config", args.config],
        env=os.environ.copy(),
    )
    artifact = f"awaitless-mcp-slurm-{int(time.time())}.json"
    command_source = (
        "printf 'compute_host=%s slurm_job_id=%s\\n' "
        '"$HOSTNAME" "$SLURM_JOB_ID"; '
        f"sleep {args.job_seconds:g}; "
        f"printf '{{\"ok\":true,\"compute_host\":\"%s\","
        f"\"slurm_job_id\":\"%s\"}}\\n' \"$HOSTNAME\" \"$SLURM_JOB_ID\" "
        f"> {artifact}"
    )
    options: dict[str, str] = {}
    for item in args.slurm_option:
        key, separator, value = item.partition("=")
        if not separator or not key or not value:
            raise ValueError(f"invalid --slurm-option {item!r}; expected NAME=VALUE")
        options[key.replace("-", "_")] = value

    submitted = await call_tool(
        parameters,
        "submit_job",
        {
            "command": ["bash", "-c", command_source],
            "backend": "slurm",
            "host": args.host,
            "cwd": args.cwd,
            "name": "awaitless-mcp-slurm-demo",
            "artifacts": [artifact],
            "slurm_options": options,
        },
        read_timeout=args.control_timeout,
    )
    emit("client_1_submit", submitted)
    emit("client_1_closed", {"job_id": submitted["job_id"]})

    completed = await call_tool(
        parameters,
        "wait_for_job",
        {"job_id": submitted["job_id"], "timeout_seconds": args.wait_timeout},
        read_timeout=args.wait_timeout + args.control_timeout,
    )
    emit("client_2_wait", completed)
    if completed.get("state") != "succeeded" or completed.get("exit_code") != 0:
        return 1
    if not isinstance(completed.get("parsed_results"), dict):
        return 1
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--config", required=True, help="Awaitless TOML config")
    result.add_argument("--host", required=True, help="configured Slurm host alias")
    result.add_argument("--cwd", help="remote working directory")
    result.add_argument(
        "--server-command", default="awaitless-mcp", help="MCP server executable"
    )
    result.add_argument("--slurm-option", action="append", default=[])
    result.add_argument("--job-seconds", type=float, default=6)
    result.add_argument("--control-timeout", type=float, default=120)
    result.add_argument("--wait-timeout", type=float, default=600)
    return result


def main() -> int:
    return asyncio.run(run(parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
