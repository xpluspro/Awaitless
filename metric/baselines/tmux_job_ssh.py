#!/usr/bin/env python3
"""Thin SSH transport for the consumer-owned tmux job wrapper."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path


LOCAL_WRAPPER = Path(__file__).with_name("tmux_job.py")


def ssh(host: str, command: list[str], *, stdin: bytes | None = None) -> int:
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", host, shlex.join(command)],
        input=stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)
    return result.returncode


def install(host: str, remote_script: str) -> int:
    parent = str(Path(remote_script).parent)
    command = [
        "bash", "-c",
        "set -eu; mkdir -p \"$1\"; temporary=\"$2.tmp.$$\"; "
        "cat >\"$temporary\"; chmod 700 \"$temporary\"; mv \"$temporary\" \"$2\"",
        "tmux-job-install", parent, remote_script,
    ]
    return ssh(host, command, stdin=LOCAL_WRAPPER.read_bytes())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--remote-script", default=".local/lib/tmux_job.py")
    parser.add_argument("--root", default=".local/share/tmux-job")
    parser.add_argument("--socket", default="tmux-job")
    parser.add_argument("action", choices=[
        "install", "submit", "wait", "status", "cancel", "queue-create", "completions"
    ])
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.action == "install":
        return install(args.host, args.remote_script)
    remote = [
        "python3", args.remote_script, "--root", args.root,
        "--socket", args.socket, args.action, *args.arguments,
    ]
    return ssh(args.host, remote)


if __name__ == "__main__":
    raise SystemExit(main())
