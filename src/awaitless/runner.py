from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

from .db import Store
from .util import process_start_ticks, terminate_group, utc_now


def run(db_path: Path, job_id: str, spec_path: Path) -> int:
    store = Store(db_path)
    try:
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        try:
            spec_path.unlink()
        except OSError:
            pass
        stdout_path = Path(spec["stdout_path"])
        stderr_path = Path(spec["stderr_path"])
        stdout_path.parent.mkdir(parents=True, exist_ok=True)
        started_at = utc_now()
        with stdout_path.open("ab", buffering=0) as stdout, stderr_path.open("ab", buffering=0) as stderr:
            try:
                process = subprocess.Popen(
                    spec["command"],
                    cwd=spec.get("cwd") or None,
                    env={**os.environ, **spec.get("env", {})},
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    start_new_session=True,
                    close_fds=True,
                )
            except Exception as exc:
                store.update_if_active(
                    job_id,
                    state="failed",
                    started_at=started_at,
                    finished_at=utc_now(),
                    error=f"failed to start command: {exc}",
                )
                return 1
            store.update_if_active(
                job_id,
                state="running",
                started_at=started_at,
                pid=process.pid,
                pid_start_ticks=process_start_ticks(process.pid),
                pgid=os.getpgid(process.pid),
            )
            timeout = spec.get("timeout_seconds")
            try:
                exit_code = process.wait(timeout=timeout)
                current = store.get(job_id)
                if current and current["state"] == "cancelled":
                    return 0
                state = "succeeded" if exit_code == 0 else "failed"
                store.update_if_active(
                    job_id, state=state, exit_code=exit_code, finished_at=utc_now()
                )
                return 0
            except subprocess.TimeoutExpired:
                terminate_group(os.getpgid(process.pid), 2.0)
                process.wait()
                store.update_if_active(
                    job_id,
                    state="timed_out",
                    exit_code=process.returncode,
                    finished_at=utc_now(),
                    error=f"job exceeded {timeout:g} seconds",
                )
                return 0
    finally:
        store.close()


def main() -> int:
    if len(sys.argv) != 4:
        return 2
    return run(Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]))


if __name__ == "__main__":
    raise SystemExit(main())
