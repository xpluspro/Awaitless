from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from ..constants import TERMINAL_STATES
from ..db import Store
from ..util import atomic_json, process_matches, process_start_ticks, terminate_group, utc_now


class LocalBackend:
    name = "local"

    def __init__(self, store: Store):
        self.store = store

    def submit(self, job: dict[str, Any], spec_path: Path) -> dict[str, Any]:
        runner = subprocess.Popen(
            [sys.executable, "-m", "awaitless.runner", str(self.store.path), job["job_id"], str(spec_path)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        self.store.update(
            job["job_id"],
            runner_pid=runner.pid,
            runner_start_ticks=process_start_ticks(runner.pid),
        )
        deadline = time.monotonic() + 4.0
        while time.monotonic() < deadline:
            current = self.store.get(job["job_id"])
            assert current
            if current["state"] in {"running", "succeeded", "failed", "timed_out"}:
                return current
            if current["error"]:
                return current
            if runner.poll() is not None:
                break
            time.sleep(0.03)
        current = self.store.get(job["job_id"])
        assert current
        if current["state"] == "starting":
            current = self.store.update_if_active(
                job["job_id"], state="failed", finished_at=utc_now(), error="local runner failed to start"
            )
        return current

    def refresh(self, job: dict[str, Any]) -> dict[str, Any]:
        if job["state"] in TERMINAL_STATES:
            return job
        if process_matches(job.get("pid"), job.get("pid_start_ticks")):
            return job
        if process_matches(job.get("runner_pid"), job.get("runner_start_ticks")):
            return job
        # Give the runner a short window to atomically commit its final state.
        if job.get("started_at") and (time.time() - Path(job["job_dir"]).stat().st_mtime) < 1:
            return job
        return self.store.update_if_active(
            job["job_id"], state="lost", finished_at=utc_now(), error="managed process disappeared before recording an exit status"
        )

    def cancel(self, job: dict[str, Any], grace_seconds: float) -> dict[str, Any]:
        if job["state"] in TERMINAL_STATES:
            return job
        job = self.store.update(job["job_id"], state="cancelled", finished_at=utc_now())
        if job.get("pgid"):
            terminate_group(int(job["pgid"]), grace_seconds)
        return job
