from __future__ import annotations

import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from ..constants import TERMINAL_STATES
from ..db import Store
from ..util import process_matches, process_start_ticks, terminate_group, utc_now


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
        try:
            if job.get("queue_name"):
                current, registered = self.store.register_queue_runner(
                    job["job_id"],
                    expected_pid=job.get("runner_pid"),
                    expected_start_ticks=job.get("runner_start_ticks"),
                    runner_pid=runner.pid,
                    runner_start_ticks=process_start_ticks(runner.pid),
                )
                if not registered:
                    runner.terminate()
                return current
            self.store.update(
                job["job_id"],
                runner_pid=runner.pid,
                runner_start_ticks=process_start_ticks(runner.pid),
            )
            deadline = time.monotonic() + 4.0
            while time.monotonic() < deadline:
                current = self.store.get(job["job_id"])
                assert current
                if current["state"] in {
                    "running",
                    "succeeded",
                    "failed",
                    "timed_out",
                }:
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
                    job["job_id"],
                    state="failed",
                    finished_at=utc_now(),
                    error="local runner failed to start",
                )
            return current
        finally:
            # The runner is deliberately detached from the MCP/CLI client. Keep
            # its Popen object alive in a daemon reaper so a long-lived server
            # neither emits ResourceWarning nor accumulates zombie runners.
            if runner.poll() is None:
                threading.Thread(
                    target=runner.wait,
                    name=f"awaitless-reap-{runner.pid}",
                    daemon=True,
                ).start()

    def refresh(self, job: dict[str, Any]) -> dict[str, Any]:
        if job["state"] in TERMINAL_STATES:
            return job
        if process_matches(job.get("pid"), job.get("pid_start_ticks")):
            return job
        if process_matches(job.get("runner_pid"), job.get("runner_start_ticks")):
            return job
        if job.get("queue_name") and job["state"] in {"queued", "starting"}:
            spec_path = Path(job["job_dir"]) / "run-spec.json"
            if spec_path.is_file():
                if job["state"] == "starting" and not job.get("pid"):
                    job, recovered = self.store.requeue_unstarted_runner(
                        job["job_id"],
                        expected_pid=job.get("runner_pid"),
                        expected_start_ticks=job.get("runner_start_ticks"),
                    )
                    if not recovered:
                        return job
                if job["state"] == "queued":
                    return self.submit(job, spec_path)
        # Give the runner a short window to atomically commit its final state.
        if job.get("started_at") and (time.time() - Path(job["job_dir"]).stat().st_mtime) < 1:
            return job
        return self.store.update_if_active(
            job["job_id"], state="lost", finished_at=utc_now(), error="managed process disappeared before recording an exit status"
        )

    def cancel(self, job: dict[str, Any], grace_seconds: float) -> dict[str, Any]:
        if job["state"] in TERMINAL_STATES:
            return job
        job = self.store.update_if_active(
            job["job_id"], state="cancelled", finished_at=utc_now()
        )
        if job["state"] != "cancelled":
            return job
        if job.get("queue_name") and not job.get("pid"):
            try:
                (Path(job["job_dir"]) / "run-spec.json").unlink()
            except OSError:
                pass
        if job.get("pgid") and process_matches(job.get("pid"), job.get("pid_start_ticks")):
            terminate_group(int(job["pgid"]), grace_seconds)
        return job
