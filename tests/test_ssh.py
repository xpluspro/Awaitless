from __future__ import annotations

import json
import os
import shlex
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from awaitless.backends.ssh import SSHBackend
from awaitless.config import Settings
from awaitless.db import Store


class SSHRefreshRaceTest(unittest.TestCase):
    def test_fresh_heartbeat_survives_cross_session_pid_isolation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            remote_home = root / "remote-home"
            remote_job = remote_home / ".awaitless" / "jobs" / "job_heartbeat"
            remote_job.mkdir(parents=True)
            (remote_job / "stdout.log").touch()
            (remote_job / "stderr.log").touch()
            (remote_job / "heartbeat").touch()
            (remote_job / "started_at").write_text(
                "2026-08-10T00:00:00.000000000Z\n", encoding="utf-8"
            )
            (remote_job / "pid").write_text("99999999\n", encoding="utf-8")
            (remote_job / "pid_start_ticks").write_text("1\n", encoding="utf-8")
            (remote_job / "pgid").write_text("99999999\n", encoding="utf-8")

            fake_bin = root / "bin"
            fake_bin.mkdir()
            fake_ssh = fake_bin / "ssh"
            fake_ssh.write_text("#!/usr/bin/env bash\nexec bash -s\n", encoding="utf-8")
            fake_ssh.chmod(0o755)

            store = Store(root / "local" / "awaitless.db")
            store.create(
                {
                    "job_id": "job_heartbeat",
                    "backend": "ssh",
                    "host": "fake",
                    "command_json": json.dumps(["sleep", "30"]),
                    "cwd": None,
                    "env_json": "{}",
                    "state": "running",
                    "started_at": "2026-08-10T00:00:00.000000000Z",
                    "pid": 99999999,
                    "pid_start_ticks": 1,
                    "pgid": 99999999,
                    "backend_id": "~/.awaitless/jobs/job_heartbeat",
                    "job_dir": str(root / "local" / "job_heartbeat"),
                    "stdout_path": str(root / "local" / "stdout.log"),
                    "stderr_path": str(root / "local" / "stderr.log"),
                    "artifacts_json": "[]",
                }
            )
            backend = SSHBackend(
                store, Settings(data_dir=root / "local", hosts={"fake": {}})
            )
            environment = {
                **os.environ,
                "HOME": str(remote_home),
                "PATH": str(fake_bin) + os.pathsep + os.environ["PATH"],
            }
            try:
                with patch.dict(os.environ, environment, clear=True):
                    refreshed = backend.refresh(store.get("job_heartbeat") or {})
                self.assertEqual(refreshed["state"], "running")
                self.assertIsNone(refreshed["error"])
                self.assertIsNotNone(refreshed["last_heartbeat_at"])
                self.assertIsNone(refreshed["last_output_at"])
            finally:
                store.close()

    def test_completion_marker_grace_avoids_false_lost_state(self) -> None:
        if not Path("/proc/self/stat").is_file():
            self.skipTest("requires Linux /proc race fixture")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            remote_home = root / "remote-home"
            remote_job = remote_home / ".awaitless" / "jobs" / "job_race"
            remote_job.mkdir(parents=True)
            (remote_job / "stdout.log").touch()
            (remote_job / "stderr.log").touch()

            fake_bin = root / "bin"
            fake_bin.mkdir()
            fake_ssh = fake_bin / "ssh"
            fake_ssh.write_text("#!/usr/bin/env bash\nexec bash -s\n", encoding="utf-8")
            fake_ssh.chmod(0o755)

            exit_path = shlex.quote(str(remote_job / "exit_code"))
            finish_path = shlex.quote(str(remote_job / "finished_at"))
            wrapper = f"""set -eu
echo $$ > {shlex.quote(str(remote_job / 'pid'))}
awk '{{print $22}}' /proc/$$/stat > {shlex.quote(str(remote_job / 'pid_start_ticks'))}
ps -o pgid= -p $$ | tr -d ' ' > {shlex.quote(str(remote_job / 'pgid'))}
date -u +%Y-%m-%dT%H:%M:%S.%NZ > {shlex.quote(str(remote_job / 'started_at'))}
nohup bash -c 'sleep 0.2; printf 0 > {exit_path}; date -u +%Y-%m-%dT%H:%M:%S.%NZ > {finish_path}' >/dev/null 2>&1 &
"""
            process = subprocess.Popen(
                ["bash", "-c", wrapper],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            process.wait(timeout=2)

            store = Store(root / "local" / "awaitless.db")
            store.create(
                {
                    "job_id": "job_race",
                    "backend": "ssh",
                    "host": "fake",
                    "command_json": json.dumps(["true"]),
                    "cwd": None,
                    "env_json": "{}",
                    "state": "running",
                    "started_at": (remote_job / "started_at").read_text().strip(),
                    "pid": int((remote_job / "pid").read_text()),
                    "pid_start_ticks": int((remote_job / "pid_start_ticks").read_text()),
                    "pgid": int((remote_job / "pgid").read_text()),
                    "backend_id": "~/.awaitless/jobs/job_race",
                    "job_dir": str(root / "local" / "job_race"),
                    "stdout_path": str(root / "local" / "stdout.log"),
                    "stderr_path": str(root / "local" / "stderr.log"),
                    "artifacts_json": "[]",
                }
            )
            settings = Settings(data_dir=root / "local", hosts={"fake": {}})
            backend = SSHBackend(store, settings)
            ssh_outputs: list[str] = []
            invoke = backend._invoke

            def recording_invoke(*args: object, **kwargs: object) -> str:
                output = invoke(*args, **kwargs)  # type: ignore[arg-type]
                ssh_outputs.append(output)
                return output

            environment = {
                **os.environ,
                "HOME": str(remote_home),
                "PATH": str(fake_bin) + os.pathsep + os.environ["PATH"],
            }
            try:
                with (
                    patch.dict(os.environ, environment, clear=True),
                    patch.object(backend, "_invoke", side_effect=recording_invoke),
                ):
                    refreshed = backend.refresh(store.get("job_race") or {})
                self.assertEqual(refreshed["state"], "succeeded", ssh_outputs)
                self.assertEqual(refreshed["exit_code"], 0)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
