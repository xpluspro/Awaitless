from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from awaitless.backends.slurm import RemoteFile, SlurmBackend, _exit_code, _sftp_quote
from awaitless.config import Settings
from awaitless.service import Service


class SlurmBackendTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.settings = Settings(
            data_dir=self.root / "data",
            poll_interval=0.01,
            hosts={
                "cluster": {
                    "backend": "slurm",
                    "slurm": {"account": "research"},
                }
            },
        )
        self.settings.jobs_dir.mkdir(parents=True)
        self.service = Service(self.settings)
        self.backend = self.service.backends["slurm"]
        assert isinstance(self.backend, SlurmBackend)

    def tearDown(self) -> None:
        self.service.close()
        self.temp.cleanup()

    def submit(self, job_id: str = "job_SLURM") -> dict[str, object]:
        with (
            patch.object(
                self.backend,
                "_ensure_job_directory",
                return_value=f"/remote/.awaitless/{job_id}",
            ),
            patch.object(self.backend, "_invoke", return_value="12345;cluster\n") as invoke,
        ):
            result = self.service.submit(
                job_id=job_id,
                command=["python", "-c", "print('scheduled')"],
                backend="slurm",
                host="cluster",
                cwd="/remote/work",
                env={"EXPERIMENT": "one"},
                timeout_seconds=60,
                stall_timeout_seconds=None,
                name="agent-job",
                artifacts=["result.json"],
                backend_options={"partition": "debug", "cpus_per_task": 2},
            )
        scheduler_command = invoke.call_args.args[1]
        script = invoke.call_args.kwargs["stdin"]
        self.assertEqual(scheduler_command[0], "sbatch")
        self.assertIn("--account=research", scheduler_command)
        self.assertIn("--partition=debug", scheduler_command)
        self.assertIn("--cpus-per-task=2", scheduler_command)
        self.assertIn("--chdir=/remote/work", scheduler_command)
        self.assertIn("export EXPERIMENT=one", script)
        self.assertIn("timeout --signal=TERM --kill-after=2s 60s", script)
        return result

    def test_submit_persists_scheduler_id_and_recovers_terminal_state(self) -> None:
        submitted = self.submit()
        self.assertEqual(submitted["state"], "pending")
        self.assertEqual(submitted["backend_id"], "12345")

        with patch.object(self.backend, "_invoke", return_value="RUNNING\n") as invoke:
            running = self.service.status("job_SLURM")
        self.assertEqual(running["state"], "running")
        self.assertEqual(invoke.call_args.args[1][0], "squeue")

        with patch.object(
            self.backend,
            "_invoke",
            side_effect=["", "12345|FAILED|7:0|9\n"],
        ) as invoke:
            finished = self.service.status("job_SLURM")
        self.assertEqual((finished["state"], finished["exit_code"]), ("failed", 7))
        self.assertEqual(finished["duration_seconds"], 9.0)
        self.assertEqual([call.args[1][0] for call in invoke.call_args_list], ["squeue", "sacct"])

        stored = self.service.inspect("job_SLURM")
        self.assertEqual(stored["backend_id"], "12345")
        self.assertEqual(stored["artifact_paths"], ["result.json"])

    def test_timeout_and_signal_exit_codes_are_mapped(self) -> None:
        self.submit("job_TIMEOUT")
        job = self.service.require("job_TIMEOUT")
        timed_out = self.backend._mapped_update(job, "FAILED", "124:0", "60")
        self.assertEqual((timed_out["state"], timed_out["exit_code"]), ("timed_out", 124))
        self.assertEqual(
            self.service.summary(timed_out)["duration_seconds"], 60.0
        )
        self.assertEqual(_exit_code("0:15"), 143)

    def test_cancel_uses_only_scancel(self) -> None:
        self.submit("job_CANCEL")
        with patch.object(self.backend, "_invoke", return_value="") as invoke:
            cancelled = self.service.cancel("job_CANCEL", 0)
        self.assertEqual(cancelled["state"], "cancelled")
        self.assertEqual(invoke.call_args.args[1], ["scancel", "12345"])

    def test_logs_and_json_artifacts_are_bounded(self) -> None:
        self.submit("job_FILES")
        files = {
            "stdout.log": RemoteFile(
                exists=True,
                size=100,
                data=b"old\nlast\n",
                modified_at="2026-08-10T00:00:00Z",
            ),
            "stderr.log": RemoteFile(exists=True, size=0),
            "result.json": RemoteFile(
                exists=True,
                size=11,
                data=b'{"ok":true}',
                modified_at="2026-08-10T00:00:01Z",
            ),
        }

        def read_remote(_host: str, path: str, _limit: int) -> RemoteFile:
            return files[Path(path).name]

        with patch.object(self.backend, "_read_remote_file", side_effect=read_remote):
            logs = self.service.logs("job_FILES", tail=1, max_bytes=20)
        self.assertEqual(logs["stdout_tail"], "last\n")
        self.assertTrue(logs["truncated"])
        self.assertEqual(logs["stdout_bytes"], 100)

        with (
            patch.object(self.backend, "_remote_stat", return_value=(True, 11)),
            patch.object(self.backend, "_read_remote_file", side_effect=read_remote),
        ):
            artifacts = self.service.artifacts(self.service.require("job_FILES"))
        self.assertEqual(artifacts[0]["content"], {"ok": True})
        self.assertEqual(artifacts[0]["size_bytes"], 11)

    def test_scheduler_command_allowlist_is_enforced_before_ssh(self) -> None:
        with self.assertRaisesRegex(ValueError, "allowlisted"):
            self.backend._invoke("cluster", ["bash", "-c", "hostname"])

        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="PENDING\n", stderr=""
        )
        with patch("awaitless.backends.slurm.subprocess.run", return_value=completed) as run:
            self.backend._invoke("cluster", ["squeue", "--jobs=12345"])
        remote_command = run.call_args.args[0][-1]
        self.assertEqual(remote_command, "squeue --jobs=12345")

    def test_sftp_paths_cannot_expand_globs_or_inject_commands(self) -> None:
        self.assertEqual(
            _sftp_quote('results/[latest]*?."json'),
            '"results/\\[latest\\]\\*\\?.\\"json"',
        )
        with self.assertRaisesRegex(ValueError, "newlines"):
            _sftp_quote("result.json\nrm other.json")


if __name__ == "__main__":
    unittest.main()
