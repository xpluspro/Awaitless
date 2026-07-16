from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class CLITest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.env = os.environ.copy()
        self.env["AWAITLESS_DATA_DIR"] = self.temp.name
        self.env["PYTHONPATH"] = str(ROOT / "src")
        config = Path(self.temp.name) / "config.toml"
        config.write_text("[defaults]\npoll_interval = 0.05\n", encoding="utf-8")
        self.env["AWAITLESS_CONFIG"] = str(config)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def run_cli(self, *args: str, expected: int = 0) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, "-m", "awaitless", *args], cwd=ROOT, env=self.env,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15,
        )
        self.assertEqual(result.returncode, expected, result.stderr or result.stdout)
        return result

    def submit(self, *command: str, options: tuple[str, ...] = ()) -> str:
        result = self.run_cli("submit", "--json", *options, "--", *command)
        value = json.loads(result.stdout)
        self.assertIn(value["state"], {"running", "succeeded"})
        return value["job_id"]

    def test_success_survives_new_client_and_separates_logs(self) -> None:
        job = self.submit("bash", "-c", "echo hello; echo warning >&2; sleep .1")
        result = self.run_cli("wait", job, "--json")
        value = json.loads(result.stdout)
        self.assertEqual(value["state"], "succeeded")
        self.assertEqual(value["exit_code"], 0)
        self.assertEqual(value["stdout_tail"], "hello\n")
        self.assertEqual(value["stderr_tail"], "warning\n")
        self.assertGreater(value["duration_seconds"], 0)

    def test_failure_preserves_real_exit_code_and_cli_contract(self) -> None:
        job = self.submit("bash", "-c", "echo error >&2; exit 7")
        result = self.run_cli("wait", job, "--json", expected=3)
        value = json.loads(result.stdout)
        self.assertEqual((value["state"], value["exit_code"]), ("failed", 7))
        self.assertEqual(value["stderr_tail"], "error\n")

    def test_runtime_timeout_terminates_job(self) -> None:
        job = self.submit("bash", "-c", "sleep 10", options=("--timeout", "0.1s"))
        value = json.loads(self.run_cli("wait", job, "--json", expected=4).stdout)
        self.assertEqual(value["state"], "timed_out")

    def test_client_wait_timeout_does_not_cancel_job(self) -> None:
        job = self.submit("bash", "-c", "sleep 1.5")
        value = json.loads(self.run_cli("wait", job, "--timeout", "0.05s", "--json", expected=4).stdout)
        self.assertTrue(value["wait_timed_out"])
        self.assertIn(value["state"], {"running", "stalled"})
        final = json.loads(self.run_cli("wait", job, "--json").stdout)
        self.assertEqual(final["state"], "succeeded")

    def test_launch_failure_is_not_reported_as_running(self) -> None:
        result = self.run_cli("submit", "--json", "--", "/definitely/not/a/command", expected=2)
        self.assertIn("failed to start command", json.loads(result.stderr)["error"])

    def test_cancel_is_terminal(self) -> None:
        job = self.submit("bash", "-c", "sleep 10 & wait")
        value = json.loads(self.run_cli("cancel", job, "--grace-period", "0.1s", "--json").stdout)
        self.assertEqual(value["state"], "cancelled")
        time.sleep(0.1)
        status = json.loads(self.run_cli("status", job, "--json").stdout)
        self.assertEqual(status["state"], "cancelled")

    def test_logs_are_bounded_and_marked_truncated(self) -> None:
        job = self.submit(sys.executable, "-c", "print('x' * 10000)")
        self.run_cli("wait", job, "--json")
        value = json.loads(self.run_cli("logs", job, "--tail", "200", "--max-bytes", "200", "--json").stdout)
        self.assertTrue(value["truncated"])
        self.assertLessEqual(len(value["stdout_tail"].encode()), 100)

    def test_json_artifact_is_parsed(self) -> None:
        work = Path(self.temp.name) / "work"
        work.mkdir()
        command = "import json; open('result.json','w').write(json.dumps({'correctness': True, 'latency': 2.5}))"
        job = self.submit(
            sys.executable, "-c", command,
            options=("--cwd", str(work), "--artifact", "result.json"),
        )
        value = json.loads(self.run_cli("wait", job, "--json").stdout)
        self.assertEqual(value["parsed_results"], {"correctness": True, "latency": 2.5})
        self.assertTrue(value["artifacts"][0]["exists"])

    def test_ssh_wrapper_recovers_status_logs_and_artifact(self) -> None:
        fake_bin = Path(self.temp.name) / "bin"
        fake_bin.mkdir()
        fake_ssh = fake_bin / "ssh"
        fake_ssh.write_text(
            "#!/usr/bin/env bash\n"
            "if [ -n \"${AWAITLESS_FAKE_SSH_FAIL_ONCE:-}\" ] && [ -f \"$AWAITLESS_FAKE_SSH_FAIL_ONCE\" ]; then\n"
            "  rm -f \"$AWAITLESS_FAKE_SSH_FAIL_ONCE\"; exit 255\n"
            "fi\n"
            "exec bash -s\n",
            encoding="utf-8",
        )
        fake_ssh.chmod(0o755)
        remote_home = Path(self.temp.name) / "remote-home"
        remote_home.mkdir()
        self.env["PATH"] = str(fake_bin) + os.pathsep + self.env["PATH"]
        self.env["HOME"] = str(remote_home)
        fail_once = Path(self.temp.name) / "fail-ssh-once"
        self.env["AWAITLESS_FAKE_SSH_FAIL_ONCE"] = str(fail_once)
        work = Path(self.temp.name) / "remote-work"
        work.mkdir()
        command = "echo remote; printf '{\"ok\":true}' > result.json; sleep .1"
        job = self.submit(
            "bash", "-c", command,
            options=("--host", "fake", "--cwd", str(work), "--artifact", "result.json"),
        )
        # Simulate a transient disconnect during wait; the CLI must reconnect internally.
        fail_once.touch()
        value = json.loads(self.run_cli("wait", job, "--json").stdout)
        self.assertEqual(value["state"], "succeeded")
        self.assertEqual(value["stdout_tail"], "remote\n")
        self.assertEqual(value["parsed_results"], {"ok": True})
        self.assertGreater(value["stdout_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
