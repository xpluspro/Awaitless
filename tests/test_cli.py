from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from awaitless.db import Store


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

    def run_cli(
        self,
        *args: str,
        expected: int = 0,
        cwd: Path = ROOT,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, "-m", "awaitless", *args], cwd=cwd, env=env or self.env,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15,
        )
        self.assertEqual(result.returncode, expected, result.stderr or result.stdout)
        return result

    def submit(
        self, *command: str, options: tuple[str, ...] = (), cwd: Path = ROOT
    ) -> str:
        result = self.run_cli("submit", "--json", *options, "--", *command, cwd=cwd)
        value = json.loads(result.stdout)
        self.assertIn(value["state"], {"running", "succeeded"})
        return value["job_id"]

    def configure_fake_ssh(self) -> Path:
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
        return remote_home

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

    def test_local_queue_is_fifo_durable_and_cancellable(self) -> None:
        created = json.loads(
            self.run_cli(
                "queue", "create", "gpu0", "--concurrency", "1", "--json"
            ).stdout
        )
        self.assertTrue(created["created"])
        replay = json.loads(
            self.run_cli(
                "queue", "create", "gpu0", "--concurrency", "1", "--json"
            ).stdout
        )
        self.assertFalse(replay["created"])

        events = Path(self.temp.name) / "queue-events.txt"

        def queued_job(label: str, delay: float) -> dict[str, object]:
            source = (
                "import time; "
                f"p={str(events)!r}; "
                f"open(p,'a').write('start-{label}\\n'); "
                f"time.sleep({delay}); "
                f"open(p,'a').write('end-{label}\\n')"
            )
            return json.loads(
                self.run_cli(
                    "submit",
                    "--queue",
                    "gpu0",
                    "--json",
                    "--",
                    sys.executable,
                    "-c",
                    source,
                ).stdout
            )

        first = queued_job("a", 0.35)
        second = queued_job("b", 0.05)
        cancelled = queued_job("cancelled", 0.05)
        self.assertIn(first["state"], {"queued", "starting", "running"})
        self.assertEqual(second["state"], "queued")
        self.assertEqual(cancelled["state"], "queued")
        cancelled_result = json.loads(
            self.run_cli("cancel", str(cancelled["job_id"]), "--json").stdout
        )
        self.assertEqual(cancelled_result["state"], "cancelled")

        self.assertEqual(
            json.loads(
                self.run_cli("wait", str(first["job_id"]), "--json").stdout
            )["state"],
            "succeeded",
        )
        second_final = json.loads(
            self.run_cli("wait", str(second["job_id"]), "--json").stdout
        )
        self.assertEqual(second_final["state"], "succeeded")
        self.assertEqual(second_final["queue"], "gpu0")
        self.assertEqual(
            events.read_text(encoding="utf-8").splitlines(),
            ["start-a", "end-a", "start-b", "end-b"],
        )
        inspected = json.loads(
            self.run_cli("inspect", str(second["job_id"]), "--json").stdout
        )
        self.assertEqual(
            [event["state"] for event in inspected["events"]],
            ["queued", "starting", "running", "succeeded"],
        )
        queues = json.loads(self.run_cli("queue", "list", "--json").stdout)
        self.assertEqual(queues[0]["name"], "gpu0")
        self.assertEqual(queues[0]["concurrency"], 1)
        self.assertEqual(queues[0]["queued_jobs"], 0)
        self.assertEqual(queues[0]["active_jobs"], 0)
        self.assertEqual(queues[0]["total_jobs"], 3)

    def test_queue_validation_conflict_and_slurm_boundary(self) -> None:
        self.run_cli("queue", "create", "gpu", "--concurrency", "2", "--json")
        conflict = self.run_cli(
            "queue",
            "create",
            "gpu",
            "--concurrency",
            "1",
            "--json",
            expected=2,
        )
        self.assertIn("already exists", conflict.stderr)
        invalid = self.run_cli(
            "queue",
            "create",
            "bad/name",
            "--concurrency",
            "1",
            "--json",
            expected=2,
        )
        self.assertIn("queue name", invalid.stderr)
        slurm = self.run_cli(
            "submit",
            "--backend",
            "slurm",
            "--host",
            "cluster",
            "--queue",
            "gpu",
            "--json",
            "--",
            "true",
            expected=2,
        )
        self.assertIn("let Slurm schedule", slurm.stderr)

    def test_local_queue_honors_concurrency_greater_than_one(self) -> None:
        self.run_cli(
            "queue", "create", "workers", "--concurrency", "2", "--json"
        )
        state_path = Path(self.temp.name) / "concurrency-state.json"
        lock_path = Path(self.temp.name) / "concurrency.lock"
        jobs: list[str] = []
        for _ in range(4):
            source = f"""
import fcntl, json, time
state_path = {str(state_path)!r}
lock_path = {str(lock_path)!r}
with open(lock_path, 'a') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    try:
        state = json.load(open(state_path))
    except FileNotFoundError:
        state = {{'active': 0, 'maximum': 0}}
    state['active'] += 1
    state['maximum'] = max(state['maximum'], state['active'])
    open(state_path, 'w').write(json.dumps(state))
    fcntl.flock(lock, fcntl.LOCK_UN)
time.sleep(0.2)
with open(lock_path, 'a') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    state = json.load(open(state_path))
    state['active'] -= 1
    open(state_path, 'w').write(json.dumps(state))
"""
            submitted = json.loads(
                self.run_cli(
                    "submit",
                    "--queue",
                    "workers",
                    "--json",
                    "--",
                    sys.executable,
                    "-c",
                    source,
                ).stdout
            )
            jobs.append(submitted["job_id"])
        for job_id in jobs:
            result = json.loads(self.run_cli("wait", job_id, "--json").stdout)
            self.assertEqual(result["state"], "succeeded")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(state, {"active": 0, "maximum": 2})

    def test_queue_delay_does_not_consume_runtime_timeout(self) -> None:
        self.run_cli(
            "queue", "create", "serial", "--concurrency", "1", "--json"
        )
        blocker = json.loads(
            self.run_cli(
                "submit",
                "--queue",
                "serial",
                "--json",
                "--",
                "bash",
                "-c",
                "sleep .35",
            ).stdout
        )
        queued = json.loads(
            self.run_cli(
                "submit",
                "--queue",
                "serial",
                "--timeout",
                "0.2s",
                "--json",
                "--",
                "bash",
                "-c",
                "sleep .05",
            ).stdout
        )
        self.assertEqual(queued["state"], "queued")
        self.assertEqual(
            json.loads(
                self.run_cli("wait", blocker["job_id"], "--json").stdout
            )["state"],
            "succeeded",
        )
        final = json.loads(
            self.run_cli("wait", queued["job_id"], "--json").stdout
        )
        self.assertEqual(final["state"], "succeeded")
        self.assertGreater(final["queue_wait_seconds"], 0.2)
        self.assertLess(final["duration_seconds"], 0.2)

    def test_local_queue_recovers_missing_runners_without_duplicate_start(self) -> None:
        self.run_cli(
            "queue", "create", "recover", "--concurrency", "1", "--json"
        )
        first = json.loads(
            self.run_cli(
                "submit",
                "--queue",
                "recover",
                "--json",
                "--",
                "bash",
                "-c",
                "sleep 30",
            ).stdout
        )
        marker = Path(self.temp.name) / "recovered.txt"
        second = json.loads(
            self.run_cli(
                "submit",
                "--queue",
                "recover",
                "--json",
                "--",
                sys.executable,
                "-c",
                f"open({str(marker)!r}, 'a').write('once\\n')",
            ).stdout
        )

        database = Path(self.temp.name) / "awaitless.db"
        deadline = time.monotonic() + 3
        running: dict[str, object] | None = None
        waiting: dict[str, object] | None = None
        while time.monotonic() < deadline:
            store = Store(database)
            running = store.get(first["job_id"])
            waiting = store.get(second["job_id"])
            store.close()
            if running and running["state"] == "running" and waiting:
                break
            time.sleep(0.03)
        assert running and waiting
        self.assertEqual(running["state"], "running")
        self.assertEqual(waiting["state"], "queued")

        # Simulate a host-level runner crash: neither wrapper gets a chance to
        # record a transition. A later wait must release the stale slot, restore
        # the queued runner, and still execute the command exactly once.
        os.kill(int(running["runner_pid"]), signal.SIGKILL)
        os.killpg(int(running["pgid"]), signal.SIGKILL)
        os.kill(int(waiting["runner_pid"]), signal.SIGKILL)
        final = json.loads(
            self.run_cli("wait", second["job_id"], "--json").stdout
        )
        self.assertEqual(final["state"], "succeeded")
        self.assertEqual(marker.read_text(encoding="utf-8"), "once\n")
        stale = json.loads(
            self.run_cli("status", first["job_id"], "--json").stdout
        )
        self.assertEqual(stale["state"], "lost")

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

    def test_implicit_cwd_is_persisted_for_artifact_recovery(self) -> None:
        work = Path(self.temp.name) / "submit-work"
        elsewhere = Path(self.temp.name) / "wait-work"
        work.mkdir()
        elsewhere.mkdir()
        command = "from pathlib import Path; Path('result.json').write_text('{\"ok\": true}')"
        job = self.submit(
            sys.executable,
            "-c",
            command,
            options=("--artifact", "result.json"),
            cwd=work,
        )
        value = json.loads(self.run_cli("wait", job, "--json", cwd=elsewhere).stdout)
        self.assertEqual(value["parsed_results"], {"ok": True})
        inspected = json.loads(
            self.run_cli("inspect", job, "--json", cwd=elsewhere).stdout
        )
        self.assertEqual(inspected["cwd"], str(work.resolve()))

    def test_custom_log_directory_is_isolated_per_job(self) -> None:
        log_root = Path(self.temp.name) / "shared-log-root"
        first = self.submit("bash", "-c", "printf first", options=("--log-dir", str(log_root)))
        first_result = json.loads(self.run_cli("wait", first, "--json").stdout)
        second = self.submit("bash", "-c", "printf second", options=("--log-dir", str(log_root)))
        second_result = json.loads(self.run_cli("wait", second, "--json").stdout)
        self.assertEqual(first_result["stdout_tail"], "first")
        self.assertEqual(second_result["stdout_tail"], "second")
        first_inspect = json.loads(self.run_cli("inspect", first, "--json").stdout)
        second_inspect = json.loads(self.run_cli("inspect", second, "--json").stdout)
        self.assertNotEqual(first_inspect["stdout_path"], second_inspect["stdout_path"])
        self.assertEqual(Path(first_inspect["stdout_path"]).parent.name, first)
        self.assertEqual(Path(second_inspect["stdout_path"]).parent.name, second)

    def test_runtime_timeout_must_be_positive(self) -> None:
        result = self.run_cli(
            "submit", "--json", "--timeout", "0s", "--", "true", expected=2
        )
        self.assertIn("must be positive", json.loads(result.stderr)["error"])

    def test_idempotent_submit_returns_original_job_and_rejects_conflict(self) -> None:
        work = Path(self.temp.name) / "idempotent-work"
        work.mkdir()
        source = (
            "from pathlib import Path; import time; time.sleep(.2); "
            "p=Path('launches.txt'); p.write_text((p.read_text() if p.exists() else '')+'x')"
        )
        arguments = (
            "submit",
            "--json",
            "--client-request-id",
            "expensive:gpu:case-1",
            "--cwd",
            str(work),
            "--",
            sys.executable,
            "-c",
            source,
        )
        first = json.loads(self.run_cli(*arguments).stdout)
        replay = json.loads(self.run_cli(*arguments).stdout)
        self.assertEqual(replay["job_id"], first["job_id"])
        self.assertFalse(first["idempotent_replay"])
        self.assertTrue(replay["idempotent_replay"])
        self.run_cli("wait", first["job_id"], "--json")
        self.assertEqual((work / "launches.txt").read_text(encoding="utf-8"), "x")

        conflict = self.run_cli(
            "submit",
            "--json",
            "--client-request-id",
            "expensive:gpu:case-1",
            "--",
            "true",
            expected=2,
        )
        self.assertIn("different submission parameters", conflict.stderr)

    def test_demo_kills_waiter_and_recovers_from_new_client(self) -> None:
        result = self.run_cli(
            "demo", "--duration", "0.35s", "--interrupt-after", "0.05s", "--json"
        )
        value = json.loads(result.stdout)
        self.assertTrue(value["ok"])
        self.assertTrue(value["first_waiter_terminated"])
        self.assertTrue(value["recovered_by_new_client"])
        self.assertEqual(value["state"], "succeeded")
        self.assertTrue(value["parsed_results"]["demo_recovered"])

    def test_ssh_wrapper_recovers_status_logs_and_artifact(self) -> None:
        remote_home = self.configure_fake_ssh()
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
        self.assertGreater(value["duration_seconds"], 0)
        self.assertGreater(value["stdout_bytes"], 0)
        remote_job = remote_home / ".awaitless" / "jobs" / job
        self.assertTrue((remote_job / "pid_start_ticks").read_text().strip().isdigit())
        self.assertTrue((remote_job / "heartbeat").is_file())

    def test_ssh_cancel_records_durable_marker(self) -> None:
        remote_home = self.configure_fake_ssh()
        job = self.submit("bash", "-c", "echo ready; sleep 30", options=("--host", "fake"))
        value = json.loads(
            self.run_cli("cancel", job, "--grace-period", "0.1s", "--json").stdout
        )
        self.assertEqual(value["state"], "cancelled")
        remote_job = remote_home / ".awaitless" / "jobs" / job
        self.assertTrue((remote_job / "cancelled_at").is_file())
        status = json.loads(self.run_cli("status", job, "--json").stdout)
        self.assertEqual(status["state"], "cancelled")

    def test_ssh_queue_is_enforced_on_the_remote_host(self) -> None:
        remote_home = self.configure_fake_ssh()
        self.run_cli(
            "queue", "create", "gpu0", "--concurrency", "1", "--json"
        )
        second_client = self.env.copy()
        second_client["AWAITLESS_DATA_DIR"] = str(Path(self.temp.name) / "other-data")
        self.run_cli(
            "queue",
            "create",
            "gpu0",
            "--concurrency",
            "1",
            "--json",
            env=second_client,
        )
        events = Path(self.temp.name) / "remote-queue-events.txt"

        def submit_remote(
            label: str, delay: float, selected_env: dict[str, str]
        ) -> dict[str, object]:
            command = (
                f"printf 'start-{label}\\n' >> {events}; "
                f"sleep {delay}; printf 'end-{label}\\n' >> {events}"
            )
            return json.loads(
                self.run_cli(
                    "submit",
                    "--host",
                    "fake",
                    "--queue",
                    "gpu0",
                    "--json",
                    "--",
                    "bash",
                    "-c",
                    command,
                    env=selected_env,
                ).stdout
            )

        first = submit_remote("a", 0.3, self.env)
        second = submit_remote("b", 0.05, second_client)
        self.assertEqual(first["state"], "queued")
        self.assertEqual(second["state"], "queued")
        self.assertEqual(
            json.loads(
                self.run_cli("wait", str(first["job_id"]), "--json").stdout
            )["state"],
            "succeeded",
        )
        second_final = json.loads(
            self.run_cli(
                "wait", str(second["job_id"]), "--json", env=second_client
            ).stdout
        )
        self.assertEqual(second_final["state"], "succeeded")
        self.assertEqual(
            events.read_text(encoding="utf-8").splitlines(),
            ["start-a", "end-a", "start-b", "end-b"],
        )
        queue_dir = remote_home / ".awaitless" / "queues" / "gpu0"
        self.assertEqual(
            (queue_dir / "concurrency").read_text(encoding="utf-8").strip(), "1"
        )
        self.assertEqual(list((queue_dir / "pending").iterdir()), [])


if __name__ == "__main__":
    unittest.main()
