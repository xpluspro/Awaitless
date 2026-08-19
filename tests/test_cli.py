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

    def test_adaptive_run_returns_quick_result_inline(self) -> None:
        value = json.loads(
            self.run_cli(
                "run",
                "--inline-timeout",
                "1s",
                "--json",
                "--",
                sys.executable,
                "-c",
                "print('adaptive-inline')",
            ).stdout
        )
        self.assertEqual(value["state"], "succeeded")
        self.assertEqual(value["delivery"], "inline")
        self.assertFalse(value["detached"])
        self.assertIsNone(value["detach_reason"])
        self.assertEqual(value["stdout_tail"], "adaptive-inline\n")

    def test_adaptive_run_detaches_without_cancelling_long_job(self) -> None:
        value = json.loads(
            self.run_cli(
                "run",
                "--inline-timeout",
                "0.05s",
                "--json",
                "--",
                sys.executable,
                "-c",
                "import time; print('started', flush=True); time.sleep(.3)",
            ).stdout
        )
        self.assertEqual(value["delivery"], "detached")
        self.assertTrue(value["detached"])
        self.assertEqual(value["detach_reason"], "inline_timeout")
        self.assertIn(value["state"], {"starting", "running", "stalled"})
        final = json.loads(
            self.run_cli("wait", value["job_id"], "--json").stdout
        )
        self.assertEqual(final["state"], "succeeded")
        self.assertEqual(final["stdout_tail"], "started\n")

    def test_adaptive_run_uses_operator_default_queue(self) -> None:
        self.run_cli("queue", "create", "gpu0", "--concurrency", "1", "--json")
        Path(self.env["AWAITLESS_CONFIG"]).write_text(
            "[defaults]\n"
            "poll_interval = 0.05\n"
            "queue = \"gpu0\"\n"
            "adaptive_inline_timeout_seconds = 1\n",
            encoding="utf-8",
        )
        value = json.loads(
            self.run_cli(
                "run",
                "--json",
                "--",
                sys.executable,
                "-c",
                "print('queued-default')",
            ).stdout
        )
        self.assertEqual(value["queue"], "gpu0")
        self.assertEqual(value["delivery"], "detached")
        self.assertTrue(value["detached"])
        self.assertEqual(value["detach_reason"], "queued")
        self.assertEqual(value["inline_timeout_seconds"], 1)
        final = json.loads(
            self.run_cli("wait", value["job_id"], "--json").stdout
        )
        self.assertEqual(final["state"], "succeeded")
        self.assertEqual(final["stdout_tail"], "queued-default\n")

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

    def test_exit_21_has_structured_device_diagnostic(self) -> None:
        job = self.submit("bash", "-c", "echo 'npu-smi is unavailable' >&2; exit 21")
        value = json.loads(self.run_cli("wait", job, "--json", expected=3).stdout)
        self.assertEqual(value["stage"], "device_unavailable")
        self.assertEqual(value["reason"], "device_unavailable")
        self.assertTrue(value["retryable"])

    def test_device_sets_ascend_environment_and_exclusive_queue(self) -> None:
        value = json.loads(
            self.run_cli(
                "run", "--device", "4", "--inline-timeout", "2s", "--json", "--",
                sys.executable, "-c",
                "import os; print(os.environ['ASCEND_DEVICE_ID'], os.environ['ASCEND_RT_VISIBLE_DEVICES'])",
            ).stdout
        )
        self.assertEqual(value["queue"], "device-4")
        final = json.loads(self.run_cli("wait", value["job_id"], "--json").stdout)
        self.assertEqual(final["stdout_tail"], "0 4\n")
        self.assertEqual(final["device"], "4")
        self.assertEqual(final["device_mode"], "physical")

    def test_submit_and_wait_group_aggregate_devices(self) -> None:
        submitted = json.loads(
            self.run_cli(
                "submit-group", "--group", "trial", "--devices", "4,5", "--json", "--",
                sys.executable, "-c", "import os; print(os.environ['ASCEND_DEVICE_ID'])",
            ).stdout
        )
        self.assertEqual(len(submitted["job_ids"]), 2)
        feed = json.loads(self.run_cli("completions", "--group", "trial", "--json").stdout)
        self.assertGreaterEqual(len(feed["completions"]), 1)
        result = json.loads(self.run_cli("wait-group", "trial", "--json").stdout)
        self.assertEqual([row["device"] for row in result["rows"]], ["4", "5"])
        self.assertTrue(all(row["state"] == "succeeded" for row in result["rows"]))

    def test_artifact_glob_collects_each_match(self) -> None:
        work = Path(self.temp.name) / "glob-work"
        work.mkdir()
        job = self.submit(
            "bash", "-c", "mkdir -p artifacts; echo '{\"n\":1}' > artifacts/a.json; echo '{\"n\":2}' > artifacts/b.json",
            options=("--cwd", str(work), "--artifact", "artifacts/*.json"),
        )
        value = json.loads(self.run_cli("wait", job, "--json").stdout)
        self.assertEqual(sorted(item["content"]["n"] for item in value["artifacts"]), [1, 2])

    def test_remote_doctor_reports_missing_cwd(self) -> None:
        self.configure_fake_ssh()
        value = json.loads(
            self.run_cli("doctor", "--host", "fake", "--cwd", "/definitely/missing", "--json", expected=1).stdout
        )
        self.assertFalse(value["ok"])
        self.assertEqual(value["stage"], "preflight_failed")

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

    def test_completion_feed_recovers_replays_paginates_and_times_out(self) -> None:
        work = Path(self.temp.name) / "completion-work"
        work.mkdir()

        def completion_job(label: str, delay: float) -> str:
            source = (
                "from pathlib import Path; import json,time; "
                f"time.sleep({delay}); "
                f"Path('{label}.json').write_text(json.dumps({{'label': {label!r}}})); "
                f"print({label!r})"
            )
            return self.submit(
                sys.executable,
                "-c",
                source,
                options=("--cwd", str(work), "--artifact", f"{label}.json"),
            )

        first_job = completion_job("first", 0.12)
        second_job = completion_job("second", 0.22)
        first_batch = json.loads(
            self.run_cli(
                "completions",
                first_job,
                second_job,
                "--limit",
                "1",
                "--timeout",
                "2s",
                "--json",
            ).stdout
        )
        self.assertEqual(len(first_batch["completions"]), 1)
        first_completion = first_batch["completions"][0]
        self.assertIn(first_completion["job_id"], {first_job, second_job})
        self.assertEqual(first_completion["state"], "succeeded")
        self.assertEqual(
            first_completion["result"]["parsed_results"]["label"],
            first_completion["result"]["stdout_tail"].strip(),
        )

        replay = json.loads(
            self.run_cli(
                "completions",
                first_job,
                second_job,
                "--limit",
                "1",
                "--json",
            ).stdout
        )
        self.assertEqual(
            replay["completions"][0]["completion_id"],
            first_completion["completion_id"],
        )

        second_batch = json.loads(
            self.run_cli(
                "completions",
                first_job,
                second_job,
                "--after",
                first_batch["next_cursor"],
                "--timeout",
                "2s",
                "--json",
            ).stdout
        )
        self.assertEqual(len(second_batch["completions"]), 1)
        self.assertNotEqual(
            second_batch["completions"][0]["completion_id"],
            first_completion["completion_id"],
        )
        self.assertEqual(second_batch["active_job_ids"], [])
        paginated = json.loads(
            self.run_cli(
                "completions",
                first_job,
                second_job,
                "--limit",
                "1",
                "--json",
            ).stdout
        )
        self.assertTrue(paginated["has_more"])

        drained = json.loads(
            self.run_cli(
                "completions",
                first_job,
                second_job,
                "--after",
                second_batch["next_cursor"],
                "--timeout",
                "0",
                "--json",
            ).stdout
        )
        self.assertEqual(drained["completions"], [])
        self.assertFalse(drained["wait_timed_out"])
        self.assertFalse(drained["has_more"])

        active_job = self.submit("bash", "-c", "sleep 10")
        timed_out = json.loads(
            self.run_cli(
                "completions",
                active_job,
                "--timeout",
                "0",
                "--json",
                expected=4,
            ).stdout
        )
        self.assertTrue(timed_out["wait_timed_out"])
        self.assertEqual(timed_out["active_job_ids"], [active_job])
        self.run_cli("cancel", active_job, "--grace-period", "0", "--json")

        malformed = self.run_cli(
            "completions",
            first_job,
            "--after",
            "not-a-cursor",
            "--json",
            expected=2,
        )
        self.assertIn("completion cursor", malformed.stderr)
        out_of_range = self.run_cli(
            "completions",
            first_job,
            "--after",
            "cmp_9223372036854775808",
            "--json",
            expected=2,
        )
        self.assertIn("out of range", out_of_range.stderr)
        unknown = self.run_cli(
            "completions", "job_unknown", "--json", expected=2
        )
        self.assertIn("unknown job ID", unknown.stderr)
        invalid_limit = self.run_cli(
            "completions", first_job, "--limit", "0", "--json", expected=2
        )
        self.assertIn("limit must be between", invalid_limit.stderr)
        future = self.run_cli(
            "completions",
            first_job,
            "--after",
            "cmp_9999999999999999",
            "--json",
            expected=2,
        )
        self.assertIn("ahead of this Awaitless store", future.stderr)

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
        release_first = Path(self.temp.name) / "release-first-job"

        def queued_job(
            label: str, delay: float, *, hold_for_release: bool = False
        ) -> dict[str, object]:
            lines = [
                "import time",
                "from pathlib import Path",
                f"p={str(events)!r}",
                f"open(p,'a').write('start-{label}\\n')",
            ]
            if hold_for_release:
                lines.extend(
                    (
                        "deadline = time.monotonic() + 10",
                        f"while not Path({str(release_first)!r}).exists():",
                        "    if time.monotonic() >= deadline:",
                        "        raise TimeoutError('queue test release was not created')",
                        "    time.sleep(0.02)",
                    )
                )
            lines.extend(
                (
                    f"time.sleep({delay})",
                    f"open(p,'a').write('end-{label}\\n')",
                )
            )
            source = "\n".join(lines)
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

        # Hold the first slot until the cancellation request has been issued.
        # This keeps the third command queued even when CLI startup is slow.
        first = queued_job("a", 0.05, hold_for_release=True)
        second = queued_job("b", 0.05)
        cancelled = queued_job("cancelled", 0.05)
        self.assertIn(first["state"], {"queued", "starting", "running"})
        self.assertEqual(second["state"], "queued")
        self.assertEqual(cancelled["state"], "queued")
        try:
            cancelled_result = json.loads(
                self.run_cli("cancel", str(cancelled["job_id"]), "--json").stdout
            )
        finally:
            release_first.touch()
        self.assertEqual(cancelled_result["state"], "cancelled")
        cancelled_completion = json.loads(
            self.run_cli(
                "completions",
                str(cancelled["job_id"]),
                "--timeout",
                "0",
                "--json",
            ).stdout
        )["completions"]
        self.assertEqual(len(cancelled_completion), 1)
        self.assertEqual(cancelled_completion[0]["state"], "cancelled")
        self.assertIsNone(cancelled_completion[0]["result"]["started_at"])

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
# Keep the slot occupied long enough for cold Python interpreters on the
# oldest supported runtime to admit the second worker deterministically.
time.sleep(0.5)
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
        Path(self.env["AWAITLESS_CONFIG"]).write_text(
            "[defaults]\npoll_interval = 0.05\nmax_return_bytes = 200\n",
            encoding="utf-8",
        )
        completion = json.loads(
            self.run_cli("completions", job, "--json").stdout
        )["completions"][0]["result"]
        self.assertTrue(completion["truncated"])
        self.assertLessEqual(len(completion["stdout_tail"].encode()), 100)

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
        self.assertEqual(value["completion_count"], 2)
        self.assertEqual(len(value["job_ids"]), 2)
        self.assertEqual(len(value["completions"]), 2)
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
        # Result delivery is independently retryable after the terminal event.
        fail_once.touch()
        completion = json.loads(
            self.run_cli(
                "completions",
                job,
                "--timeout",
                "0",
                "--json",
                expected=4,
            ).stdout
        )
        self.assertEqual(completion["completions"], [])
        self.assertEqual(completion["unreachable_job_ids"], [job])
        self.assertTrue(completion["has_more"])
        completion = json.loads(
            self.run_cli(
                "completions", job, "--timeout", "2s", "--json"
            ).stdout
        )["completions"]
        self.assertEqual(len(completion), 1)
        self.assertEqual(completion[0]["state"], "succeeded")
        self.assertEqual(completion[0]["result"]["parsed_results"], {"ok": True})

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
