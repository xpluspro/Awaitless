from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path

from awaitless.db import Store


class StoreConcurrencyTest(unittest.TestCase):
    @staticmethod
    def submission_values(root: Path, job_id: str) -> dict[str, object]:
        return {
            "job_id": job_id,
            "backend": "local",
            "command_json": '["true"]',
            "cwd": str(root),
            "env_json": "{}",
            "state": "starting",
            "job_dir": str(root / job_id),
            "stdout_path": str(root / job_id / "stdout.log"),
            "stderr_path": str(root / job_id / "stderr.log"),
            "artifacts_json": "[]",
        }

    def test_only_one_racing_terminal_transition_wins(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "awaitless.db"
            setup = Store(database)
            setup.create(
                {
                    "job_id": "job_race",
                    "backend": "local",
                    "command_json": "[]",
                    "cwd": str(root),
                    "env_json": "{}",
                    "state": "running",
                    "job_dir": str(root / "job"),
                    "stdout_path": str(root / "stdout.log"),
                    "stderr_path": str(root / "stderr.log"),
                    "artifacts_json": "[]",
                }
            )
            setup.connection.execute(
                """
                CREATE TRIGGER pause_terminal_update
                BEFORE UPDATE OF state ON jobs
                WHEN NEW.state IN ('succeeded', 'cancelled')
                BEGIN
                    SELECT pause_state_update();
                END
                """
            )
            setup.close()

            ready = [threading.Event(), threading.Event()]
            start = threading.Event()
            results: list[str] = []
            errors: list[BaseException] = []

            def transition(index: int, state: str) -> None:
                store: Store | None = None
                try:
                    store = Store(database)
                    store.connection.create_function(
                        "pause_state_update", 0, lambda: time.sleep(0.1)
                    )
                    ready[index].set()
                    if not start.wait(timeout=2):
                        raise TimeoutError("race did not start")
                    result = store.update_if_active(
                        "job_race", state=state, finished_at="2026-01-01T00:00:00Z"
                    )
                    results.append(result["state"])
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    if store:
                        store.close()

            threads = [
                threading.Thread(target=transition, args=(0, "succeeded")),
                threading.Thread(target=transition, args=(1, "cancelled")),
            ]
            for thread in threads:
                thread.start()
            self.assertTrue(ready[0].wait(timeout=2))
            self.assertTrue(ready[1].wait(timeout=2))
            start.set()
            for thread in threads:
                thread.join(timeout=5)
                self.assertFalse(thread.is_alive())

            self.assertEqual(errors, [])
            check = Store(database)
            final = check.get("job_race")
            assert final
            terminal_events = [
                event
                for event in check.events("job_race")
                if event["state"] in {"succeeded", "cancelled"}
            ]
            check.close()
            self.assertEqual(len(terminal_events), 1)
            self.assertEqual(results, [final["state"], final["state"]])

    def test_idempotency_key_is_reserved_once_across_racing_connections(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = root / "awaitless.db"
            Store(database).close()
            ready = [threading.Event(), threading.Event()]
            start = threading.Event()
            results: list[tuple[str, bool]] = []
            errors: list[BaseException] = []

            def reserve(index: int) -> None:
                store: Store | None = None
                try:
                    store = Store(database)
                    ready[index].set()
                    if not start.wait(timeout=2):
                        raise TimeoutError("race did not start")
                    job, created = store.reserve_submission(
                        self.submission_values(root, f"job_{index}"),
                        client_request_id="gpu:training:42",
                        fingerprint="same-fingerprint",
                    )
                    results.append((job["job_id"], created))
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    if store:
                        store.close()

            threads = [threading.Thread(target=reserve, args=(index,)) for index in range(2)]
            for thread in threads:
                thread.start()
            self.assertTrue(ready[0].wait(timeout=2))
            self.assertTrue(ready[1].wait(timeout=2))
            start.set()
            for thread in threads:
                thread.join(timeout=5)
                self.assertFalse(thread.is_alive())

            self.assertEqual(errors, [])
            self.assertEqual(len({job_id for job_id, _ in results}), 1)
            self.assertEqual(sorted(created for _, created in results), [False, True])
            check = Store(database)
            self.assertEqual(len(check.list()), 1)
            with self.assertRaisesRegex(ValueError, "different submission parameters"):
                check.reserve_submission(
                    self.submission_values(root, "job_conflict"),
                    client_request_id="gpu:training:42",
                    fingerprint="different-fingerprint",
                )
            check.close()

    def test_queue_runner_registration_and_claim_use_compare_and_swap(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            store = Store(root / "awaitless.db")
            store.create_queue("gpu0", 1)
            values = self.submission_values(root, "job_queue")
            values.update(state="queued", queue_name="gpu0")
            store.create(values)

            registered, won = store.register_queue_runner(
                "job_queue",
                expected_pid=None,
                expected_start_ticks=None,
                runner_pid=101,
                runner_start_ticks=1001,
            )
            self.assertTrue(won)
            self.assertEqual(registered["runner_pid"], 101)
            current, lost = store.register_queue_runner(
                "job_queue",
                expected_pid=None,
                expected_start_ticks=None,
                runner_pid=202,
                runner_start_ticks=2002,
            )
            self.assertFalse(lost)
            self.assertEqual(current["runner_pid"], 101)

            current, wrong_owner = store.claim_queue_slot(
                "job_queue", runner_pid=202
            )
            self.assertFalse(wrong_owner)
            self.assertEqual(current["state"], "queued")
            current, claimed = store.claim_queue_slot(
                "job_queue", runner_pid=101
            )
            self.assertTrue(claimed)
            self.assertEqual(current["state"], "starting")

            current, wrong_recovery = store.requeue_unstarted_runner(
                "job_queue", expected_pid=202, expected_start_ticks=2002
            )
            self.assertFalse(wrong_recovery)
            self.assertEqual(current["state"], "starting")
            current, recovered = store.requeue_unstarted_runner(
                "job_queue", expected_pid=101, expected_start_ticks=1001
            )
            self.assertTrue(recovered)
            self.assertEqual(current["state"], "queued")
            self.assertEqual(
                [event["state"] for event in store.events("job_queue")],
                ["queued", "starting", "queued"],
            )
            store.close()


if __name__ == "__main__":
    unittest.main()
