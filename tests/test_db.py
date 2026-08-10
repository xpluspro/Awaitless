from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path

from awaitless.db import Store


class StoreConcurrencyTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
