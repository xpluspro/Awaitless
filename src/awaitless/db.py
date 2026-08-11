from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .constants import TERMINAL_STATES
from .util import utc_now


SCHEMA = """
CREATE TABLE IF NOT EXISTS queues (
    name TEXT PRIMARY KEY,
    concurrency INTEGER NOT NULL CHECK(concurrency > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    client_request_id TEXT,
    submission_fingerprint TEXT,
    name TEXT,
    backend TEXT NOT NULL,
    host TEXT,
    command_json TEXT NOT NULL,
    cwd TEXT,
    env_json TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    exit_code INTEGER,
    timeout_seconds REAL,
    stall_timeout_seconds REAL,
    runner_pid INTEGER,
    runner_start_ticks INTEGER,
    pid INTEGER,
    pid_start_ticks INTEGER,
    pgid INTEGER,
    backend_id TEXT,
    job_dir TEXT NOT NULL,
    stdout_path TEXT NOT NULL,
    stderr_path TEXT NOT NULL,
    stdout_bytes INTEGER NOT NULL DEFAULT 0,
    stderr_bytes INTEGER NOT NULL DEFAULT 0,
    last_output_at TEXT,
    artifacts_json TEXT NOT NULL DEFAULT '[]',
    mcp_task_ttl_ms INTEGER,
    queue_name TEXT,
    queue_order INTEGER,
    error TEXT,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS jobs_state_idx ON jobs(state);
CREATE INDEX IF NOT EXISTS jobs_host_idx ON jobs(host);
CREATE TABLE IF NOT EXISTS state_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    state TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    detail TEXT,
    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
);
"""


JSON_FIELDS = {"command_json": "command", "env_json": "env", "artifacts_json": "artifact_paths"}


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.connection = sqlite3.connect(path, timeout=30, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(SCHEMA)
        # Lightweight forward migration for databases created by earlier v0.1 snapshots.
        existing = {row[1] for row in self.connection.execute("PRAGMA table_info(jobs)")}
        for column, definition in (
            ("stdout_bytes", "INTEGER NOT NULL DEFAULT 0"),
            ("stderr_bytes", "INTEGER NOT NULL DEFAULT 0"),
            ("last_output_at", "TEXT"),
            ("client_request_id", "TEXT"),
            ("submission_fingerprint", "TEXT"),
            ("mcp_task_ttl_ms", "INTEGER"),
            ("queue_name", "TEXT"),
            ("queue_order", "INTEGER"),
        ):
            if column not in existing:
                self.connection.execute(f"ALTER TABLE jobs ADD COLUMN {column} {definition}")
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS jobs_queue_idx "
            "ON jobs(queue_name,backend,host,state,queue_order)"
        )
        self.connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS jobs_client_request_id_idx "
            "ON jobs(client_request_id) WHERE client_request_id IS NOT NULL"
        )

    def close(self) -> None:
        self.connection.close()

    def create(self, values: dict[str, Any]) -> None:
        now = utc_now()
        row = dict(values, created_at=values.get("created_at", now), updated_at=now)
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self._prepare_queue_order(row)
            self._insert(row, now)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def reserve_submission(
        self, values: dict[str, Any], *, client_request_id: str, fingerprint: str
    ) -> tuple[dict[str, Any], bool]:
        """Atomically reserve an idempotency key before any backend side effects.

        The returned boolean is true only for the process that inserted the job.
        Replays with the same fingerprint receive the existing row; reusing a key
        for a different request is rejected rather than starting ambiguous work.
        """
        now = utc_now()
        row = dict(
            values,
            client_request_id=client_request_id,
            submission_fingerprint=fingerprint,
            created_at=values.get("created_at", now),
            updated_at=now,
        )
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self.connection.execute(
                "SELECT * FROM jobs WHERE client_request_id=?", (client_request_id,)
            ).fetchone()
            if existing:
                decoded = self._decode(existing)
                if decoded.get("submission_fingerprint") != fingerprint:
                    raise ValueError(
                        f"client_request_id {client_request_id!r} was already used "
                        "with different submission parameters"
                    )
                self.connection.commit()
                return decoded, False
            self._prepare_queue_order(row)
            self._insert(row, now)
            inserted = self.connection.execute(
                "SELECT * FROM jobs WHERE job_id=?", (row["job_id"],)
            ).fetchone()
            assert inserted
            decoded = self._decode(inserted)
            self.connection.commit()
            return decoded, True
        except Exception:
            self.connection.rollback()
            raise

    def _prepare_queue_order(self, row: dict[str, Any]) -> None:
        queue_name = row.get("queue_name")
        if not queue_name:
            return
        queue = self.connection.execute(
            "SELECT 1 FROM queues WHERE name=?", (queue_name,)
        ).fetchone()
        if not queue:
            raise KeyError(f"unknown queue: {queue_name}")
        if row.get("queue_order") is None:
            next_order = self.connection.execute(
                "SELECT COALESCE(MAX(queue_order), 0) + 1 FROM jobs WHERE queue_name=?",
                (queue_name,),
            ).fetchone()[0]
            row["queue_order"] = int(next_order)

    def create_queue(self, name: str, concurrency: int) -> tuple[dict[str, Any], bool]:
        now = utc_now()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self.connection.execute(
                "SELECT * FROM queues WHERE name=?", (name,)
            ).fetchone()
            if existing:
                value = dict(existing)
                if value["concurrency"] != concurrency:
                    raise ValueError(
                        f"queue {name!r} already exists with concurrency "
                        f"{value['concurrency']}"
                    )
                self.connection.commit()
                return value, False
            self.connection.execute(
                "INSERT INTO queues(name,concurrency,created_at,updated_at) VALUES(?,?,?,?)",
                (name, concurrency, now, now),
            )
            created = self.connection.execute(
                "SELECT * FROM queues WHERE name=?", (name,)
            ).fetchone()
            assert created
            self.connection.commit()
            return dict(created), True
        except Exception:
            self.connection.rollback()
            raise

    def get_queue(self, name: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM queues WHERE name=?", (name,)
        ).fetchone()
        return dict(row) if row else None

    def list_queues(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT q.*,
                   COALESCE(SUM(CASE WHEN j.state='queued' THEN 1 ELSE 0 END), 0) AS queued_jobs,
                   COALESCE(SUM(CASE WHEN j.state IN ('starting','running','stalled') THEN 1 ELSE 0 END), 0) AS active_jobs,
                   COUNT(j.job_id) AS total_jobs
              FROM queues AS q
              LEFT JOIN jobs AS j ON j.queue_name=q.name
             GROUP BY q.name
             ORDER BY q.name
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def claim_queue_slot(
        self, job_id: str, *, runner_pid: int | None = None
    ) -> tuple[dict[str, Any], bool]:
        """Atomically admit the oldest queued job when its target has capacity."""
        now = utc_now()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if not row:
                raise KeyError(job_id)
            job = self._decode(row)
            if (
                job["state"] != "queued"
                or not job.get("queue_name")
                or (runner_pid is not None and job.get("runner_pid") != runner_pid)
            ):
                self.connection.commit()
                return job, False
            queue = self.connection.execute(
                "SELECT concurrency FROM queues WHERE name=?", (job["queue_name"],)
            ).fetchone()
            if not queue:
                raise KeyError(f"unknown queue: {job['queue_name']}")
            target_clause = "host IS NULL" if job.get("host") is None else "host=?"
            target_params: list[Any] = [] if job.get("host") is None else [job["host"]]
            oldest = self.connection.execute(
                f"""
                SELECT job_id FROM jobs
                 WHERE queue_name=? AND backend=? AND {target_clause} AND state='queued'
                 ORDER BY queue_order, created_at, job_id
                 LIMIT 1
                """,
                [job["queue_name"], job["backend"], *target_params],
            ).fetchone()
            active = self.connection.execute(
                f"""
                SELECT COUNT(*) FROM jobs
                 WHERE queue_name=? AND backend=? AND {target_clause}
                   AND state IN ('starting','running','stalled')
                """,
                [job["queue_name"], job["backend"], *target_params],
            ).fetchone()[0]
            if not oldest or oldest["job_id"] != job_id or active >= queue["concurrency"]:
                self.connection.commit()
                return job, False
            transition = self.connection.execute(
                "UPDATE jobs SET state='starting',updated_at=? WHERE job_id=? AND state='queued'",
                (now, job_id),
            )
            if transition.rowcount == 1:
                self.connection.execute(
                    "INSERT INTO state_events(job_id,state,occurred_at) VALUES(?,?,?)",
                    (job_id, "starting", now),
                )
            claimed = self.connection.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            assert claimed
            result = self._decode(claimed)
            self.connection.commit()
            return result, transition.rowcount == 1
        except Exception:
            self.connection.rollback()
            raise

    def register_queue_runner(
        self,
        job_id: str,
        *,
        expected_pid: int | None,
        expected_start_ticks: int | None,
        runner_pid: int,
        runner_start_ticks: int | None,
    ) -> tuple[dict[str, Any], bool]:
        """CAS a detached waiter into a queued job without replacing a live peer."""
        now = utc_now()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if not row:
                raise KeyError(job_id)
            current_identity = (row["runner_pid"], row["runner_start_ticks"])
            expected_identity = (expected_pid, expected_start_ticks)
            registered = row["state"] == "queued" and current_identity == expected_identity
            if registered:
                self.connection.execute(
                    """
                    UPDATE jobs
                       SET runner_pid=?,runner_start_ticks=?,updated_at=?
                     WHERE job_id=?
                    """,
                    (runner_pid, runner_start_ticks, now, job_id),
                )
            updated = self.connection.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            assert updated
            result = self._decode(updated)
            self.connection.commit()
            return result, registered
        except Exception:
            self.connection.rollback()
            raise

    def requeue_unstarted_runner(
        self,
        job_id: str,
        *,
        expected_pid: int | None,
        expected_start_ticks: int | None,
    ) -> tuple[dict[str, Any], bool]:
        """Recover a claimed slot only if the exact missing runner still owns it."""
        now = utc_now()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if not row:
                raise KeyError(job_id)
            current_identity = (row["runner_pid"], row["runner_start_ticks"])
            expected_identity = (expected_pid, expected_start_ticks)
            recovered = (
                row["state"] == "starting"
                and row["pid"] is None
                and current_identity == expected_identity
            )
            if recovered:
                self.connection.execute(
                    "UPDATE jobs SET state='queued',updated_at=? WHERE job_id=?",
                    (now, job_id),
                )
                self.connection.execute(
                    "INSERT INTO state_events(job_id,state,occurred_at) VALUES(?,?,?)",
                    (job_id, "queued", now),
                )
            updated = self.connection.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            assert updated
            result = self._decode(updated)
            self.connection.commit()
            return result, recovered
        except Exception:
            self.connection.rollback()
            raise

    def get_by_client_request_id(self, client_request_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM jobs WHERE client_request_id=?", (client_request_id,)
        ).fetchone()
        return self._decode(row) if row else None

    def _insert(self, row: dict[str, Any], occurred_at: str) -> None:
        columns = ", ".join(row)
        placeholders = ", ".join("?" for _ in row)
        self.connection.execute(
            f"INSERT INTO jobs ({columns}) VALUES ({placeholders})", tuple(row.values())
        )
        self.connection.execute(
            "INSERT INTO state_events(job_id,state,occurred_at) VALUES(?,?,?)",
            (row["job_id"], row["state"], occurred_at),
        )

    def get(self, job_id: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return self._decode(row) if row else None

    def update(self, job_id: str, **values: Any) -> dict[str, Any]:
        return self._update(job_id, values, active_only=False)

    def update_if_active(self, job_id: str, **values: Any) -> dict[str, Any]:
        return self._update(job_id, values, active_only=True)

    def _update(
        self, job_id: str, values: dict[str, Any], *, active_only: bool
    ) -> dict[str, Any]:
        if not values:
            result = self.get(job_id)
            if not result:
                raise KeyError(job_id)
            return result
        values = dict(values)
        values["updated_at"] = utc_now()
        assignments = ", ".join(f"{key}=?" for key in values)
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            row = self.connection.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if not row:
                raise KeyError(job_id)
            previous = self._decode(row)
            if active_only and previous["state"] in TERMINAL_STATES:
                self.connection.commit()
                return previous
            self.connection.execute(
                f"UPDATE jobs SET {assignments} WHERE job_id=?", (*values.values(), job_id)
            )
            if "state" in values and values["state"] != previous["state"]:
                self.connection.execute(
                    "INSERT INTO state_events(job_id,state,occurred_at,detail) VALUES(?,?,?,?)",
                    (job_id, values["state"], values["updated_at"], values.get("error")),
                )
            updated = self.connection.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            assert updated
            result = self._decode(updated)
            self.connection.commit()
            return result
        except Exception:
            self.connection.rollback()
            raise

    def list(
        self,
        *,
        state: str | None = None,
        host: str | None = None,
        queue_name: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if state:
            clauses.append("state=?")
            params.append(state)
        if host:
            clauses.append("host=?")
            params.append(host)
        if queue_name:
            clauses.append("queue_name=?")
            params.append(queue_name)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        limit_clause = " LIMIT ?" if limit is not None else ""
        if limit is not None:
            params.append(limit)
        rows = self.connection.execute(
            f"SELECT * FROM jobs{where} ORDER BY created_at DESC{limit_clause}", params
        ).fetchall()
        return [self._decode(row) for row in rows]

    def events(self, job_id: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT state,occurred_at,detail FROM state_events WHERE job_id=? ORDER BY id", (job_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _decode(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        for source, target in JSON_FIELDS.items():
            value[target] = json.loads(value.pop(source))
        return value
