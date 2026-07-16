from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from .util import utc_now


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
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
        ):
            if column not in existing:
                self.connection.execute(f"ALTER TABLE jobs ADD COLUMN {column} {definition}")

    def close(self) -> None:
        self.connection.close()

    def create(self, values: dict[str, Any]) -> None:
        now = utc_now()
        row = dict(values, created_at=values.get("created_at", now), updated_at=now)
        columns = ", ".join(row)
        placeholders = ", ".join("?" for _ in row)
        with self.connection:
            self.connection.execute(
                f"INSERT INTO jobs ({columns}) VALUES ({placeholders})", tuple(row.values())
            )
            self.connection.execute(
                "INSERT INTO state_events(job_id,state,occurred_at) VALUES(?,?,?)",
                (row["job_id"], row["state"], now),
            )

    def get(self, job_id: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return self._decode(row) if row else None

    def update(self, job_id: str, **values: Any) -> dict[str, Any]:
        if not values:
            result = self.get(job_id)
            if not result:
                raise KeyError(job_id)
            return result
        values["updated_at"] = utc_now()
        assignments = ", ".join(f"{key}=?" for key in values)
        previous = self.get(job_id)
        if not previous:
            raise KeyError(job_id)
        with self.connection:
            self.connection.execute(
                f"UPDATE jobs SET {assignments} WHERE job_id=?", (*values.values(), job_id)
            )
            if "state" in values and values["state"] != previous["state"]:
                self.connection.execute(
                    "INSERT INTO state_events(job_id,state,occurred_at,detail) VALUES(?,?,?,?)",
                    (job_id, values["state"], values["updated_at"], values.get("error")),
                )
        result = self.get(job_id)
        assert result
        return result

    def update_if_active(self, job_id: str, **values: Any) -> dict[str, Any]:
        current = self.get(job_id)
        if not current:
            raise KeyError(job_id)
        if current["state"] in {"succeeded", "failed", "cancelled", "timed_out", "lost"}:
            return current
        return self.update(job_id, **values)

    def list(self, *, state: str | None = None, host: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[str] = []
        if state:
            clauses.append("state=?")
            params.append(state)
        if host:
            clauses.append("host=?")
            params.append(host)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.connection.execute(
            f"SELECT * FROM jobs{where} ORDER BY created_at DESC", params
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
