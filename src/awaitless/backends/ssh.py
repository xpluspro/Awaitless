from __future__ import annotations

import base64
import json
import shlex
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import Settings, ssh_target_and_options
from ..constants import TERMINAL_STATES
from ..db import Store
from ..util import utc_now


class SSHError(RuntimeError):
    pass


def _remote_path_expression(root: str, job_id: str) -> str:
    if root == "~" or root.startswith("~/"):
        suffix = root[2:] if root.startswith("~/") else ""
        path = "/".join(part for part in (suffix.rstrip("/"), job_id) if part)
        return '"$HOME"/' + shlex.quote(path)
    return shlex.quote(root.rstrip("/") + "/" + job_id)


class SSHBackend:
    name = "ssh"

    def __init__(self, store: Store, settings: Settings):
        self.store = store
        self.settings = settings

    def _invoke(self, host: str, script: str, *, timeout: float = 10) -> str:
        target, options, _ = ssh_target_and_options(self.settings, host)
        try:
            result = subprocess.run(
                ["ssh", *options, target, "bash -s"],
                input=script,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SSHError(f"SSH connection to {host!r} failed: {exc}") from exc
        if result.returncode != 0:
            detail = result.stderr.strip()[-1000:]
            raise SSHError(f"SSH command on {host!r} failed ({result.returncode}): {detail}")
        return result.stdout

    def submit(self, job: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
        assert job["host"]
        _, _, root = ssh_target_and_options(self.settings, job["host"])
        remote_expr = _remote_path_expression(root, job["job_id"])
        command = shlex.join(spec["command"])
        cwd_line = f"cd -- {shlex.quote(spec['cwd'])}" if spec.get("cwd") else ":"
        exports = "\n".join(
            f"export {key}={shlex.quote(value)}" for key, value in spec.get("env", {}).items()
        )
        timeout = spec.get("timeout_seconds")
        if timeout:
            command = f"timeout --signal=TERM --kill-after=2s {float(timeout):g}s {command}"
        wrapper = f"""#!/usr/bin/env bash
set +e
umask 077
job_dir=$(cd -- "$(dirname -- "$0")" && pwd)
tmp="$job_dir/.tmp.$$"
date -u +%Y-%m-%dT%H:%M:%SZ > "$tmp.started_at" && mv "$tmp.started_at" "$job_dir/started_at"
echo $$ > "$tmp.pid" && mv "$tmp.pid" "$job_dir/pid"
ps -o pgid= -p $$ | tr -d ' ' > "$tmp.pgid" && mv "$tmp.pgid" "$job_dir/pgid"
{cwd_line}
cd_rc=$?
{exports}
if [ "$cd_rc" -eq 0 ]; then
  {command} >"$job_dir/stdout.log" 2>"$job_dir/stderr.log"
  rc=$?
else
  echo "working directory does not exist: {shlex.quote(spec.get('cwd') or '')}" >"$job_dir/stderr.log"
  rc=125
fi
echo "$rc" > "$tmp.exit_code" && mv "$tmp.exit_code" "$job_dir/exit_code"
date -u +%Y-%m-%dT%H:%M:%SZ > "$tmp.finished_at" && mv "$tmp.finished_at" "$job_dir/finished_at"
: > "$job_dir/command.sh"
"""
        encoded = base64.b64encode(wrapper.encode()).decode()
        metadata = base64.b64encode(json.dumps({
            "job_id": job["job_id"], "command": spec["command"], "cwd": spec.get("cwd")
        }).encode()).decode()
        script = f"""set -eu
job_dir={remote_expr}
umask 077
mkdir -p "$job_dir"
chmod 700 "$job_dir"
printf %s {shlex.quote(encoded)} | base64 -d > "$job_dir/command.sh"
printf %s {shlex.quote(metadata)} | base64 -d > "$job_dir/metadata.json"
chmod 700 "$job_dir/command.sh"
: > "$job_dir/stdout.log"
: > "$job_dir/stderr.log"
setsid nohup bash "$job_dir/command.sh" </dev/null >/dev/null 2>&1 &
for _ in 1 2 3 4 5 6 7 8 9 10; do
  [ -s "$job_dir/pid" ] && exit 0
  sleep 0.1
done
echo 'remote wrapper did not start' >&2
exit 1
"""
        self._invoke(job["host"], script, timeout=5)
        self.store.update(job["job_id"], backend_id=root.rstrip("/") + "/" + job["job_id"])
        return self.refresh(self.store.get(job["job_id"]) or job)

    def refresh(self, job: dict[str, Any]) -> dict[str, Any]:
        if job["state"] in TERMINAL_STATES:
            return job
        assert job["host"]
        _, _, root = ssh_target_and_options(self.settings, job["host"])
        remote_expr = _remote_path_expression(root, job["job_id"])
        script = f"""set -u
job_dir={remote_expr}
[ -d "$job_dir" ] || {{ echo 'MISSING=1'; exit 0; }}
emit_file() {{ [ -f "$job_dir/$1" ] && printf '%s=' "$2" && base64 < "$job_dir/$1" | tr -d '\\n' && printf '\\n'; }}
emit_file pid PID
emit_file pgid PGID
emit_file started_at STARTED
emit_file finished_at FINISHED
emit_file exit_code EXIT
[ -f "$job_dir/stdout.log" ] && echo "STDOUT_BYTES=$(stat -c %s "$job_dir/stdout.log")"
[ -f "$job_dir/stderr.log" ] && echo "STDERR_BYTES=$(stat -c %s "$job_dir/stderr.log")"
latest=$(stat -c %Y "$job_dir/stdout.log" "$job_dir/stderr.log" 2>/dev/null | sort -nr | head -1)
[ -n "${{latest:-}}" ] && echo "LAST_OUTPUT_EPOCH=$latest"
if [ ! -f "$job_dir/exit_code" ] && [ -f "$job_dir/pid" ]; then
  pid=$(cat "$job_dir/pid")
  kill -0 "$pid" 2>/dev/null && echo 'ALIVE=1' || echo 'ALIVE=0'
fi
"""
        output = self._invoke(job["host"], script)
        values: dict[str, str] = {}
        for line in output.splitlines():
            key, sep, value = line.partition("=")
            if sep:
                values[key] = value
        if values.get("MISSING") == "1":
            return self.store.update_if_active(
                job["job_id"], state="lost", finished_at=utc_now(), error="remote job directory is missing"
            )
        def decoded(key: str) -> str | None:
            try:
                return base64.b64decode(values[key]).decode().strip() if key in values else None
            except (ValueError, UnicodeDecodeError):
                return None
        updates: dict[str, Any] = {}
        for key, target in (("PID", "pid"), ("PGID", "pgid")):
            value = decoded(key)
            if value and value.isdigit():
                updates[target] = int(value)
        started = decoded("STARTED")
        finished = decoded("FINISHED")
        exit_value = decoded("EXIT")
        if started:
            updates["started_at"] = started
            updates["state"] = "running"
        if exit_value is not None:
            try:
                exit_code = int(exit_value)
                updates.update(
                    state="succeeded" if exit_code == 0 else ("timed_out" if exit_code == 124 and job.get("timeout_seconds") else "failed"),
                    exit_code=exit_code,
                    finished_at=finished or utc_now(),
                )
            except ValueError:
                pass
        elif values.get("ALIVE") == "0":
            updates.update(state="lost", finished_at=utc_now(), error="remote process exited without an exit marker")
        if values.get("STDOUT_BYTES", "").isdigit():
            updates["stdout_bytes"] = int(values["STDOUT_BYTES"])
        if values.get("STDERR_BYTES", "").isdigit():
            updates["stderr_bytes"] = int(values["STDERR_BYTES"])
        if values.get("LAST_OUTPUT_EPOCH", "").isdigit():
            updates["last_output_at"] = datetime.fromtimestamp(
                int(values["LAST_OUTPUT_EPOCH"]), timezone.utc
            ).isoformat().replace("+00:00", "Z")
        return self.store.update_if_active(job["job_id"], **updates) if updates else job

    def cancel(self, job: dict[str, Any], grace_seconds: float) -> dict[str, Any]:
        if job["state"] in TERMINAL_STATES:
            return job
        assert job["host"]
        _, _, root = ssh_target_and_options(self.settings, job["host"])
        remote_expr = _remote_path_expression(root, job["job_id"])
        script = f"""set -u
job_dir={remote_expr}
[ -f "$job_dir/pgid" ] || exit 0
pgid=$(cat "$job_dir/pgid")
kill -TERM -- "-$pgid" 2>/dev/null || true
deadline=$((SECONDS + {max(0, int(grace_seconds))}))
while kill -0 -- "-$pgid" 2>/dev/null && [ "$SECONDS" -lt "$deadline" ]; do sleep 1; done
kill -KILL -- "-$pgid" 2>/dev/null || true
"""
        self._invoke(job["host"], script, timeout=grace_seconds + 5)
        return self.store.update(job["job_id"], state="cancelled", finished_at=utc_now())

    def read_logs(self, job: dict[str, Any], tail: int, max_bytes: int) -> dict[str, Any]:
        assert job["host"]
        _, _, root = ssh_target_and_options(self.settings, job["host"])
        remote_expr = _remote_path_expression(root, job["job_id"])
        each = max(1, max_bytes // 2)
        script = f"""set -u
job_dir={remote_expr}
for stream in stdout stderr; do
  file="$job_dir/$stream.log"
  [ -f "$file" ] || continue
  size=$(stat -c %s "$file")
  data=$(tail -n {int(tail)} "$file" | tail -c {each} | base64 | tr -d '\\n')
  echo "${{stream^^}}_SIZE=$size"
  echo "${{stream^^}}_DATA=$data"
done
"""
        output = self._invoke(job["host"], script)
        values = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
        result: dict[str, Any] = {"truncated": False, "stdout_tail": "", "stderr_tail": ""}
        for stream in ("stdout", "stderr"):
            data = base64.b64decode(values.get(f"{stream.upper()}_DATA", "")).decode(errors="replace")
            size = int(values.get(f"{stream.upper()}_SIZE", "0"))
            result[f"{stream}_tail"] = data
            result["truncated"] = result["truncated"] or size > len(data.encode())
        return result

    def artifacts(self, job: dict[str, Any], max_bytes: int) -> list[dict[str, Any]]:
        assert job["host"]
        if not job["artifact_paths"]:
            return []
        snippets: list[str] = []
        for index, declared in enumerate(job["artifact_paths"]):
            if declared.startswith("/"):
                path_expr = shlex.quote(declared)
            elif job.get("cwd"):
                path_expr = shlex.quote(str(job["cwd"]).rstrip("/") + "/" + declared)
            else:
                path_expr = '"$HOME"/' + shlex.quote(declared)
            snippets.append(f"""
path={path_expr}
echo 'ARTIFACT_{index}_BEGIN=1'
if [ -f "$path" ]; then
  echo 'EXISTS=1'
  echo "SIZE=$(stat -c %s "$path")"
  echo "MTIME=$(stat -c %Y "$path")"
  case "$path" in *.json) [ "$(stat -c %s "$path")" -le {int(max_bytes)} ] && echo "CONTENT=$(base64 < "$path" | tr -d '\\n')" ;; esac
else
  echo 'EXISTS=0'
fi
echo 'ARTIFACT_{index}_END=1'
""")
        output = self._invoke(job["host"], "set -u\n" + "\n".join(snippets))
        items: list[dict[str, Any]] = []
        lines = output.splitlines()
        for index, declared in enumerate(job["artifact_paths"]):
            begin = lines.index(f"ARTIFACT_{index}_BEGIN=1")
            end = lines.index(f"ARTIFACT_{index}_END=1")
            values = dict(line.split("=", 1) for line in lines[begin + 1:end] if "=" in line)
            item: dict[str, Any] = {"path": declared, "remote": True, "exists": values.get("EXISTS") == "1"}
            if item["exists"]:
                item["size_bytes"] = int(values.get("SIZE", "0"))
                if values.get("MTIME", "").isdigit():
                    item["modified_at"] = datetime.fromtimestamp(
                        int(values["MTIME"]), timezone.utc
                    ).isoformat().replace("+00:00", "Z")
                if "CONTENT" in values:
                    try:
                        item["content"] = json.loads(base64.b64decode(values["CONTENT"]))
                    except (ValueError, json.JSONDecodeError) as exc:
                        item["parse_error"] = str(exc)
            items.append(item)
        return items
