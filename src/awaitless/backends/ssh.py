from __future__ import annotations

import base64
import json
import math
import shlex
import subprocess
from datetime import datetime, timezone
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
        configured_timeout = self.settings.hosts.get(host, {}).get("operation_timeout")
        if configured_timeout is not None:
            if isinstance(configured_timeout, bool):
                raise ValueError(f"hosts.{host}.operation_timeout must be positive")
            try:
                configured_timeout = float(configured_timeout)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"hosts.{host}.operation_timeout must be positive") from exc
            if configured_timeout <= 0:
                raise ValueError(f"hosts.{host}.operation_timeout must be positive")
            timeout = max(timeout, configured_timeout)
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
        queue = spec.get("queue")
        queue_setup = ""
        queue_admission = ""
        registration_cleanup = ""
        registration_complete = ""
        ready_started_check = '[ -s "$job_dir/started_at" ] && '
        if queue:
            queue_root = str(
                self.settings.hosts.get(job["host"], {}).get(
                    "remote_queue_dir", "~/.awaitless/queues"
                )
            )
            queue_expr = _remote_path_expression(queue_root, queue["name"])
            concurrency = int(queue["concurrency"])
            queue_setup = f"""
queue_dir={queue_expr}
command -v flock >/dev/null 2>&1 || {{ echo 'remote queues require flock' >&2; exit 1; }}
mkdir -p "$queue_dir/pending" "$queue_dir/slots"
chmod 700 "$queue_dir" "$queue_dir/pending" "$queue_dir/slots"
exec 9>"$queue_dir/dispatch.lock"
flock -x 9
if [ -s "$queue_dir/concurrency" ]; then
  configured=$(cat "$queue_dir/concurrency")
  [ "$configured" = "{concurrency}" ] || {{ echo "queue {queue['name']} already has concurrency $configured on this host" >&2; exit 1; }}
else
  printf '%s\n' {concurrency} > "$queue_dir/concurrency"
fi
counter=0
[ -s "$queue_dir/counter" ] && counter=$(cat "$queue_dir/counter")
case "$counter" in ''|*[!0-9]*) echo 'invalid remote queue counter' >&2; exit 1 ;; esac
counter=$((counter + 1))
printf '%s\n' "$counter" > "$queue_dir/counter"
ticket=$(printf '%020d' "$counter")
pending_name="$ticket-{job['job_id']}"
pending_file="$queue_dir/pending/$pending_name"
printf '%s\n' "$job_dir" > "$pending_file"
printf '%s\n' "$pending_name" > "$job_dir/queue_pending"
registered=1
"""
            queue_admission = f"""
queue_dir={queue_expr}
pending_name=$(cat "$job_dir/queue_pending")
pending_file="$queue_dir/pending/$pending_name"
slot_fd=
while [ -z "$slot_fd" ]; do
  [ -f "$job_dir/cancelled_at" ] && exit 0
  exec 9>"$queue_dir/dispatch.lock"
  flock -x 9
  while true; do
    first=$(LC_ALL=C find "$queue_dir/pending" -mindepth 1 -maxdepth 1 -type f -printf '%f\n' | LC_ALL=C sort | head -1)
    [ -n "$first" ] || break
    candidate_file="$queue_dir/pending/$first"
    candidate_dir=$(cat "$candidate_file" 2>/dev/null || true)
    candidate_pid=$(cat "$candidate_dir/pid" 2>/dev/null || true)
    candidate_ticks=$(cat "$candidate_dir/pid_start_ticks" 2>/dev/null || true)
    alive=0
    case "$candidate_pid" in
      ''|*[!0-9]*) ;;
      *)
        case "$candidate_ticks" in
          ''|*[!0-9]*) ;;
          *)
            current_ticks=$(awk '{{print $22}}' "/proc/$candidate_pid/stat" 2>/dev/null || true)
            current_state=$(awk '{{print $3}}' "/proc/$candidate_pid/stat" 2>/dev/null || true)
            [ "$current_ticks" = "$candidate_ticks" ] && [ "$current_state" != "Z" ] && alive=1
            ;;
        esac
        ;;
    esac
    if [ "$alive" -eq 1 ]; then break; fi
    rm -f "$candidate_file"
  done
  if [ "$first" = "$pending_name" ]; then
    slot=0
    concurrency=$(cat "$queue_dir/concurrency")
    while [ "$slot" -lt "$concurrency" ]; do
      slot_path="$queue_dir/slots/$slot"
      : > "$slot_path"
      exec {{candidate_fd}}>"$slot_path"
      if flock -n "$candidate_fd"; then
        slot_fd=$candidate_fd
        rm -f "$pending_file"
        : > "$job_dir/admitted"
        break
      fi
      eval "exec ${{candidate_fd}}>&-"
      slot=$((slot + 1))
    done
  fi
  flock -u 9
  exec 9>&-
  [ -n "$slot_fd" ] || sleep 0.2
done
"""
            registration_cleanup = """
registered=0
cleanup_registration() {
  [ "$registered" -eq 0 ] || rm -f "$pending_file"
}
trap cleanup_registration EXIT
"""
            registration_complete = "registered=0"
            ready_started_check = ""
        command = shlex.join(spec["command"])
        cwd_line = f"cd -- {shlex.quote(spec['cwd'])}" if spec.get("cwd") else ":"
        exports = "\n".join(
            f"export {key}={shlex.quote(value)}" for key, value in spec.get("env", {}).items()
        )
        device_preflight = ""
        if spec.get("device") is not None:
            physical = str(spec["device"])
            device_preflight = f'''\
if command -v npu-smi >/dev/null 2>&1; then
  npu_ok=0
  npu_err="$job_dir/npu-smi.stderr"
  for attempt in 1 2 3; do
    npu_tmp="$job_dir/.npu-smi.$attempt"
    npu-smi info >"$npu_tmp" 2>"$npu_err"
    npu_rc=$?
    if [ "$npu_rc" -eq 0 ] && grep -Eq '(^|[^0-9]){physical}([^0-9]|$)' "$npu_tmp"; then npu_ok=1; break; fi
    sleep 0.2
  done
  if [ "$npu_ok" -ne 1 ]; then
    cat "$npu_err" > "$job_dir/stderr.log"
    if [ "$npu_rc" -ne 0 ]; then echo 'device_driver_call_failed' >> "$job_dir/stderr.log"; else echo 'device_not_visible' >> "$job_dir/stderr.log"; fi
    rc=21
  fi
else
  echo 'npu-smi is unavailable' > "$job_dir/stderr.log"
  rc=21
fi
'''
        timeout = spec.get("timeout_seconds")
        if timeout:
            command = f"timeout --signal=TERM --kill-after=2s {float(timeout):g}s {command}"
        wrapper = f"""#!/usr/bin/env bash
set +e
umask 077
job_dir=$(cd -- "$(dirname -- "$0")" && pwd)
tmp="$job_dir/.tmp.$$"
echo $$ > "$tmp.pid" && mv "$tmp.pid" "$job_dir/pid"
awk '{{print $22}}' "/proc/$$/stat" > "$tmp.pid_start_ticks" && mv "$tmp.pid_start_ticks" "$job_dir/pid_start_ticks"
ps -o pgid= -p $$ | tr -d ' ' > "$tmp.pgid" && mv "$tmp.pgid" "$job_dir/pgid"
pending_file=
heartbeat_pid=
cleanup() {{
  [ -z "$pending_file" ] || rm -f "$pending_file"
  if [ -n "$heartbeat_pid" ]; then
    kill "$heartbeat_pid" 2>/dev/null || true
    wait "$heartbeat_pid" 2>/dev/null || true
  fi
}}
trap cleanup EXIT
trap 'exit 143' HUP INT TERM
wrapper_pid=$$
(
  while kill -0 "$wrapper_pid" 2>/dev/null; do
    touch "$job_dir/heartbeat"
    sleep 2
  done
) &
heartbeat_pid=$!
{queue_admission}
date -u +%Y-%m-%dT%H:%M:%S.%NZ > "$tmp.started_at" && mv "$tmp.started_at" "$job_dir/started_at"
{cwd_line}
cd_rc=$?
{exports}
rc=0
{device_preflight}
if [ "$cd_rc" -ne 0 ]; then
  echo "working directory does not exist: {shlex.quote(spec.get('cwd') or '')}" >"$job_dir/stderr.log"
  rc=125
elif [ "$rc" -eq 0 ]; then
  (
    # Keep the slot lock in this wrapper, but never leak its descriptor into
    # user processes (including children that intentionally daemonize).
    [ -z "$slot_fd" ] || eval "exec ${{slot_fd}}>&-"
    {command}
  ) >"$job_dir/stdout.log" 2>"$job_dir/stderr.log"
  rc=$?
fi
kill "$heartbeat_pid" 2>/dev/null || true
wait "$heartbeat_pid" 2>/dev/null || true
heartbeat_pid=
echo "$rc" > "$tmp.exit_code" && mv "$tmp.exit_code" "$job_dir/exit_code"
date -u +%Y-%m-%dT%H:%M:%S.%NZ > "$tmp.finished_at" && mv "$tmp.finished_at" "$job_dir/finished_at"
: > "$job_dir/command.sh"
"""
        encoded = base64.b64encode(wrapper.encode()).decode()
        metadata = base64.b64encode(json.dumps({
            "job_id": job["job_id"], "command": spec["command"],
            "cwd": spec.get("cwd"), "queue": queue["name"] if queue else None,
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
{registration_cleanup}
{queue_setup}
setsid nohup bash "$job_dir/command.sh" </dev/null >/dev/null 2>&1 9>&- &
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if [ -s "$job_dir/pid" ] && [ -s "$job_dir/pid_start_ticks" ] && [ -s "$job_dir/pgid" ] && {ready_started_check}[ -f "$job_dir/heartbeat" ]; then
    echo "PID=$(cat "$job_dir/pid")"
    echo "PID_START_TICKS=$(cat "$job_dir/pid_start_ticks")"
    echo "PGID=$(cat "$job_dir/pgid")"
    [ ! -s "$job_dir/started_at" ] || echo "STARTED=$(cat "$job_dir/started_at")"
    [ -s "$job_dir/started_at" ] || echo "QUEUED=1"
    {registration_complete}
    exit 0
  fi
  sleep 0.1
done
echo 'remote wrapper did not start' >&2
exit 1
"""
        output = self._invoke(job["host"], script, timeout=5)
        values = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
        if not queue and not values.get("STARTED"):
            raise SSHError("remote wrapper returned no start timestamp")
        updates: dict[str, Any] = {
            "backend_id": root.rstrip("/") + "/" + job["job_id"],
            "state": "queued" if queue and not values.get("STARTED") else "running",
        }
        if values.get("STARTED"):
            updates["started_at"] = values["STARTED"]
        for source, target in (
            ("PID", "pid"),
            ("PID_START_TICKS", "pid_start_ticks"),
            ("PGID", "pgid"),
        ):
            if not values.get(source, "").isdigit():
                raise SSHError(f"remote wrapper returned an invalid {source.lower()}")
            updates[target] = int(values[source])
        return self.store.update_if_active(job["job_id"], **updates)

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
emit_file pid_start_ticks PID_START_TICKS
emit_file pgid PGID
emit_file started_at STARTED
[ -f "$job_dir/stdout.log" ] && echo "STDOUT_BYTES=$(stat -c %s "$job_dir/stdout.log")"
[ -f "$job_dir/stderr.log" ] && echo "STDERR_BYTES=$(stat -c %s "$job_dir/stderr.log")"
latest=$(stat -c %Y "$job_dir/stdout.log" "$job_dir/stderr.log" 2>/dev/null | sort -nr | head -1)
[ -n "${{latest:-}}" ] && echo "LAST_OUTPUT_EPOCH=$latest"
heartbeat_fresh=0
if [ -f "$job_dir/heartbeat" ]; then
  heartbeat_epoch=$(stat -c %Y "$job_dir/heartbeat")
  now_epoch=$(date +%s)
  heartbeat_age=$((now_epoch - heartbeat_epoch))
  [ "$heartbeat_age" -le 15 ] && heartbeat_fresh=1
  echo "HEARTBEAT_EPOCH=$heartbeat_epoch"
fi
if [ ! -f "$job_dir/exit_code" ] && [ ! -f "$job_dir/cancelled_at" ] && [ "$heartbeat_fresh" -eq 1 ]; then
  # Some clusters isolate separate SSH sessions in different PID namespaces.
  # The wrapper-owned heartbeat remains observable through the shared job dir.
  echo 'ALIVE=1'
elif [ ! -f "$job_dir/exit_code" ] && [ ! -f "$job_dir/cancelled_at" ] && [ -f "$job_dir/pid" ]; then
  pid=$(cat "$job_dir/pid")
  alive=0
  case "$pid" in
    ''|*[!0-9]*) ;;
    *)
      if kill -0 "$pid" 2>/dev/null; then
        alive=1
        current_state=$(awk '{{print $3}}' "/proc/$pid/stat" 2>/dev/null || true)
        [ "$current_state" = "Z" ] && alive=0
        if [ -s "$job_dir/pid_start_ticks" ]; then
          expected=$(cat "$job_dir/pid_start_ticks")
          current=$(awk '{{print $22}}' "/proc/$pid/stat" 2>/dev/null || true)
          [ "$current" = "$expected" ] || alive=0
        fi
        if [ "$alive" -eq 1 ] && [ -s "$job_dir/pgid" ]; then
          expected_pgid=$(cat "$job_dir/pgid")
          current_pgid=$(awk '{{print $5}}' "/proc/$pid/stat" 2>/dev/null || true)
          [ "$current_pgid" = "$expected_pgid" ] || alive=0
        fi
      fi
      ;;
  esac
  if [ "$alive" -eq 0 ]; then
    # The wrapper writes exit_code immediately after the child exits. A refresh
    # can otherwise land in that tiny gap and permanently misclassify success
    # as lost, so give the atomic completion marker a short visibility window.
    for _ in 1 2 3 4 5; do
      if [ -f "$job_dir/exit_code" ] || [ -f "$job_dir/cancelled_at" ]; then break; fi
      sleep 0.1
    done
    if [ -f "$job_dir/exit_code" ]; then
      emit_file exit_code EXIT
      emit_file finished_at FINISHED
    elif [ -f "$job_dir/cancelled_at" ]; then
      emit_file cancelled_at CANCELLED
    else
      echo 'ALIVE=0'
    fi
  else
    echo 'ALIVE=1'
  fi
fi
# Read terminal markers last. Besides producing a coherent snapshot for jobs
# that were already complete, this closes the window where exit_code appeared
# after the first read but before the liveness branch.
emit_file finished_at FINISHED
emit_file exit_code EXIT
emit_file cancelled_at CANCELLED
exit 0
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
        for key, target in (
            ("PID", "pid"),
            ("PID_START_TICKS", "pid_start_ticks"),
            ("PGID", "pgid"),
        ):
            value = decoded(key)
            if value and value.isdigit():
                updates[target] = int(value)
        started = decoded("STARTED")
        finished = decoded("FINISHED")
        exit_value = decoded("EXIT")
        cancelled = decoded("CANCELLED")
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
        elif cancelled:
            updates.update(state="cancelled", finished_at=cancelled)
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
        grace_checks = max(0, math.ceil(grace_seconds / 0.1))
        script = f"""set -u
job_dir={remote_expr}
[ -d "$job_dir" ] || {{ echo 'OUTCOME=lost'; exit 0; }}
[ -f "$job_dir/exit_code" ] && {{ echo 'OUTCOME=finished'; exit 0; }}
[ -f "$job_dir/cancelled_at" ] && {{ echo 'OUTCOME=cancelled'; exit 0; }}
[ -s "$job_dir/pid" ] && [ -s "$job_dir/pgid" ] || {{ echo 'OUTCOME=lost'; exit 0; }}
pid=$(cat "$job_dir/pid")
pgid=$(cat "$job_dir/pgid")
case "$pid:$pgid" in *[!0-9:]*) echo 'OUTCOME=lost'; exit 0 ;; esac
kill -0 "$pid" 2>/dev/null || {{ echo 'OUTCOME=lost'; exit 0; }}
current_pgid=$(awk '{{print $5}}' "/proc/$pid/stat" 2>/dev/null || true)
[ "$current_pgid" = "$pgid" ] || {{ echo 'OUTCOME=lost'; exit 0; }}
if [ -s "$job_dir/pid_start_ticks" ]; then
  expected=$(cat "$job_dir/pid_start_ticks")
  current=$(awk '{{print $22}}' "/proc/$pid/stat" 2>/dev/null || true)
  [ "$current" = "$expected" ] || {{ echo 'OUTCOME=lost'; exit 0; }}
fi
tmp="$job_dir/.cancelled.$$"
date -u +%Y-%m-%dT%H:%M:%S.%NZ > "$tmp" && mv "$tmp" "$job_dir/cancelled_at"
if [ -f "$job_dir/exit_code" ]; then
  rm -f "$job_dir/cancelled_at"
  echo 'OUTCOME=finished'
  exit 0
fi
if ! kill -TERM -- "-$pgid" 2>/dev/null; then
  rm -f "$job_dir/cancelled_at"
  [ -f "$job_dir/exit_code" ] && echo 'OUTCOME=finished' || echo 'OUTCOME=lost'
  exit 0
fi
remaining={grace_checks}
while kill -0 -- "-$pgid" 2>/dev/null && [ "$remaining" -gt 0 ]; do
  sleep 0.1
  remaining=$((remaining - 1))
done
kill -KILL -- "-$pgid" 2>/dev/null || true
[ -f "$job_dir/exit_code" ] && {{ rm -f "$job_dir/cancelled_at"; echo 'OUTCOME=finished'; }} || echo 'OUTCOME=cancelled'
"""
        output = self._invoke(job["host"], script, timeout=grace_seconds + 5)
        outcome = dict(
            line.split("=", 1) for line in output.splitlines() if "=" in line
        ).get("OUTCOME")
        current = self.store.get(job["job_id"]) or job
        if outcome == "finished":
            return self.refresh(current)
        if outcome == "cancelled":
            return self.store.update_if_active(
                job["job_id"], state="cancelled", finished_at=utc_now()
            )
        if outcome == "lost":
            return self.store.update_if_active(
                job["job_id"],
                state="lost",
                finished_at=utc_now(),
                error="remote process identity could not be verified during cancellation",
            )
        raise SSHError(f"SSH cancellation on {job['host']!r} returned no outcome")

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
        payload = base64.b64encode(json.dumps({"patterns": job["artifact_paths"], "cwd": job.get("cwd"), "max_bytes": max_bytes}).encode()).decode()
        source = r'''import base64, datetime, glob, json, os, pathlib
spec=json.loads(base64.b64decode(os.environ["AWAITLESS_ARTIFACT_SPEC"]))
items=[]
for declared in spec["patterns"]:
    pattern=declared if os.path.isabs(declared) else os.path.join(spec["cwd"] or os.path.expanduser("~"), declared)
    matches=[p for p in glob.glob(os.path.expanduser(pattern), recursive=True) if os.path.isfile(p)]
    if not matches:
        items.append({"path":declared,"remote":True,"exists":False})
    for matched in sorted(matches):
        stat=os.stat(matched)
        item={"path":matched,"declared_path":declared,"remote":True,"exists":True,"size_bytes":stat.st_size,"modified_at":datetime.datetime.fromtimestamp(stat.st_mtime,datetime.timezone.utc).isoformat().replace("+00:00","Z")}
        if pathlib.Path(matched).suffix.lower()==".json" and stat.st_size<=spec["max_bytes"]:
            try:
                with open(matched,encoding="utf-8") as handle: item["content"]=json.load(handle)
            except Exception as exc: item["parse_error"]=str(exc)
        items.append(item)
print(base64.b64encode(json.dumps(items).encode()).decode())'''
        script = f"export AWAITLESS_ARTIFACT_SPEC={shlex.quote(payload)}\npython3 - <<'PY'\n{source}\nPY\n"
        output = self._invoke(job["host"], script)
        try:
            return json.loads(base64.b64decode(output.strip()))
        except (ValueError, json.JSONDecodeError) as exc:
            raise SSHError(f"invalid artifact response from {job['host']!r}: {exc}") from exc
