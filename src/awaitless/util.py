from __future__ import annotations

import json
import os
import re
import secrets
import signal
from datetime import datetime, timezone
from pathlib import Path


_DURATION_RE = re.compile(r"^(?P<value>\d+(?:\.\d+)?)(?P<unit>s|m|h|d)?$")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_duration(value: str | int | float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = _DURATION_RE.fullmatch(value.strip())
    if not match:
        raise ValueError(f"invalid duration: {value!r}; use values such as 30s, 20m, 2h")
    multipliers = {None: 1, "s": 1, "m": 60, "h": 3600, "d": 86400}
    return float(match.group("value")) * multipliers[match.group("unit")]


def new_job_id() -> str:
    # Time-sortable prefix plus enough randomness for practical uniqueness.
    millis = int(datetime.now(timezone.utc).timestamp() * 1000)
    return f"job_{millis:012X}{secrets.token_hex(5).upper()}"


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def process_start_ticks(pid: int | None) -> int | None:
    if not pid or not sys_platform_linux():
        return None
    try:
        # The comm field can contain spaces and parentheses; fields after the last ')' are stable.
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        return int(fields[19])  # proc field 22, offset by pid/comm and zero-based indexing
    except (OSError, ValueError, IndexError):
        return None


def process_matches(pid: int | None, expected_ticks: int | None) -> bool:
    if not pid:
        return False
    if sys_platform_linux() and expected_ticks is not None:
        return process_start_ticks(pid) == expected_ticks
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def sys_platform_linux() -> bool:
    return os.uname().sysname == "Linux"


def terminate_group(pgid: int, grace_seconds: float) -> None:
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    import time

    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
