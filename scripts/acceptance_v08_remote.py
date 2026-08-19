#!/usr/bin/env python3
"""Run and persist the v0.8 protocol acceptance against a real SSH target."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from metric.provenance import git_state  # noqa: E402


EXPECTED_VERSION = "0.8.0"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class AcceptanceError(RuntimeError):
    pass


class Runner:
    def __init__(self, args: argparse.Namespace, data_dir: Path):
        self.args = args
        python_path = os.pathsep.join(
            item for item in (str(ROOT / "src"), os.environ.get("PYTHONPATH")) if item
        )
        self.environment = {
            **os.environ,
            "AWAITLESS_DATA_DIR": str(data_dir),
            "PYTHONPATH": python_path,
        }
        self.base = [sys.executable, "-m", "awaitless.cli"]
        if args.config:
            self.base.extend(["--config", str(args.config)])

    def command(self, *arguments: str) -> list[str]:
        return [*self.base, *arguments]

    def json(
        self, *arguments: str, expected: set[int] | None = None
    ) -> dict[str, Any]:
        result = subprocess.run(
            self.command(*arguments), cwd=ROOT, env=self.environment, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        if result.returncode not in (expected or {0}):
            raise AcceptanceError(
                f"command failed ({result.returncode}): {' '.join(arguments[:3])}: "
                f"{result.stderr.strip()[-1000:]}"
            )
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise AcceptanceError(
                f"command returned invalid JSON: {' '.join(arguments[:3])}"
            ) from exc
        if not isinstance(value, dict):
            raise AcceptanceError("Awaitless JSON response was not an object")
        return value


def job_profile(args: argparse.Namespace) -> list[str]:
    result = ["--host", args.host, "--cwd", args.cwd]
    for source in args.source:
        result.extend(["--source", source])
    if args.user_group:
        result.extend(["--user-group", args.user_group])
    for value in args.env:
        result.extend(["--env", value])
    return result


def validate_manifest(result: dict[str, Any], artifact_path: str) -> dict[str, Any]:
    matches = [
        item for item in result.get("artifacts", [])
        if item.get("path") == artifact_path and item.get("exists") is True
    ]
    if len(matches) != 1:
        raise AcceptanceError("terminal result did not contain the declared Artifact")
    item = matches[0]
    if not isinstance(item.get("size_bytes"), int) or item["size_bytes"] <= 0:
        raise AcceptanceError("Artifact size_bytes was missing or invalid")
    sha256 = item.get("sha256")
    if not isinstance(sha256, str) or len(sha256) != 64:
        raise AcceptanceError("Artifact sha256 was missing or invalid")
    content = item.get("content")
    checks = (
        "group_active", "profile_sourced", "cmake_visible", "toolchain_visible",
        "npu_smi_visible", "npu_smi_succeeded", "flock_visible",
    )
    if not isinstance(content, dict) or not all(content.get(key) for key in checks):
        raise AcceptanceError("Artifact prerequisite checks were incomplete or failed")
    return item


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--host", default="zhiyuan")
    result.add_argument("--cwd", required=True, help="existing remote project directory")
    result.add_argument("--source", action="append", default=[])
    result.add_argument("--env", action="append", default=[])
    result.add_argument("--user-group", default="HwHiAiUser")
    result.add_argument("--devices", default="0", help="comma-separated physical device IDs")
    result.add_argument("--config", type=Path)
    result.add_argument("--expected-version", default=EXPECTED_VERSION)
    result.add_argument("--output", type=Path, required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if not args.source:
        args.source = ["scripts/env.sh"]
    if args.output.exists():
        print(f"acceptance: refusing to overwrite {args.output}", file=sys.stderr)
        return 2
    version_file = ROOT / "src" / "awaitless" / "__init__.py"
    if f'__version__ = "{args.expected_version}"' not in version_file.read_text(encoding="utf-8"):
        print(f"acceptance: source is not Awaitless {args.expected_version}", file=sys.stderr)
        return 2
    commit, dirty, untracked = git_state(ROOT)
    provenance = {
        "awaitless_version": args.expected_version,
        "awaitless_source": str((ROOT / "src" / "awaitless").resolve()),
        "git_commit": commit,
        "git_dirty": dirty,
        "git_untracked_files": untracked,
        "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    nonce = uuid.uuid4().hex[:12]
    queue = f"accept-v08-{nonce}"
    remote_root = f"/tmp/awaitless-v08-{nonce}"
    artifact_path = f"{remote_root}/acceptance.json"
    started_at = utc_now()
    try:
        with tempfile.TemporaryDirectory(prefix="awaitless-v08-acceptance-") as temporary:
            local_root = Path(temporary)
            runner = Runner(args, local_root / "data")
            doctor_arguments = [
                "doctor", "--host", args.host, "--cwd", args.cwd,
                "--devices", args.devices, "--queue", "--json",
            ]
            for source in args.source:
                doctor_arguments.extend(["--source", source])
            for value in args.env:
                doctor_arguments.extend(["--env", value])
            if args.user_group:
                doctor_arguments.extend(["--user-group", args.user_group])
            doctor = runner.json(*doctor_arguments, expected={0, 1})
            if doctor.get("ok") is not True:
                blocked = {
                    "schema_version": 1,
                    "acceptance": "awaitless-v0.8-protocol-remote",
                    "status": "blocked",
                    "provenance": provenance,
                    "started_at": started_at,
                    "finished_at": utc_now(),
                    "target": {
                        "host": args.host, "cwd": args.cwd, "sources": args.source,
                        "user_group": args.user_group, "devices": args.devices.split(","),
                    },
                    "doctor": doctor,
                    "checks": {
                        "execution_profile": False, "cmake_and_toolchain": False,
                        "npu_device_visibility": False, "remote_queue_and_flock": False,
                        "disconnect_recovery": False, "quiet_job_heartbeat": False,
                        "artifact_path_size_sha256": False,
                    },
                    "blocked_reason": doctor.get("reason"),
                }
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(json.dumps(blocked, indent=2) + "\n", encoding="utf-8")
                raise AcceptanceError(f"remote doctor failed: {doctor.get('reason')}")

            created_queue = runner.json(
                "queue", "create", queue, "--concurrency", "1", "--json"
            )
            profile = job_profile(args)
            blocker = runner.json(
                "submit", *profile, "--queue", queue,
                "--client-request-id", f"accept-v08-blocker-{nonce}", "--json",
                "--", "bash", "-c", "sleep 8",
            )

            script_path = local_root / "acceptance.sh"
            script_path.write_text(ACCEPTANCE_SCRIPT, encoding="utf-8")
            target = runner.json(
                "submit", *profile, "--queue", queue,
                "--env", f"AWAITLESS_ACCEPTANCE_ROOT={remote_root}",
                "--env", f"AWAITLESS_ACCEPTANCE_ARTIFACT={artifact_path}",
                "--env", f"AWAITLESS_EXPECTED_GROUP={args.user_group}",
                "--artifact", artifact_path, "--stall-timeout", "30s",
                "--client-request-id", f"accept-v08-target-{nonce}",
                "--script-file", str(script_path), "--json",
            )
            queued = runner.json("status", target["job_id"], "--json")
            if queued.get("queue_state") != "queued":
                raise AcceptanceError("second remote Job was not observed queued")

            waiter = subprocess.Popen(
                runner.command("wait", target["job_id"], "--json"), cwd=ROOT,
                env=runner.environment, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True,
            )
            time.sleep(1)
            waiter.terminate()
            waiter.wait(timeout=5)
            disconnected_waiter = {
                "terminated": True, "returncode": waiter.returncode,
                "job_id": target["job_id"],
            }

            blocker_result = runner.json("wait", blocker["job_id"], "--json")
            if blocker_result.get("state") != "succeeded":
                raise AcceptanceError("queue blocker did not succeed")
            time.sleep(2)
            quiet_status = runner.json("status", target["job_id"], "--json")
            if quiet_status.get("state") not in {"running", "stalled"}:
                raise AcceptanceError("target was not active during heartbeat observation")
            if not quiet_status.get("last_heartbeat_at"):
                raise AcceptanceError("active quiet target had no heartbeat")
            if quiet_status.get("stdout_bytes") != 0 or quiet_status.get("stderr_bytes") != 0:
                raise AcceptanceError("target was not quiet during heartbeat observation")

            recovered = runner.json("wait", target["job_id"], "--json")
            if recovered.get("state") != "succeeded":
                raise AcceptanceError("recovered Job did not succeed")
            manifest = validate_manifest(recovered, artifact_path)
            artifact = {
                "schema_version": 1,
                "acceptance": "awaitless-v0.8-protocol-remote",
                "status": "passed",
                "provenance": provenance,
                "started_at": started_at,
                "finished_at": utc_now(),
                "target": {
                    "host": args.host, "cwd": args.cwd, "sources": args.source,
                    "user_group": args.user_group, "devices": args.devices.split(","),
                },
                "doctor": doctor,
                "queue": {
                    "name": queue, "created": created_queue,
                    "queued_snapshot": queued, "blocker_result": blocker_result,
                },
                "disconnect_recovery": disconnected_waiter,
                "quiet_heartbeat_snapshot": quiet_status,
                "terminal_result": recovered,
                "artifact_manifest": manifest,
                "checks": {
                    "execution_profile": True, "cmake_and_toolchain": True,
                    "npu_device_visibility": True, "remote_queue_and_flock": True,
                    "disconnect_recovery": True, "quiet_job_heartbeat": True,
                    "artifact_path_size_sha256": True,
                },
            }
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")
    except (AcceptanceError, OSError, subprocess.SubprocessError) as exc:
        print(f"acceptance: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"ok": True, "output": str(args.output)}, separators=(",", ":")))
    return 0


ACCEPTANCE_SCRIPT = r'''set -eu
mkdir -p -- "$AWAITLESS_ACCEPTANCE_ROOT"
cmake_path=$(command -v cmake || true)
cc_path=$(command -v "${CC:-cc}" || true)
cxx_path=$(command -v "${CXX:-c++}" || true)
npu_path=$(command -v npu-smi || true)
flock_path=$(command -v flock || true)
group_active=0
id -Gn | tr ' ' '\n' | grep -Fx "$AWAITLESS_EXPECTED_GROUP" >/dev/null && group_active=1
profile_sourced=0
[ -n "${ASCEND_INSTALL_ROOT:-${ASCEND_HOME_PATH:-}}" ] && profile_sourced=1
npu_rc=127
if [ -n "$npu_path" ]; then
  npu-smi info >"$AWAITLESS_ACCEPTANCE_ROOT/npu-smi.txt" 2>&1 && npu_rc=0 || npu_rc=$?
fi
sleep 8
export cmake_path cc_path cxx_path npu_path flock_path group_active profile_sourced npu_rc
python3 - "$AWAITLESS_ACCEPTANCE_ARTIFACT" <<'PY'
import json, os, sys
value = {
    "group_active": os.environ["group_active"] == "1",
    "profile_sourced": os.environ["profile_sourced"] == "1",
    "cmake_visible": bool(os.environ["cmake_path"]),
    "toolchain_visible": bool(os.environ["cc_path"] and os.environ["cxx_path"]),
    "npu_smi_visible": bool(os.environ["npu_path"]),
    "npu_smi_succeeded": os.environ["npu_rc"] == "0",
    "flock_visible": bool(os.environ["flock_path"]),
    "cmake_path": os.environ["cmake_path"],
    "cc_path": os.environ["cc_path"],
    "cxx_path": os.environ["cxx_path"],
    "npu_smi_path": os.environ["npu_path"],
    "flock_path": os.environ["flock_path"],
}
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(value, handle, sort_keys=True)
    handle.write("\n")
PY
'''


if __name__ == "__main__":
    raise SystemExit(main())
