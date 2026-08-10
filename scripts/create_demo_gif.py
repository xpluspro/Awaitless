#!/usr/bin/env python3
"""Capture a real low-load SSH recovery session and render the README GIF."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CAPTURE = ROOT / "assets" / "demo-session.json"
DEFAULT_GIF = ROOT / "assets" / "awaitless-demo.gif"
FONT = Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf")


def run(
    command: list[str],
    *,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
    timeout: float = 180,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[-3000:]
        raise RuntimeError(f"command failed ({result.returncode}): {detail}")
    return result


def note(message: str) -> None:
    print(f"[demo] {message}", file=sys.stderr, flush=True)


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def capture(host: str, display_host: str, output: Path, workload_seconds: float) -> None:
    artifact_remote = f"awaitless-demo-{uuid.uuid4().hex[:12]}.json"
    job_id: str | None = None
    with tempfile.TemporaryDirectory(prefix="awaitless-demo-", dir="/tmp") as temp:
        temporary = Path(temp)
        config = temporary / "config.toml"
        data_dir = temporary / "client-state"
        config.write_text(
            "\n".join(
                [
                    "[defaults]",
                    "poll_interval = 2",
                    "max_return_bytes = 65536",
                    "",
                    "[hosts.demo]",
                    f"hostname = {json.dumps(host)}",
                    "gssapi_authentication = false",
                    "connect_timeout = 10",
                    "operation_timeout = 30",
                    'remote_job_dir = "~/.awaitless/jobs"',
                    "",
                ]
            ),
            encoding="utf-8",
        )
        environment = os.environ.copy()
        environment["AWAITLESS_DATA_DIR"] = str(data_dir)
        environment["PYTHONPATH"] = str(ROOT / "src")
        artifact_value = {
            "correctness": True,
            "artifact": "demo-result.json",
            "backend": "ssh",
        }
        workload = "\n".join(
            [
                "set -eu",
                "printf 'phase=started\\n'",
                f"sleep {workload_seconds:g}",
                (
                    f"printf %s {shlex.quote(json.dumps(artifact_value, separators=(',', ':')))} "
                    f"> {shlex.quote(artifact_remote)}"
                ),
                "printf 'phase=complete\\n'",
            ]
        )
        submit_command = [
            sys.executable,
            "-m",
            "awaitless",
            "--config",
            str(config),
            "submit",
            "--host",
            "demo",
            "--artifact",
            artifact_remote,
            "--json",
            "--",
            "bash",
            "-c",
            workload,
        ]
        try:
            submit_started = time.monotonic()
            submitted = run(submit_command, env=environment, timeout=45)
            submit_elapsed = time.monotonic() - submit_started
            submit_value = json.loads(submitted.stdout)
            job_id = submit_value["job_id"]
            note(f"submitted {job_id}; first CLI process exited")

            # A separate process, launched later with only the persisted job ID,
            # models closing the first client and opening a new one.
            time.sleep(1)
            wait_command = [
                sys.executable,
                "-m",
                "awaitless",
                "--config",
                str(config),
                "wait",
                job_id,
                "--json",
            ]
            wait_started = time.monotonic()
            waited = run(wait_command, env=environment, timeout=180)
            wait_elapsed = time.monotonic() - wait_started
            wait_value = json.loads(waited.stdout)
            expected_stdout = "phase=started\nphase=complete\n"
            checks = {
                "terminal_success": wait_value.get("state") == "succeeded",
                "exit_code_zero": wait_value.get("exit_code") == 0,
                "complete_log_tail": wait_value.get("stdout_tail") == expected_stdout,
                "artifact_exists": any(
                    item.get("exists") for item in wait_value.get("artifacts", [])
                ),
                "artifact_parsed": wait_value.get("parsed_results") == artifact_value,
                "separate_client_processes": True,
            }
            if not all(checks.values()):
                raise RuntimeError(f"demo verification failed: {json.dumps(checks)}")

            captured_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            public_capture: dict[str, Any] = {
                "schema_version": 1,
                "captured_at": captured_at,
                "environment": {
                    "backend": "real SSH login node",
                    "host": display_host,
                    "workload": "sleep-only; no CPU/GPU-intensive work",
                    "workload_seconds": workload_seconds,
                },
                "submit": {
                    "job_id": job_id,
                    "state": submit_value["state"],
                    "backend": submit_value["backend"],
                    "wall_seconds": round(submit_elapsed, 3),
                },
                "resume": {
                    "new_client": True,
                    "wall_seconds": round(wait_elapsed, 3),
                    "state": wait_value["state"],
                    "exit_code": wait_value["exit_code"],
                    "duration_seconds": wait_value["duration_seconds"],
                    "stdout_tail": wait_value["stdout_tail"],
                    "parsed_results": wait_value["parsed_results"],
                },
                "checks": checks,
            }
            atomic_json(output, public_capture)
            note(f"verified capture written to {output}")
        finally:
            if job_id:
                cleanup = f"""set -eu
job_dir="$HOME/.awaitless/jobs/{job_id}"
artifact="$HOME/{artifact_remote}"
rm -rf -- "$job_dir"
rm -f -- "$artifact"
"""
                run(
                    [
                        "ssh",
                        "-o",
                        "BatchMode=yes",
                        "-o",
                        "GSSAPIAuthentication=no",
                        "-o",
                        "ConnectTimeout=10",
                        host,
                        "bash -s",
                    ],
                    input_text=cleanup,
                    timeout=30,
                    check=False,
                )


def ass_time(seconds: float) -> str:
    centiseconds = round(seconds * 100)
    hours, remainder = divmod(centiseconds, 360_000)
    minutes, remainder = divmod(remainder, 6_000)
    whole_seconds, fraction = divmod(remainder, 100)
    return f"{hours}:{minutes:02d}:{whole_seconds:02d}.{fraction:02d}"


def ass_escape(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("{", "\\{")
        .replace("}", "\\}")
        .replace("\n", "\\N")
    )


def render(capture_path: Path, output: Path) -> None:
    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg is required to render the demo GIF")
    if not FONT.is_file():
        raise RuntimeError(f"monospace font not found: {FONT}")
    capture_value = json.loads(capture_path.read_text(encoding="utf-8"))
    if not all(capture_value.get("checks", {}).values()):
        raise RuntimeError("refusing to render an unverified demo capture")

    job_id = capture_value["submit"]["job_id"]
    result = capture_value["resume"]
    display_host = capture_value["environment"]["host"]
    stages = [
        (0, 3, "Terminal", "$ # client A — submit an SSH job"),
        (
            3,
            9,
            "Terminal",
            "$ awaitless submit --host gpu --artifact demo-result.json --json -- ./run.sh\n"
            f"  connecting to {display_host} ...",
        ),
        (
            9,
            15,
            "Terminal",
            "$ awaitless submit --host gpu --artifact demo-result.json --json -- ./run.sh\n"
            f'{{"job_id":"{job_id}","state":"running","backend":"ssh"}}\n\n'
            "$ exit",
        ),
        (
            15,
            20,
            "Accent",
            "client A closed\n\nThe remote job keeps running with the same job_id.",
        ),
        (
            20,
            24,
            "Terminal",
            "$ # client B — only the persisted job_id is needed",
        ),
        (
            24,
            33,
            "Terminal",
            f"$ awaitless wait {job_id} --json\n\n"
            "  waiting ...  one blocking call, no agent polling",
        ),
        (
            33,
            42,
            "Success",
            f"$ awaitless wait {job_id} --json\n"
            "{\n"
            f'  "state": "{result["state"]}",\n'
            f'  "exit_code": {result["exit_code"]},\n'
            '  "stdout_tail": "phase=started ... phase=complete",\n'
            '  "parsed_results": {\n'
            '    "correctness": true, "artifact": "demo-result.json"\n'
            "  }\n"
            "}",
        ),
        (
            42,
            45,
            "Accent",
            "same job_id  •  new client  •  SSH Artifact recovered",
        ),
    ]
    ass_header = """[Script Info]
ScriptType: v4.00+
PlayResX: 1200
PlayResY: 675
WrapStyle: 2

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Terminal,DejaVu Sans Mono,23,&H00E5E7EB,&H00E5E7EB,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,0,0,7,92,92,105,1
Style: Accent,DejaVu Sans Mono,25,&H00F6C177,&H00F6C177,&H00000000,&H00000000,-1,0,0,0,100,100,0,0,1,0,0,7,92,92,125,1
Style: Success,DejaVu Sans Mono,23,&H0097E6A7,&H0097E6A7,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,0,0,7,92,92,92,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    ass_events = "".join(
        "Dialogue: 0,"
        f"{ass_time(start)},{ass_time(end)},{style},,0,0,0,,{ass_escape(text)}\n"
        for start, end, style, text in stages
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="awaitless-gif-", dir="/tmp") as temp:
        temporary = Path(temp)
        subtitles = temporary / "terminal.ass"
        subtitles.write_text(ass_header + ass_events, encoding="utf-8")
        font = str(FONT)
        filter_graph = (
            "[0:v]"
            "drawbox=x=45:y=35:w=1110:h=600:color=0x111827:t=fill,"
            "drawbox=x=45:y=35:w=1110:h=48:color=0x1F2937:t=fill,"
            f"drawtext=fontfile={font}:text='●  ●  ●':fontcolor=0x64748B:fontsize=18:x=68:y=50,"
            f"drawtext=fontfile={font}:text='awaitless — SSH recovery demo':fontcolor=0xCBD5E1:fontsize=18:x=386:y=50,"
            f"drawtext=fontfile={font}:text='real SSH capture • sleep-only workload • no CPU/GPU-intensive work':fontcolor=0x64748B:fontsize=15:x=278:y=647,"
            f"subtitles=filename={subtitles}:fontsdir={FONT.parent},"
            "split[frames][palette_input];"
            "[palette_input]palettegen=stats_mode=diff[palette];"
            "[frames][palette]paletteuse=dither=bayer:bayer_scale=3"
        )
        run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "color=c=0x070B14:s=1200x675:d=45:r=8",
                "-filter_complex",
                filter_graph,
                "-loop",
                "0",
                "-y",
                str(output),
            ],
            timeout=180,
        )
    note(f"45-second GIF written to {output}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-host", help="OpenSSH host or alias for a real capture")
    parser.add_argument("--display-host", default="gpu-login")
    parser.add_argument("--workload-seconds", type=float, default=18)
    parser.add_argument("--capture", type=Path, default=DEFAULT_CAPTURE)
    parser.add_argument("--output", type=Path, default=DEFAULT_GIF)
    parser.add_argument("--render-only", action="store_true")
    args = parser.parse_args()
    if args.workload_seconds <= 0:
        parser.error("--workload-seconds must be positive")
    if args.render_only and args.capture_host:
        parser.error("--render-only cannot be combined with --capture-host")
    if not args.render_only:
        if not args.capture_host:
            parser.error("--capture-host is required unless --render-only is used")
        capture(args.capture_host, args.display_host, args.capture, args.workload_seconds)
    render(args.capture, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
