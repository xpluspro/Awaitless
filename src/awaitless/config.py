from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .constants import DEFAULT_MAX_RETURN_BYTES, DEFAULT_POLL_INTERVAL, DEFAULT_TAIL_LINES


@dataclass
class Settings:
    data_dir: Path
    default_backend: str = "local"
    log_tail_lines: int = DEFAULT_TAIL_LINES
    max_return_bytes: int = DEFAULT_MAX_RETURN_BYTES
    poll_interval: float = DEFAULT_POLL_INTERVAL
    hosts: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def db_path(self) -> Path:
        return self.data_dir / "awaitless.db"

    @property
    def jobs_dir(self) -> Path:
        return self.data_dir / "jobs"


def load_settings(config_path: str | None = None) -> Settings:
    data_dir = Path(
        os.environ.get("AWAITLESS_DATA_DIR", "~/.local/share/awaitless")
    ).expanduser().resolve()
    path = Path(config_path).expanduser() if config_path else Path(
        os.environ.get("AWAITLESS_CONFIG", "~/.config/awaitless/config.toml")
    ).expanduser()
    raw: dict[str, Any] = {}
    if path.exists():
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    defaults = raw.get("defaults", {})
    configured_data_dir = defaults.get("data_dir")
    if configured_data_dir:
        data_dir = Path(configured_data_dir).expanduser().resolve()
    settings = Settings(
        data_dir=data_dir,
        default_backend=str(defaults.get("backend", "local")),
        log_tail_lines=int(defaults.get("log_tail_lines", DEFAULT_TAIL_LINES)),
        max_return_bytes=int(defaults.get("max_return_bytes", DEFAULT_MAX_RETURN_BYTES)),
        poll_interval=float(defaults.get("poll_interval", DEFAULT_POLL_INTERVAL)),
        hosts=dict(raw.get("hosts", {})),
    )
    settings.jobs_dir.mkdir(parents=True, exist_ok=True)
    return settings


def ssh_target_and_options(settings: Settings, host: str) -> tuple[str, list[str], str]:
    cfg = settings.hosts.get(host, {})
    hostname = str(cfg.get("hostname", host))
    user = cfg.get("user")
    target = f"{user}@{hostname}" if user else hostname
    options: list[str] = []
    if cfg.get("port"):
        options += ["-p", str(cfg["port"])]
    if cfg.get("identity_file"):
        options += ["-i", str(Path(str(cfg["identity_file"])).expanduser())]
    options += ["-o", "BatchMode=yes"]
    remote_root = str(cfg.get("remote_job_dir", "~/.awaitless/jobs"))
    return target, options, remote_root
