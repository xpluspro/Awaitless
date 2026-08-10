from __future__ import annotations

import argparse
from contextlib import contextmanager
from typing import Any, Iterator, Literal

from mcp.server.mcpserver import MCPServer

from . import __version__
from .config import load_settings
from .service import AwaitlessError, Service
from .util import new_job_id


_config_path: str | None = None

server = MCPServer(
    name="awaitless",
    title="Awaitless",
    description="Durable local, SSH, and Slurm jobs for AI agents",
    instructions=(
        "Use submit_job once, retain the returned job_id, then use wait_for_job. "
        "A client disconnect never cancels the submitted job."
    ),
    version=__version__,
)


@contextmanager
def _service() -> Iterator[Service]:
    service = Service(load_settings(_config_path))
    try:
        yield service
    finally:
        service.close()


def _selected_target(
    service: Service,
    backend: Literal["local", "ssh", "slurm"] | None,
    host: str | None,
) -> tuple[str, str | None]:
    selected_host = host or (
        None if backend == "local" else service.settings.default_host
    )
    if backend:
        return backend, selected_host
    if selected_host:
        selected_backend = service.settings.hosts.get(selected_host, {}).get(
            "backend", "ssh"
        )
        return str(selected_backend), selected_host
    return service.settings.default_backend, None


@server.tool()
def submit_job(
    command: list[str],
    backend: Literal["local", "ssh", "slurm"] | None = None,
    host: str | None = None,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    timeout_seconds: float | None = None,
    stall_timeout_seconds: float | None = None,
    name: str | None = None,
    artifacts: list[str] | None = None,
    slurm_options: dict[str, str | int | float] | None = None,
) -> dict[str, Any]:
    """Submit a durable job and return its stable ID without waiting.

    Omitted backend and host values use the Awaitless configuration defaults.
    Slurm options may contain account, constraint, cpus_per_task, gres, mem,
    nodes, ntasks, partition, qos, or time. Cluster config supplies defaults.
    """
    with _service() as service:
        selected, selected_host = _selected_target(service, backend, host)
        return service.submit(
            job_id=new_job_id(),
            command=command,
            backend=selected,
            host=selected_host,
            cwd=cwd,
            env=env or {},
            timeout_seconds=timeout_seconds,
            stall_timeout_seconds=stall_timeout_seconds,
            name=name,
            artifacts=artifacts or [],
            backend_options=slurm_options or {},
        )


@server.tool()
def wait_for_job(job_id: str, timeout_seconds: float | None = None) -> dict[str, Any]:
    """Wait for a durable job and return state, exit code, bounded logs, and Artifacts."""
    with _service() as service:
        result, _ = service.wait(job_id, timeout_seconds)
        return result


@server.tool()
def get_job_status(job_id: str) -> dict[str, Any]:
    """Get the latest durable state for one job."""
    with _service() as service:
        return service.status(job_id)


@server.tool()
def get_job_logs(
    job_id: str, tail: int | None = None, max_bytes: int | None = None
) -> dict[str, Any]:
    """Return bounded stdout and stderr tails for one job."""
    with _service() as service:
        selected_tail = service.settings.log_tail_lines if tail is None else tail
        selected_bytes = (
            service.settings.max_return_bytes if max_bytes is None else max_bytes
        )
        if selected_tail < 0 or selected_bytes <= 0:
            raise AwaitlessError("tail must be non-negative and max_bytes must be positive")
        return {
            **service.status(job_id),
            **service.logs(job_id, selected_tail, selected_bytes),
        }


@server.tool()
def cancel_job(job_id: str, grace_seconds: float = 5.0) -> dict[str, Any]:
    """Cancel a durable job without relying on a client-side process handle."""
    if grace_seconds < 0:
        raise AwaitlessError("grace_seconds must be non-negative")
    with _service() as service:
        return service.cancel(job_id, grace_seconds)


@server.tool()
def list_jobs(
    state: str | None = None, host: str | None = None, limit: int = 50
) -> dict[str, Any]:
    """List recent durable jobs, optionally filtered by state or host."""
    if limit <= 0 or limit > 500:
        raise AwaitlessError("limit must be between 1 and 500")
    with _service() as service:
        jobs = service.list(state=state, host=host, limit=limit)
        return {"jobs": jobs, "count": len(jobs)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Awaitless stdio MCP server")
    parser.add_argument("--config", help="Awaitless configuration TOML path")
    args = parser.parse_args(argv)
    global _config_path
    _config_path = args.config
    server.run(transport="stdio")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
