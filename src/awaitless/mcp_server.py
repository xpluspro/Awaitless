from __future__ import annotations

import argparse
from contextlib import contextmanager
from typing import Any, Iterator, Literal

from mcp.server.mcpserver import MCPServer

from . import __version__
from .config import load_settings
from .mcp_tasks import AwaitlessTasksExtension, RunJobArguments
from .service import AwaitlessError, Service
from .util import new_job_id


_config_path: str | None = None


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


def _submit_with_service(
    service: Service,
    *,
    command: list[str],
    backend: Literal["local", "ssh", "slurm"] | None,
    host: str | None,
    cwd: str | None,
    env: dict[str, str] | None,
    timeout_seconds: float | None,
    stall_timeout_seconds: float | None,
    name: str | None,
    artifacts: list[str] | None,
    slurm_options: dict[str, str | int | float] | None,
    client_request_id: str | None,
    queue: str | None,
    as_mcp_task: bool,
) -> dict[str, Any]:
    selected, selected_host = _selected_target(service, backend, host)
    task_ttl_ms = (
        max(1, round(service.settings.mcp_task_ttl_seconds * 1000))
        if as_mcp_task
        else None
    )
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
        client_request_id=client_request_id,
        mcp_task_ttl_ms=task_ttl_ms,
        queue_name=queue,
    )


def _submit_task(arguments: RunJobArguments) -> dict[str, Any]:
    with _service() as service:
        return _submit_with_service(
            service,
            **arguments.model_dump(),
            as_mcp_task=True,
        )


server = MCPServer(
    name="awaitless",
    title="Awaitless",
    description="Durable execution and completion feeds for coding agents across local, SSH, and Slurm",
    instructions=(
        "For MCP Tasks clients, call run_job with a stable client_request_id and retain "
        "the returned taskId. Other clients can use submit_job plus wait_for_job. "
        "For multiple independent jobs, use wait_for_completions and advance its "
        "durable cursor only after processing each batch. "
        "A client disconnect never cancels the submitted job. Use a preconfigured "
        "named queue when work must wait for scarce local or SSH capacity."
    ),
    version=__version__,
    extensions=[AwaitlessTasksExtension(_service, _submit_task)],
)


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
    client_request_id: str | None = None,
    queue: str | None = None,
) -> dict[str, Any]:
    """Submit a durable job and return its stable ID without waiting.

    Omitted backend and host values use the Awaitless configuration defaults.
    Reuse client_request_id only when retrying the same logical submission; an
    identical retry returns the original job and a conflicting retry is rejected.
    A named queue provides FIFO, non-preemptive admission for local or SSH work.
    Slurm options may contain account, constraint, cpus_per_task, gres, mem,
    nodes, ntasks, partition, qos, or time. Cluster config supplies defaults.
    """
    with _service() as service:
        return _submit_with_service(
            service,
            command=command,
            backend=backend,
            host=host,
            cwd=cwd,
            env=env,
            timeout_seconds=timeout_seconds,
            stall_timeout_seconds=stall_timeout_seconds,
            name=name,
            artifacts=artifacts,
            slurm_options=slurm_options,
            client_request_id=client_request_id,
            queue=queue,
            as_mcp_task=False,
        )


@server.tool()
def run_job(
    command: list[str],
    client_request_id: str,
    backend: Literal["local", "ssh", "slurm"] | None = None,
    host: str | None = None,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    timeout_seconds: float | None = None,
    stall_timeout_seconds: float | None = None,
    name: str | None = None,
    artifacts: list[str] | None = None,
    slurm_options: dict[str, str | int | float] | None = None,
    queue: str | None = None,
) -> dict[str, Any]:
    """Run one durable job.

    A client declaring io.modelcontextprotocol/tasks receives a Task handle
    immediately. Older clients block and receive the ordinary final tool result.
    The stable client_request_id makes a lost creation response safe to retry.
    """
    with _service() as service:
        submitted = _submit_with_service(
            service,
            command=command,
            backend=backend,
            host=host,
            cwd=cwd,
            env=env,
            timeout_seconds=timeout_seconds,
            stall_timeout_seconds=stall_timeout_seconds,
            name=name,
            artifacts=artifacts,
            slurm_options=slurm_options,
            client_request_id=client_request_id,
            queue=queue,
            as_mcp_task=True,
        )
        result, _ = service.wait(submitted["job_id"])
        return result


@server.tool()
def wait_for_job(job_id: str, timeout_seconds: float | None = None) -> dict[str, Any]:
    """Wait for a durable job and return state, exit code, bounded logs, and Artifacts."""
    with _service() as service:
        result, _ = service.wait(job_id, timeout_seconds)
        return result


@server.tool()
def wait_for_completions(
    job_ids: list[str],
    after_cursor: str | None = None,
    timeout_seconds: float | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """Return durable terminal results across multiple jobs after a cursor.

    Existing completions return immediately. Otherwise the call waits until at
    least one selected job completes or the optional call-level timeout expires.
    Reusing the same cursor replays results; advancing to next_cursor consumes
    the returned batch. A timeout never cancels a managed job.
    """
    with _service() as service:
        return service.completions(
            job_ids,
            after_cursor=after_cursor,
            wait_timeout=timeout_seconds,
            limit=limit,
        )


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
    state: str | None = None,
    host: str | None = None,
    queue: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    """List recent durable jobs, optionally filtered by state or host."""
    if limit <= 0 or limit > 500:
        raise AwaitlessError("limit must be between 1 and 500")
    with _service() as service:
        jobs = service.list(state=state, host=host, queue_name=queue, limit=limit)
        return {"jobs": jobs, "count": len(jobs)}


@server.tool()
def create_queue(name: str, concurrency: int) -> dict[str, Any]:
    """Create an immutable named FIFO queue with a fixed concurrency limit."""
    with _service() as service:
        return service.create_queue(name, concurrency)


@server.tool()
def list_queues() -> dict[str, Any]:
    """List named queues and their current queued/active job counts."""
    with _service() as service:
        queues = service.list_queues()
        return {"queues": queues, "count": len(queues)}


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
