from __future__ import annotations

import argparse
from contextlib import contextmanager
from typing import Any, Iterator, Literal

from mcp.server.mcpserver import MCPServer

from . import __version__
from .config import adaptive_queue, load_settings
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
    use_default_queue: bool = False,
    capture_logs: list[str] | None = None,
    resources: dict[str, str] | None = None,
) -> dict[str, Any]:
    selected, selected_host = _selected_target(service, backend, host)
    selected_queue = (
        adaptive_queue(
            service.settings,
            backend=selected,
            host=selected_host,
            explicit_queue=queue,
        )
        if use_default_queue
        else queue
    )
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
        queue_name=selected_queue,
        capture_logs=capture_logs,
        resources=resources,
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
    description=(
        "Durable execution and named concurrency queues for coding agents across "
        "local, SSH, and Slurm"
    ),
    instructions=(
        "Tool choice: use run by default when starting one non-interactive command, "
        "regardless of uncertain duration. Use submit_job only for explicit asynchronous "
        "submission or fan-out. Use run_job only when an MCP Tasks client explicitly "
        "needs a Task handle. For an existing job_id, never submit again: use wait_for_job "
        "to consume one result, get_job_status for one immediate non-waiting snapshot, or "
        "get_job_logs for bounded failure diagnostics. For multiple submitted jobs, use "
        "wait_for_completions and advance its durable cursor only after processing each batch. "
        "A client disconnect never cancels the submitted job. Use a preconfigured "
        "named queue when work must wait for scarce local or SSH capacity."
    ),
    version=__version__,
    extensions=[AwaitlessTasksExtension(_service, _submit_task)],
)


@server.tool()
def run(
    command: list[str],
    backend: Literal["local", "ssh", "slurm"] | None = None,
    host: str | None = None,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
    timeout_seconds: float | None = None,
    stall_timeout_seconds: float | None = None,
    inline_timeout_seconds: float | None = None,
    name: str | None = None,
    artifacts: list[str] | None = None,
    slurm_options: dict[str, str | int | float] | None = None,
    client_request_id: str | None = None,
    queue: str | None = None,
    capture_logs: list[str] | None = None,
    resources: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Default tool for starting one non-interactive command of uncertain duration.

    Every command is a durable Job from launch. Commands that finish within the
    inline timeout return their ordinary bounded result. Longer or queued work
    returns a detached Job handle without cancelling the workload. Omit queue to
    use an operator-configured default queue for the selected target.

    Do not use this tool for explicit fire-and-forget or batch fan-out; use
    submit_job. Do not use it when an MCP Tasks handle is explicitly required;
    use run_job. Do not use it to resume an existing job_id; wait for or inspect
    that job instead. If a detached handle is returned, keep its job_id and call
    wait_for_job once when the result is needed rather than polling status.
    """
    with _service() as service:
        selected_inline_timeout = (
            service.settings.adaptive_inline_timeout_seconds
            if inline_timeout_seconds is None
            else inline_timeout_seconds
        )
        if selected_inline_timeout < 0:
            raise AwaitlessError("inline_timeout_seconds must be non-negative")
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
            as_mcp_task=False,
            use_default_queue=True,
            capture_logs=capture_logs,
            resources=resources,
        )
        return service.adaptive_wait(
            submitted["job_id"],
            selected_inline_timeout,
            detach_immediately=submitted["state"] == "queued",
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
    capture_logs: list[str] | None = None,
    resources: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Explicitly submit one asynchronous or fan-out job without waiting.

    Omitted backend and host values use the Awaitless configuration defaults.
    Reuse client_request_id only when retrying the same logical submission; an
    identical retry returns the original job and a conflicting retry is rejected.
    A named queue provides FIFO, non-preemptive admission for local or SSH work.
    Slurm options may contain account, constraint, cpus_per_task, gres, mem,
    nodes, ntasks, partition, qos, or time. Cluster config supplies defaults.

    Do not use this as the default for a single command with uncertain duration;
    use run. Do not use it for MCP Tasks creation; use run_job. Do not resubmit
    merely because a client disconnected or a wait timed out: keep the original
    job_id, or retry the identical logical submission with the same
    client_request_id if the creation response was lost.
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
            capture_logs=capture_logs,
            resources=resources,
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
    capture_logs: list[str] | None = None,
    resources: dict[str, str] | None = None,
) -> dict[str, Any]:
    """MCP Tasks compatibility entry point for explicitly creating a Task handle.

    A client declaring io.modelcontextprotocol/tasks receives a Task handle
    immediately. Older clients block and receive the ordinary final tool result.
    The stable client_request_id makes a lost creation response safe to retry.

    Do not choose this for ordinary command execution: use run. Do not choose it
    for generic asynchronous submission or fan-out: use submit_job. Only retry a
    lost Task creation with the same client_request_id and identical arguments.
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
            capture_logs=capture_logs,
            resources=resources,
        )
        result, _ = service.wait(submitted["job_id"])
        return result


@server.tool()
def wait_for_job(job_id: str, timeout_seconds: float | None = None) -> dict[str, Any]:
    """Wait once for a known job_id and consume its durable terminal result.

    Returns state, exit code, bounded logs, and declared Artifacts. A client-side
    timeout or disconnect does not cancel the Job; call this tool again later with
    the same job_id. Do not use it to start work, do not poll it repeatedly, and
    use wait_for_completions instead when collecting several independent Jobs.
    """
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
    """Collect durable terminal results across multiple known jobs after a cursor.

    Existing completions return immediately. Otherwise the call waits until at
    least one selected job completes or the optional call-level timeout expires.
    Reusing the same cursor replays results; advancing to next_cursor consumes
    the returned batch. A timeout never cancels a managed job.

    Submit all independent jobs before calling this tool. Treat delivery as
    at-least-once: process and deduplicate by completion_id before advancing the
    cursor. Do not use this for one job, do not poll get_job_status between
    continuation calls, and never resubmit active jobs after a waiter disconnects.
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
    """Get one immediate, non-waiting state snapshot for a known job_id.

    Use only when the caller needs current status now. Do not use this to wait for
    completion or build a polling loop; use wait_for_job for one Job or
    wait_for_completions for several. This tool never starts or retries work.
    """
    with _service() as service:
        return service.status(job_id)


@server.tool()
def get_job_logs(
    job_id: str, tail: int | None = None, max_bytes: int | None = None
) -> dict[str, Any]:
    """Inspect bounded stdout and stderr tails for a known failed or stalled job.

    Use after a terminal wait reports failure, timeout, stall, or loss and its
    bounded result needs focused diagnostics. Do not use as a progress stream,
    do not repeatedly tail a running healthy Job, and do not use it instead of
    wait_for_job to learn that work completed.
    """
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
