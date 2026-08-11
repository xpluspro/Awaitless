"""Experimental io.modelcontextprotocol/tasks compatibility for Awaitless.

The extension follows SEP-2663's 2026-07-28 wire shape instead of the removed
Python SDK 1.x experimental Tasks API. Awaitless job IDs are the durable task
handles, so reconnecting clients can query the same local, SSH, or Slurm job.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from functools import partial
from typing import Any, Literal

import anyio
from mcp.client.extension import ClaimContext, ClientExtension, ResultClaim
from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.server.extension import Extension, MethodBinding
from mcp.shared.exceptions import MCPError
from mcp_types import (
    CLIENT_CAPABILITIES_META_KEY,
    INVALID_PARAMS,
    CallToolRequestParams,
    CallToolResult,
    Request,
    RequestParams,
    Result,
    TextContent,
)
from mcp_types.version import MODERN_PROTOCOL_VERSIONS
from pydantic import BaseModel, ConfigDict, Field

from .constants import ACTIVE_STATES
from .service import JobNotFound, Service
from .util import parse_time


TASKS_EXTENSION_ID = "io.modelcontextprotocol/tasks"
TASKS_MISSING_CAPABILITY = -32003
TASKS_PROTOCOL_VERSIONS = frozenset(MODERN_PROTOCOL_VERSIONS)
TaskStatus = Literal["working", "input_required", "completed", "failed", "cancelled"]


class RunJobArguments(BaseModel):
    """Arguments shared by the blocking ``run_job`` tool and Tasks interceptor."""

    model_config = ConfigDict(extra="forbid")

    command: list[str] = Field(min_length=1)
    client_request_id: str = Field(min_length=1, max_length=200)
    backend: Literal["local", "ssh", "slurm"] | None = None
    host: str | None = None
    cwd: str | None = None
    env: dict[str, str] | None = None
    timeout_seconds: float | None = None
    stall_timeout_seconds: float | None = None
    name: str | None = None
    artifacts: list[str] | None = None
    slurm_options: dict[str, str | int | float] | None = None
    queue: str | None = None


class CreateTaskResult(Result):
    result_type: Literal["task"] = "task"
    task_id: str
    status: TaskStatus
    status_message: str | None = None
    created_at: str
    last_updated_at: str
    ttl_ms: int | None
    poll_interval_ms: int | None = None


class TaskResult(Result):
    result_type: Literal["complete"] = "complete"
    task_id: str
    status: TaskStatus
    status_message: str | None = None
    created_at: str
    last_updated_at: str
    ttl_ms: int | None
    poll_interval_ms: int | None = None
    input_requests: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None


class GetTaskParams(RequestParams):
    task_id: str


class GetTaskRequest(Request[GetTaskParams, Literal["tasks/get"]]):
    method: Literal["tasks/get"] = "tasks/get"
    params: GetTaskParams
    name_param = "taskId"


class UpdateTaskParams(RequestParams):
    task_id: str
    input_responses: dict[str, Any]


class UpdateTaskRequest(Request[UpdateTaskParams, Literal["tasks/update"]]):
    method: Literal["tasks/update"] = "tasks/update"
    params: UpdateTaskParams
    name_param = "taskId"


class CancelTaskParams(RequestParams):
    task_id: str


class CancelTaskRequest(Request[CancelTaskParams, Literal["tasks/cancel"]]):
    method: Literal["tasks/cancel"] = "tasks/cancel"
    params: CancelTaskParams
    name_param = "taskId"


class TaskAck(Result):
    result_type: Literal["complete"] = "complete"


ServiceFactory = Callable[[], AbstractContextManager[Service]]
TaskSubmitter = Callable[[RunJobArguments], dict[str, Any]]


class AwaitlessTasksExtension(Extension):
    """Server-side MCP Tasks extension backed by the Awaitless job store."""

    identifier = TASKS_EXTENSION_ID

    def __init__(self, service_factory: ServiceFactory, submitter: TaskSubmitter):
        self._service_factory = service_factory
        self._submitter = submitter

    def methods(self) -> Sequence[MethodBinding]:
        return (
            MethodBinding(
                method="tasks/get",
                params_type=GetTaskParams,
                handler=self._get_task,
                protocol_versions=TASKS_PROTOCOL_VERSIONS,
            ),
            MethodBinding(
                method="tasks/update",
                params_type=UpdateTaskParams,
                handler=self._update_task,
                protocol_versions=TASKS_PROTOCOL_VERSIONS,
            ),
            MethodBinding(
                method="tasks/cancel",
                params_type=CancelTaskParams,
                handler=self._cancel_task,
                protocol_versions=TASKS_PROTOCOL_VERSIONS,
            ),
        )

    async def intercept_tool_call(
        self,
        params: CallToolRequestParams,
        ctx: ServerRequestContext[Any, Any],
        call_next: CallNext,
    ) -> HandlerResult:
        if params.name != "run_job" or not _has_tasks_capability(params.meta):
            return await call_next(ctx)
        try:
            arguments = RunJobArguments.model_validate(params.arguments or {})
        except Exception as exc:
            raise MCPError(
                code=INVALID_PARAMS,
                message="Invalid run_job arguments",
                data={"detail": str(exc)},
            ) from exc
        submitted = await anyio.to_thread.run_sync(partial(self._submitter, arguments))
        # Creation (including an idempotent replay) must never return a handle
        # that tasks/get cannot immediately resolve.
        _ensure_not_expired(submitted)
        return CreateTaskResult(
            task_id=submitted["job_id"],
            status=_task_status(submitted["state"]),
            status_message=f"Awaitless job is {submitted['state']}",
            created_at=submitted["created_at"],
            last_updated_at=submitted["updated_at"],
            ttl_ms=submitted["mcp_task_ttl_ms"],
            poll_interval_ms=self._poll_interval_ms(),
        )

    async def _get_task(
        self, ctx: ServerRequestContext[Any, Any], params: GetTaskParams
    ) -> HandlerResult:
        _require_tasks_capability(ctx.meta)
        return await anyio.to_thread.run_sync(partial(self._get_task_sync, params.task_id))

    def _get_task_sync(self, task_id: str) -> TaskResult:
        with self._service_factory() as service:
            job = _require_task(service, task_id)
            _ensure_not_expired(job)
            summary = service.status(task_id)
            job = service.require(task_id)
            fields = _task_fields(job, service)
            if summary["state"] in ACTIVE_STATES:
                return TaskResult(
                    **fields,
                    status="working",
                    status_message=f"Awaitless job is {summary['state']}",
                )
            if summary["state"] == "cancelled":
                return TaskResult(
                    **fields,
                    status="cancelled",
                    status_message="Awaitless job was cancelled",
                )
            final, _ = service.wait(task_id, 0)
            return TaskResult(
                **fields,
                status="completed",
                status_message=f"Awaitless job finished with state {final['state']}",
                result=_call_tool_result(final),
            )

    async def _update_task(
        self, ctx: ServerRequestContext[Any, Any], params: UpdateTaskParams
    ) -> HandlerResult:
        _require_tasks_capability(ctx.meta)
        await anyio.to_thread.run_sync(partial(self._validate_known_task, params.task_id))
        # Awaitless command jobs never enter input_required. SEP-2663 says unknown
        # or already-satisfied input keys are ignored, so an empty ack is correct.
        return {"resultType": "complete"}

    async def _cancel_task(
        self, ctx: ServerRequestContext[Any, Any], params: CancelTaskParams
    ) -> HandlerResult:
        _require_tasks_capability(ctx.meta)
        await anyio.to_thread.run_sync(partial(self._cancel_task_sync, params.task_id))
        return {"resultType": "complete"}

    def _validate_known_task(self, task_id: str) -> None:
        with self._service_factory() as service:
            job = _require_task(service, task_id)
            _ensure_not_expired(job)

    def _cancel_task_sync(self, task_id: str) -> None:
        with self._service_factory() as service:
            job = _require_task(service, task_id)
            _ensure_not_expired(job)
            service.cancel(task_id, 5.0)

    def _poll_interval_ms(self) -> int:
        with self._service_factory() as service:
            return max(1, round(service.settings.mcp_task_poll_interval_seconds * 1000))


class AwaitlessTasksClientExtension(ClientExtension):
    """Small reference client adapter that resolves Awaitless Task handles."""

    identifier = TASKS_EXTENSION_ID

    def claims(self) -> Sequence[ResultClaim[Any]]:
        return (
            ResultClaim(
                result_type="task",
                model=CreateTaskResult,
                resolve=self._resolve,
                protocol_versions=TASKS_PROTOCOL_VERSIONS,
            ),
        )

    async def _resolve(
        self, task: CreateTaskResult, ctx: ClaimContext
    ) -> CallToolResult:
        poll_interval_ms = task.poll_interval_ms or 1000
        while True:
            current = await ctx.session.send_request(
                GetTaskRequest(params=GetTaskParams(task_id=task.task_id)),
                TaskResult,
                request_read_timeout_seconds=ctx.read_timeout_seconds,
            )
            if current.status == "completed" and current.result is not None:
                return CallToolResult.model_validate(current.result, by_name=False)
            if current.status == "cancelled":
                return CallToolResult(
                    content=[TextContent(type="text", text="Awaitless task was cancelled")],
                    is_error=True,
                )
            if current.status == "failed":
                return CallToolResult(
                    content=[
                        TextContent(
                            type="text",
                            text=json.dumps(current.error or {}, ensure_ascii=False),
                        )
                    ],
                    is_error=True,
                )
            if current.status == "input_required":
                return CallToolResult(
                    content=[
                        TextContent(
                            type="text",
                            text="Awaitless command tasks do not support input_required",
                        )
                    ],
                    is_error=True,
                )
            poll_interval_ms = current.poll_interval_ms or poll_interval_ms
            await anyio.sleep(max(0.001, poll_interval_ms / 1000))


def _has_tasks_capability(meta: dict[str, Any] | None) -> bool:
    capabilities = (meta or {}).get(CLIENT_CAPABILITIES_META_KEY)
    if isinstance(capabilities, BaseModel):
        extensions = getattr(capabilities, "extensions", None)
    elif isinstance(capabilities, dict):
        extensions = capabilities.get("extensions")
    else:
        return False
    return isinstance(extensions, dict) and TASKS_EXTENSION_ID in extensions


def _require_tasks_capability(meta: dict[str, Any] | None) -> None:
    if not _has_tasks_capability(meta):
        raise MCPError(
            code=TASKS_MISSING_CAPABILITY,
            message="Missing required client capability",
            data={"requiredCapabilities": {"extensions": {TASKS_EXTENSION_ID: {}}}},
        )


def _require_task(service: Service, task_id: str) -> dict[str, Any]:
    try:
        job = service.require(task_id)
    except JobNotFound as exc:
        raise MCPError(
            code=INVALID_PARAMS,
            message=f"Unknown task ID: {task_id}",
        ) from exc
    if job.get("mcp_task_ttl_ms") is None:
        raise MCPError(code=INVALID_PARAMS, message=f"Unknown task ID: {task_id}")
    return job


def _ensure_not_expired(job: dict[str, Any]) -> None:
    ttl_ms = job.get("mcp_task_ttl_ms")
    created = parse_time(job.get("created_at"))
    if ttl_ms is None or created is None:
        return
    age_ms = (datetime.now(timezone.utc) - created).total_seconds() * 1000
    if age_ms >= ttl_ms:
        raise MCPError(code=INVALID_PARAMS, message=f"Expired task ID: {job['job_id']}")


def _task_status(state: str) -> TaskStatus:
    if state in ACTIVE_STATES:
        return "working"
    if state == "cancelled":
        return "cancelled"
    return "completed"


def _task_fields(job: dict[str, Any], service: Service) -> dict[str, Any]:
    return {
        "task_id": job["job_id"],
        "created_at": job["created_at"],
        "last_updated_at": job["updated_at"],
        "ttl_ms": job.get("mcp_task_ttl_ms"),
        "poll_interval_ms": max(
            1, round(service.settings.mcp_task_poll_interval_seconds * 1000)
        ),
    }


def _call_tool_result(value: dict[str, Any]) -> dict[str, Any]:
    result = CallToolResult(
        content=[
            TextContent(
                type="text",
                text=json.dumps(value, ensure_ascii=False, separators=(",", ":")),
            )
        ],
        structured_content=value,
        is_error=False,
    )
    return result.model_dump(by_alias=True, mode="json", exclude_none=True)
