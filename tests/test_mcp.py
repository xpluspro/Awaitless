from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

try:
    from mcp import Client, ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    from mcp.shared.exceptions import MCPError

    from awaitless.mcp_server import server as inprocess_server
    from awaitless.mcp_tasks import (
        TASKS_EXTENSION_ID,
        TASKS_MISSING_CAPABILITY,
        AwaitlessTasksClientExtension,
        CancelTaskParams,
        CancelTaskRequest,
        CreateTaskResult,
        GetTaskParams,
        GetTaskRequest,
        TaskAck,
        TaskResult,
    )
except ModuleNotFoundError:
    Client = None  # type: ignore[assignment,misc]
    ClientSession = None  # type: ignore[assignment,misc]
    StdioServerParameters = None  # type: ignore[assignment,misc]
    stdio_client = None  # type: ignore[assignment]


@unittest.skipUnless(ClientSession is not None, "mcp dependency is not installed")
class MCPServerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.work = self.root / "work"
        self.work.mkdir()
        config = self.root / "config.toml"
        config.write_text("[defaults]\npoll_interval = 0.02\n", encoding="utf-8")
        self.environment = {
            **os.environ,
            "AWAITLESS_CONFIG": str(config),
            "AWAITLESS_DATA_DIR": str(self.root / "data"),
        }
        self.parameters = StdioServerParameters(
            command=sys.executable,
            args=["-m", "awaitless.mcp_server"],
            env=self.environment,
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    async def call(self, name: str, arguments: dict[str, Any]) -> Any:
        assert stdio_client is not None
        assert ClientSession is not None
        async with stdio_client(self.parameters) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(name, arguments)
                self.assertFalse(result.is_error, result.content)
                return result.structured_content

    async def test_tools_submit_disconnect_and_resume_with_artifact(self) -> None:
        assert stdio_client is not None
        assert ClientSession is not None
        async with stdio_client(self.parameters) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                self.assertEqual(
                    {tool.name for tool in tools.tools},
                    {
                        "create_queue",
                        "run",
                        "submit_job",
                        "run_job",
                        "wait_for_job",
                        "wait_for_completions",
                        "get_job_status",
                        "get_job_logs",
                        "cancel_job",
                        "list_jobs",
                        "list_queues",
                    },
                )
                descriptions = {tool.name: tool.description or "" for tool in tools.tools}
                required_guidance = {
                    "run": ("Default tool", "Do not use this tool for explicit"),
                    "submit_job": ("asynchronous or fan-out", "Do not use this as the default"),
                    "run_job": ("MCP Tasks compatibility", "Do not choose this for ordinary"),
                    "wait_for_job": ("Wait once for a known job_id", "do not poll it repeatedly"),
                    "wait_for_completions": ("multiple known jobs", "at-least-once"),
                    "get_job_status": ("immediate, non-waiting", "Do not use this to wait"),
                    "get_job_logs": ("failed or stalled", "Do not use as a progress stream"),
                }
                for name, phrases in required_guidance.items():
                    for phrase in phrases:
                        self.assertIn(phrase, descriptions[name], f"{name}: {phrase}")
                created_queue = await session.call_tool(
                    "create_queue", {"name": "mcp-gpu", "concurrency": 1}
                )
                self.assertFalse(created_queue.is_error, created_queue.content)
                self.assertTrue(created_queue.structured_content["created"])

                inline = await session.call_tool(
                    "run",
                    {
                        "command": [sys.executable, "-c", "print('adaptive-inline')"],
                        "inline_timeout_seconds": 1,
                    },
                )
                self.assertFalse(inline.is_error, inline.content)
                self.assertEqual(inline.structured_content["delivery"], "inline")
                self.assertFalse(inline.structured_content["detached"])
                self.assertEqual(
                    inline.structured_content["stdout_tail"], "adaptive-inline\n"
                )

                detached = await session.call_tool(
                    "run",
                    {
                        "command": [
                            sys.executable,
                            "-c",
                            "import time; print('adaptive-detached', flush=True); time.sleep(.3)",
                        ],
                        "inline_timeout_seconds": 0.05,
                    },
                )
                self.assertFalse(detached.is_error, detached.content)
                self.assertEqual(detached.structured_content["delivery"], "detached")
                self.assertTrue(detached.structured_content["detached"])
                self.assertEqual(
                    detached.structured_content["detach_reason"], "inline_timeout"
                )
                detached_job_id = detached.structured_content["job_id"]
                source = (
                    "from pathlib import Path; import time; "
                    "time.sleep(0.2); "
                    "Path('result.json').write_text('"
                    + json.dumps({"agent_native": True}).replace('"', '\\"')
                    + "'); print('mcp-resume-ok')"
                )
                submitted = await session.call_tool(
                    "submit_job",
                    {
                        "command": [sys.executable, "-c", source],
                        "cwd": str(self.work),
                        "artifacts": ["result.json"],
                        "queue": "mcp-gpu",
                    },
                )
                self.assertFalse(submitted.is_error, submitted.content)
                job_id = submitted.structured_content["job_id"]

        completed = await asyncio.wait_for(
            self.call("wait_for_job", {"job_id": job_id}), timeout=15
        )
        self.assertEqual(completed["state"], "succeeded")
        self.assertEqual(completed["exit_code"], 0)
        self.assertEqual(completed["stdout_tail"], "mcp-resume-ok\n")
        self.assertEqual(completed["parsed_results"], {"agent_native": True})
        adaptive_completed = await asyncio.wait_for(
            self.call("wait_for_job", {"job_id": detached_job_id}), timeout=15
        )
        self.assertEqual(adaptive_completed["state"], "succeeded")
        self.assertEqual(
            adaptive_completed["stdout_tail"], "adaptive-detached\n"
        )

        status = await self.call("get_job_status", {"job_id": job_id})
        self.assertEqual(status["state"], "succeeded")
        logs = await self.call(
            "get_job_logs", {"job_id": job_id, "tail": 1, "max_bytes": 1024}
        )
        self.assertEqual(logs["stdout_tail"], "mcp-resume-ok\n")

        listed = await self.call("list_jobs", {"limit": 10})
        self.assertEqual(listed["jobs"][0]["job_id"], job_id)
        self.assertEqual(listed["count"], 3)
        queues = await self.call("list_queues", {})
        self.assertEqual(queues["queues"][0]["name"], "mcp-gpu")
        self.assertEqual(queues["queues"][0]["total_jobs"], 1)

        cancellable = await self.call(
            "submit_job",
            {
                "command": [sys.executable, "-c", "import time; time.sleep(30)"],
                "name": "mcp-cancel-test",
            },
        )
        cancelled = await self.call(
            "cancel_job",
            {"job_id": cancellable["job_id"], "grace_seconds": 0.05},
        )
        self.assertEqual(cancelled["state"], "cancelled")

    async def test_completion_feed_survives_stdio_disconnects(self) -> None:
        assert stdio_client is not None
        assert ClientSession is not None
        job_ids: list[str] = []
        async with stdio_client(self.parameters) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                for label, delay in (("alpha", 0.15), ("beta", 0.25)):
                    source = (
                        "from pathlib import Path; import json,time; "
                        f"time.sleep({delay}); "
                        f"Path('{label}.json').write_text(json.dumps({{'label': {label!r}}})); "
                        f"print({label!r})"
                    )
                    submitted = await session.call_tool(
                        "submit_job",
                        {
                            "command": [sys.executable, "-c", source],
                            "cwd": str(self.work),
                            "artifacts": [f"{label}.json"],
                        },
                    )
                    self.assertFalse(submitted.is_error, submitted.content)
                    job_ids.append(submitted.structured_content["job_id"])

        cursor: str | None = None
        recovered: list[dict[str, Any]] = []
        while True:
            arguments: dict[str, Any] = {
                "job_ids": job_ids,
                "timeout_seconds": 5,
                "limit": 1,
            }
            if cursor is not None:
                arguments["after_cursor"] = cursor
            batch = await asyncio.wait_for(
                self.call("wait_for_completions", arguments), timeout=15
            )
            recovered.extend(batch["completions"])
            cursor = batch["next_cursor"]
            if not batch["active_job_ids"] and not batch["has_more"]:
                break

        self.assertEqual({item["job_id"] for item in recovered}, set(job_ids))
        self.assertEqual(len({item["completion_id"] for item in recovered}), 2)
        self.assertEqual(
            {item["result"]["parsed_results"]["label"] for item in recovered},
            {"alpha", "beta"},
        )
        replay = await self.call(
            "wait_for_completions",
            {"job_ids": job_ids, "timeout_seconds": 0, "limit": 50},
        )
        self.assertEqual(len(replay["completions"]), 2)
        drained = await self.call(
            "wait_for_completions",
            {
                "job_ids": job_ids,
                "after_cursor": cursor,
                "timeout_seconds": 0,
            },
        )
        self.assertEqual(drained["completions"], [])
        self.assertFalse(drained["wait_timed_out"])

    async def test_adaptive_run_uses_operator_default_queue(self) -> None:
        created = await self.call(
            "create_queue", {"name": "default-gpu", "concurrency": 1}
        )
        self.assertTrue(created["created"])
        Path(self.environment["AWAITLESS_CONFIG"]).write_text(
            "[defaults]\n"
            "poll_interval = 0.02\n"
            "queue = \"default-gpu\"\n"
            "adaptive_inline_timeout_seconds = 1\n",
            encoding="utf-8",
        )
        routed = await self.call(
            "run",
            {"command": [sys.executable, "-c", "print('default-queue')"]},
        )
        self.assertEqual(routed["queue"], "default-gpu")
        self.assertEqual(routed["delivery"], "detached")
        self.assertTrue(routed["detached"])
        self.assertEqual(routed["detach_reason"], "queued")
        self.assertEqual(routed["inline_timeout_seconds"], 1)
        completed = await self.call(
            "wait_for_job", {"job_id": routed["job_id"]}
        )
        self.assertEqual(completed["state"], "succeeded")
        self.assertEqual(completed["stdout_tail"], "default-queue\n")

    async def test_tasks_extension_disconnect_replay_and_inline_result(self) -> None:
        assert Client is not None
        arguments = {
            "command": [
                sys.executable,
                "-c",
                (
                    "from pathlib import Path; import time; time.sleep(.5); "
                    "Path('task-result.json').write_text('{\"tasks\": true}'); "
                    "print('tasks-resume-ok')"
                ),
            ],
            "client_request_id": "mcp-task:disconnect:1",
            "cwd": str(self.work),
            "artifacts": ["task-result.json"],
        }
        with patch.dict(os.environ, self.environment, clear=False):
            async with Client(
                inprocess_server,
                mode="auto",
                extensions=[AwaitlessTasksClientExtension()],
            ) as client:
                self.assertIn(
                    TASKS_EXTENSION_ID, client.server_capabilities.extensions or {}
                )
                created = await client.session.call_tool(
                    "run_job", arguments, allow_claimed=True
                )
                self.assertIsInstance(created, CreateTaskResult)
                assert isinstance(created, CreateTaskResult)
                self.assertEqual(created.result_type, "task")
                self.assertEqual(created.status, "working")
                task_id = created.task_id

            # A server policy change is not a change to the client's logical
            # request. The replay must still resolve to the original task.
            Path(self.environment["AWAITLESS_CONFIG"]).write_text(
                "[defaults]\npoll_interval = 0.02\nmcp_task_ttl_seconds = 300\n",
                encoding="utf-8",
            )

            # A fresh client retries the creation request after a hypothetical
            # lost response. The idempotency key must resolve to the same task.
            async with Client(
                inprocess_server,
                mode="auto",
                extensions=[AwaitlessTasksClientExtension()],
            ) as resumed:
                replay = await resumed.session.call_tool(
                    "run_job", arguments, allow_claimed=True
                )
                self.assertIsInstance(replay, CreateTaskResult)
                assert isinstance(replay, CreateTaskResult)
                self.assertEqual(replay.task_id, task_id)

                while True:
                    current = await resumed.session.send_request(
                        GetTaskRequest(params=GetTaskParams(task_id=task_id)),
                        TaskResult,
                        request_read_timeout_seconds=5,
                    )
                    if current.status != "working":
                        break
                    await asyncio.sleep(0.02)
                self.assertEqual(current.status, "completed")
                assert current.result is not None
                structured = current.result["structuredContent"]
                self.assertEqual(structured["state"], "succeeded")
                self.assertEqual(structured["stdout_tail"], "tasks-resume-ok\n")
                self.assertEqual(structured["parsed_results"], {"tasks": True})
                resolved = await resumed.call_tool("run_job", arguments)
                self.assertFalse(resolved.is_error)
                self.assertEqual(resolved.structured_content["job_id"], task_id)

    async def test_tasks_capability_gate_blocking_fallback_cancel_and_ttl(self) -> None:
        assert Client is not None
        with patch.dict(os.environ, self.environment, clear=False):
            async with Client(inprocess_server, mode="auto") as legacy:
                with self.assertRaises(MCPError) as missing:
                    await legacy.session.send_request(
                        GetTaskRequest(params=GetTaskParams(task_id="job_unknown")),
                        TaskResult,
                    )
                self.assertEqual(missing.exception.code, TASKS_MISSING_CAPABILITY)

                blocking = await legacy.call_tool(
                    "run_job",
                    {
                        "command": [sys.executable, "-c", "print('blocking-fallback')"],
                        "client_request_id": "mcp-task:blocking-fallback",
                    },
                )
                self.assertFalse(blocking.is_error)
                self.assertEqual(blocking.structured_content["state"], "succeeded")

            async with Client(
                inprocess_server,
                mode="auto",
                extensions=[AwaitlessTasksClientExtension()],
            ) as tasks_client:
                created = await tasks_client.session.call_tool(
                    "run_job",
                    {
                        "command": [sys.executable, "-c", "import time; time.sleep(30)"],
                        "client_request_id": "mcp-task:cancel",
                    },
                    allow_claimed=True,
                )
                assert isinstance(created, CreateTaskResult)
                ack = await tasks_client.session.send_request(
                    CancelTaskRequest(
                        params=CancelTaskParams(task_id=created.task_id)
                    ),
                    TaskAck,
                )
                self.assertEqual(ack.result_type, "complete")
                cancelled = await tasks_client.session.send_request(
                    GetTaskRequest(params=GetTaskParams(task_id=created.task_id)),
                    TaskResult,
                )
                self.assertEqual(cancelled.status, "cancelled")

            config = Path(self.environment["AWAITLESS_CONFIG"])
            config.write_text(
                "[defaults]\n"
                "poll_interval = 0.02\n"
                "mcp_task_poll_interval_seconds = 0.02\n"
                "mcp_task_ttl_seconds = 0.6\n",
                encoding="utf-8",
            )
            async with Client(
                inprocess_server,
                mode="auto",
                extensions=[AwaitlessTasksClientExtension()],
            ) as ttl_client:
                expiring = await ttl_client.session.call_tool(
                    "run_job",
                    {
                        "command": [sys.executable, "-c", "print('expires')"],
                        "client_request_id": "mcp-task:ttl",
                    },
                    allow_claimed=True,
                )
                assert isinstance(expiring, CreateTaskResult)
                self.assertEqual(expiring.ttl_ms, 600)
                await asyncio.sleep(0.65)
                with self.assertRaises(MCPError) as expired:
                    await ttl_client.session.send_request(
                        GetTaskRequest(params=GetTaskParams(task_id=expiring.task_id)),
                        TaskResult,
                    )
                self.assertEqual(expired.exception.code, -32602)
                with self.assertRaises(MCPError) as expired_replay:
                    await ttl_client.session.call_tool(
                        "run_job",
                        {
                            "command": [sys.executable, "-c", "print('expires')"],
                            "client_request_id": "mcp-task:ttl",
                        },
                        allow_claimed=True,
                    )
                self.assertEqual(expired_replay.exception.code, -32602)


if __name__ == "__main__":
    unittest.main()
