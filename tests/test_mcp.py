from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

try:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ModuleNotFoundError:
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
                        "submit_job",
                        "wait_for_job",
                        "get_job_status",
                        "get_job_logs",
                        "cancel_job",
                        "list_jobs",
                    },
                )
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

        status = await self.call("get_job_status", {"job_id": job_id})
        self.assertEqual(status["state"], "succeeded")
        logs = await self.call(
            "get_job_logs", {"job_id": job_id, "tail": 1, "max_bytes": 1024}
        )
        self.assertEqual(logs["stdout_tail"], "mcp-resume-ok\n")

        listed = await self.call("list_jobs", {"limit": 10})
        self.assertEqual(listed["jobs"][0]["job_id"], job_id)
        self.assertEqual(listed["count"], 1)

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


if __name__ == "__main__":
    unittest.main()
