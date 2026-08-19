from __future__ import annotations

import json
import unittest
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10
    import tomli as tomllib

from awaitless import __version__
from awaitless.mcp_tasks import CreateTaskResult, TaskResult


ROOT = Path(__file__).resolve().parents[1]
MCP_NAME = "io.github.xpluspro/awaitless"


class ReleaseMetadataTest(unittest.TestCase):
    def test_versions_registry_identity_and_entry_point_stay_in_sync(self) -> None:
        project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        server = json.loads((ROOT / "server.json").read_text(encoding="utf-8"))
        plugin = json.loads(
            (ROOT / ".codex-plugin" / "plugin.json").read_text(encoding="utf-8")
        )
        plugin_mcp = json.loads((ROOT / ".mcp.json").read_text(encoding="utf-8"))
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")

        version = project["project"]["version"]
        self.assertEqual(version, __version__)
        self.assertEqual(server["version"], version)
        self.assertEqual(server["packages"][0]["version"], version)
        self.assertEqual(plugin["version"].partition("+")[0], version)
        self.assertEqual(plugin["name"], "awaitless")
        self.assertEqual(plugin["skills"], "./skills/")
        self.assertEqual(plugin["mcpServers"], "./.mcp.json")
        skill = (ROOT / "skills" / "awaitless" / "SKILL.md").read_text(encoding="utf-8")
        for tool in (
            "run", "submit_job", "run_job", "wait_for_job", "get_job_status",
            "get_job_logs", "wait_for_completions",
        ):
            self.assertIn(f"`{tool}`", skill)
        self.assertEqual(
            plugin_mcp["mcpServers"]["awaitless"],
            {"command": "uvx", "args": ["awaitless-runner"]},
        )
        self.assertEqual(server["name"], MCP_NAME)
        self.assertIn(f"<!-- mcp-name: {MCP_NAME} -->", readme)
        self.assertEqual(server["packages"][0]["identifier"], project["project"]["name"])
        self.assertIn(f"## {version} ", changelog)
        self.assertEqual(
            project["project"]["scripts"]["awaitless-runner"],
            "awaitless.mcp_server:main",
        )

    def test_tasks_models_serialize_with_wire_field_names(self) -> None:
        created = CreateTaskResult(
            task_id="job_test",
            status="working",
            created_at="2026-08-10T00:00:00Z",
            last_updated_at="2026-08-10T00:00:01Z",
            ttl_ms=1000,
            poll_interval_ms=20,
        ).model_dump(by_alias=True, mode="json", exclude_none=True)
        self.assertEqual(
            set(created),
            {
                "resultType",
                "taskId",
                "status",
                "createdAt",
                "lastUpdatedAt",
                "ttlMs",
                "pollIntervalMs",
            },
        )
        self.assertEqual(created["resultType"], "task")

        completed = TaskResult(
            task_id="job_test",
            status="completed",
            created_at="2026-08-10T00:00:00Z",
            last_updated_at="2026-08-10T00:00:01Z",
            ttl_ms=1000,
            result={"structuredContent": {"exit_code": 0}},
        ).model_dump(by_alias=True, mode="json", exclude_none=True)
        self.assertEqual(completed["resultType"], "complete")
        self.assertEqual(completed["taskId"], "job_test")
        self.assertEqual(completed["result"]["structuredContent"]["exit_code"], 0)


if __name__ == "__main__":
    unittest.main()
