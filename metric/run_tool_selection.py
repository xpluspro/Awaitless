#!/usr/bin/env python3
"""Evaluate Agent tool routing against the fixed v0.8 scenario suite."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from .run_agent import (
        DEFAULT_ENV,
        ModelClient,
        LLMConfig,
        LLMUsage,
        history_message,
        model_step,
        parse_arguments,
        parse_final_answer,
        tool_calls,
    )
except ImportError:
    from run_agent import (  # type: ignore[no-redef]
        DEFAULT_ENV,
        ModelClient,
        LLMConfig,
        LLMUsage,
        history_message,
        model_step,
        parse_arguments,
        parse_final_answer,
        tool_calls,
    )

from awaitless import __version__
from awaitless import mcp_server as mcp_module
from awaitless.mcp_server import server


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "metric" / "configs" / "tool-selection-v0.8.json"
JOB_TOOLS = {
    "run",
    "submit_job",
    "run_job",
    "wait_for_job",
    "wait_for_completions",
    "get_job_status",
    "get_job_logs",
}
CREATION_TOOLS = {"run", "submit_job", "run_job"}
SYSTEM_PROMPT = """You are an autonomous Agent using Awaitless. Select tools from
their descriptions and finish the user's request without polling or duplicate
execution. Independent fan-out submissions may be called in parallel. Tool responses are authoritative.
When complete, return one JSON object containing every result field the user asked
for. Do not wrap JSON in Markdown."""


async def load_tools() -> list[dict[str, Any]]:
    result = []
    for tool in await server.list_tools():
        if tool.name not in JOB_TOOLS:
            continue
        result.append(
            {
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description or "",
                    "parameters": tool.input_schema,
                },
            }
        )
    return result


def contains(actual: Any, expected: Any) -> bool:
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(
            key in actual and contains(actual[key], value)
            for key, value in expected.items()
        )
    if isinstance(expected, list):
        return isinstance(actual, list) and len(actual) == len(expected) and all(
            contains(item, wanted) for item, wanted in zip(actual, expected)
        )
    return actual == expected


def leaf_values(value: Any) -> list[Any]:
    if isinstance(value, dict):
        return [leaf for item in value.values() for leaf in leaf_values(item)]
    if isinstance(value, list):
        return [leaf for item in value for leaf in leaf_values(item)]
    return [value]


def contains_semantics(actual: Any, expected: Any) -> bool:
    actual_values = leaf_values(actual)
    return all(wanted in actual_values for wanted in leaf_values(expected))


def validate_config(config: dict[str, Any]) -> None:
    scenarios = config.get("scenarios")
    if config.get("schema_version") != 1 or not isinstance(scenarios, list):
        raise ValueError("tool-selection config must use schema_version 1 and scenarios")
    if len(scenarios) != 20:
        raise ValueError("v0.8 tool-selection suite must contain exactly 20 scenarios")
    if config.get("expected_version") != __version__:
        raise ValueError(
            f"tool-selection suite requires Awaitless {config.get('expected_version')}; "
            f"imported {__version__}"
        )
    ids = [item.get("id") for item in scenarios]
    if len(set(ids)) != len(ids) or any(not isinstance(item, str) for item in ids):
        raise ValueError("scenario IDs must be unique strings")
    for scenario in scenarios:
        calls = scenario.get("expected_calls")
        responses = scenario.get("responses")
        if not isinstance(calls, list) or not calls or len(calls) != len(responses or []):
            raise ValueError(f"{scenario['id']}: expected_calls and responses must align")
        if any(name not in JOB_TOOLS for name in calls):
            raise ValueError(f"{scenario['id']}: unknown expected tool")


def score_record(
    scenario: dict[str, Any],
    calls: list[dict[str, Any]],
    final: dict[str, Any] | None,
    usage: LLMUsage,
    error: str | None,
) -> dict[str, Any]:
    names = [item["name"] for item in calls]
    expected = scenario["expected_calls"]
    expected_arguments = scenario.get("expected_arguments", [{}] * len(expected))
    argument_checks = [
        index < len(calls) and contains(calls[index]["arguments"], wanted)
        for index, wanted in enumerate(expected_arguments)
    ]
    exact_sequence = names == expected and all(argument_checks)
    common = [0] * (len(expected) + 1)
    for name in names:
        previous = common[:]
        for index, wanted in enumerate(expected, 1):
            common[index] = (
                previous[index - 1] + 1
                if name == wanted
                else max(previous[index], common[index - 1])
            )
    unnecessary_calls = len(names) - common[-1]
    unexpected_status = sum(
        1
        for index, name in enumerate(names)
        if name == "get_job_status" and (index >= len(expected) or expected[index] != name)
    )
    expected_creations = sum(name in CREATION_TOOLS for name in expected)
    actual_creations = sum(name in CREATION_TOOLS for name in names)
    duplicate_submission = actual_creations > expected_creations
    final_correct = contains_semantics(final, scenario.get("final_contains", {}))
    artifact_expected = "artifact_sha256" in scenario.get("final_contains", {})
    artifact_consumed = None if not artifact_expected else final_correct
    return {
        "scenario_id": scenario["id"],
        "tags": scenario.get("tags", []),
        "expected_calls": expected,
        "observed_calls": calls,
        "final": final,
        "metrics": {
            "first_tool_correct": bool(names and names[0] == expected[0]),
            "unnecessary_calls": unnecessary_calls,
            "incorrect_polling": unexpected_status > 0,
            "duplicate_submission": duplicate_submission,
            "artifact_consumed": artifact_consumed,
            "tool_sequence_correct": exact_sequence,
            "final_task_completed": exact_sequence and final_correct and error is None,
            "input_tokens": usage.sum_required("prompt_tokens") if usage.traces else 0,
            "output_tokens": usage.sum_required("completion_tokens") if usage.traces else 0,
            "total_tokens": (
                usage.sum_required("prompt_tokens") + usage.sum_required("completion_tokens")
                if usage.traces
                else 0
            ),
        },
        "error": error,
        "usage": usage.traces,
    }


def run_scenario(
    scenario: dict[str, Any],
    tools: list[dict[str, Any]],
    client: ModelClient,
    config: dict[str, Any],
) -> dict[str, Any]:
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": scenario["prompt"]},
    ]
    observed: list[dict[str, Any]] = []
    usage = LLMUsage()
    progress = 0
    final = None
    failure = None
    try:
        for turn in range(int(config["max_agent_turns"])):
            available = tools if progress < len(scenario["expected_calls"]) else []
            message = model_step(
                client=client,
                usage=usage,
                phase=f"{scenario['id']}:{turn + 1}",
                messages=messages,
                tools=available,
                experiment_config=config,
            )
            messages.append(history_message(message))
            emitted = tool_calls(message)
            if not available:
                if emitted:
                    raise ValueError("model called a tool after the workflow completed")
                final = parse_final_answer(message.get("content"))
                if final is None:
                    raise ValueError("final response was not a JSON object")
                break
            if not emitted:
                raise ValueError("model emitted no tool call before the workflow completed")
            for call in emitted:
                name = call.get("function", {}).get("name")
                arguments = parse_arguments(call)
                observed.append({"name": name, "arguments": arguments})
                expected_name = (
                    scenario["expected_calls"][progress]
                    if progress < len(scenario["expected_calls"])
                    else None
                )
                if name == expected_name:
                    response = scenario["responses"][progress]
                    progress += 1
                else:
                    response = {
                        "error": "wrong_tool_for_scenario",
                        "state_changed": False,
                        "expected_intent": expected_name,
                    }
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.get("id"),
                        "content": json.dumps(response, separators=(",", ":")),
                    }
                )
        else:
            raise ValueError("model exceeded max_agent_turns")
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"
    return score_record(scenario, observed, final, usage, failure)


def summarize(
    records: list[dict[str, Any]], config: dict[str, Any], expected_model: str | None = None
) -> dict[str, Any]:
    count = len(records)

    def metric(name: str) -> int:
        return sum(bool(item["metrics"][name]) for item in records)

    recovery = [item for item in records if "recovery" in item["tags"]]
    artifact = [item for item in records if item["metrics"]["artifact_consumed"] is not None]
    first = metric("first_tool_correct")
    completed = metric("final_task_completed")
    recovery_clean = sum(not item["metrics"]["duplicate_submission"] for item in recovery)
    observed_models = sorted(
        {
            trace["model"]
            for item in records
            for trace in item["usage"]
            if isinstance(trace.get("model"), str)
        }
    )
    model_matches = expected_model is None or observed_models == [expected_model]
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    ).stdout.strip()
    return {
        "schema_version": 1,
        "record_type": "tool_selection_summary",
        "suite": config["name"],
        "recorded_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "scenario_count": count,
        "expected_model": expected_model,
        "observed_models": observed_models,
        "provenance": {
            "awaitless_version": __version__,
            "awaitless_package_file": str(Path(mcp_module.__file__).resolve()),
            "git_commit": commit,
            "git_dirty": bool(subprocess.run(
                ["git", "status", "--porcelain"], cwd=ROOT, text=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
            ).stdout.strip()),
            "config_sha256": hashlib.sha256(
                json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        },
        "first_tool_correct": {"numerator": first, "denominator": count, "rate": first / count},
        "final_task_completed": {"numerator": completed, "denominator": count, "rate": completed / count},
        "unnecessary_calls": sum(item["metrics"]["unnecessary_calls"] for item in records),
        "incorrect_polling_scenarios": metric("incorrect_polling"),
        "duplicate_submission_scenarios": metric("duplicate_submission"),
        "artifact_consumption": {
            "numerator": sum(bool(item["metrics"]["artifact_consumed"]) for item in artifact),
            "denominator": len(artifact),
        },
        "recovery_without_duplicate_execution": {
            "numerator": recovery_clean,
            "denominator": len(recovery),
            "rate": recovery_clean / len(recovery),
        },
        "tokens": {
            "input": sum(item["metrics"]["input_tokens"] for item in records),
            "output": sum(item["metrics"]["output_tokens"] for item in records),
            "total": sum(item["metrics"]["total_tokens"] for item in records),
        },
        "release_gate": {
            "tool_selection_at_least_95_percent": first / count >= 0.95,
            "all_scenarios_completed": completed == count,
            "recovery_has_no_duplicate_execution": recovery_clean == len(recovery),
            "all_responses_match_model": model_matches,
            "passed": (
                first / count >= 0.95
                and completed == count
                and recovery_clean == len(recovery)
                and model_matches
            ),
        },
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    result.add_argument("--model", default="gpt-5.6-luna", help="model identifier for this evidence run")
    result.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    result.add_argument("--output", type=Path, required=True)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.output.exists():
        print(f"tool selection: refusing to overwrite {args.output}", file=sys.stderr)
        return 2
    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
        validate_config(config)
        client = ModelClient(LLMConfig.load(args.env_file, model_override=args.model))
        tools = asyncio.run(load_tools())
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"tool selection: {exc}", file=sys.stderr)
        return 2
    records = []
    for index, scenario in enumerate(config["scenarios"], 1):
        print(f"[tool-selection] {index}/20 {scenario['id']}", file=sys.stderr, flush=True)
        records.append(run_scenario(scenario, tools, client, config))
    result = {"summary": summarize(records, config, args.model), "scenarios": records}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result["summary"], separators=(",", ":")))
    return 0 if result["summary"]["release_gate"]["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
