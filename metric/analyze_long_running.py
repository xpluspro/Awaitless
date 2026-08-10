#!/usr/bin/env python3
"""Validate and summarize Blocking versus Awaitless long-running JSONL."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ARMS = {"blocking", "blocking_parallel", "awaitless"}
SCENARIOS = {"single", "batch", "disconnect"}
DISTRIBUTIONS = (
    "agent_tool_calls",
    "agent_visible_bytes",
    "agent_blocked_seconds",
    "agent_available_seconds",
    "agent_occupancy_ratio",
    "time_to_agent_release_seconds",
    "wall_time_seconds",
    "task_duration_sum_seconds",
    "parallelism_factor",
    "system_command_invocations",
    "disk_bytes",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def discover(inputs: list[Path]) -> list[Path]:
    files: list[Path] = []
    for item in inputs:
        if item.is_dir():
            files.extend(sorted(item.rglob("*.jsonl")))
        elif item.is_file():
            files.append(item)
        else:
            raise ValueError(f"input does not exist: {item}")
    unique = sorted({path.resolve() for path in files})
    if not unique:
        raise ValueError("no JSONL inputs found")
    return unique


def validate_trial(record: dict[str, Any], source: str) -> None:
    required = {
        "schema_version",
        "record_type",
        "experiment_id",
        "case_id",
        "trial_id",
        "arm",
        "scenario",
        "workload",
        "adapter",
        "seed",
        "expected",
        "observed",
        "metrics",
        "events",
        "error",
    }
    missing = required - set(record)
    if missing:
        raise ValueError(f"{source}: missing trial fields {sorted(missing)}")
    if record["schema_version"] != 1 or record["record_type"] != "trial":
        raise ValueError(f"{source}: unsupported trial record")
    if record["arm"] not in ARMS or record["scenario"] not in SCENARIOS:
        raise ValueError(f"{source}: unknown arm or scenario")
    metrics = record["metrics"]
    if not isinstance(metrics, dict) or not isinstance(metrics.get("result_correct"), bool):
        raise ValueError(f"{source}: invalid result_correct")
    for field in DISTRIBUTIONS:
        value = metrics.get(field)
        if value is not None and (
            not isinstance(value, (int, float)) or isinstance(value, bool) or value < 0
        ):
            raise ValueError(f"{source}: invalid metric {field}")


def validate_skip(record: dict[str, Any], source: str) -> None:
    required = {"schema_version", "record_type", "experiment_id", "workload", "adapter", "reason"}
    missing = required - set(record)
    if missing or record.get("record_type") != "skip" or record.get("schema_version") != 1:
        raise ValueError(f"{source}: invalid skip record")


def load_records(files: list[Path]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    trials: list[dict[str, Any]] = []
    skips: list[dict[str, Any]] = []
    identifiers: set[str] = set()
    for path in files:
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            source = f"{path}:{line_number}"
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{source}: invalid JSON: {exc}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"{source}: record must be an object")
            if record.get("record_type") == "skip":
                validate_skip(record, source)
                skips.append(record)
                continue
            validate_trial(record, source)
            trial_id = str(record["trial_id"])
            if trial_id in identifiers:
                raise ValueError(f"{source}: duplicate trial_id {trial_id!r}")
            identifiers.add(trial_id)
            trials.append(record)
    return trials, skips


def percentile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(probability * len(ordered)))
    return ordered[rank - 1]


def distribution(records: list[dict[str, Any]], field: str) -> dict[str, Any]:
    values = [
        float(record["metrics"][field])
        for record in records
        if record["metrics"].get(field) is not None
    ]
    return {
        "n": len(values),
        "median": statistics.median(values) if values else None,
        "p90": percentile(values, 0.9),
        "mean": statistics.fmean(values) if values else None,
        "sum": sum(values) if values else None,
    }


def wilson(successes: int, total: int) -> tuple[float | None, float | None]:
    if total == 0:
        return None, None
    z = 1.959963984540054
    rate = successes / total
    denominator = 1 + z * z / total
    center = (rate + z * z / (2 * total)) / denominator
    margin = z * math.sqrt(rate * (1 - rate) / total + z * z / (4 * total * total)) / denominator
    return max(0.0, center - margin), min(1.0, center + margin)


def rate(records: list[dict[str, Any]], field: str) -> dict[str, Any]:
    values = [record["metrics"].get(field) for record in records]
    applicable = [value for value in values if isinstance(value, bool)]
    successes = sum(applicable)
    low, high = wilson(successes, len(applicable))
    return {
        "successes": successes,
        "n": len(applicable),
        "rate": successes / len(applicable) if applicable else None,
        "ci95_low": low,
        "ci95_high": high,
    }


def aggregate(records: list[dict[str, Any]]) -> dict[str, Any]:
    correct = sum(record["metrics"]["result_correct"] for record in records)
    return {
        "n_records": len(records),
        "n_cases": len({record["case_id"] for record in records}),
        "n_errors": sum(record["error"] is not None for record in records),
        "rates": {
            "result_correct": rate(records, "result_correct"),
            "recovery_success": rate(records, "recovery_success"),
        },
        "distributions": {field: distribution(records, field) for field in DISTRIBUTIONS},
        "cost_per_correct_job": {
            "correct_jobs": correct,
            "agent_tool_calls": (
                sum(record["metrics"]["agent_tool_calls"] for record in records) / correct
                if correct
                else None
            ),
            "agent_visible_bytes": (
                sum(record["metrics"]["agent_visible_bytes"] for record in records) / correct
                if correct
                else None
            ),
            "agent_blocked_seconds": (
                sum(record["metrics"]["agent_blocked_seconds"] for record in records) / correct
                if correct
                else None
            ),
        },
    }


def grouped(
    records: list[dict[str, Any]], keys: tuple[str, ...]
) -> dict[str, dict[str, dict[str, Any]]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        scope = "/".join(str(record[key]) for key in keys)
        groups[(scope, record["arm"])].append(record)
    result: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for (scope, arm), items in sorted(groups.items()):
        result[scope][arm] = aggregate(items)
    return dict(result)


def reduction(reference: float | None, baseline: float | None) -> float | None:
    if reference is None or baseline in {None, 0}:
        return None
    return 100 * (1 - reference / baseline)


def comparisons(
    scopes: dict[str, dict[str, dict[str, Any]]]
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for scope, arms in scopes.items():
        if "awaitless" not in arms:
            continue
        reference = arms["awaitless"]
        for baseline_name in ("blocking", "blocking_parallel"):
            if baseline_name not in arms:
                continue
            baseline = arms[baseline_name]
            reference_rate = reference["rates"]["result_correct"]["rate"]
            baseline_rate = baseline["rates"]["result_correct"]["rate"]
            comparable_efficiency = reference_rate == 1.0 and baseline_rate == 1.0
            result.append(
                {
                    "scope": scope,
                    "baseline": baseline_name,
                    "fidelity_delta_percentage_points": (
                        100 * (reference_rate - baseline_rate)
                        if reference_rate is not None and baseline_rate is not None
                        else None
                    ),
                    "agent_blocked_reduction_percent": reduction(
                        reference["distributions"]["agent_blocked_seconds"]["median"],
                        baseline["distributions"]["agent_blocked_seconds"]["median"],
                    )
                    if comparable_efficiency
                    else None,
                    "agent_release_reduction_percent": reduction(
                        reference["distributions"]["time_to_agent_release_seconds"]["median"],
                        baseline["distributions"]["time_to_agent_release_seconds"]["median"],
                    )
                    if comparable_efficiency
                    else None,
                    "makespan_reduction_percent": reduction(
                        reference["distributions"]["wall_time_seconds"]["median"],
                        baseline["distributions"]["wall_time_seconds"]["median"],
                    )
                    if comparable_efficiency
                    else None,
                    "median_tool_call_delta": (
                        reference["distributions"]["agent_tool_calls"]["median"]
                        - baseline["distributions"]["agent_tool_calls"]["median"]
                    ),
                }
            )
    return result


def build_summary(
    files: list[Path], trials: list[dict[str, Any]], skips: list[dict[str, Any]]
) -> dict[str, Any]:
    case_arms: dict[str, set[str]] = defaultdict(set)
    expectations: dict[str, tuple[int, str]] = {}
    for record in trials:
        case_arms[record["case_id"]].add(record["arm"])
        signature = (
            int(record["seed"]),
            json.dumps(record["expected"], sort_keys=True, separators=(",", ":")),
        )
        prior = expectations.setdefault(record["case_id"], signature)
        if prior != signature:
            raise ValueError(f"case {record['case_id']!r} is not paired across arms")
    expected_arms = {record["arm"] for record in trials}
    incomplete = sorted(case for case, arms in case_arms.items() if arms != expected_arms)
    overall_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in trials:
        overall_groups[record["arm"]].append(record)
    overall = {arm: aggregate(items) for arm, items in sorted(overall_groups.items())}
    by_scenario = grouped(trials, ("scenario",))
    by_workload_scenario = grouped(trials, ("workload", "scenario"))
    scope_values = {"overall": overall, **by_scenario, **by_workload_scenario}
    warnings: list[str] = []
    if skips:
        warnings.append("Unavailable workloads were skipped and are listed with probe reasons.")
    if incomplete:
        warnings.append("Some cases do not contain every observed arm; paired comparisons may be biased.")
    if trials and any(
        value["n_records"] < 20
        for scenario in by_workload_scenario.values()
        for value in scenario.values()
    ):
        warnings.append("At least one workload × scenario × arm cell has fewer than 20 trials.")
    if any(record["error"] is not None for record in trials):
        warnings.append("Execution errors remain in all denominators.")
    if "blocking_parallel" not in expected_arms:
        warnings.append(
            "No parallel Blocking baseline was run; parallelization claims apply only to a single-slot executor."
        )
    if trials and all(record["adapter"] != "command" for record in trials):
        warnings.append(
            "All completed workloads used controlled fixtures; validate external project commands separately."
        )
    return {
        "schema_version": 1,
        "generated_at": utc_now(),
        "sources": [str(path) for path in files],
        "trial_count": len(trials),
        "skip_count": len(skips),
        "skips": [
            {
                "workload": record["workload"],
                "adapter": record["adapter"],
                "reason": record["reason"],
            }
            for record in skips
        ],
        "overall": overall,
        "by_scenario": by_scenario,
        "by_workload_scenario": by_workload_scenario,
        "comparisons": comparisons(scope_values),
        "quality": {
            "case_count": len(case_arms),
            "expected_arms": sorted(expected_arms),
            "incomplete_cases": incomplete,
            "warnings": warnings,
        },
        "definitions": {
            "agent_blocked_seconds": "union of agent-visible synchronous call intervals",
            "agent_available_seconds": "wall time outside those call intervals; availability is not proof of useful reasoning",
            "reasoning_idle_seconds": "not inferred from wall time and intentionally null",
            "parallelism_factor": "sum of completed inner task durations divided by case makespan",
            "positive_reduction": "positive percentages mean Awaitless used less time than the named baseline",
        },
    }


def percent(value: float | None) -> str:
    return "—" if value is None else f"{100 * value:.1f}%"


def number(value: float | None, digits: int = 2) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def comparison_percent(value: float | None) -> str:
    return "—" if value is None else f"{value:+.1f}%"


def markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Blocking vs Awaitless long-running benchmark",
        "",
        f"Generated `{summary['generated_at']}` from {summary['trial_count']} trials; "
        f"{summary['skip_count']} workload probes skipped.",
        "",
        "`agent_blocked_seconds` is orchestration wall time, not model reasoning or token usage.",
        "",
        "## By scenario",
        "",
        "| Scenario | Arm | Fidelity | Recovery | Median wall | Median blocked | Median available | Agent release | Calls | Parallelism |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for scenario, arms in summary["by_scenario"].items():
        for arm, value in arms.items():
            fidelity = value["rates"]["result_correct"]
            recovery = value["rates"]["recovery_success"]
            dist = value["distributions"]
            recovery_text = (
                "—"
                if recovery["n"] == 0
                else f"{recovery['successes']}/{recovery['n']} ({percent(recovery['rate'])})"
            )
            lines.append(
                f"| {scenario} | {arm} | {fidelity['successes']}/{fidelity['n']} "
                f"({percent(fidelity['rate'])}) | "
                f"{recovery_text} | "
                f"{number(dist['wall_time_seconds']['median'])} s | "
                f"{number(dist['agent_blocked_seconds']['median'])} s | "
                f"{number(dist['agent_available_seconds']['median'])} s | "
                f"{number(dist['time_to_agent_release_seconds']['median'])} s | "
                f"{number(dist['agent_tool_calls']['median'], 1)} | "
                f"{number(dist['parallelism_factor']['median'])}× |"
            )
    lines += [
        "",
        "## Awaitless comparisons",
        "",
        "| Scope | Baseline | Fidelity delta | Blocked reduction | Release reduction | Makespan reduction | Tool-call delta |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for item in summary["comparisons"]:
        fidelity = item["fidelity_delta_percentage_points"]
        lines.append(
            f"| {item['scope']} | {item['baseline']} | "
            f"{'—' if fidelity is None else f'{fidelity:+.1f} pp'} | "
            f"{comparison_percent(item['agent_blocked_reduction_percent'])} | "
            f"{comparison_percent(item['agent_release_reduction_percent'])} | "
            f"{comparison_percent(item['makespan_reduction_percent'])} | "
            f"{number(item['median_tool_call_delta'], 1)} |"
        )
    lines += ["", "## Skipped workloads", ""]
    if summary["skips"]:
        lines.extend(
            f"- `{item['workload']}` ({item['adapter']}): {item['reason']}"
            for item in summary["skips"]
        )
    else:
        lines.append("- None.")
    lines += ["", "## Data-quality warnings", ""]
    if summary["quality"]["warnings"]:
        lines.extend(f"- {item}" for item in summary["quality"]["warnings"])
    else:
        lines.append("- None.")
    lines += [
        "",
        "A direct blocking call can use fewer tool calls for one task. A parallel-capable tool host can also "
        "match Awaitless makespan; disconnect recovery and a durable result protocol remain separate claims.",
        "",
    ]
    return "\n".join(lines)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("inputs", nargs="+", type=Path)
    result.add_argument("--json-out", type=Path)
    result.add_argument("--markdown-out", type=Path)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        files = discover(args.inputs)
        trials, skips = load_records(files)
        summary = build_summary(files, trials, skips)
    except ValueError as exc:
        print(f"long benchmark analyze: {exc}", file=sys.stderr)
        return 2
    rendered_json = json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
    rendered_markdown = markdown(summary)
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(rendered_json, encoding="utf-8")
    if args.markdown_out:
        args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_out.write_text(rendered_markdown, encoding="utf-8")
    if not args.json_out and not args.markdown_out:
        print(rendered_json, end="")
    else:
        print(
            json.dumps(
                {
                    "trials": summary["trial_count"],
                    "skips": summary["skip_count"],
                    "json_out": str(args.json_out) if args.json_out else None,
                    "markdown_out": str(args.markdown_out) if args.markdown_out else None,
                    "warnings": summary["quality"]["warnings"],
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
