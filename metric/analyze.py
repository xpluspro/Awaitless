#!/usr/bin/env python3
"""Validate trial JSONL and build decision-oriented Awaitless value summaries."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


RATE_FIELDS = (
    "result_correct",
    "state_correct",
    "exit_code_correct",
    "artifact_correct",
    "log_contract_correct",
    "recovery_success",
    "cancel_cleanup_success",
    "duplicate_launch",
)
NUMBER_FIELDS = (
    "agent_tool_calls",
    "agent_visible_bytes",
    "system_command_invocations",
    "duplicated_log_bytes",
    "wall_time_seconds",
    "cpu_time_seconds",
    "peak_rss_bytes",
    "disk_bytes",
    "ssh_request_count",
    "manual_interventions",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def discover(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(sorted(path.rglob("*.jsonl")))
        elif path.is_file():
            files.append(path)
        else:
            raise ValueError(f"input does not exist: {path}")
    unique = list(dict.fromkeys(item.resolve() for item in files))
    if not unique:
        raise ValueError("no JSONL inputs found")
    return unique


def validate_record(record: dict[str, Any], source: str) -> None:
    required = {
        "schema_version",
        "record_type",
        "experiment_id",
        "case_id",
        "trial_id",
        "recorded_at",
        "arm",
        "scenario",
        "seed",
        "environment",
        "expected",
        "observed",
        "metrics",
        "events",
        "arm_metadata",
        "error",
    }
    missing = required - record.keys()
    if missing:
        raise ValueError(f"{source}: missing fields {sorted(missing)}")
    if record["schema_version"] != 1 or record["record_type"] != "trial":
        raise ValueError(f"{source}: unsupported schema or record type")
    if isinstance(record.get("seed"), bool) or not isinstance(record.get("seed"), int):
        raise ValueError(f"{source}: seed must be an integer")
    if record["arm"] not in {"shell", "tmux_plain", "tmux_wrapped", "awaitless"}:
        raise ValueError(f"{source}: unknown arm {record['arm']!r}")
    metrics = record["metrics"]
    for name in RATE_FIELDS:
        if name not in metrics or metrics[name] not in {True, False, None}:
            raise ValueError(f"{source}: {name} must be boolean or null")
    for name in NUMBER_FIELDS:
        if name not in metrics:
            raise ValueError(f"{source}: missing metric {name}")
        value = metrics[name]
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0):
            raise ValueError(f"{source}: {name} must be a non-negative number or null")
    for name in ("input_tokens", "output_tokens", "cached_input_tokens", "reasoning_tokens"):
        if name not in metrics:
            raise ValueError(f"{source}: missing token metric {name}")
        value = metrics[name]
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
            raise ValueError(f"{source}: {name} must be a non-negative integer or null")
    events = record["events"]
    if not isinstance(events, list):
        raise ValueError(f"{source}: events must be a list")
    calls = sum(bool(event.get("agent_call")) for event in events)
    visible = sum(int(event.get("response_bytes", -1)) for event in events)
    if calls != metrics["agent_tool_calls"]:
        raise ValueError(f"{source}: agent_tool_calls does not equal the event total")
    if visible != metrics["agent_visible_bytes"]:
        raise ValueError(f"{source}: agent_visible_bytes does not equal the event total")


def load_records(files: list[Path]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    trial_ids: set[str] = set()
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
            validate_record(record, source)
            if record["trial_id"] in trial_ids:
                raise ValueError(f"{source}: duplicate trial_id {record['trial_id']!r}")
            trial_ids.add(record["trial_id"])
            records.append(record)
    if not records:
        raise ValueError("inputs contained no trial records")
    return records


def percentile(values: Iterable[float], quantile: float) -> float | None:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def numeric_summary(values: Iterable[int | float | None]) -> dict[str, Any]:
    available = [float(value) for value in values if value is not None]
    return {
        "n": len(available),
        "median": percentile(available, 0.5),
        "p90": percentile(available, 0.9),
        "mean": statistics.fmean(available) if available else None,
        "sum": sum(available) if available else None,
    }


def rate_summary(values: Iterable[bool | None]) -> dict[str, Any]:
    available = [value for value in values if value is not None]
    n = len(available)
    successes = sum(bool(value) for value in available)
    if n == 0:
        return {"successes": 0, "n": 0, "rate": None, "ci95_low": None, "ci95_high": None}
    rate = successes / n
    z = 1.959963984540054
    denominator = 1 + z * z / n
    center = (rate + z * z / (2 * n)) / denominator
    margin = z * math.sqrt(rate * (1 - rate) / n + z * z / (4 * n * n)) / denominator
    return {
        "successes": successes,
        "n": n,
        "rate": rate,
        "ci95_low": max(0.0, center - margin),
        "ci95_high": min(1.0, center + margin),
    }


def usage_tokens(record: dict[str, Any]) -> int | None:
    metrics = record["metrics"]
    if metrics["input_tokens"] is None or metrics["output_tokens"] is None:
        return None
    return int(metrics["input_tokens"]) + int(metrics["output_tokens"])


def consistent_metadata(records: list[dict[str, Any]]) -> dict[str, Any]:
    metadata = records[0]["arm_metadata"]
    if any(record["arm_metadata"] != metadata for record in records[1:]):
        raise ValueError(f"inconsistent arm_metadata for {records[0]['arm']}")
    return metadata


def aggregate(records: list[dict[str, Any]]) -> dict[str, Any]:
    metrics = [record["metrics"] for record in records]
    correct_count = sum(bool(metric["result_correct"]) for metric in metrics)
    token_values = [usage_tokens(record) for record in records]
    complete_token_coverage = all(value is not None for value in token_values)
    total_calls = sum(int(metric["agent_tool_calls"]) for metric in metrics)
    total_bytes = sum(int(metric["agent_visible_bytes"]) for metric in metrics)
    total_tokens = sum(int(value) for value in token_values if value is not None)
    result = {
        "arm": records[0]["arm"],
        "n_records": len(records),
        "n_cases": len({record["case_id"] for record in records}),
        "n_errors": sum(record["error"] is not None for record in records),
        "rates": {
            name: rate_summary(metric[name] for metric in metrics)
            for name in RATE_FIELDS
        },
        "distributions": {
            name: numeric_summary(metric[name] for metric in metrics)
            for name in NUMBER_FIELDS
        },
        "usage_tokens": numeric_summary(token_values),
        "token_coverage": {
            "records_with_usage": sum(value is not None for value in token_values),
            "records": len(records),
            "complete": complete_token_coverage,
        },
        "cost_per_correct_job": {
            "correct_jobs": correct_count,
            "agent_tool_calls": total_calls / correct_count if correct_count else None,
            "agent_visible_bytes": total_bytes / correct_count if correct_count else None,
            "usage_tokens": (
                total_tokens / correct_count
                if correct_count and complete_token_coverage
                else None
            ),
        },
        "arm_metadata": consistent_metadata(records),
        "experiment_ids": sorted({record["experiment_id"] for record in records}),
    }
    return result


def grouped(records: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    values: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        values[(str(record[key]), str(record["arm"]))].append(record)
    result: dict[str, dict[str, Any]] = defaultdict(dict)
    for (group, arm), group_records in sorted(values.items()):
        result[group][arm] = aggregate(group_records)
    return dict(result)


def reduction(reference: float | None, baseline: float | None) -> float | None:
    if reference is None or baseline is None or baseline == 0:
        return None
    return 100 * (1 - reference / baseline)


def comparisons(scopes: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for scope, arms in scopes.items():
        reference = arms.get("awaitless")
        if reference is None:
            continue
        for baseline_name in ("shell", "tmux_plain", "tmux_wrapped"):
            baseline = arms.get(baseline_name)
            if baseline is None:
                continue
            reference_rate = reference["rates"]["result_correct"]["rate"]
            baseline_rate = baseline["rates"]["result_correct"]["rate"]
            results.append(
                {
                    "scope": scope,
                    "reference": "awaitless",
                    "baseline": baseline_name,
                    "result_fidelity_delta_percentage_points": (
                        100 * (reference_rate - baseline_rate)
                        if reference_rate is not None and baseline_rate is not None
                        else None
                    ),
                    "median_agent_tool_call_reduction_percent": reduction(
                        reference["distributions"]["agent_tool_calls"]["median"],
                        baseline["distributions"]["agent_tool_calls"]["median"],
                    ),
                    "p90_agent_visible_byte_reduction_percent": reduction(
                        reference["distributions"]["agent_visible_bytes"]["p90"],
                        baseline["distributions"]["agent_visible_bytes"]["p90"],
                    ),
                    "usage_token_reduction_percent": reduction(
                        reference["cost_per_correct_job"]["usage_tokens"],
                        baseline["cost_per_correct_job"]["usage_tokens"],
                    ),
                    "custom_glue_sloc_delta": (
                        reference["arm_metadata"]["custom_glue_sloc"]
                        - baseline["arm_metadata"]["custom_glue_sloc"]
                    ),
                }
            )
    return results


def build_summary(files: list[Path], records: list[dict[str, Any]]) -> dict[str, Any]:
    overall_records: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        overall_records[record["arm"]].append(record)
    overall = {arm: aggregate(items) for arm, items in sorted(overall_records.items())}
    by_scenario = grouped(records, "scenario")
    scopes = {"overall": overall, **by_scenario}
    case_arms: dict[str, set[str]] = defaultdict(set)
    case_expectations: dict[str, tuple[int, str]] = {}
    for record in records:
        case_arms[record["case_id"]].add(record["arm"])
        signature = (
            int(record["seed"]),
            json.dumps(record["expected"], sort_keys=True, separators=(",", ":")),
        )
        previous = case_expectations.setdefault(record["case_id"], signature)
        if previous != signature:
            raise ValueError(
                f"case {record['case_id']!r} used different seeds or expected workloads across arms"
            )
    expected_arms = {record["arm"] for record in records}
    incomplete_cases = sorted(case for case, arms in case_arms.items() if arms != expected_arms)
    warnings: list[str] = []
    if any(not value["token_coverage"]["complete"] for value in overall.values()):
        warnings.append("Actual usage tokens are incomplete; no token-saving percentage is claimed.")
    if any(
        aggregate_value["n_records"] < 20
        for scenario in by_scenario.values()
        for aggregate_value in scenario.values()
    ):
        warnings.append("At least one arm × scenario cell has fewer than 20 trials; treat it as directional only.")
    if incomplete_cases:
        warnings.append("Some cases do not contain every observed arm; paired comparison may be biased.")
    if any(value["n_errors"] for value in overall.values()):
        warnings.append("At least one trial has an execution error; errors remain in all denominators.")
    llm_length_truncations = sum(
        request.get("finish_reason") == "length"
        for record in records
        for request in record.get("llm", {}).get("requests", [])
    )
    if llm_length_truncations:
        warnings.append(
            f"{llm_length_truncations} LLM request(s) hit the completion-token limit; "
            "treat affected comparisons as invalid until rerun with a sufficient limit."
        )
    return {
        "schema_version": 1,
        "generated_at": utc_now(),
        "sources": [str(path) for path in files],
        "record_count": len(records),
        "experiment_ids": sorted({record["experiment_id"] for record in records}),
        "overall": overall,
        "by_scenario": by_scenario,
        "comparisons": comparisons(scopes),
        "quality": {
            "expected_arms": sorted(expected_arms),
            "case_count": len(case_arms),
            "incomplete_cases": incomplete_cases,
            "llm_length_truncations": llm_length_truncations,
            "warnings": warnings,
        },
        "definitions": {
            "result_fidelity": "state, exit code, log contract, artifact, and cancellation cleanup are all correct when applicable",
            "agent_visible_bytes": "exact stdout plus stderr bytes returned by agent-visible calls",
            "usage_tokens": "input_tokens plus output_tokens; reasoning tokens are reported separately and not double-counted",
            "positive_reduction": "positive percentages mean Awaitless used less than the named baseline",
        },
    }


def percent(value: float | None, digits: int = 1) -> str:
    return "—" if value is None else f"{100 * value:.{digits}f}%"


def signed_percent(value: float | None, digits: int = 1) -> str:
    return "—" if value is None else f"{value:+.{digits}f}%"


def number(value: float | None, digits: int = 1) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def bytes_text(value: float | None) -> str:
    if value is None:
        return "—"
    units = ("B", "KiB", "MiB", "GiB")
    amount = float(value)
    unit = units[0]
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            break
        amount /= 1024
    return f"{amount:.1f} {unit}"


def rate_cell(value: dict[str, Any]) -> str:
    if value["n"] == 0:
        return "—"
    return (
        f"{value['successes']}/{value['n']} ({percent(value['rate'])}; "
        f"95% CI {percent(value['ci95_low'])}–{percent(value['ci95_high'])})"
    )


def markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Awaitless value metric summary",
        "",
        f"Generated: `{summary['generated_at']}` from {summary['record_count']} trial records.",
        "",
        "## Overall",
        "",
        "| Arm | Result fidelity | Recovery | Duplicate launch | Cancel cleanup | Median calls | P90 visible bytes | Calls / correct job | Usage tokens / correct job | Custom glue SLOC |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for arm, value in summary["overall"].items():
        lines.append(
            "| "
            + " | ".join(
                [
                    arm,
                    rate_cell(value["rates"]["result_correct"]),
                    rate_cell(value["rates"]["recovery_success"]),
                    rate_cell(value["rates"]["duplicate_launch"]),
                    rate_cell(value["rates"]["cancel_cleanup_success"]),
                    number(value["distributions"]["agent_tool_calls"]["median"]),
                    bytes_text(value["distributions"]["agent_visible_bytes"]["p90"]),
                    number(value["cost_per_correct_job"]["agent_tool_calls"], 2),
                    number(value["cost_per_correct_job"]["usage_tokens"], 1),
                    str(value["arm_metadata"]["custom_glue_sloc"]),
                ]
            )
            + " |"
        )

    lines += [
        "",
        "## By scenario",
        "",
        "| Scenario | Arm | Result fidelity | Median calls | P90 visible bytes | Median wall time | Errors |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for scenario, arms in summary["by_scenario"].items():
        for arm, value in arms.items():
            lines.append(
                f"| {scenario} | {arm} | {rate_cell(value['rates']['result_correct'])} | "
                f"{number(value['distributions']['agent_tool_calls']['median'])} | "
                f"{bytes_text(value['distributions']['agent_visible_bytes']['p90'])} | "
                f"{number(value['distributions']['wall_time_seconds']['median'], 3)} s | "
                f"{value['n_errors']} |"
            )

    lines += [
        "",
        "## Awaitless relative to each baseline",
        "",
        "Positive reductions mean Awaitless returned/used less. Negative values mean it used more.",
        "",
        "| Scope | Baseline | Fidelity delta | Median call reduction | P90 byte reduction | Usage token reduction | Glue SLOC delta |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for value in summary["comparisons"]:
        fidelity = value["result_fidelity_delta_percentage_points"]
        lines.append(
            f"| {value['scope']} | {value['baseline']} | "
            f"{'—' if fidelity is None else f'{fidelity:+.1f} pp'} | "
            f"{signed_percent(value['median_agent_tool_call_reduction_percent'])} | "
            f"{signed_percent(value['p90_agent_visible_byte_reduction_percent'])} | "
            f"{signed_percent(value['usage_token_reduction_percent'])} | "
            f"{value['custom_glue_sloc_delta']:+d} |"
        )

    lines += ["", "## Data-quality warnings", ""]
    warnings = summary["quality"]["warnings"]
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- None.")
    lines += [
        "",
        "Token fields are never inferred from bytes. Review raw JSONL, environment metadata, every failure, and the qualitative rubric before making a project-value claim.",
        "",
    ]
    return "\n".join(lines)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("inputs", nargs="+", type=Path, help="trial JSONL files or directories")
    result.add_argument("--json-out", type=Path)
    result.add_argument("--markdown-out", type=Path)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        files = discover(args.inputs)
        records = load_records(files)
        summary = build_summary(files, records)
    except ValueError as exc:
        print(f"metric analyze: {exc}", file=sys.stderr)
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
                    "records": summary["record_count"],
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
