#!/usr/bin/env python3
"""Audit the implementation surface required to substitute for Awaitless."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def source_sloc(path: Path) -> int:
    return sum(
        1
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )


def matching_tests(patterns: list[str], test_files: list[Path]) -> list[str]:
    matches: set[str] = set()
    texts = [(path, path.read_text(encoding="utf-8")) for path in test_files]
    for pattern in patterns:
        pattern_matches = {
            str(path.relative_to(ROOT))
            for path, content in texts
            if re.search(pattern, content, re.MULTILINE)
        }
        if not pattern_matches:
            return []
        matches.update(pattern_matches)
    return sorted(matches)


def validate_config(config: dict[str, Any]) -> None:
    if config.get("schema_version") != 1:
        raise ValueError("schema_version must be 1")
    capabilities = config.get("capabilities")
    arms = config.get("arms")
    if not isinstance(capabilities, list) or not capabilities:
        raise ValueError("capabilities must be a non-empty list")
    if len(capabilities) != len(set(capabilities)):
        raise ValueError("capabilities must be unique")
    if not isinstance(arms, dict) or set(arms) != {"tmux_wrapped", "awaitless"}:
        raise ValueError("arms must contain tmux_wrapped and awaitless")
    for arm_name, arm in arms.items():
        declared = arm.get("capabilities")
        if not isinstance(declared, dict) or set(declared) != set(capabilities):
            raise ValueError(f"{arm_name} must declare every capability exactly once")
        for capability, evidence in declared.items():
            if not isinstance(evidence, dict) or not isinstance(evidence.get("supported"), bool):
                raise ValueError(f"{arm_name}.{capability} must declare supported")
            if evidence["supported"]:
                if not evidence.get("implementation_patterns"):
                    raise ValueError(f"{arm_name}.{capability} lacks implementation evidence")
                if not evidence.get("test_patterns"):
                    raise ValueError(f"{arm_name}.{capability} lacks test evidence")


def audit_arm(name: str, arm: dict[str, Any]) -> dict[str, Any]:
    implementation_files = [ROOT / value for value in arm["implementation_files"]]
    test_files = [ROOT / value for value in arm["test_files"]]
    for path in [*implementation_files, *test_files]:
        if not path.is_file():
            raise ValueError(f"evidence file does not exist: {path.relative_to(ROOT)}")

    implementation_text = "\n".join(
        path.read_text(encoding="utf-8") for path in implementation_files
    )
    capability_results: dict[str, Any] = {}
    for capability, declaration in arm["capabilities"].items():
        if not declaration["supported"]:
            capability_results[capability] = {
                "supported": False,
                "implementation_evidence": [],
                "test_evidence": [],
            }
            continue
        missing = [
            pattern
            for pattern in declaration["implementation_patterns"]
            if not re.search(pattern, implementation_text, re.MULTILINE)
        ]
        tests = matching_tests(declaration["test_patterns"], test_files)
        if missing:
            raise ValueError(f"{name}.{capability} implementation patterns missing: {missing}")
        if not tests:
            raise ValueError(f"{name}.{capability} has no matching test evidence")
        capability_results[capability] = {
            "supported": True,
            "implementation_evidence": declaration["implementation_patterns"],
            "test_evidence": tests,
        }

    backend_files = [ROOT / value for value in arm.get("backend_specific_files", [])]
    return {
        "name": name,
        "supported_capabilities": sum(
            item["supported"] for item in capability_results.values()
        ),
        "total_capabilities": len(capability_results),
        "capabilities": capability_results,
        "consumer_glue_sloc": (
            sum(source_sloc(path) for path in implementation_files)
            if arm["ownership"] == "consumer"
            else 0
        ),
        "product_implementation_sloc": sum(source_sloc(path) for path in implementation_files),
        "implementation_files": len(implementation_files),
        "backend_specific_sloc": sum(source_sloc(path) for path in backend_files),
        "backend_specific_files": [str(path.relative_to(ROOT)) for path in backend_files],
        "test_files": len(test_files),
        "test_evidence_files": sorted(
            {
                path
                for item in capability_results.values()
                for path in item["test_evidence"]
            }
        ),
        "supported_backends": arm["supported_backends"],
        "runtime_dependencies": arm["runtime_dependencies"],
        "ownership": arm["ownership"],
    }


def audit(config: dict[str, Any]) -> dict[str, Any]:
    validate_config(config)
    arms = {name: audit_arm(name, value) for name, value in config["arms"].items()}
    return {
        "schema_version": 1,
        "name": config["name"],
        "generated_at": utc_now(),
        "method": {
            "supported_requires_implementation_and_test_evidence": True,
            "sloc": "non-empty, non-comment physical lines",
            "consumer_glue_and_product_implementation_reported_separately": True,
            "limitations": config["limitations"],
        },
        "capabilities": config["capabilities"],
        "arms": arms,
        "comparison": {
            "capability_gap": (
                arms["awaitless"]["supported_capabilities"]
                - arms["tmux_wrapped"]["supported_capabilities"]
            ),
            "consumer_glue_sloc_avoided": arms["tmux_wrapped"]["consumer_glue_sloc"],
            "additional_backends": sorted(
                set(arms["awaitless"]["supported_backends"])
                - set(arms["tmux_wrapped"]["supported_backends"])
            ),
        },
    }


def markdown(result: dict[str, Any]) -> str:
    lines = [
        f"# {result['name']}",
        "",
        "| Arm | Capabilities | Backends | Consumer glue SLOC | Product implementation SLOC | Backend-specific SLOC | Evidence test files |",
        "|---|---:|---|---:|---:|---:|---:|",
    ]
    for arm in result["arms"].values():
        lines.append(
            f"| {arm['name']} | {arm['supported_capabilities']}/{arm['total_capabilities']} | "
            f"{', '.join(arm['supported_backends'])} | {arm['consumer_glue_sloc']} | "
            f"{arm['product_implementation_sloc']} | {arm['backend_specific_sloc']} | "
            f"{len(arm['test_evidence_files'])} |"
        )
    lines.extend(["", "| Capability | tmux wrapper | Awaitless |", "|---|---:|---:|"])
    for capability in result["capabilities"]:
        lines.append(
            f"| `{capability}` | "
            f"{'yes' if result['arms']['tmux_wrapped']['capabilities'][capability]['supported'] else 'no'} | "
            f"{'yes' if result['arms']['awaitless']['capabilities'][capability]['supported'] else 'no'} |"
        )
    lines.extend(
        [
            "",
            "This is a static, evidence-linked maintenance audit, not a runtime reliability result. "
            "SLOC is a maintenance proxy and is not a quality score.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--json-out", type=Path, required=True)
    parser.add_argument("--markdown-out", type=Path, required=True)
    args = parser.parse_args(argv)
    for output in (args.json_out, args.markdown_out):
        if output.exists():
            parser.error(f"refusing to overwrite {output}")
    result = audit(json.loads(args.config.read_text(encoding="utf-8")))
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    args.markdown_out.write_text(markdown(result), encoding="utf-8")
    print(json.dumps({"ok": True, "json": str(args.json_out), "markdown": str(args.markdown_out)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
