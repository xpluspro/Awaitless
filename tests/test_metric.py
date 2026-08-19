from __future__ import annotations

import copy
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from awaitless import __version__
from metric import (
    analyze,
    analyze_long_running,
    long_workload,
    run_agent,
    run_local,
    run_long_running,
    run_spectrum,
    run_tool_selection,
)


ROOT = Path(__file__).resolve().parents[1]


def metric_config(*, arm: str, scenario: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "name": f"test-{arm}-{scenario}",
        "description": "test fixture",
        "trials": 1,
        "seed": 42,
        "arms": [arm],
        "poll_interval_seconds": 0.01,
        "wait_interrupt_after_seconds": 0.05,
        "log_tail_lines": 50,
        "max_return_bytes": 4096,
        "scenarios": {
            scenario: {
                "duration_seconds": 0.25 if scenario == "recovery" else 0.02,
                "line_count": 3,
                "line_bytes": 64,
                "exit_codes": [0],
            }
        },
    }


class ToolSelectionMetricTest(unittest.TestCase):
    def test_v08_suite_has_twenty_valid_fixed_scenarios(self) -> None:
        config = json.loads(
            (ROOT / "metric" / "configs" / "tool-selection-v0.8.json").read_text(
                encoding="utf-8"
            )
        )
        run_tool_selection.validate_config(config)
        self.assertEqual(len(config["scenarios"]), 20)
        self.assertEqual(
            {scenario["expected_calls"][0] for scenario in config["scenarios"]},
            run_tool_selection.JOB_TOOLS,
        )

    def test_tool_selection_scores_polling_duplicate_and_artifact_contract(self) -> None:
        scenario = {
            "id": "fixture",
            "tags": ["recovery"],
            "expected_calls": ["wait_for_job"],
            "expected_arguments": [{"job_id": "job_1"}],
            "final_contains": {"artifact_sha256": "a" * 64},
        }
        usage = run_agent.LLMUsage()
        record = run_tool_selection.score_record(
            scenario,
            [
                {"name": "get_job_status", "arguments": {"job_id": "job_1"}},
                {"name": "submit_job", "arguments": {"command": ["again"]}},
                {"name": "wait_for_job", "arguments": {"job_id": "job_1"}},
            ],
            {"artifact_sha256": "wrong"},
            usage,
            None,
        )
        self.assertFalse(record["metrics"]["first_tool_correct"])
        self.assertTrue(record["metrics"]["incorrect_polling"])
        self.assertTrue(record["metrics"]["duplicate_submission"])
        self.assertFalse(record["metrics"]["artifact_consumed"])
        self.assertFalse(record["metrics"]["final_task_completed"])


class SpectrumMetricTest(unittest.TestCase):
    def test_command_failure_exit_still_parses_terminal_json(self) -> None:
        result = subprocess.CompletedProcess(
            ["awaitless", "run"], 3,
            stdout=b'{"state":"failed","exit_code":7}', stderr=b"",
        )
        self.assertEqual(run_spectrum.parse_json(result, "fixture")["exit_code"], 7)


class MetricWorkloadTest(unittest.TestCase):
    def test_workload_has_exact_log_bytes_and_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary) / "result.json"
            marker = "exact-marker"
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "metric" / "workload.py"),
                    "--scenario",
                    "normal",
                    "--trial-id",
                    "case-1",
                    "--duration-seconds",
                    "0",
                    "--line-count",
                    "3",
                    "--line-bytes",
                    "64",
                    "--exit-code",
                    "0",
                    "--marker",
                    marker,
                    "--score",
                    "9.5",
                    "--artifact",
                    str(artifact),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=5,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(len(result.stdout), 3 * 64 + len(f"FINAL_MARKER={marker}\n"))
            self.assertEqual(result.stderr.decode(), f"STDERR_MARKER={marker}\n")
            self.assertEqual(
                json.loads(artifact.read_text(encoding="utf-8")),
                {
                    "ok": True,
                    "scenario": "normal",
                    "trial_id": "case-1",
                    "score": 9.5,
                },
            )

    def test_awaitless_trial_validates_and_summarizes_without_fake_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "config.json"
            output = root / "trials.jsonl"
            config_path.write_text(
                json.dumps(metric_config(arm="awaitless", scenario="normal")),
                encoding="utf-8",
            )
            self.assertEqual(
                run_local.main(["--config", str(config_path), "--output", str(output)]),
                0,
            )
            records = analyze.load_records([output])
            summary = analyze.build_summary([output], records)
            self.assertTrue(records[0]["metrics"]["result_correct"])
            self.assertIsNone(records[0]["metrics"]["input_tokens"])
            self.assertEqual(
                records[0]["environment"]["awaitless_version"], __version__
            )
            self.assertIsNone(summary["overall"]["awaitless"]["cost_per_correct_job"]["usage_tokens"])
            self.assertTrue(summary["quality"]["warnings"])

            mismatched = copy.deepcopy(records[0])
            mismatched["arm"] = "tmux_plain"
            mismatched["trial_id"] += ":mismatch"
            mismatched["seed"] += 1
            mismatched["arm_metadata"] = run_local.PlainTmuxArm.metadata
            with self.assertRaisesRegex(ValueError, "different seeds or expected workloads"):
                analyze.build_summary([output], [records[0], mismatched])

    def test_shell_recovery_is_recorded_as_lost_without_relaunch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = metric_config(arm="shell", scenario="recovery")
            config["expected_version"] = __version__
            config_path = root / "config.json"
            output = root / "trials.jsonl"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            self.assertEqual(
                run_local.main(["--config", str(config_path), "--output", str(output)]),
                0,
            )
            record = analyze.load_records([output])[0]
            self.assertEqual(record["observed"]["state"], "lost")
            self.assertTrue(record["observed"]["recovery_injected"])
            self.assertFalse(record["metrics"]["recovery_success"])
            self.assertFalse(record["metrics"]["duplicate_launch"])

    def test_pre_command_controls_real_workload_exit_code(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            artifact = Path(temporary) / "result.json"
            result = subprocess.run(
                [
                    sys.executable, str(ROOT / "metric" / "workload.py"),
                    "--scenario", "normal", "--trial-id", "pre-command",
                    "--duration-seconds", "0", "--line-count", "0",
                    "--line-bytes", "64", "--exit-code", "0", "--marker", "pre",
                    "--score", "1", "--artifact", str(artifact),
                    "--pre-command-json", json.dumps([sys.executable, "-c", "raise SystemExit(7)"]),
                ],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, timeout=5,
            )
            self.assertEqual(result.returncode, 7)
            self.assertFalse(json.loads(artifact.read_text(encoding="utf-8"))["ok"])

    @unittest.skipUnless(shutil.which("tmux"), "tmux is not installed")
    def test_wrapped_tmux_recovers_after_interrupted_waiter(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "config.json"
            output = root / "trials.jsonl"
            config_path.write_text(
                json.dumps(metric_config(arm="tmux_wrapped", scenario="recovery")),
                encoding="utf-8",
            )
            self.assertEqual(
                run_local.main(["--config", str(config_path), "--output", str(output)]),
                0,
            )
            record = analyze.load_records([output])[0]
            self.assertTrue(record["observed"]["recovery_injected"])
            self.assertTrue(record["metrics"]["recovery_success"])
            self.assertEqual(record["metrics"]["agent_tool_calls"], 3)

    def test_trial_schema_is_valid_json(self) -> None:
        schema = json.loads(
            (ROOT / "metric" / "schemas" / "trial.schema.json").read_text(encoding="utf-8")
        )
        self.assertEqual(schema["properties"]["schema_version"]["const"], 1)

        long_schema = json.loads(
            (ROOT / "metric" / "schemas" / "long-running-trial.schema.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(long_schema["$schema"], "https://json-schema.org/draft/2020-12/schema")

    def test_agent_dotenv_and_final_answer_parsing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            env_file = Path(temporary) / ".env"
            env_file.write_text(
                "# comment\nexport LLM_MODEL=deepseek-v4-flash\n"
                "LLM_API_KEY=\"test-key\"\n",
                encoding="utf-8",
            )
            self.assertEqual(
                run_agent.parse_dotenv(env_file),
                {"LLM_MODEL": "deepseek-v4-flash", "LLM_API_KEY": "test-key"},
            )
        self.assertEqual(
            run_agent.parse_final_answer(
                '```json\n{"state":"succeeded","exit_code":0,"final_log_marker":"m","score":1}\n```'
            ),
            {
                "state": "succeeded",
                "exit_code": 0,
                "final_log_marker": "m",
                "score": 1,
            },
        )
        with tempfile.TemporaryDirectory() as temporary:
            env_file = Path(temporary) / ".env"
            env_file.write_text(
                "API_KEY=alias-key\nBASE_URL=https://example.test/v1\n",
                encoding="utf-8",
            )
            config = run_agent.LLMConfig.load(env_file)
            self.assertEqual(config.api_key, "alias-key")
            self.assertEqual(config.base_url, "https://example.test/v1")
            self.assertEqual(config.model, "gpt-5.6-luna")
        self.assertIsNone(run_agent.parse_final_answer(""))
        observation = run_agent.ManagerObservation(log_snapshot_bytes=[10, 20, 30])
        self.assertEqual(observation.duplicated_log_bytes, 30)

    def test_analyzer_flags_completion_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "config.json"
            output = root / "trials.jsonl"
            config_path.write_text(
                json.dumps(metric_config(arm="awaitless", scenario="normal")),
                encoding="utf-8",
            )
            self.assertEqual(
                run_local.main(["--config", str(config_path), "--output", str(output)]),
                0,
            )
            record = analyze.load_records([output])[0]
            record["llm"] = {"requests": [{"finish_reason": "length"}]}
            summary = analyze.build_summary([output], [record])
            self.assertEqual(summary["quality"]["llm_length_truncations"], 1)
            self.assertTrue(
                any("completion-token limit" in item for item in summary["quality"]["warnings"])
            )

    def test_long_workload_writes_verifiable_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            artifact = root / "result.json"
            marker = "0123456789abcdefabcd"
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "metric" / "long_workload.py"),
                    "run",
                    "--workload-json",
                    json.dumps(
                        {
                            "id": "sleep_test",
                            "adapter": "sleep",
                            "duration_seconds": 0.01,
                            "timeout_seconds": 5,
                        }
                    ),
                    "--task-id",
                    "task-1",
                    "--task-dir",
                    str(root),
                    "--artifact",
                    str(artifact),
                    "--marker",
                    marker,
                    "--duration-seconds",
                    "0.01",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(f"FINAL_MARKER={marker}".encode(), result.stdout)
            value = json.loads(artifact.read_text(encoding="utf-8"))
            self.assertTrue(value["ok"])
            self.assertEqual(value["task_id"], "task-1")
            self.assertEqual(value["adapter"], "sleep")

    def test_long_running_smoke_pairs_arms_and_recovers(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config_path = root / "config.json"
            output = root / "long.jsonl"
            config_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "name": "test-long-running",
                        "description": "test fixture",
                        "trials": 1,
                        "seed": 42,
                        "arms": ["blocking", "blocking_parallel", "awaitless"],
                        "scenarios": ["single", "batch", "disconnect"],
                        "batch_size": 2,
                        "disconnect_after_seconds": 0.05,
                        "defer_before_collect_seconds": 0.01,
                        "defer_before_collect_ratio": 0.0,
                        "model_inference_defer_seconds": 0.0,
                        "poll_interval_seconds": 0.01,
                        "log_tail_lines": 50,
                        "max_return_bytes": 4096,
                        "workloads": [
                            {
                                "id": "sleep_test",
                                "adapter": "sleep",
                                "duration_seconds": 0.25,
                                "timeout_seconds": 10,
                                "enabled": True,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                run_long_running.main(
                    ["--config", str(config_path), "--output", str(output)]
                ),
                0,
            )
            trials, skips = analyze_long_running.load_records([output])
            self.assertEqual(len(trials), 9)
            self.assertFalse(skips)
            by_key = {(item["scenario"], item["arm"]): item for item in trials}
            self.assertTrue(by_key[("single", "blocking")]["metrics"]["result_correct"])
            self.assertTrue(by_key[("single", "awaitless")]["metrics"]["result_correct"])
            self.assertEqual(by_key[("single", "blocking")]["metrics"]["agent_tool_calls"], 1)
            self.assertEqual(by_key[("single", "awaitless")]["metrics"]["agent_tool_calls"], 2)
            self.assertFalse(by_key[("disconnect", "blocking")]["metrics"]["recovery_success"])
            self.assertTrue(by_key[("disconnect", "awaitless")]["metrics"]["recovery_success"])
            self.assertIsNone(
                by_key[("single", "awaitless")]["metrics"]["reasoning_idle_seconds"]
            )
            summary = analyze_long_running.build_summary([output], trials, skips)
            self.assertEqual(summary["quality"]["case_count"], 3)
            self.assertFalse(summary["quality"]["incomplete_cases"])

    def test_long_workload_probe_rejects_missing_native_command(self) -> None:
        probe = long_workload.probe_workload(
            {
                "id": "missing",
                "adapter": "command",
                "command": ["not-used"],
                "required_commands": ["awaitless-command-that-does-not-exist"],
            },
            env_file=ROOT / ".env",
        )
        self.assertFalse(probe["available"])

        recorder = run_long_running.EventRecorder(epoch=0.0)
        recorder.events = [
            {
                "agent_call": True,
                "started_offset_seconds": 1.0,
                "ended_offset_seconds": 3.0,
            },
            {
                "agent_call": True,
                "started_offset_seconds": 2.0,
                "ended_offset_seconds": 4.0,
            },
        ]
        self.assertEqual(recorder.agent_blocked_seconds, 3.0)
        response = run_long_running.bounded_response(b"123456", 4)
        self.assertEqual(response, b"[lon")
        self.assertLessEqual(len(response), 4)


if __name__ == "__main__":
    unittest.main()
