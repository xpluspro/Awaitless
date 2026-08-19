#!/usr/bin/env python3
"""Run paired tool-using Agent trials and record actual API usage."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import socket
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request

try:
    from . import run_local
except ImportError:
    import run_local  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parents[1]
METRIC_ROOT = Path(__file__).resolve().parent
DEFAULT_ENV = ROOT / ".env"
SYSTEM_PROMPT = """You are an autonomous job-management Agent in a controlled benchmark.
Use only the provided tools and never ask a human for help. Never guess job status,
exit codes, logs, or artifacts. Issue exactly one tool call per assistant response;
never make parallel tool calls. Continue until the job is terminal and the JSON
artifact has been read. Your final response must be one JSON object with exactly
these fields: state, exit_code, final_log_marker, score. final_log_marker must be
the hexadecimal token after FINAL_MARKER=, without the prefix. Do not wrap the
JSON in Markdown."""
SUBMIT_PROMPT = """Submit the preconfigured benchmark using the submit tool. This
client will close immediately after the tool returns, so do not do any other work."""


class AgentExperimentError(RuntimeError):
    """An experiment, protocol, or API failure safe to persist in a trial."""


class ModelAPIError(AgentExperimentError):
    """An OpenAI-compatible model request failed after bounded retries."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, value = line.partition("=")
        key = key.strip()
        if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError(f"{path}:{line_number}: invalid .env assignment")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        values[key] = value
    return values


@dataclass(frozen=True)
class LLMConfig:
    api_key: str
    base_url: str
    model: str
    timeout_seconds: float

    @classmethod
    def load(cls, path: Path, *, model_override: str | None = None) -> "LLMConfig":
        file_values = parse_dotenv(path)

        def value(name: str, *aliases: str, default: str = "") -> str:
            result = next(
                (
                    os.environ.get(candidate, file_values.get(candidate, ""))
                    for candidate in (name, *aliases)
                    if os.environ.get(candidate, file_values.get(candidate, ""))
                ),
                default,
            ).strip()
            if not result:
                raise ValueError(f"missing {name} in environment or {path}")
            return result

        base_url = value("LLM_BASE_URL", "BASE_URL").rstrip("/")
        parsed = parse.urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("LLM_BASE_URL must be an absolute HTTP(S) URL")
        timeout = float(value("LLM_TIMEOUT_SECONDS", default="60"))
        if timeout <= 0:
            raise ValueError("LLM_TIMEOUT_SECONDS must be positive")
        return cls(
            api_key=value("LLM_API_KEY", "API_KEY"),
            base_url=base_url,
            model=model_override or value("LLM_MODEL", "MODEL", default="gpt-5.6-luna"),
            timeout_seconds=timeout,
        )

    @property
    def endpoint(self) -> str:
        return self.base_url + "/chat/completions"

    @property
    def safe_origin(self) -> str:
        parsed = parse.urlparse(self.base_url)
        return f"{parsed.scheme}://{parsed.netloc}"


@dataclass
class APIResponse:
    value: dict[str, Any]
    attempts: int
    duration_seconds: float


class ModelClient:
    def __init__(self, config: LLMConfig, *, retries: int = 4):
        self.config = config
        self.retries = retries

    def chat(self, payload: dict[str, Any]) -> APIResponse:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        started = time.monotonic()
        retryable_status = {408, 409, 429, 500, 502, 503, 504}
        last_error = "unknown API error"
        for attempt in range(1, self.retries + 2):
            call = request.Request(
                self.config.endpoint,
                data=encoded,
                method="POST",
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "User-Agent": "awaitless-metric/1",
                },
            )
            try:
                with request.urlopen(call, timeout=self.config.timeout_seconds) as response:
                    raw = response.read()
                value = json.loads(raw.decode("utf-8"))
                if not isinstance(value, dict):
                    raise ModelAPIError("model returned a non-object response")
                return APIResponse(value, attempt, time.monotonic() - started)
            except error.HTTPError as exc:
                body = exc.read(4096).decode("utf-8", errors="replace")
                try:
                    parsed_error = json.loads(body)
                    message = str(parsed_error.get("error", {}).get("message", "request failed"))
                except (json.JSONDecodeError, AttributeError):
                    message = "request failed"
                last_error = f"HTTP {exc.code}: {message[:500]}"
                if exc.code not in retryable_status:
                    break
            except (error.URLError, TimeoutError, socket.timeout, json.JSONDecodeError) as exc:
                last_error = f"{type(exc).__name__}: {str(exc)[:500]}"
            if attempt <= self.retries:
                time.sleep(min(8.0, 2.0 ** (attempt - 1)))
        raise ModelAPIError(
            f"model request failed after {min(attempt, self.retries + 1)} attempts: {last_error}"
        )


def empty_parameters() -> dict[str, Any]:
    return {"type": "object", "properties": {}, "additionalProperties": False}


def job_parameters() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {"job_id": {"type": "string"}},
        "required": ["job_id"],
        "additionalProperties": False,
    }


def function_tool(name: str, description: str, parameters: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {"name": name, "description": description, "parameters": parameters},
    }


SUBMIT_TOOL = function_tool(
    "submit_job",
    "Submit the preconfigured benchmark and return its durable job ID immediately.",
    empty_parameters(),
)
PLAIN_TOOLS = [
    function_tool(
        "poll_job",
        "Poll one tmux job snapshot. It returns current state and the pane log snapshot. Poll again if running.",
        job_parameters(),
    ),
    function_tool(
        "read_artifact",
        "Read result.json after the job is terminal. Do not call while the job is running.",
        job_parameters(),
    ),
]
DURABLE_TOOLS = [
    function_tool(
        "wait_for_job",
        "Block without further model turns until the durable job is terminal, then return exit code, bounded logs, and parsed JSON artifact.",
        job_parameters(),
    )
]


@dataclass
class LLMUsage:
    traces: list[dict[str, Any]] = field(default_factory=list)

    def add(self, *, phase: str, response: APIResponse) -> dict[str, Any]:
        choices = response.value.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ModelAPIError("model response contained no choices")
        choice = choices[0]
        message = choice.get("message")
        usage = response.value.get("usage")
        if not isinstance(message, dict) or not isinstance(usage, dict):
            raise ModelAPIError("model response omitted message or usage")
        for required in ("prompt_tokens", "completion_tokens", "total_tokens"):
            if not isinstance(usage.get(required), int):
                raise ModelAPIError(f"model usage omitted integer {required}")
        tool_names = [
            item.get("function", {}).get("name")
            for item in message.get("tool_calls", []) or []
            if isinstance(item, dict)
        ]
        message_hash = hashlib.sha256(
            json.dumps(message, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        trace = {
            "request_index": len(self.traces) + 1,
            "phase": phase,
            "duration_seconds": round(response.duration_seconds, 6),
            "attempts": response.attempts,
            "finish_reason": choice.get("finish_reason"),
            "tool_names": tool_names,
            "assistant_message_sha256": message_hash,
            "model": response.value.get("model"),
            "system_fingerprint": response.value.get("system_fingerprint"),
            "usage": {
                "prompt_tokens": usage["prompt_tokens"],
                "completion_tokens": usage["completion_tokens"],
                "total_tokens": usage["total_tokens"],
                "prompt_cache_hit_tokens": usage.get("prompt_cache_hit_tokens"),
                "prompt_cache_miss_tokens": usage.get("prompt_cache_miss_tokens"),
                "reasoning_tokens": (usage.get("completion_tokens_details") or {}).get(
                    "reasoning_tokens"
                ),
            },
        }
        self.traces.append(trace)
        return message

    def sum_required(self, name: str) -> int:
        return sum(int(trace["usage"][name]) for trace in self.traces)

    def sum_optional(self, name: str) -> int | None:
        values = [trace["usage"].get(name) for trace in self.traces]
        if not values or any(value is None for value in values):
            return None
        return sum(int(value) for value in values)


def history_message(message: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"role": "assistant", "content": message.get("content")}
    if message.get("tool_calls") is not None:
        result["tool_calls"] = message["tool_calls"]
    # Thinking-mode tool calls require this field to be passed back, but its
    # contents are deliberately not persisted in experiment records.
    if message.get("reasoning_content") is not None:
        result["reasoning_content"] = message["reasoning_content"]
    return result


def tool_calls(message: dict[str, Any]) -> list[dict[str, Any]]:
    calls = message.get("tool_calls") or []
    if not isinstance(calls, list):
        raise AgentExperimentError("assistant tool_calls was not a list")
    return [item for item in calls if isinstance(item, dict)]


def parse_arguments(call: dict[str, Any]) -> dict[str, Any]:
    raw = call.get("function", {}).get("arguments", "{}")
    try:
        value = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise AgentExperimentError(f"tool arguments were not JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise AgentExperimentError("tool arguments must be a JSON object")
    return value


@dataclass
class ManagerObservation:
    state: str | None = None
    exit_code: int | None = None
    artifact: dict[str, Any] | None = None
    log_text: str = ""
    truncated: bool | None = None
    log_snapshot_bytes: list[int] = field(default_factory=list)

    @property
    def terminal(self) -> bool:
        return self.state in {"succeeded", "failed", "cancelled", "timed_out", "lost"}

    @property
    def duplicated_log_bytes(self) -> int:
        if not self.log_snapshot_bytes:
            return 0
        return max(0, sum(self.log_snapshot_bytes) - self.log_snapshot_bytes[-1])


class AgentToolSession:
    def __init__(
        self,
        *,
        arm: str,
        spec: run_local.ScenarioSpec,
        work: Path,
        config: dict[str, Any],
    ):
        self.arm = arm
        self.spec = spec
        self.work = work
        self.config = config
        self.manager_recorder = run_local.Recorder()
        self.runner = run_local.arm_class(arm)(self.manager_recorder, spec, work, config)
        self.job_id: str | None = None
        self.events: list[dict[str, Any]] = []
        self.observation = ManagerObservation()

    @property
    def phase_two_tools(self) -> list[dict[str, Any]]:
        return PLAIN_TOOLS if self.arm == "tmux_plain" else DURABLE_TOOLS

    def _system_commands(self) -> int:
        result = self.manager_recorder.system_commands
        if self.arm == "tmux_wrapped":
            result += run_local.trace_lines(self.runner.trace)
        return result

    def call(self, name: str, arguments: dict[str, Any]) -> str:
        before = self._system_commands()
        started_text = utc_now()
        started = time.monotonic()
        return_code = 0
        try:
            value = self._dispatch(name, arguments)
        except Exception as exc:
            return_code = 2
            value = {"error": f"{type(exc).__name__}: {exc}"}
        rendered = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        encoded = rendered.encode()
        self.events.append(
            {
                "operation": name,
                "started_at": started_text,
                "duration_seconds": round(time.monotonic() - started, 6),
                "agent_call": True,
                "response_bytes": len(encoded),
                "response_sha256": hashlib.sha256(encoded).hexdigest(),
                "response_text": rendered,
                "return_code": return_code,
                "system_command_invocations": max(0, self._system_commands() - before),
                "interrupted": False,
            }
        )
        return rendered

    def _require_id(self, arguments: dict[str, Any]) -> None:
        if not self.job_id or arguments.get("job_id") != self.job_id:
            raise AgentExperimentError("unknown job_id")

    def _dispatch(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if name == "submit_job":
            if self.job_id is not None:
                raise AgentExperimentError("a job has already been submitted")
            self.runner.submit()
            self.job_id = self.runner.session if self.arm == "tmux_plain" else self.runner.job_id
            return {"job_id": self.job_id, "state": "running", "backend": self.arm}
        self._require_id(arguments)
        if name == "poll_job" and self.arm == "tmux_plain":
            time.sleep(float(self.config["poll_interval_seconds"]))
            dead, exit_code, log, _ = self.runner.poll()
            state = "running" if not dead else ("succeeded" if exit_code == 0 else "failed")
            self.observation.state = state
            self.observation.exit_code = exit_code
            self.observation.log_text = log
            self.observation.log_snapshot_bytes.append(len(log.encode()))
            self.observation.truncated = (
                None
                if run_local.expected_truncation(self.spec, self.config)
                else False
            )
            return {
                "job_id": self.job_id,
                "state": state,
                "exit_code": exit_code,
                "log_snapshot": log,
                "truncated": self.observation.truncated,
            }
        if name == "read_artifact" and self.arm == "tmux_plain":
            if not self.observation.terminal:
                raise AgentExperimentError("job is not terminal; poll again")
            result = self.manager_recorder.command(
                "read_artifact", ["cat", "--", str(self.work / "result.json")]
            )
            artifact = run_local.json_stdout(result)
            self.observation.artifact = artifact
            return {"job_id": self.job_id, "artifact": artifact}
        if name == "wait_for_job" and self.arm in {"tmux_wrapped", "awaitless"}:
            result = self.manager_recorder.command(
                "wait_for_job",
                self.runner.wait_command(),
                env=self.runner.env,
                timeout=max(180.0, self.spec.duration_seconds + 30),
            )
            value = run_local.json_stdout(result)
            self.observation.state = value.get("state")
            self.observation.exit_code = value.get("exit_code")
            self.observation.artifact = value.get("parsed_results")
            self.observation.log_text = str(value.get("stdout_tail", "")) + str(
                value.get("stderr_tail", "")
            )
            self.observation.truncated = value.get("truncated")
            return value
        raise AgentExperimentError(f"tool {name!r} is unavailable for arm {self.arm!r}")

    def complete(self) -> bool:
        return (
            self.observation.terminal
            and self.observation.artifact is not None
            and self.spec.marker in self.observation.log_text
        )

    def cleanup(self) -> None:
        self.runner.cleanup()


def model_step(
    *,
    client: ModelClient,
    usage: LLMUsage,
    phase: str,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]],
    experiment_config: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "model": client.config.model,
        "messages": messages,
        "temperature": experiment_config["temperature"],
        "max_tokens": experiment_config["max_completion_tokens"],
        "response_format": {"type": "json_object"},
        "stream": False,
    }
    # The configured OpenAI-compatible thinking model supports tools but rejects
    # tool_choice. When a
    # tool is required, the runner validates that the model actually emitted
    # exactly one call. Once the result is complete, tools are omitted so the
    # model must produce the final JSON response.
    if tools:
        payload["tools"] = tools
    response = client.chat(payload)
    message = usage.add(phase=phase, response=response)
    observed_model = usage.traces[-1]["model"]
    if observed_model != client.config.model:
        raise ModelAPIError(
            f"requested model {client.config.model!r}, response reported {observed_model!r}"
        )
    return message


def final_prompt(job_id: str) -> str:
    return f"""The first client closed after submitting job_id {json.dumps(job_id)}.
Resume this job using only its ID. Wait or poll until it is terminal, read the JSON
artifact, and then return the required JSON object. Never infer a result from the
passage of time."""


def parse_final_answer(content: Any) -> dict[str, Any] | None:
    if not isinstance(content, str):
        return None
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE)
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def answer_correct(answer: dict[str, Any] | None, spec: run_local.ScenarioSpec) -> bool:
    if answer is None:
        return False
    score = answer.get("score")
    return (
        answer.get("state") == spec.expected_state
        and answer.get("exit_code") == spec.exit_code
        and answer.get("final_log_marker") == spec.marker
        and isinstance(score, (int, float))
        and not isinstance(score, bool)
        and abs(float(score) - spec.score) <= 1e-6
    )


def manager_assessment(
    observation: ManagerObservation,
    spec: run_local.ScenarioSpec,
    config: dict[str, Any],
) -> dict[str, bool]:
    state = observation.state == spec.expected_state
    exit_code = observation.exit_code == spec.exit_code
    artifact = observation.artifact == spec.expected_artifact
    marker = spec.marker in observation.log_text
    log_contract = marker and (
        not run_local.expected_truncation(spec, config) or observation.truncated is True
    )
    return {
        "state_correct": state,
        "exit_code_correct": exit_code,
        "artifact_correct": artifact,
        "log_contract_correct": log_contract,
    }


def run_agent_trial(
    *,
    arm: str,
    spec: run_local.ScenarioSpec,
    work: Path,
    experiment_config: dict[str, Any],
    client: ModelClient,
) -> tuple[AgentToolSession, LLMUsage, str | None, dict[str, Any] | None, str | None]:
    session = AgentToolSession(arm=arm, spec=spec, work=work, config=experiment_config)
    usage = LLMUsage()
    final_content: str | None = None
    parsed_final: dict[str, Any] | None = None
    trial_error: str | None = None
    try:
        phase_one = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": SUBMIT_PROMPT},
        ]
        message = model_step(
            client=client,
            usage=usage,
            phase="submit_client",
            messages=phase_one,
            tools=[SUBMIT_TOOL],
            experiment_config=experiment_config,
        )
        calls = tool_calls(message)
        if len(calls) != 1 or calls[0].get("function", {}).get("name") != "submit_job":
            raise AgentExperimentError("submit client did not issue exactly one submit_job call")
        submit_result = session.call("submit_job", parse_arguments(calls[0]))
        submit_value = json.loads(submit_result)
        if submit_value.get("error") or not session.job_id:
            raise AgentExperimentError(f"submit tool failed: {submit_value.get('error')}")

        # This reset is the injected client loss: no phase-one messages or tool
        # result survive. The new client receives only the stable job ID.
        phase_two: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": final_prompt(session.job_id)},
        ]
        for _ in range(int(experiment_config["max_agent_turns"])):
            require_tool = not session.complete()
            message = model_step(
                client=client,
                usage=usage,
                phase="recovery_client",
                messages=phase_two,
                tools=session.phase_two_tools if require_tool else [],
                experiment_config=experiment_config,
            )
            phase_two.append(history_message(message))
            calls = tool_calls(message)
            if require_tool:
                if len(calls) != 1:
                    raise AgentExperimentError(
                        f"recovery client issued {len(calls)} tool calls; expected exactly one"
                    )
                call = calls[0]
                name = call.get("function", {}).get("name")
                if not isinstance(name, str):
                    raise AgentExperimentError("tool call omitted function name")
                rendered = session.call(name, parse_arguments(call))
                phase_two.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.get("id"),
                        "content": rendered,
                    }
                )
                continue
            if calls:
                raise AgentExperimentError("model called a tool after the complete result was available")
            final_content = message.get("content")
            parsed_final = parse_final_answer(final_content)
            if parsed_final is None:
                raise AgentExperimentError("final response was not a valid JSON object")
            break
        else:
            raise AgentExperimentError("Agent exceeded max_agent_turns")
    except Exception as exc:
        trial_error = f"{type(exc).__name__}: {exc}"
    return session, usage, final_content, parsed_final, trial_error


def expected_value(spec: run_local.ScenarioSpec) -> dict[str, Any]:
    return {
        "state": spec.expected_state,
        "exit_code": spec.exit_code,
        "artifact": spec.expected_artifact,
        "final_log_marker": spec.marker,
        "full_log_bytes": spec.full_log_bytes,
        "workload": {
            "duration_seconds": spec.duration_seconds,
            "line_count": spec.line_count,
            "line_bytes": spec.line_bytes,
            "cancel_after_seconds": spec.cancel_after_seconds,
        },
    }


def build_record(
    *,
    experiment_id: str,
    case_id: str,
    seed: int,
    arm: str,
    spec: run_local.ScenarioSpec,
    session: AgentToolSession,
    usage: LLMUsage,
    final_content: str | None,
    parsed_final: dict[str, Any] | None,
    trial_error: str | None,
    elapsed: float,
    experiment_config: dict[str, Any],
    environment: dict[str, Any],
    trial_root: Path,
) -> dict[str, Any]:
    assessment = manager_assessment(session.observation, spec, experiment_config)
    final_correct = answer_correct(parsed_final, spec)
    result_correct = final_correct and all(assessment.values()) and trial_error is None
    input_tokens = usage.sum_required("prompt_tokens") if usage.traces else None
    output_tokens = usage.sum_required("completion_tokens") if usage.traces else None
    cache_hit = usage.sum_optional("prompt_cache_hit_tokens")
    cache_miss = usage.sum_optional("prompt_cache_miss_tokens")
    reasoning = usage.sum_optional("reasoning_tokens")
    return {
        "schema_version": 1,
        "record_type": "trial",
        "experiment_id": experiment_id,
        "case_id": case_id,
        "trial_id": f"{case_id}:{arm}",
        "recorded_at": utc_now(),
        "arm": arm,
        "scenario": spec.name,
        "seed": seed,
        "environment": environment,
        "expected": expected_value(spec),
        "observed": {
            "state": session.observation.state,
            "exit_code": session.observation.exit_code,
            "artifact": session.observation.artifact,
            "final_log_marker_seen": spec.marker in session.observation.log_text,
            "truncated": session.observation.truncated,
            "orphan_processes": None,
            "recovery_injected": True,
            "agent_answer": parsed_final,
        },
        "metrics": {
            "result_correct": result_correct,
            "state_correct": assessment["state_correct"],
            "exit_code_correct": assessment["exit_code_correct"],
            "artifact_correct": assessment["artifact_correct"],
            "log_contract_correct": assessment["log_contract_correct"],
            "recovery_success": result_correct,
            "cancel_cleanup_success": None,
            "duplicate_launch": sum(
                event["operation"] == "submit_job" for event in session.events
            ) > 1,
            "agent_answer_correct": final_correct,
            "agent_tool_calls": len(session.events),
            "agent_visible_bytes": sum(event["response_bytes"] for event in session.events),
            "system_command_invocations": sum(
                event["system_command_invocations"] for event in session.events
            ),
            "duplicated_log_bytes": session.observation.duplicated_log_bytes,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cached_input_tokens": cache_hit,
            "prompt_cache_miss_tokens": cache_miss,
            "reasoning_tokens": reasoning,
            "api_requests": len(usage.traces),
            "wall_time_seconds": round(elapsed, 6),
            "cpu_time_seconds": None,
            "peak_rss_bytes": None,
            "disk_bytes": run_local.disk_bytes(trial_root),
            "ssh_request_count": None,
            "manual_interventions": 0,
        },
        "events": session.events,
        "arm_metadata": run_local.arm_class(arm).metadata,
        "llm": {
            "provider": "openai-compatible",
            "model": environment["llm_model"],
            "base_url_origin": environment["llm_base_url_origin"],
            "client_reset_after_submit": True,
            "thinking_mode": "provider_default",
            "max_completion_tokens": experiment_config["max_completion_tokens"],
            "system_prompt": SYSTEM_PROMPT,
            "submit_prompt": SUBMIT_PROMPT,
            "recovery_prompt": final_prompt(session.job_id or "missing"),
            "requests": usage.traces,
            "final_answer_text": final_content,
            "final_answer_json": parsed_final,
        },
        "error": trial_error,
    }


def validate_agent_config(config: dict[str, Any]) -> None:
    run_local.validate_config(config)
    if set(config["scenarios"]) != {"recovery"}:
        raise ValueError("Agent token experiment currently requires exactly the recovery scenario")
    if int(config.get("max_agent_turns", 0)) <= 0:
        raise ValueError("max_agent_turns must be positive")
    if int(config.get("max_completion_tokens", 0)) <= 0:
        raise ValueError("max_completion_tokens must be positive")


def preflight(client: ModelClient, experiment_config: dict[str, Any]) -> dict[str, Any]:
    usage = LLMUsage()
    response = model_step(
        client=client,
        usage=usage,
        phase="preflight",
        messages=[
            {
                "role": "system",
                "content": "Call the provided tool exactly once and use JSON-compatible output.",
            },
            {"role": "user", "content": "Call api_preflight now."},
        ],
        tools=[function_tool("api_preflight", "Validate tool calling.", empty_parameters())],
        experiment_config=experiment_config,
    )
    calls = tool_calls(response)
    if len(calls) != 1 or calls[0].get("function", {}).get("name") != "api_preflight":
        raise AgentExperimentError("preflight did not return the required tool call")
    trace = usage.traces[0]
    return {
        "ok": True,
        "model": trace["model"],
        "tool_call": "api_preflight",
        "prompt_tokens": trace["usage"]["prompt_tokens"],
        "completion_tokens": trace["usage"]["completion_tokens"],
        "cache_usage_available": trace["usage"]["prompt_cache_hit_tokens"] is not None,
        "reasoning_usage_available": trace["usage"]["reasoning_tokens"] is not None,
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--env-file", type=Path, default=DEFAULT_ENV)
    result.add_argument("--model", default="gpt-5.6-luna", help="model identifier for this evidence run")
    result.add_argument(
        "--config", type=Path, default=METRIC_ROOT / "configs" / "agent-smoke.json"
    )
    result.add_argument("--output", type=Path)
    result.add_argument("--append", action="store_true")
    result.add_argument("--trials", type=int)
    result.add_argument("--preflight", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        llm_config = LLMConfig.load(args.env_file, model_override=args.model)
        experiment_config = json.loads(args.config.read_text(encoding="utf-8"))
        if args.trials is not None:
            experiment_config["trials"] = args.trials
        validate_agent_config(experiment_config)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"agent metric: {exc}", file=sys.stderr)
        return 2
    client = ModelClient(llm_config)
    if args.preflight:
        try:
            print(json.dumps(preflight(client, experiment_config), separators=(",", ":")))
            return 0
        except AgentExperimentError as exc:
            print(f"agent metric preflight: {exc}", file=sys.stderr)
            return 1
    if args.output is None:
        print("agent metric: --output is required unless --preflight is used", file=sys.stderr)
        return 2
    if args.output.exists() and not args.append:
        print(
            f"agent metric: refusing to overwrite {args.output}; pass --append or use a new path",
            file=sys.stderr,
        )
        return 2

    environment = run_local.base_environment(args.config, experiment_config)
    environment.update(
        experiment_kind="llm_agent",
        llm_provider="openai-compatible",
        llm_model=llm_config.model,
        llm_base_url_origin=llm_config.safe_origin,
        llm_timeout_seconds=llm_config.timeout_seconds,
        agent_runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    )
    config_hash = hashlib.sha256(
        json.dumps(experiment_config, sort_keys=True).encode()
    ).hexdigest()[:10]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    experiment_id = f"{experiment_config['name']}-{timestamp}-{config_hash}"
    order_rng = random.Random(int(experiment_config["seed"]))
    records = 0
    incorrect = 0
    errors = 0
    total_tokens = 0

    for trial_index in range(int(experiment_config["trials"])):
        scenario = "recovery"
        case_id = f"{experiment_id}:{scenario}:{trial_index:03d}"
        seed_material = f"{experiment_config['seed']}:{trial_index}:{scenario}".encode()
        case_seed = int.from_bytes(hashlib.sha256(seed_material).digest()[:8], "big")
        spec = run_local.sample_spec(
            scenario,
            experiment_config["scenarios"][scenario],
            case_id=case_id,
            rng=random.Random(case_seed),
        )
        arms = list(experiment_config["arms"])
        order_rng.shuffle(arms)
        for arm in arms:
            print(
                f"[agent-metric] case={trial_index + 1}/{experiment_config['trials']} arm={arm} "
                f"tokens_so_far={total_tokens}",
                file=sys.stderr,
                flush=True,
            )
            with tempfile.TemporaryDirectory(prefix=f"awaitless-agent-{arm}-") as temporary:
                trial_root = Path(temporary)
                work = trial_root / "work"
                work.mkdir()
                started = time.monotonic()
                session, usage, final_content, parsed_final, trial_error = run_agent_trial(
                    arm=arm,
                    spec=spec,
                    work=work,
                    experiment_config=experiment_config,
                    client=client,
                )
                elapsed = time.monotonic() - started
                try:
                    record = build_record(
                        experiment_id=experiment_id,
                        case_id=case_id,
                        seed=case_seed,
                        arm=arm,
                        spec=spec,
                        session=session,
                        usage=usage,
                        final_content=final_content,
                        parsed_final=parsed_final,
                        trial_error=trial_error,
                        elapsed=elapsed,
                        experiment_config=experiment_config,
                        environment=environment,
                        trial_root=trial_root,
                    )
                    run_local.append_record(args.output, record)
                finally:
                    try:
                        session.cleanup()
                    except Exception as cleanup_exc:
                        print(
                            f"[agent-metric] cleanup warning arm={arm}: {type(cleanup_exc).__name__}",
                            file=sys.stderr,
                        )
            records += 1
            incorrect += int(not record["metrics"]["result_correct"])
            errors += int(record["error"] is not None)
            total_tokens += int(record["metrics"]["input_tokens"] or 0) + int(
                record["metrics"]["output_tokens"] or 0
            )
            if errors >= 3 and errors == records:
                print("agent metric: stopping after three consecutive infrastructure errors", file=sys.stderr)
                return 1

    print(
        json.dumps(
            {
                "experiment_id": experiment_id,
                "records": records,
                "incorrect_records": incorrect,
                "error_records": errors,
                "total_usage_tokens": total_tokens,
                "output": str(args.output),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
