# Awaitless

<!-- mcp-name: io.github.xpluspro/awaitless -->

[![CI](https://github.com/xpluspro/Awaitless/actions/workflows/ci.yml/badge.svg)](https://github.com/xpluspro/Awaitless/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/awaitless-runner.svg)](https://pypi.org/project/awaitless-runner/)
[![Python](https://img.shields.io/pypi/pyversions/awaitless-runner.svg)](https://pypi.org/project/awaitless-runner/)

**Durable MCP Tasks on infrastructure you already own — local, SSH, and Slurm.**

Awaitless turns a long command into a persistent MCP Task and stable job ID.
The task survives client restarts and returns its exit code, bounded logs, and
declared JSON Artifacts without moving the workload into a hosted sandbox.

[简体中文](https://github.com/xpluspro/Awaitless/blob/main/README.zh-CN.md)

![Awaitless SSH submit, disconnect, resume, and Artifact demo](https://raw.githubusercontent.com/xpluspro/Awaitless/main/assets/awaitless-demo.gif)

## Why Awaitless

- **Survives the client:** closing the terminal or interrupting `wait` does not
  cancel the managed job. Reuse the same ID from a new client.
- **MCP Tasks compatibility:** `run_job` exposes a durable Task handle with
  `tasks/get`, `tasks/update`, `tasks/cancel`, TTL, and reconnect recovery.
- **Backward-compatible MCP tools:** `submit_job`, `wait_for_job`,
  `get_job_status`, `get_job_logs`, `cancel_job`, and `list_jobs` remain
  available over stdio.
- **Schedules cluster work:** the Slurm backend persists scheduler IDs and maps
  queue/accounting state, exit codes, cancellation, logs, and Artifacts.
- **Returns bounded context:** stdout and stderr tails share a configurable byte
  budget; complete logs stay on disk.
- **Returns machine-readable results:** declared JSON Artifacts are parsed into
  `parsed_results`.
- **Handles real cluster edges:** SSH liveness uses a wrapper-owned heartbeat and
  does not assume separate login sessions can see the same PID namespace.

## Install

The distribution name is `awaitless-runner`. It installs the `awaitless` CLI,
the `awaitless-mcp` stdio server, and the Registry-compatible
`awaitless-runner` server alias. Awaitless requires Linux, Python 3.10+, and
Bash. SSH and Slurm hosts also require OpenSSH (`ssh` and `sftp`) locally.

```bash
python -m pip install awaitless-runner
awaitless doctor --json
```

MCP Registry clients can launch the published server in one command:

```bash
uvx awaitless-runner
```

From a source checkout:

```bash
python -m pip install -e .
```

## Agent-native MCP quick start

Point your MCP client at the installed stdio command (adapt the outer key to
your client's configuration format):

```json
{
  "mcpServers": {
    "awaitless": {
      "command": "awaitless-mcp",
      "args": ["--config", "/home/me/.config/awaitless/config.toml"]
    }
  }
}
```

The server uses the official
[`modelcontextprotocol/python-sdk`](https://github.com/modelcontextprotocol/python-sdk).
Tasks-aware clients call `run_job` with an argv array and a stable
`client_request_id`; they immediately receive a durable Task handle. Existing
clients can continue to call `submit_job` and later `wait_for_job`. Each MCP
invocation opens the same SQLite store; there is no Awaitless daemon, HTTP
endpoint, or Web service. Stopping the stdio server does not stop a submitted
job.

> Acceptance criterion: install the PyPI package and configure one MCP server;
> the Agent can submit to Slurm, survive a client disconnect, and receive a
> structured result without writing Awaitless CLI commands.

## MCP Tasks compatibility

Awaitless implements the current `io.modelcontextprotocol/tasks` extension on
top of the MCP Python SDK 2.x extension API. It advertises the extension through
`server/discover`; a client opts in through
`_meta.io.modelcontextprotocol/clientCapabilities.extensions`.

For opted-in clients, `tools/call` on `run_job` returns immediately with
`resultType: "task"`, a stable `taskId`, status, timestamps, TTL, and suggested
poll interval. A later client can use the same handle with:

- `tasks/get` — refresh state and return the final `CallToolResult` inline;
- `tasks/cancel` — cancel the verified local process group, SSH job, or Slurm job;
- `tasks/update` — acknowledge input responses (command jobs never request input).

Awaitless maps `pending`/`starting`/`running` to `working`, cancellation to
`cancelled`, and all other terminal job states to `completed`. The final
Awaitless state and real exit code remain in the structured tool result, so a
non-zero command exit is not confused with a JSON-RPC protocol failure. The
legacy MCP tool surface is retained as a compatibility layer. See
[`docs/MCP_TASKS.md`](docs/MCP_TASKS.md) for the wire contract and migration
notes.

## Quick start

Submit returns before the job finishes:

```bash
awaitless submit --json --name build -- ninja -C build
```

```json
{"job_id":"job_019F...","state":"running","backend":"local"}
```

For a retry-safe expensive job, reuse a caller-generated request ID:

```bash
awaitless submit --json --client-request-id training:run-2026-08-10 -- ./train.sh
```

The ID and normalized submission fingerprint are reserved atomically before
any backend launch. Retrying the same request returns the original `job_id`;
reusing the ID with different arguments is rejected. This prevents a lost SSH
or MCP response from launching a second GPU or Slurm job.

Then make one blocking call:

```bash
awaitless wait job_019F... --json
```

If that client is closed or interrupted, start a new one and run the same
`wait` command with the saved ID. The managed job keeps running.

Useful one-shot operations:

```bash
awaitless status <job-id> --json
awaitless logs <job-id> --tail 200 --json
awaitless cancel <job-id> --grace-period 5s --json
awaitless list --state running --json
awaitless inspect <job-id> --json
```

Run the five-minute recovery story locally, without Slurm:

```bash
awaitless demo --json
```

The demo submits a job, terminates the first waiting client, starts a fresh
client using only the durable job ID, and verifies the JSON Artifact.

## SSH and structured Artifacts

Declare a host in `~/.config/awaitless/config.toml`:

```toml
[defaults]
backend = "local"
log_tail_lines = 200
max_return_bytes = 65536
poll_interval = 2

[hosts.gpu]
hostname = "gpu.example.com"
port = 22
user = "developer"
identity_file = "~/.ssh/id_ed25519"
remote_job_dir = "~/.awaitless/jobs"
# gssapi_authentication = false
# connect_timeout = 8
# operation_timeout = 20
```

`operation_timeout` is the minimum timeout for one SSH control operation, not a
job runtime limit. Use `submit --timeout` to limit the job itself.

Submit a remote command and declare its result:

```bash
awaitless submit --json \
  --host gpu \
  --cwd /workspace/project \
  --timeout 2h \
  --artifact results/benchmark.json \
  -- ./run_benchmark.sh
```

On completion, `wait --json` reports Artifact existence, size, and modification
time. A declared JSON file within the return budget is also exposed directly:

```json
{
  "state": "succeeded",
  "exit_code": 0,
  "truncated": false,
  "parsed_results": {
    "correctness": true,
    "latency_us": 24.7
  }
}
```

Relative local Artifacts are resolved from the submission working directory,
even if a later client runs elsewhere. `--log-dir /path/to/logs` creates an
isolated `/path/to/logs/<job-id>/` directory per job.

## Slurm backend

Configure a scheduler host and its default resource request:

```toml
[defaults]
backend = "slurm"
host = "cluster"
poll_interval = 10
log_tail_lines = 200
max_return_bytes = 65536

[hosts.cluster]
hostname = "login.cluster.example"
user = "developer"
backend = "slurm"
gssapi_authentication = false
operation_timeout = 30
slurm_accounting_grace = 120
slurm_job_dir = ".awaitless/slurm/jobs"

[hosts.cluster.slurm]
partition = "compute"
account = "research"
nodes = 1
ntasks = 1
cpus_per_task = 1
time = "00:30:00"
```

With `defaults.host` configured, MCP calls may omit both `backend` and `host`.
`submit_job` may override the allowlisted options `account`, `constraint`,
`cpus_per_task`, `gres`, `mem`, `nodes`, `ntasks`, `partition`, `qos`, and
`time` through `slurm_options`. The backend sends the batch script to `sbatch`
over stdin, persists the returned Slurm ID, checks active state with `squeue`,
recovers terminal state/exit code/runtime with `sacct`, and cancels with
`scancel`. User computation is therefore scheduled on an allocated compute
node—never launched as a process on the SSH login node. A separate SFTP data
channel creates the private job directory and reads only the bounded log tails
and declared Artifacts.

Slurm `PENDING` maps to Awaitless `pending`; active scheduler states map to
`running`; `COMPLETED` maps to `succeeded`; `CANCELLED` maps to `cancelled`;
`TIMEOUT`/`DEADLINE` map to `timed_out`; scheduler, node, launch, OOM, and
preemption failures map to `failed`. `ExitCode` values such as `7:0` and signal
terminations are preserved as process-style exit codes.

### Real MCP → Slurm disconnect demo

On 2026-08-10, two separate MCP stdio clients ran the checked-in demo against
a real Slurm 25.11.2 cluster:

| Phase | Observed result |
|---|---|
| Client 1 `submit_job` | Awaitless `job_019FE9CB2847AC929E0B2F`, Slurm `60597793`, `pending` |
| Client 1 exits | No daemon or waiter remains attached |
| Client 2 `wait_for_job` | `succeeded`, exit `0`, runtime `8.0s` |
| Bounded stdout | `compute_host=node099 slurm_job_id=60597793` (43 bytes) |
| JSON Artifact | Parsed `{ "ok": true, "compute_host": "node099", "slurm_job_id": "60597793" }` |

`node099` is the allocated compute node. The reproducible driver is
[`scripts/mcp_slurm_demo.py`](https://github.com/xpluspro/Awaitless/blob/main/scripts/mcp_slurm_demo.py),
and the raw structured evidence is
[`assets/mcp-slurm-demo.json`](https://github.com/xpluspro/Awaitless/blob/main/assets/mcp-slurm-demo.json).

## Real experiment: 12 polls to 2 calls

On 2026-08-10, the reproducible experiment ran the same sleep-only workload on
a real SSH login node: twelve 1 KiB log records, 4.5 seconds apart. It used no
CPU- or GPU-intensive work. The traditional side repeatedly fetched its entire
log snapshot twelve times; Awaitless used one `submit` and one `wait`.

| Measured result | Traditional SSH polling | Awaitless |
|---|---:|---:|
| Poll/check calls after launch | 12 | 0 |
| Agent-visible CLI calls, including launch | 13 | 2 |
| Logical log bytes returned | 84,992 B | 12,288 B |
| Repeated log bytes | 72,704 B | 0 B |
| Exit code | 0 | 0 |
| Parsed JSON Artifact | No | Yes |

That is **72,704 fewer returned log bytes (85.5%)** and **13 → 2 agent-visible
CLI calls (84.6%)**. The twelve traditional log snapshots were
`[1024, 2048, 3072, 4096, 5120, 6144, 8192, 9216, 10240, 11264, 12288, 12288]`
bytes. "Calls" here means agent-visible CLI invocations; Awaitless's internal
SSH control operations do not trigger additional agent turns. The byte figures
are decoded log content, not estimated tokens or network wire bytes.

The runnable method and raw result are in
[`benchmarks/`](https://github.com/xpluspro/Awaitless/tree/main/benchmarks).

## Measuring project value

The repository also contains a pre-registered comparison framework in
[`metric/`](https://github.com/xpluspro/Awaitless/tree/main/metric). It runs the
same randomized workloads through plain tmux, a strong tmux wrapper, and
Awaitless; records one JSONL row per trial; and reports result fidelity,
disconnect recovery, agent-visible calls/bytes, real usage tokens when supplied,
process-tree cleanup, latency, and consumer-owned glue code. The smoke profile
validates the harness only. Publishable claims require the evidence profile,
real SSH fault injection, and real Agent API usage.

The first live Agent report is now available in
[`metric/results/deepseek-agent-v2-report.md`](metric/results/deepseek-agent-v2-report.md):
20 paired DeepSeek cases measured a 71.4% median tool-call reduction and 85.3%
fewer usage tokens per correct job versus plain tmux. The strong tmux wrapper
matched Awaitless on calls and used 9.2% fewer tokens per correct job, while
requiring 319 lines of consumer-owned glue. These are scoped experimental
results, not universal savings claims.

For orchestration-level testing, [`metric/LONG_RUNNING.md`](metric/LONG_RUNNING.md)
adds a Blocking-vs-Awaitless benchmark over controlled `cargo build`, pytest,
Docker build, `npm install`, and model-inference workloads. It includes a strong
parallel-Blocking baseline and measures synchronous Agent occupancy separately
from model reasoning, so it can show where direct blocking is faster as well as
where durable submission and reconnect recovery help.

## Awaitless vs. alternatives

| Tool | Primary abstraction | Survives client exit | Durable status / exit code | Agent-bounded JSON result | Scheduling / resources | Best fit |
|---|---|:---:|:---:|:---:|:---:|---|
| **Awaitless** | Local, SSH, or Slurm job ID + MCP tools | Yes | Yes | Yes | Slurm | Agent-native jobs that need scheduling, resume, bounded logs, and Artifacts |
| **nohup** | Ignore SIGHUP + redirect output | Often | Manual | No | No | Keeping one shell command alive when manual PID/log handling is enough |
| **tmux** | Persistent interactive terminal | Yes | Manual | No | No | Humans detaching from and reattaching to an interactive shell |
| **Pueue** | Daemon-backed local task queue | Yes | Yes | Partial; status/log JSON | Local queue only | Human-operated queues and parallel task groups on one machine |
| **Slurm** | Cluster workload manager | Yes | Yes, with accounting | Job-defined | Yes | Allocating and scheduling cluster CPU/GPU resources |
| **Codex Goal mode** | Durable agent objective across turns | Yes | Not a process supervisor | Tool-dependent | No | Multi-turn agent orchestration; complementary to Awaitless |

Source notes: GNU [`nohup`](https://www.gnu.org/software/coreutils/manual/html_node/nohup-invocation.html),
[`tmux`](https://tmux.github.io/), [`Pueue`](https://github.com/Nukesor/pueue),
the Slurm [overview](https://slurm.schedmd.com/overview.html), and the Codex
[Goal mode guide](https://learn.chatgpt.com/use-cases/follow-goals). Awaitless
uses Slurm for allocation instead of replacing the cluster scheduler.

## Reliability model

- The local runner and user command have independent sessions and process
  groups; cancellation targets the whole validated group.
- SQLite uses WAL, and active-to-terminal transitions are transactional so
  completion, cancellation, and stall detection cannot overwrite each other.
- SSH wrappers atomically persist `exit_code` and `finished_at`. A lightweight
  heartbeat handles hosts where separate SSH sessions cannot inspect the same
  PID namespace; PID, process group, and `/proc` start time remain a fallback.
- SSH cancellation persists intent before signaling the validated process
  group. OpenSSH host-key verification keeps its secure defaults.
- Slurm control-plane SSH calls are restricted to
  `sbatch`/`squeue`/`sacct`/`scancel`; file access uses SFTP, and arbitrary
  computation exists only inside the submitted batch script.
- Suspected credential values are redacted from metadata, and the executable
  run specification is stored with mode `0600`.

States are `starting`, `running`, `stalled`, `succeeded`, `failed`, `cancelled`,
`timed_out`, and `lost`. `--stall-timeout 20m` reports a stalled job but does
not cancel it automatically.

CLI exit codes: 0 success, 1 internal error, 2 invalid usage, 3 job failure,
4 job/client wait timeout, 5 cancelled, 6 lost, and 7 SSH connection failure.

## Development

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
ruff check src tests benchmarks scripts metric
```

GitHub Actions runs the test suite on every supported CPython release from 3.10
through 3.14, then builds the distributions, checks the PyPI README, and runs an
installed-wheel CLI/Artifact smoke test. Version tags use PyPI Trusted
Publishing without a stored API token.

The Codex Skill lives in
[`skills/awaitless`](https://github.com/xpluspro/Awaitless/tree/main/skills/awaitless).
The v0.1 product requirements are in
[`docs/PRD.zh-CN.md`](https://github.com/xpluspro/Awaitless/blob/main/docs/PRD.zh-CN.md),
and the v0.2 Agent/Slurm acceptance contract is in
[`docs/v0.2.zh-CN.md`](https://github.com/xpluspro/Awaitless/blob/main/docs/v0.2.zh-CN.md).

## License

[MIT](https://github.com/xpluspro/Awaitless/blob/main/LICENSE)
