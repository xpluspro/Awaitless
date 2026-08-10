# Awaitless

Durable, bounded, event-driven jobs for AI coding agents.

Awaitless turns a long local or SSH command into a persistent job with a stable
`job_id`. An agent submits once, waits once, and receives the exit code, bounded
logs, and declared JSON Artifacts—without repeatedly spending tool calls and
context on `sleep`, `ps`, `tail`, or SSH polling.

[简体中文](https://github.com/xpluspro/Awaitless/blob/main/README.zh-CN.md)

![Awaitless SSH submit, disconnect, resume, and Artifact demo](https://raw.githubusercontent.com/xpluspro/Awaitless/main/assets/awaitless-demo.gif)

## Why Awaitless

- **Survives the client:** closing the terminal or interrupting `wait` does not
  cancel the managed job. Reuse the same ID from a new client.
- **Works locally and over SSH:** each job has durable metadata, logs, exit state,
  and a remotely persisted wrapper.
- **Returns bounded context:** stdout and stderr tails share a configurable byte
  budget; complete logs stay on disk.
- **Returns machine-readable results:** declared JSON Artifacts are parsed into
  `parsed_results`.
- **Handles real cluster edges:** SSH liveness uses a wrapper-owned heartbeat and
  does not assume separate login sessions can see the same PID namespace.

## Install

The distribution name is `awaitless-runner`; the command remains `awaitless`.
Awaitless requires Linux, Python 3.10+, and Bash. The SSH backend also requires
an OpenSSH client.

```bash
python -m pip install awaitless-runner
awaitless doctor --json
```

From a source checkout:

```bash
python -m pip install -e .
```

## Quick start

Submit returns before the job finishes:

```bash
awaitless submit --json --name build -- ninja -C build
```

```json
{"job_id":"job_019F...","state":"running","backend":"local"}
```

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

## Awaitless vs. alternatives

| Tool | Primary abstraction | Survives client exit | Durable status / exit code | Agent-bounded JSON result | Scheduling / resources | Best fit |
|---|---|:---:|:---:|:---:|:---:|---|
| **Awaitless** | Local or SSH job ID | Yes | Yes | Yes | No | Non-interactive agent jobs that need resume, bounded logs, and Artifacts |
| **nohup** | Ignore SIGHUP + redirect output | Often | Manual | No | No | Keeping one shell command alive when manual PID/log handling is enough |
| **tmux** | Persistent interactive terminal | Yes | Manual | No | No | Humans detaching from and reattaching to an interactive shell |
| **Pueue** | Daemon-backed local task queue | Yes | Yes | Partial; status/log JSON | Local queue only | Human-operated queues and parallel task groups on one machine |
| **Slurm** | Cluster workload manager | Yes | Yes, with accounting | Job-defined | Yes | Allocating and scheduling cluster CPU/GPU resources |
| **Codex Goal mode** | Durable agent objective across turns | Yes | Not a process supervisor | Tool-dependent | No | Multi-turn agent orchestration; complementary to Awaitless |

Source notes: GNU [`nohup`](https://www.gnu.org/software/coreutils/manual/html_node/nohup-invocation.html),
[`tmux`](https://tmux.github.io/), [`Pueue`](https://github.com/Nukesor/pueue),
the Slurm [overview](https://slurm.schedmd.com/overview.html), and the Codex
[Goal mode guide](https://learn.chatgpt.com/use-cases/follow-goals). If a cluster
requires Slurm, use Slurm for allocation; Awaitless v0.1 is not a scheduler.

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
ruff check src tests benchmarks
```

The Codex Skill lives in
[`skills/awaitless`](https://github.com/xpluspro/Awaitless/tree/main/skills/awaitless).
The v0.1 product requirements are in
[`docs/PRD.zh-CN.md`](https://github.com/xpluspro/Awaitless/blob/main/docs/PRD.zh-CN.md).

## License

[MIT](https://github.com/xpluspro/Awaitless/blob/main/LICENSE)
