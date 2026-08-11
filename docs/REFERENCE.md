# Awaitless reference guide

This guide contains the technical detail intentionally kept out of the project
README. It describes Awaitless 0.3.x as shipped by the `awaitless-runner`
distribution.

## Requirements and installation

Awaitless requires Linux, Python 3.10 or newer, and Bash. SSH and Slurm targets
also require local OpenSSH commands (`ssh` and `sftp`). A Slurm backend host must
provide `sbatch`, `squeue`, `sacct`, and `scancel`.
Named queues on an SSH target additionally require `flock` from util-linux.

The distribution installs three commands:

- `awaitless` — direct CLI;
- `awaitless-mcp` — stdio MCP server;
- `awaitless-runner` — Registry-compatible alias for the same MCP server.

```bash
uv tool install awaitless-runner
awaitless doctor --json
```

Equivalent pip installation and a source checkout:

```bash
python -m pip install awaitless-runner
python -m pip install -e .
```

## Architecture and runtime model

Awaitless separates a client invocation from the managed job. The client writes
a durable job record to SQLite, launches a backend-owned runner or scheduler
job, and receives a stable Awaitless ID. Later CLI or MCP processes open the
same store and reconcile the backend state.

There is no Awaitless daemon, Web service, or HTTP endpoint. Stopping the MCP
stdio server or interrupting `wait` does not stop a submitted job.

Backend behavior:

- **Local:** a runner and user command have independent sessions and process
  groups. Cancellation targets the validated command process group.
- **SSH:** a remote wrapper owns the job files and heartbeat. Separate SSH
  control calls inspect that durable state; they do not have to share a PID
  namespace with the command.
- **Slurm:** Awaitless submits a batch script to `sbatch`, persists the scheduler
  ID, and uses Slurm queue/accounting data as the workload control plane.

SQLite uses WAL. Active-to-terminal state transitions are transactional so
completion, cancellation, and stall detection cannot overwrite each other.

## Configuration

The default configuration file is `~/.config/awaitless/config.toml`. Override
it with `--config` or `AWAITLESS_CONFIG`. Durable data defaults to
`~/.local/share/awaitless`; override it with `AWAITLESS_DATA_DIR` or
`defaults.data_dir`.

```toml
[defaults]
backend = "local"
log_tail_lines = 200
max_return_bytes = 65536
poll_interval = 2
mcp_task_ttl_seconds = 604800
mcp_task_poll_interval_seconds = 2
```

`backend` may be `local`, `ssh`, or `slurm`. Set `defaults.host` when an SSH or
Slurm target should be selected automatically.

## CLI reference

Global options:

```text
--config PATH     use a specific TOML configuration
--json            emit machine-readable JSON
--verbose         increase diagnostic output
--quiet           suppress non-result output
--version         print the installed version
```

Commands:

| Command | Purpose |
|---|---|
| `submit` | Create a durable local, SSH, or Slurm job and return before it finishes. |
| `wait` | Block until a job is terminal or the client-side wait timeout expires. |
| `status` | Reconcile and return the current state. |
| `logs` | Return bounded stdout and stderr tails. |
| `cancel` | Persist cancellation intent and stop the managed process group or scheduler job. |
| `list` | List recent jobs, optionally by state, host, or queue. |
| `queue create` | Create an immutable named FIFO queue and concurrency limit. |
| `queue list` | List queues and their queued, active, and total job counts. |
| `inspect` | Return job metadata and state history. |
| `doctor` | Check local and configured SSH prerequisites. |
| `demo` | Exercise submit, waiter interruption, reconnect, and JSON Artifact recovery locally. |

Common operations:

```bash
awaitless submit --json --name build -- ninja -C build
awaitless wait <job-id> --json
awaitless status <job-id> --json
awaitless logs <job-id> --tail 200 --json
awaitless cancel <job-id> --grace-period 5s --json
awaitless list --state running --json
awaitless queue create gpu0 --concurrency 1 --json
awaitless queue list --json
awaitless inspect <job-id> --json
```

Important `submit` options:

```text
--backend {local,ssh,slurm}
--host NAME
--cwd PATH
--env NAME=VALUE
--timeout DURATION
--stall-timeout DURATION
--log-dir PATH
--artifact PATH
--slurm-option NAME=VALUE
--name NAME
--queue NAME
--client-request-id ID
```

The command follows `--`, which prevents command arguments from being parsed as
Awaitless flags.

### Idempotent submission

For an expensive job, use a caller-generated request ID:

```bash
awaitless submit --json \
  --client-request-id training:run-2026-08-10 \
  -- ./train.sh
```

Awaitless reserves the ID and normalized submission fingerprint atomically
before backend launch. An identical retry returns the original `job_id`;
reusing the ID with different arguments is rejected. This prevents a lost SSH
or MCP response from launching a second GPU or Slurm job.

`wait --timeout` limits only how long that client waits. `submit --timeout`
limits the managed job runtime.

## Named concurrency queues

Queues let an Agent submit intent before a local or SSH resource is free:

```bash
awaitless queue create gpu0 --concurrency 1
awaitless submit --queue gpu0 -- python train_a.py
awaitless submit --queue gpu0 -- python train_b.py
awaitless submit --queue gpu0 -- python train_c.py
```

Queue creation is idempotent when the name and concurrency match. Reusing a name
with a different concurrency is rejected. Names contain at most 64 letters,
digits, dots, underscores, or hyphens.

The admission policy is deliberately small:

- FIFO order with a fixed positive concurrency limit;
- no priorities, preemption, reordering, DAGs, or automatic GPU discovery;
- runtime timeouts begin only after the command actually starts;
- cancelling a queued job prevents its command from starting;
- queue selection participates in the `client_request_id` fingerprint.

A queue definition is a policy applied independently to each execution target.
Local jobs coordinate transactionally through the shared Awaitless SQLite data
directory. SSH jobs coordinate through locks and durable queue files on the
target host, so separate clients using the same remote queue directory cannot
both claim the same capacity. The default remote location is
`~/.awaitless/queues`; override it per host with `remote_queue_dir`.
Independent clients with different `AWAITLESS_DATA_DIR` values each define the
same queue locally; the first SSH submission fixes its remote concurrency and a
mismatch is rejected. `queue list` counts only jobs known to the current local
data store, while admission on the target still includes every remote client.

Detached local and SSH wrappers wait for admission and start the next job, so no
Awaitless daemon or waiting Agent is required. SSH slot locks are released by
the operating system if a wrapper exits, and later wrappers discard stale FIFO
entries. If a local waiting wrapper disappears, the next `status`, `wait`,
`list`, or submission reconciles stale capacity and relaunches it; transactional
claiming prevents duplicate command starts. A host reboot still requires a new
Awaitless invocation because there is intentionally no boot-time daemon.

Full resource scheduling remains Slurm's responsibility, so combining
`--queue` with `--backend slurm` is rejected; Slurm `PENDING` is exposed as the
same Awaitless `queued` lifecycle state.

## SSH backend

Declare a named host:

```toml
[hosts.gpu]
hostname = "gpu.example.com"
port = 22
user = "developer"
identity_file = "~/.ssh/id_ed25519"
remote_job_dir = "~/.awaitless/jobs"
remote_queue_dir = "~/.awaitless/queues"
gssapi_authentication = false
connect_timeout = 8
operation_timeout = 20
```

`hostname` defaults to the host name used in the table key, so OpenSSH aliases
work without repeating their target. Host-key verification keeps OpenSSH's
secure defaults. Awaitless adds `BatchMode=yes` and never prompts for a password.

`connect_timeout` controls connection establishment. `operation_timeout` is the
minimum budget for one SSH control operation, not the job runtime. Use the
submission timeout to limit the job itself.

```bash
awaitless submit --json \
  --host gpu \
  --cwd /workspace/project \
  --timeout 2h \
  --artifact results/benchmark.json \
  -- ./run_benchmark.sh
```

The remote wrapper atomically persists `exit_code` and `finished_at`. Its
heartbeat supports systems where separate login sessions cannot see the same
PID namespace; PID, process group, and `/proc` start time remain a fallback.
Cancellation persists intent before signaling the validated process group.

## Slurm backend

Configure the login host and default resource request:

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

Per-job `slurm_options` / `--slurm-option` may override only these allowlisted
keys: `account`, `constraint`, `cpus_per_task`, `gres`, `mem`, `nodes`,
`ntasks`, `partition`, `qos`, and `time`.

Awaitless sends the batch script to `sbatch` over stdin, checks active state
with `squeue`, recovers terminal state, exit code, and runtime through `sacct`,
and cancels with `scancel`. User computation runs inside a Slurm allocation,
never as an arbitrary process on the SSH login node. A separate SFTP data
channel creates the private job directory and reads only bounded log tails and
declared Artifacts.

Slurm state mapping:

| Slurm | Awaitless |
|---|---|
| `PENDING` | `queued` |
| active scheduler states | `running` |
| `COMPLETED` | `succeeded` |
| `CANCELLED` | `cancelled` |
| `TIMEOUT`, `DEADLINE` | `timed_out` |
| scheduler, node, launch, OOM, preemption failures | `failed` |

Slurm exit values such as `7:0` and signal terminations are preserved as
process-style exit codes.

### Real MCP → Slurm disconnect evidence

On 2026-08-10, two separate MCP stdio clients ran the checked-in demo against a
real Slurm 25.11.2 cluster:

| Phase | Observed result |
|---|---|
| Client 1 `submit_job` | Awaitless `job_019FE9CB2847AC929E0B2F`, Slurm `60597793`, `pending` |
| Client 1 exits | No daemon or waiter remains attached |
| Client 2 `wait_for_job` | `succeeded`, exit `0`, runtime `8.0s` |
| Bounded stdout | `compute_host=node099 slurm_job_id=60597793` (43 bytes) |
| JSON Artifact | Parsed `{ "ok": true, "compute_host": "node099", "slurm_job_id": "60597793" }` |

The evidence table preserves the v0.2 response text; current releases call the
same Slurm admission state `queued` rather than `pending`.

The reproducible driver is [`scripts/mcp_slurm_demo.py`](../scripts/mcp_slurm_demo.py)
and the raw structured evidence is [`assets/mcp-slurm-demo.json`](../assets/mcp-slurm-demo.json).

## Persistence and recovery semantics

The stable Awaitless job ID is the recovery handle. A new process may call
`wait`, `status`, `logs`, `cancel`, or `inspect` without inheriting the process
handle or terminal session that submitted the job.

States are `queued`, `starting`, `running`, `stalled`, `succeeded`, `failed`,
`cancelled`, `timed_out`, and `lost`. A stall timeout reports `stalled`; it does
not cancel the job automatically.

CLI exit codes:

| Code | Meaning |
|---:|---|
| 0 | success |
| 1 | internal error |
| 2 | invalid usage |
| 3 | managed job failure |
| 4 | job/client wait timeout |
| 5 | cancelled |
| 6 | lost |
| 7 | SSH connection failure |

## Logs and Artifacts

`stdout` and `stderr` tails share the configured `max_return_bytes` budget.
Full logs remain in the job directory. The result explicitly reports when
returned content is truncated.

Declare one or more result paths at submission time:

```bash
awaitless submit --json \
  --artifact results/benchmark.json \
  -- ./run_benchmark.sh
```

At completion, Awaitless reports each Artifact's existence, size, and
modification time. A declared JSON file within the return budget is parsed into
`parsed_results`:

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
isolated `/path/to/logs/<job-id>/` directory for each job.

## MCP tools and protocol

Awaitless exposes `run_job`, `submit_job`, `wait_for_job`, `get_job_status`,
`get_job_logs`, `cancel_job`, `list_jobs`, `create_queue`, and `list_queues` over
stdio. `run_job` and `submit_job` accept an optional `queue`. Tasks-aware clients
can receive a durable Task handle and use `tasks/get`, `tasks/cancel`, and
`tasks/update`.

See [MCP Tasks protocol](MCP_TASKS.md) for discovery, capability negotiation,
wire shapes, TTL behavior, state mapping, and legacy-client migration.

## Cancellation and safety

- Local and SSH cancellation targets a validated process group, not an
  unverified reused PID.
- SSH cancellation intent is durable before a signal is sent.
- Slurm cancellation uses the persisted scheduler ID and `scancel`.
- Suspected credentials are redacted from metadata.
- Executable run specifications are stored with mode `0600`.
- Remote arbitrary computation is confined to the submitted wrapper or Slurm
  batch allocation; Slurm control calls are allowlisted.

## Troubleshooting

Start with:

```bash
awaitless doctor --json
awaitless inspect <job-id> --json
awaitless logs <job-id> --tail 200 --json
```

Common checks:

- **SSH exits immediately:** confirm key-based authentication works in
  `BatchMode=yes`, the host key is trusted, and the configured identity, user,
  port, and hostname are correct.
- **The wait timed out but the job still runs:** a wait timeout is a client
  timeout. Inspect the job or call `wait` again; use submission `--timeout` for
  a runtime limit.
- **A job is `stalled`:** the stall threshold is observational and does not
  cancel work. Check heartbeat, filesystem, SSH reachability, and scheduler
  state before cancelling.
- **A JSON result is absent:** verify the Artifact path is relative to the
  submission working directory, the command created valid JSON, and its size is
  within the return budget.
- **A Slurm job remains pending:** inspect the requested account, partition,
  constraints, QoS, and resources with normal Slurm tools. Awaitless preserves
  scheduler state; it does not bypass scheduling policy.

## Benchmark methodology and raw evidence

Headline results belong in the project README; definitions and evidence live
in the benchmark documentation:

- [Metric framework and reproduction guide](../metric/README.md)
- [Metric definitions](../metric/METRICS.md)
- [Fair-comparison protocol](../metric/PROTOCOL.md)
- [DeepSeek Agent report](../metric/results/deepseek-agent-v2-report.md)
- [SSH polling experiment](../benchmarks/README.md)
- [SSH polling raw result](../benchmarks/results/polling-vs-awaitless.json)
- [Blocking vs. Awaitless design](../metric/LONG_RUNNING.md)

Returned log bytes are not token estimates. Agent usage-token claims are made
only when the model provider returned actual usage fields. Smoke and one-trial
calibration runs validate the harness and are not publishable performance
evidence.

## Design rationale

Awaitless optimizes non-interactive work that needs a durable result contract:
builds, tests, benchmarks, remote commands, and scheduled cluster work. It is
not intended to replace a human-operated `tmux` session, REPL, TUI, debugger,
or the resource scheduler itself.

The strong tmux comparison is intentional. A carefully written wrapper can
match Awaitless's two-call flow for simple jobs. Awaitless packages that glue
into one maintained interface across local, SSH, Slurm, CLI, and MCP, and adds
idempotent submission, persistence, bounded context, typed results, and
recovery semantics.

The product decisions and acceptance history are recorded in
[the v0.2 contract](v0.2.zh-CN.md) and [the original PRD](PRD.zh-CN.md).

## Development

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
ruff check src tests benchmarks scripts metric
```

CI tests CPython 3.10 through 3.14, builds the distributions, checks the PyPI
README, and runs an installed-wheel CLI/Artifact smoke test. See
[`CONTRIBUTING.md`](../CONTRIBUTING.md) for the contribution workflow.
