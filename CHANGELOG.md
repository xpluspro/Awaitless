# Changelog

## 0.7.0 — 2026-08-19

- Add immutable terminal result snapshots with SHA-256 metadata for replayable wait and completion results.
- Make wait, completion drain, cancellation, timing, environment, resource, phase, heartbeat, and capture-log fields explicit and machine-readable.
- Capture command-created redirected logs when paths are declared or detected in shell redirection, and preserve bounded tails in terminal results.
- Add device/resource diagnostics, dependency/test/validation failure categories, and non-interactive environment snapshots.
- Add `--capture-log`, `--resource`, `wait --progress-interval`, and `completions --drain` CLI paths.
- Align plugin metadata and package version at 0.7.0.

## Unreleased

- Reframe named queues as durable fixed-concurrency admission control for scarce
  resources, and document that discovery, topology, leases, multi-resource
  allocation, and physical cluster scheduling remain outside Awaitless.
- Move performance evidence below the execution-layer model in both READMEs so
  durable jobs, named queues, and completion recovery define the product first.
- Harden the adaptive detach test for slower Python 3.14 startup by asserting
  output at the durable completion boundary instead of the 50 ms detach snapshot.

## 0.6.0 — 2026-08-18

- Add the preferred adaptive `run` MCP tool and `awaitless run` CLI command.
  Every invocation creates a durable Job before launch; quick commands return
  bounded results inline, while longer commands detach at a configurable inline
  deadline without restarting or cancelling the workload.
- Return an explicit `delivery` and `detached` contract. Detached responses keep
  the stable Job ID, current state, timing, queue identity, and bounded log tails
  so an Agent can continue useful work and later consume the durable result.
- Add global and per-host default queue routing for adaptive runs. Operators can
  bind a target to a named fixed-concurrency queue, so Agents do not select or
  poll scarce resources on every invocation.
- Keep `submit_job`, `run_job`, MCP Tasks, `wait_for_job`, completion cursors,
  and all existing CLI commands backward compatible as low-level or explicit
  lifecycle interfaces.
- Reposition Awaitless from a long-running command helper to the adaptive
  execution layer for non-interactive Agent commands, and update the bundled
  Skill to route through `run` by default.

## 0.5.0 — 2026-08-12

- Add a durable multi-job completion feed backed by transactional terminal state
  events, with opaque monotonic cursors, deterministic replay, pagination, and
  at-least-once result delivery.
- Add `awaitless completions` and the `wait_for_completions` MCP tool. Both can
  block for the next available result, return already-finished work immediately,
  and preserve every managed job when the client wait times out or disconnects.
- Return bounded logs, exit status, timing, and declared JSON Artifacts in each
  completion while preserving the existing single-job `wait` and MCP Tasks
  contracts.
- Retry transient SSH/Slurm result-delivery failures without advancing the
  cursor, and expose unreachable jobs, active jobs, and `has_more` explicitly.
- Migrate v0.4 databases in place by deduplicating or backfilling terminal events
  and enforcing exactly one durable completion source per terminal job.
- Upgrade the built-in recovery demo and installed-wheel smoke test to prove two
  jobs survive a killed completion waiter and are consumed from new clients.
- Reposition Awaitless as the durable execution layer for coding agents and
  update the bundled Agent Skill for multi-job continuation workflows.

## 0.4.0 — 2026-08-11

- Add durable named FIFO queues for local and SSH jobs with fixed concurrency,
  non-preemptive admission, queued-job cancellation, and runtime timeouts that
  begin only after execution starts.
- Coordinate local admission transactionally in SQLite and SSH admission on the
  target host with daemonless queue wrappers and automatically released locks.
- Add `awaitless queue create/list`, `submit --queue`, queue filtering, MCP queue
  tools/arguments, and expose Slurm `PENDING` consistently as `queued`.
- Scope Awaitless queues to Local and SSH backends; SSH queue admission requires
  `flock` on the target host.
- Keep Slurm as the sole scheduler for Slurm jobs: `submit --queue` is rejected
  for that backend, while scheduler `PENDING` is reported as `queued`.
- Rename the user-visible `pending` lifecycle state to `queued` for scripts and
  MCP clients. After a host reboot, one Awaitless invocation is required to
  trigger recovery because Awaitless intentionally installs no boot-time daemon.

## 0.3.0 — 2026-08-10

- Reposition Awaitless as durable MCP Tasks for infrastructure users already
  own: local machines, SSH hosts, and Slurm clusters.
- Add concurrency-safe `client_request_id` submission. Identical retries return
  one stable job; conflicting reuse is rejected before backend side effects.
- Add experimental `io.modelcontextprotocol/tasks` negotiation and
  server-directed Task handles backed directly by durable Awaitless job IDs.
- Implement `tasks/get`, `tasks/update`, `tasks/cancel`, TTL, poll intervals,
  status mapping, inline final tool results, and a legacy blocking fallback.
- Add actual MCP SDK protocol tests for capability gates, disconnect recovery,
  retry replay, result retrieval, cancellation, and expiry.
- Add `awaitless demo`, which kills one waiting client and proves a new client
  can recover the job and JSON Artifact using only its durable ID.
- Add official MCP Registry metadata, PyPI ownership proof, a Registry-compatible
  `uvx awaitless-runner` alias, contribution guidance, and MCP Tasks docs.
- Extend the tag workflow to create GitHub Releases and publish Registry metadata
  with GitHub Actions OIDC after PyPI Trusted Publishing succeeds.

## 0.2.0 — 2026-08-10

- Add an official-SDK stdio MCP server with six durable job tools.
- Add a Slurm backend using `sbatch`, `squeue`, `sacct`, and `scancel` over SSH.
- Persist Slurm job IDs and recover state, exit codes, runtime, bounded logs,
  and JSON Artifacts after client disconnects.
- Add allowlisted Slurm resource options and an SFTP file data channel so user
  computation never runs directly on login nodes.
- Add a real two-client MCP → Slurm recovery and cancellation demonstration.
- Add a reproducible value-metric suite comparing plain tmux, a strong tmux
  wrapper, and Awaitless across correctness, recovery, context, and cleanup.
- Add a live DeepSeek Agent runner with actual API usage, client-reset recovery,
  completion-truncation safeguards, and a reviewed 20 × 3 evidence report.
- Add a Blocking-vs-Awaitless long-task benchmark with serial and parallel
  Blocking baselines, disconnect injection, controlled cargo/pytest/Docker/npm/
  model-inference adapters, capability skips, and blocked-time/makespan analysis.

## 0.1.1 — 2026-08-10

- Restore Python 3.10 support with the conditional `tomli` dependency.
- Accept GNU `date` nanosecond timestamps on Python 3.10.
- Add Python 3.10–3.14 CI and stronger Trusted Publishing release gates.

## 0.1.0 — 2026-08-10

- Initial local and SSH durable job runner.
