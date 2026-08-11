# Changelog

## Unreleased

- Add durable named FIFO queues for local and SSH jobs with fixed concurrency,
  non-preemptive admission, queued-job cancellation, and runtime timeouts that
  begin only after execution starts.
- Coordinate local admission transactionally in SQLite and SSH admission on the
  target host with daemonless queue wrappers and automatically released locks.
- Add `awaitless queue create/list`, `submit --queue`, queue filtering, MCP queue
  tools/arguments, and expose Slurm `PENDING` consistently as `queued`.

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
