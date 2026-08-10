# Changelog

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
