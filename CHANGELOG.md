# Changelog

## 0.2.0 — 2026-08-10

- Add an official-SDK stdio MCP server with six durable job tools.
- Add a Slurm backend using `sbatch`, `squeue`, `sacct`, and `scancel` over SSH.
- Persist Slurm job IDs and recover state, exit codes, runtime, bounded logs,
  and JSON Artifacts after client disconnects.
- Add allowlisted Slurm resource options and an SFTP file data channel so user
  computation never runs directly on login nodes.
- Add a real two-client MCP → Slurm recovery and cancellation demonstration.

## 0.1.1 — 2026-08-10

- Restore Python 3.10 support with the conditional `tomli` dependency.
- Accept GNU `date` nanosecond timestamps on Python 3.10.
- Add Python 3.10–3.14 CI and stronger Trusted Publishing release gates.

## 0.1.0 — 2026-08-10

- Initial local and SSH durable job runner.
