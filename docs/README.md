# Awaitless documentation

The project README is the product overview and shortest path to a working job.
This directory is the reference layer for configuration, behavior, protocols,
and implementation details.

## Start here

- [Reference guide](REFERENCE.md) — installation, CLI, configuration, SSH,
  Slurm, persistence, recovery, Artifacts, cancellation, troubleshooting, and
  architecture.
- [MCP Tasks protocol](MCP_TASKS.md) — discovery, task creation, status,
  cancellation, TTL, reconnect behavior, and migration from legacy tools.
- [Benchmark methodology](../metric/README.md) — metric definitions, controlled
  comparisons, reproducibility requirements, and reporting boundaries.
- [DeepSeek Agent report](../metric/results/deepseek-agent-v2-report.md) — the
  checked-in 20-case comparison behind the README headline numbers.
- [SSH polling experiment](../benchmarks/README.md) — method and raw-result link
  for the 13-to-2 agent-visible call comparison.
- [Multi-Job completion benchmark](../benchmarks/README.md#multi-job-completion-benchmark)
  — deterministic v0.5 protocol case and raw-result link.

## Product and design records

- [Product positioning and evolution principles](PRD.zh-CN.md)
- [v0.5 durable completion feed](v0.5.zh-CN.md)
- [v0.4 queue release and real-machine acceptance](v0.4.zh-CN.md)
- [v0.2 Agent and Slurm acceptance contract](v0.2.zh-CN.md)
- [Blocking vs. Awaitless benchmark design](../metric/LONG_RUNNING.md)
- [Metrics](../metric/METRICS.md) and [experiment protocol](../metric/PROTOCOL.md)

The GitHub Wiki is not currently enabled for this repository. These Markdown
pages are deliberately written so they can be mirrored into a Wiki without
making README links depend on an unpublished external documentation surface.
