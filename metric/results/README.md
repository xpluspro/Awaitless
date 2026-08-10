# Generated metric results

`run_local.py` writes append-only trial JSONL under `raw/`; `analyze.py` produces JSON and Markdown
summaries. Generated results are ignored by git so a smoke run does not dirty the repository.

Commit a result only when its config, raw JSONL, environment metadata, git commit and analysis summary
are all reviewed together. Never commit only the favorable summary table.

The reviewed 2026-08-10 DeepSeek experiment is documented in
[`deepseek-agent-v2-report.md`](deepseek-agent-v2-report.md). Its raw JSONL and generated JSON/Markdown
summary remain local ignored artifacts; the checked-in report includes the exact protocol, headline
aggregates, failure disclosure, and excluded-pilot rationale.

Blocking-vs-Awaitless smoke/demo JSONL and summaries are also generated artifacts. The benchmark
contract and current directional calibration are checked in at [`../LONG_RUNNING.md`](../LONG_RUNNING.md);
do not publish the one-trial calibration as a performance claim.
