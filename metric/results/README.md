# Generated metric results

`run_local.py` writes append-only trial JSONL under `raw/`; `analyze.py` produces JSON and Markdown
summaries. Generated results are ignored by git so a smoke run does not dirty the repository.

Commit a result only when its config, raw JSONL, environment metadata, git commit and analysis summary
are all reviewed together. Never commit only the favorable summary table.

The v0.8 evidence manifest will identify the `gpt-5.6-luna` model, config hashes,
git commit, raw records, summaries, failures, and skipped workloads. Do not commit
an aggregate without its reviewed raw input.

Blocking-vs-Awaitless smoke/demo JSONL and summaries are also generated artifacts. The benchmark
contract and current directional calibration are checked in at [`../LONG_RUNNING.md`](../LONG_RUNNING.md);
do not publish the one-trial calibration as a performance claim.
