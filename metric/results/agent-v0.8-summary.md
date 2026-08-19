# Awaitless value metric summary

Generated: `2026-08-19T15:49:39.720752Z` from 60 trial records.

## Overall

| Arm | Result fidelity | Recovery | Duplicate launch | Cancel cleanup | Median calls | P90 visible bytes | Calls / correct job | Usage tokens / correct job | Custom glue SLOC |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awaitless | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 0/20 (0.0%; 95% CI 0.0%–16.1%) | — | 2.0 | 4.9 KiB | 2.00 | 2079.7 | 0 |
| tmux_plain | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 0/20 (0.0%; 95% CI 0.0%–16.1%) | — | 3.0 | 2.6 KiB | 3.00 | 2145.5 | 0 |
| tmux_wrapped | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 0/20 (0.0%; 95% CI 0.0%–16.1%) | — | 2.0 | 2.7 KiB | 2.00 | 1331.0 | 319 |

## By scenario

| Scenario | Arm | Result fidelity | Median calls | P90 visible bytes | Median wall time | Errors |
|---|---|---:|---:|---:|---:|---:|
| recovery | awaitless | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 4.9 KiB | 9.687 s | 0 |
| recovery | tmux_plain | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 3.0 | 2.6 KiB | 10.233 s | 0 |
| recovery | tmux_wrapped | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 2.7 KiB | 7.467 s | 0 |

## Awaitless relative to each baseline

Positive reductions mean Awaitless returned/used less. Negative values mean it used more.

| Scope | Baseline | Fidelity delta | Median call reduction | P90 byte reduction | Usage token reduction | Glue SLOC delta |
|---|---|---:|---:|---:|---:|---:|
| overall | tmux_plain | +0.0 pp | +33.3% | -88.8% | +3.1% | +0 |
| overall | tmux_wrapped | +0.0 pp | +0.0% | -79.0% | -56.3% | -319 |
| recovery | tmux_plain | +0.0 pp | +33.3% | -88.8% | +3.1% | +0 |
| recovery | tmux_wrapped | +0.0 pp | +0.0% | -79.0% | -56.3% | -319 |

## Data-quality warnings

- None.

Token fields are never inferred from bytes. Review raw JSONL, environment metadata, every failure, and the qualitative rubric before making a project-value claim.
