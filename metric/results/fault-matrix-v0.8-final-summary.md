# Awaitless value metric summary

Generated: `2026-08-19T15:49:41.853466Z` from 400 trial records.

## Overall

| Arm | Result fidelity | Recovery | Duplicate launch | Cancel cleanup | Median calls | P90 visible bytes | Calls / correct job | Usage tokens / correct job | Custom glue SLOC |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awaitless | 100/100 (100.0%; 95% CI 96.3%–100.0%) | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 0/100 (0.0%; 95% CI 0.0%–3.7%) | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 22.7 KiB | 2.20 | — | 0 |
| shell | 60/100 (60.0%; 95% CI 50.2%–69.1%) | 0/20 (0.0%; 95% CI 0.0%–16.1%) | 0/100 (0.0%; 95% CI 0.0%–3.7%) | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 1.0 MiB | 2.67 | — | 0 |
| tmux_plain | 80/100 (80.0%; 95% CI 71.1%–86.7%) | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 0/100 (0.0%; 95% CI 0.0%–3.7%) | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 7.0 | 388.1 KiB | 13.50 | — | 0 |
| tmux_wrapped | 100/100 (100.0%; 95% CI 96.3%–100.0%) | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 0/100 (0.0%; 95% CI 0.0%–3.7%) | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 20.5 KiB | 2.20 | — | 319 |

## By scenario

| Scenario | Arm | Result fidelity | Median calls | P90 visible bytes | Median wall time | Errors |
|---|---|---:|---:|---:|---:|---:|
| cancel_tree | awaitless | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 888.0 B | 0.608 s | 0 |
| cancel_tree | shell | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 1.0 | 36.0 B | 0.355 s | 0 |
| cancel_tree | tmux_plain | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 17.0 B | 0.445 s | 0 |
| cancel_tree | tmux_wrapped | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 242.0 B | 0.564 s | 0 |
| failure | awaitless | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 5.7 KiB | 0.365 s | 0 |
| failure | shell | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 2.9 KiB | 0.265 s | 0 |
| failure | tmux_plain | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 7.0 | 8.5 KiB | 0.302 s | 0 |
| failure | tmux_wrapped | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 3.4 KiB | 0.342 s | 0 |
| large_log | awaitless | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 22.7 KiB | 0.301 s | 0 |
| large_log | shell | 0/20 (0.0%; 95% CI 0.0%–16.1%) | 2.0 | 1.0 MiB | 0.215 s | 0 |
| large_log | tmux_plain | 0/20 (0.0%; 95% CI 0.0%–16.1%) | 6.5 | 618.1 KiB | 0.264 s | 0 |
| large_log | tmux_wrapped | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 20.5 KiB | 0.307 s | 0 |
| normal | awaitless | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 5.6 KiB | 0.461 s | 0 |
| normal | shell | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 3.0 KiB | 0.370 s | 0 |
| normal | tmux_plain | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 9.0 | 9.7 KiB | 0.431 s | 0 |
| normal | tmux_wrapped | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 2.0 | 3.5 KiB | 0.470 s | 0 |
| recovery | awaitless | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 3.0 | 6.8 KiB | 2.349 s | 0 |
| recovery | shell | 0/20 (0.0%; 95% CI 0.0%–16.1%) | 1.0 | 512.0 B | 0.213 s | 0 |
| recovery | tmux_plain | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 30.5 | 65.7 KiB | 2.296 s | 0 |
| recovery | tmux_wrapped | 20/20 (100.0%; 95% CI 83.9%–100.0%) | 3.0 | 4.7 KiB | 2.367 s | 0 |

## Awaitless relative to each baseline

Positive reductions mean Awaitless returned/used less. Negative values mean it used more.

| Scope | Baseline | Fidelity delta | Median call reduction | P90 byte reduction | Usage token reduction | Glue SLOC delta |
|---|---|---:|---:|---:|---:|---:|
| overall | shell | +40.0 pp | +0.0% | +97.8% | — | +0 |
| overall | tmux_plain | +20.0 pp | +71.4% | +94.2% | — | +0 |
| overall | tmux_wrapped | +0.0 pp | +0.0% | -10.7% | — | -319 |
| cancel_tree | shell | +0.0 pp | -100.0% | -2366.7% | — | +0 |
| cancel_tree | tmux_plain | +0.0 pp | +0.0% | -5123.5% | — | +0 |
| cancel_tree | tmux_wrapped | +0.0 pp | +0.0% | -266.9% | — | -319 |
| failure | shell | +0.0 pp | +0.0% | -93.7% | — | +0 |
| failure | tmux_plain | +0.0 pp | +71.4% | +32.8% | — | +0 |
| failure | tmux_wrapped | +0.0 pp | +0.0% | -68.0% | — | -319 |
| large_log | shell | +100.0 pp | +0.0% | +97.8% | — | +0 |
| large_log | tmux_plain | +100.0 pp | +69.2% | +96.3% | — | +0 |
| large_log | tmux_wrapped | +0.0 pp | +0.0% | -10.7% | — | -319 |
| normal | shell | +0.0 pp | +0.0% | -87.2% | — | +0 |
| normal | tmux_plain | +0.0 pp | +77.8% | +41.7% | — | +0 |
| normal | tmux_wrapped | +0.0 pp | +0.0% | -62.9% | — | -319 |
| recovery | shell | +100.0 pp | -200.0% | -1266.8% | — | +0 |
| recovery | tmux_plain | +0.0 pp | +90.2% | +89.6% | — | +0 |
| recovery | tmux_wrapped | +0.0 pp | +0.0% | -46.9% | — | -319 |

## Data-quality warnings

- Actual usage tokens are incomplete; no token-saving percentage is claimed.

Token fields are never inferred from bytes. Review raw JSONL, environment metadata, every failure, and the qualitative rubric before making a project-value claim.
