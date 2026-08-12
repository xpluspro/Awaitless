# Polling vs. Awaitless experiment

This experiment compares twelve repeated SSH log reads with one Awaitless `submit` and one `wait` call. Both sides execute the same sleep-only workload on a real SSH login node: twelve fixed-size log lines separated by short sleeps. It does not run CPU- or GPU-intensive work.

Run it with an OpenSSH host or alias:

```bash
PYTHONPATH=src python3 benchmarks/polling_vs_awaitless.py --host your-ssh-alias
```

The script opens a temporary SSH control connection for the polling baseline, launches both workloads, records the logical log bytes returned to the caller, verifies the Awaitless JSON Artifact, writes `benchmarks/results/polling-vs-awaitless.json`, and removes the remote experiment files it created.

## Recorded result

The 2026-08-10 run completed both workloads with exit code 0 and verified the
Awaitless Artifact. Traditional polling returned 84,992 decoded log bytes over
twelve snapshots. Awaitless returned 12,288 bytes once, saving 72,704 bytes
(85.5%). The agent-visible call count was 13 (launch plus twelve polls) versus 2
(`submit` plus `wait`). See `results/polling-vs-awaitless.json` for every sample.

The call count covers agent-visible CLI invocations, because those are the calls
that trigger agent/tool turns and return context. Awaitless's internal SSH
control operations are deliberately not counted as agent calls. The comparison
also distinguishes the twelve traditional polling calls from its separate
launch: the baseline therefore has thirteen agent-visible invocations in total,
while Awaitless has two. Log byte counts are decoded content returned to the
caller, excluding protocol and network framing.

## Multi-Job completion benchmark

`completion_feed.py` is a separate deterministic local protocol case for v0.5.
It runs the same three sleep-only Jobs twice: one arm repeatedly calls `status`
for each active Job and later retrieves each result, while the other consumes
whichever terminal results become available through the durable completion
cursor.

```bash
PYTHONPATH=src python3 benchmarks/completion_feed.py
```

The checked-in result is
[`results/completion-feed.json`](results/completion-feed.json). It counts
separate Agent-visible CLI invocations and verifies equivalent terminal states,
unique completion IDs, and JSON Artifacts. It is not a model benchmark and
makes no token or reasoning-quality claim. In the recorded three-Job run,
per-Job polling and retrieval used 13 calls; submission plus the completion feed
used 6, a 53.8% reduction for this controlled case.
