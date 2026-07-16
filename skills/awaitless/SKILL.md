---
name: awaitless
description: Run, wait for, recover, inspect, or cancel durable long-running local and SSH commands with the Awaitless CLI. Use when a coding task launches a non-interactive command expected to run longer than 30 seconds, when an SSH job must survive disconnects, or when bounded logs and structured benchmark artifacts should be returned without repeated sleep/ps/tail polling.
---

# Use Awaitless

## Choose the execution mode

- Run ordinary commands expected to finish within 30 seconds directly.
- Use a PTY for interactive commands that need prompts or terminal input.
- Use Awaitless for long, non-interactive local or SSH commands.
- Use the appropriate scheduler rather than Awaitless local/SSH when a cluster requires one.

## Run a durable job

1. Submit the command and request JSON:

   ```bash
   awaitless submit --json --cwd /path/to/project -- command arg1 arg2
   ```

   Add `--host <configured-host>` for SSH. Declare machine-readable output with `--artifact results.json`.

2. Save the returned `job_id`.

3. Call wait exactly once:

   ```bash
   awaitless wait <job_id> --json
   ```

   Let this command block. Do not insert `sleep`, `ps`, `tail`, repeated SSH calls, or periodic `status` calls.

4. Analyze `state`, `exit_code`, bounded `stdout_tail`/`stderr_tail`, and `parsed_results`.

5. Read additional bounded logs only when wait reports `failed`, `timed_out`, `stalled`, or `lost`:

   ```bash
   awaitless logs <job_id> --tail 200 --json
   ```

## Recover or intervene

- After an Agent, shell, or SSH interruption, reuse the original ID with `awaitless wait <job_id> --json`.
- Use `awaitless status <job_id> --json` for a user-requested one-time check, not as a polling loop.
- Use `awaitless cancel <job_id> --json` only when the task should actually stop.
- Treat a client-side wait timeout as a detached waiter: the managed job continues running.
- On `stalled`, inspect a bounded log tail before deciding whether to keep waiting or cancel.

Never print or ingest complete large logs by default.
