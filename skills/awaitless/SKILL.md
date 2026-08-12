---
name: awaitless
description: Submit, queue, recover, consume completions from, inspect, or cancel durable long-running local, SSH, and Slurm commands with the Awaitless CLI. Use when a coding task launches a non-interactive command expected to run longer than 30 seconds, multiple independent jobs should finish without per-job polling, work must wait for a named concurrency-limited resource, a remote job must survive disconnects, or bounded logs and structured artifacts should return across Agent sessions.
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

   When the user has named a preconfigured scarce resource, add
   `--queue <name>`. Submit once even when the queue is busy; do not check the
   resource first. Create or change queue policy only when the user asks.

2. Save the returned `job_id`.

3. For one Job, call wait exactly once:

   ```bash
   awaitless wait <job_id> --json
   ```

   Let this command block. Do not insert `sleep`, `ps`, `tail`, repeated SSH calls, or periodic `status` calls.

4. Analyze `state`, `exit_code`, bounded `stdout_tail`/`stderr_tail`, and `parsed_results`.

5. Read additional bounded logs only when wait reports `failed`, `timed_out`, `stalled`, or `lost`:

   ```bash
   awaitless logs <job_id> --tail 200 --json
   ```

## Consume multiple completions

1. Submit every independent Job first and save every `job_id`. Do not wait for
   one Job before submitting the next.

2. Continue useful work when possible, then wait for the first available batch:

   ```bash
   awaitless completions <job-a> <job-b> <job-c> --json
   ```

3. Process every returned completion, then save `next_cursor`. Each completion
   contains the same bounded result contract as `wait`.

4. If `has_more` is true or `active_job_ids` is non-empty, wait again after the
   saved cursor:

   ```bash
   awaitless completions <job-a> <job-b> <job-c> \
     --after <next_cursor> --json
   ```

   This is a continuation boundary, not a polling loop: each call blocks until
   a new result exists. Do not insert `sleep`, periodic `status`, or repeated
   `tasks/get` calls between completion reads.

5. Treat delivery as at-least-once. Advance the cursor only after processing a
   batch; if a response is lost, reuse the previous cursor and deduplicate by
   `completion_id`.

## Recover or intervene

- After an Agent, shell, or SSH interruption, reuse the original ID with `awaitless wait <job_id> --json`, or reuse the saved completion cursor for a multi-Job workflow.
- Use `awaitless status <job_id> --json` for a user-requested one-time check, not as a polling loop.
- Use `awaitless cancel <job_id> --json` only when the task should actually stop.
- Treat a client-side wait or completion timeout as a detached waiter: managed Jobs continue running.
- When completion delivery reports an unreachable Job, keep the same cursor and retry later; never skip past an undelivered result.
- On `stalled`, inspect a bounded log tail before deciding whether to keep waiting or cancel.

Never print or ingest complete large logs by default.
