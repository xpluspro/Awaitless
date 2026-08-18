---
name: awaitless
description: Run non-interactive builds, tests, benchmarks, and local, SSH, or Slurm commands through Awaitless's adaptive durable execution layer. Use by default when command duration is uncertain, work may outlive the current turn, a named scarce resource may be queued, multiple jobs should complete without polling, or bounded logs and artifacts must survive disconnects.
---

# Use Awaitless

## Route execution

- Run quick repository inspection such as `rg`, `cat`, and `git status` directly.
- Use a PTY for interactive commands that need prompts or terminal input.
- Use `awaitless run` for non-interactive builds, tests, benchmarks, remote
  commands, and commands whose duration is uncertain. Do not estimate whether
  they will cross a duration threshold; adaptive run handles that boundary.
- Use the appropriate scheduler rather than Awaitless local/SSH when a cluster requires one.

## Run adaptively

1. Run the command and request JSON:

   ```bash
   awaitless run --json --cwd /path/to/project -- command arg1 arg2
   ```

   Add `--host <configured-host>` for SSH. Declare machine-readable output with `--artifact results.json`.

   Omit `--queue` when the Operator configured a default for the target. When the
   user explicitly names another preconfigured scarce resource, add
   `--queue <name>`. Never probe the resource first. Create or change queue
   policy only when the user asks.

2. Inspect `delivery`:

   - `inline`: analyze the terminal state, exit code, bounded logs, and Artifact
     immediately.
   - `detached`: save `job_id`, continue useful work, then consume the result at
     one blocking boundary.

3. For one detached Job, call wait exactly once when its result is needed:

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
