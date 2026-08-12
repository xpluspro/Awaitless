# MCP Tasks compatibility

Awaitless 0.5 implements the `io.modelcontextprotocol/tasks` extension described
by the MCP Tasks specification dated 2026-07-28. It uses the MCP Python SDK 2.x
extension API; it does not depend on the removed SDK 1.x experimental Tasks API.

The implementation is intentionally a compatibility layer over Awaitless's
existing durable job store. An Awaitless `job_id` is also the MCP `taskId`, so
there is no second lifecycle database and no in-memory handle that disappears
with the stdio process.

The extension is still evolving. Awaitless keeps `submit_job`, `wait_for_job`,
`wait_for_completions`, `get_job_status`, `get_job_logs`, `cancel_job`, and
`list_jobs` available while the ecosystem migrates.

## Negotiation

The server advertises this entry in `server/discover`:

```json
{
  "capabilities": {
    "extensions": {
      "io.modelcontextprotocol/tasks": {}
    }
  }
}
```

A client opts in on each relevant request:

```json
{
  "_meta": {
    "io.modelcontextprotocol/clientCapabilities": {
      "extensions": {
        "io.modelcontextprotocol/tasks": {}
      }
    }
  }
}
```

Calling an extension method without that capability produces JSON-RPC error
`-32003` and a `requiredCapabilities` hint. Older protocol versions cannot use
the extension methods.

## Creating a Task

Call the ordinary MCP tool `run_job`. `client_request_id` is required and must
be stable across retries of the same logical submission.

```json
{
  "name": "run_job",
  "arguments": {
    "command": ["python", "train.py"],
    "backend": "slurm",
    "host": "cluster",
    "artifacts": ["results/metrics.json"],
    "client_request_id": "training:resnet50:seed-42"
  }
}
```

A Tasks-aware client immediately receives a server-directed Task result:

```json
{
  "resultType": "task",
  "taskId": "job_019F...",
  "status": "working",
  "statusMessage": "Awaitless job is queued",
  "createdAt": "2026-08-10T12:00:00+00:00",
  "lastUpdatedAt": "2026-08-10T12:00:00+00:00",
  "ttlMs": 604800000,
  "pollIntervalMs": 2000
}
```

If the client does not declare Tasks support, the same `run_job` tool blocks
and returns its normal `CallToolResult`. This makes one tool safe for both new
and old MCP clients.

## Idempotency guarantee

Awaitless validates the request ID and creates a deterministic fingerprint from
all launch-affecting fields. A SQLite `BEGIN IMMEDIATE` transaction atomically
reserves the unique request ID before filesystem, SSH, `sbatch`, or process
launch side effects.

- Same request ID and same fingerprint: return the original job and set
  `idempotent_replay` to true.
- Same request ID and different fingerprint: reject the request.
- Concurrent identical requests: exactly one caller wins the launch reservation.
- Process failure after reservation but before backend identity is known: keep
  the durable job in `starting`; a retry never guesses that it is safe to launch
  an expensive duplicate.

The last case deliberately chooses at-most-once launch over automatic duplicate
execution. Operators can inspect the stable job record before deciding whether
to cancel or use a new request ID.

## Get, result, update, and cancel

Use `tasks/get` to refresh a handle:

```json
{"taskId":"job_019F..."}
```

While active, the response has `resultType: "complete"`, task metadata, and
`status: "working"`. When terminal, `tasks/get` returns the final MCP
`CallToolResult` inline in `result`. The 2026-07-28 extension has no separate
`tasks/result` method.

The final structured content contains the full Awaitless result contract:

```json
{
  "state": "failed",
  "exit_code": 7,
  "duration_seconds": 83.4,
  "stdout_tail": "...",
  "stderr_tail": "...",
  "truncated": false,
  "parsed_results": {"score": 0.91}
}
```

`tasks/cancel` delegates to the existing verified cancellation path. Local and
SSH jobs cancel the managed process group; Slurm jobs use the persisted
scheduler ID with `scancel`. `tasks/update` validates the task and acknowledges
unknown or already-satisfied input keys. Awaitless command jobs never enter
`input_required`.

## Multi-Job completion feed

MCP Tasks are durable single-Job handles. Awaitless 0.5 additionally exposes the
ordinary `wait_for_completions` tool for clients that submit several independent
Jobs and need whichever bounded result becomes available next:

```json
{
  "job_ids": ["job_019F_A", "job_019F_B"],
  "after_cursor": "cmp_0000000000000042",
  "timeout_seconds": 600,
  "limit": 50
}
```

The tool works whether or not the client negotiated MCP Tasks. Completion IDs
are persistent and at-least-once: process the returned batch, save
`next_cursor`, and reuse the prior cursor after a lost response. A call timeout
never cancels a Job. Awaitless does not advance past an unreachable SSH or Slurm
result; the response exposes `unreachable_job_ids` and keeps the cursor stable.

This feed avoids a client-driven `tasks/get` loop across many handles. It is not
a new MCP push capability and does not wake a client process that is no longer
running.

## Status mapping

| Awaitless state | MCP Task status | Notes |
|---|---|---|
| `queued`, `starting`, `running`, `stalled` | `working` | Continue with `tasks/get` after `pollIntervalMs`. |
| `cancelled` | `cancelled` | Cancellation is durable. |
| `succeeded` | `completed` | Result includes exit code `0`. |
| `failed`, `timed_out`, `lost` | `completed` | Command outcome remains in structured content. |

MCP Task `failed` is reserved for an error in the request or protocol operation.
A command returning exit code 7 is a successfully completed tool invocation
whose structured job result reports `state: "failed"` and `exit_code: 7`.

## TTL

`[defaults].mcp_task_ttl_seconds` controls how long the Task handle is exposed;
the default is seven days. `mcp_task_poll_interval_seconds` controls the
suggested polling interval and defaults to the ordinary Awaitless poll interval.
Both must be positive.

An expired or unknown Task returns `INVALID_PARAMS` (`-32602`). Awaitless keeps
the underlying job record and logs according to its normal storage policy; TTL
limits protocol visibility and does not cancel running work.

```toml
[defaults]
mcp_task_ttl_seconds = 604800
mcp_task_poll_interval_seconds = 2
```

## Recovery demo

Run this without an SSH host or Slurm installation:

```bash
awaitless demo --json
```

The command starts two local durable Jobs and a separate completion waiter,
terminates that first client, then consumes both bounded results and JSON
Artifacts from new CLI processes using a durable cursor. The output includes
both Job IDs, both completions, `completion_count: 2`, and
`recovered_by_new_client: true`.

## Protocol verification

`tests/test_mcp.py` runs an in-process MCP SDK 2.x client/server pair and checks:

- capability discovery and per-request capability enforcement;
- the exact server-directed Task result shape;
- disconnect, new client, retry, and stable task ID;
- inline final result and JSON Artifact recovery;
- blocking fallback for clients without the extension;
- `tasks/cancel`, status mapping, and TTL expiry;
- multi-Job completion replay across fresh stdio clients.

`tests/test_db.py` separately races independent SQLite connections to verify
that one and only one caller owns an idempotent submission.

## References

- [MCP Tasks overview](https://modelcontextprotocol.io/extensions/tasks/overview)
- [Current Tasks extension specification](https://tasks.extensions.modelcontextprotocol.io/)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
