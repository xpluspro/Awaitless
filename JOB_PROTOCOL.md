# Awaitless Agent Job Protocol

Status: v0.8 protocol specification. The keywords MUST, MUST NOT, SHOULD, and
MAY are normative.

This document defines the backend-independent contract between an Agent client
and Awaitless. CLI spelling, MCP Tasks framing, SSH wrappers, and Slurm are
adapters around the same Job model.

## Tool selection

| Intent | MCP tool | Do not use it for |
|---|---|---|
| Start one non-interactive command; duration is unknown | `run` | Explicit fan-out, Task creation, or resuming a known Job |
| Submit explicitly asynchronous work or independent fan-out | `submit_job` | Default single-command execution or resuming a known Job |
| Create an MCP Task in a Tasks-capable client | `run_job` | Ordinary execution or generic asynchronous submission |
| Consume one known Job's terminal result | `wait_for_job` | Starting work, polling, or collecting several Jobs |
| Collect terminal results for several known Jobs | `wait_for_completions` | Starting work or status polling |
| Read one immediate state snapshot | `get_job_status` | Waiting or polling until completion |
| Inspect bounded diagnostics after failure or stall | `get_job_logs` | Progress streaming or completion detection |

`run` is the default creation operation. A detached `run` response contains the
same durable `job_id` used by all continuation operations. A client MUST NOT
submit a replacement when `run`, a waiter, or the client connection detaches.

## Identity and idempotency

`job_id` is the server-assigned durable identity of one execution. It is stable
across client sessions, waits, status reads, completion delivery, and terminal
snapshot replay. A backend's native identifier, such as a PID, SSH Job directory,
or Slurm allocation ID, is metadata and MUST NOT replace `job_id` on the wire.

`client_request_id` is an optional client-assigned idempotency key, except that
`run_job` requires it. Its scope is one Awaitless store. A conforming
implementation MUST atomically bind it to a fingerprint of all launch-affecting
arguments before creating backend side effects.

- An identical retry MUST return the original `job_id` and report an idempotent
  replay.
- Reuse with different launch arguments MUST fail before another command starts.
- A retry after a lost creation response MUST reuse the same key and arguments.
- A client that already knows `job_id` MUST continue that Job instead of retrying
  creation.

These rules provide idempotent creation at the protocol boundary. Backend process
launch remains at-most-one only to the extent guaranteed by that backend's
atomic registration mechanism.

## Lifecycle

The public state machine is:

```text
queued -> starting -> running <-> stalled -> succeeded
   |          |          |          |       -> failed
   |          |          |          |       -> timed_out
   +----------+----------+----------+------- -> cancelled
                         +------------------ -> lost
```

`queued`, `starting`, `running`, and `stalled` are active. `succeeded`, `failed`,
`cancelled`, `timed_out`, and `lost` are terminal. A backend MAY skip active
states it cannot observe. Terminal state is immutable. Exactly one durable
completion event and one immutable terminal result snapshot MUST be associated
with each terminal Job.

`exit_code` is null before terminal observation and when the backend cannot
recover it. `succeeded` requires exit code `0`. `failed` normally carries the
command's non-zero code. `cancelled`, `timed_out`, and `lost` describe lifecycle
outcomes and MUST NOT be inferred solely from a client-side timeout.

## Orthogonal progress fields

The following fields are deliberately not aliases for lifecycle `state`:

| Field | Meaning |
|---|---|
| `phase` | Workload-reported semantic phase; `unknown` when none is reported |
| `queue_state` | Admission only: `queued` before a named queue or scheduler admits work, otherwise `running` |
| `last_heartbeat_at` | Latest backend-wrapper liveness signal; null when that backend does not emit one |
| `heartbeat_at` | Compatibility alias of `last_heartbeat_at`; clients SHOULD use the latter |
| `last_output_at` | Latest observed stdout/stderr write; null when no output has been observed |

A heartbeat proves wrapper liveness, not command progress or success. Output
proves bytes changed, not wrapper liveness. A long quiet command with a fresh
heartbeat remains healthy. `phase` MUST NOT be synthesized from lifecycle,
queue, heartbeat, or output timestamps.

## Creation and continuation semantics

### Submit

Creation MUST persist the Job identity before launch. A successful response
means Awaitless owns continuation even if the client disconnects immediately.
`run` MAY return either:

- `delivery: inline`, with a terminal bounded result; or
- `delivery: detached`, with `job_id`, current state, `detach_reason`, and a
  continuation command.

Detachment changes result delivery only; it MUST NOT restart or cancel work.

### Wait

`wait_for_job` blocks until terminal state or an optional call-level deadline.
The terminal result MUST be replayable from an immutable snapshot. Repeating a
wait is recovery, not another execution.

`wait_for_completions` is a durable, ordered completion feed. `after_cursor`
names the last processed completion. Reusing a cursor MUST replay the same next
items. A client MUST process and deduplicate by `completion_id` before persisting
`next_cursor`. Implementations MUST NOT advance past an earlier completion whose
remote result cannot currently be read.

### Status and logs

Status is a single non-blocking observation. It does not acknowledge or consume
a completion. Logs are bounded diagnostic tails and MUST report truncation and
observed byte counts. Neither operation is a polling protocol.

## Timeouts and disconnection

`timeout_seconds` on Job creation is the Job runtime limit. For queued work it
starts when execution starts, not when submission is created. Reaching it moves
the Job to terminal `timed_out` and stops the managed command.

`timeout_seconds` on a wait or completion call is a client/waiter deadline.
Reaching it returns a non-terminal wait outcome such as `wait_timed_out` or
`wait_state: client_timeout`; it MUST NOT alter Job state or cancel work.

A transport disconnect has the same ownership semantics as a client wait
timeout. Recovery uses the original `job_id`, or the previous completion cursor.

## Completion delivery

Completion delivery is at-least-once. Servers provide stable `completion_id`
values and replay from a prior cursor; clients provide exactly-once effects by
deduplicating before advancing their durable cursor. Empty completion responses
MUST distinguish active Jobs, temporarily unreachable Jobs, drained feeds, and
client wait timeout.

The terminal result returned by single-Job wait, multi-Job completion, and MCP
Tasks result retrieval MUST have the same content and immutable snapshot digest.

## Artifact manifest

Artifacts are declared at creation by paths, directories, or glob patterns.
Terminal results contain an `artifacts` array. Every manifest entry MUST contain:

| Field | Requirement |
|---|---|
| `path` | Resolved or declared path identifying this entry |
| `exists` | Whether a regular file was found |
| `declared_path` | Original declaration when one declaration expands to files |
| `remote` | `true` for SSH or Slurm artifacts; omitted or `false` locally |
| `size_bytes` | Required when `exists` is true |
| `sha256` | Lowercase SHA-256 of the complete file, required when `exists` is true |
| `modified_at` | UTC RFC 3339 timestamp when the backend can recover it |
| `content` | Parsed JSON when the file is JSON and within the configured return bound |
| `parse_error` | Bounded parse diagnostic when eligible JSON cannot be parsed |

Missing matches remain explicit entries with `exists: false`. Artifact content
may be bounded; size and SHA-256 always describe the complete file. Consumers
MUST verify `exists`, `size_bytes`, and `sha256` before trusting `content` or an
out-of-band file transfer.

## Errors

Command failure is a successful protocol operation with a terminal Job result,
not an MCP transport error. Diagnosed terminal failures use:

| Field | Meaning |
|---|---|
| `stage` | Stable failure stage such as `preflight_failed`, `build_failed`, `test_failed`, `infrastructure_failed`, `runtime_failed`, or `timed_out` |
| `reason` | Stable machine-readable reason within the stage |
| `retryable` | Whether retry may succeed without changing command inputs |
| `suggestion` | Bounded operator-facing remediation |
| `error` | Bounded backend detail; nullable |

SSH control failures SHOULD additionally expose `host`, `wrapper_stage`,
`remote_stderr`, and `missing_command` when known. Slurm failures SHOULD expose
`host`, `scheduler_command`, `scheduler_job_id`, and a bounded scheduler message
when known. Secrets, full environments, full logs, and SSH command lines MUST
NOT be included.

Invalid input, unknown identity, idempotency conflict, or a backend control
operation that prevents Job creation is a protocol operation error. Clients MUST
use `retryable` when present and MUST NOT infer retryability from message text.

## Versioning and compatibility

This document is protocol series `0.8`. Additive optional response fields and new
diagnostic enum values are backward compatible within the series. Clients MUST
ignore unknown fields and unknown `stage` or `reason` values. Servers MUST NOT
change the meaning or type of a documented field within the series.

Removing a field, changing a field type, changing cursor interpretation, adding
a terminal lifecycle state, or weakening identity, snapshot, completion, or
Artifact integrity guarantees requires a new protocol series. Compatibility
aliases are retained for at least one series and are explicitly marked above.

MCP Tasks is a transport extension over this protocol. Task status mapping and
TTL do not alter Job identity, ownership, lifecycle, terminal snapshot, or
Artifact semantics; see [MCP Tasks](docs/MCP_TASKS.md).
