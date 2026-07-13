# agentbus wire protocol v1

This document is the published wire contract for agentbus protocol major
version 1. Independent clients and servers MUST treat this document as
authoritative for v1 behavior.

agentbus is an agent-agnostic execution service. It exposes sessions, turns,
background jobs, and generic structural policy validation. It does not define
workflow semantics such as delegation, review, rescue, or orchestration.

## Transport and framing

The daemon listens on the Unix domain socket:

```text
$XDG_STATE_HOME/agentbus/agentbus.sock
```

If `XDG_STATE_HOME` is unset, the state root is
`~/.local/state/agentbus`. The state directory MUST be mode `0700`. The socket
path MUST be created with mode `0600` where the platform supports socket file
modes.

The wire protocol is JSON-RPC 2.0 over newline-delimited JSON:

- Each frame MUST be exactly one UTF-8 encoded JSON object followed by `\n`.
- A frame MUST NOT contain embedded unescaped newlines outside JSON strings.
- A client request MUST use JSON-RPC 2.0 request shape.
- A server response MUST use JSON-RPC 2.0 response shape.
- A server notification MUST use JSON-RPC 2.0 notification shape and MUST NOT
  include `id`.

Example request frame:

```json
{"jsonrpc":"2.0","id":"1","method":"protocol.hello","params":{"clientProtocolVersion":1,"token":"..."}}
```

Example response frame:

```json
{"jsonrpc":"2.0","id":"1","result":{"protocolVersion":1,"backends":["codex","claude"],"backendMetadata":[{"backend":"codex","models":["gpt-5.4"],"efforts":["low","medium","high","xhigh"]},{"backend":"claude","models":["sonnet","opus"],"efforts":["low","medium","high","xhigh","max"]}],"capabilities":{"policy.shape":true,"policy.jsonSchema":true,"models.discovery":true,"jobs.requestId":true}}}
```

`protocol.hello` MUST be the first request sent on a connection. A server MUST
reject all other methods before a successful hello. A protocol major-version
mismatch MUST return a structured JSON-RPC error whose stable identifier is
`error.data.code: "version_mismatch"`. The JSON-RPC `error.code` field MUST
remain numeric and is implementation-defined.

## Trust model

agentbus trusts all processes running as the same OS user. The socket mode and
state-file modes prevent accidental access by other users on the same machine,
but they are not a security boundary between same-user clients.

State files contain sensitive data: prompts, diffs, tool output, backend logs,
and final model output. Clients MUST treat paths returned by agentbus as private
same-user state.

The daemon MUST maintain a token file with mode `0600` and check the supplied
token during `protocol.hello`. That token is accident-prevention only. It is
explicitly NOT a security boundary because same-user code can read it.

## Daemon, jobs, and state

Implementations MUST provide `agentbus serve [--foreground]`. Client packages
MUST autostart the daemon when a connection is requested and no live daemon is
available.

The daemon MUST support idle shutdown. The default idle shutdown threshold is
30 minutes, and the daemon MUST shut down only when there are no client
connections and no active or queued jobs or turns. A running background job
always counts as activity. The daemon MUST support concurrent multi-job
execution; the one-active-turn rule is per session, not per daemon.

This is also an operational upgrade behavior, not a protocol change. At startup
the daemon records its resolved executable's modification time and size. If a
later housekeeping check finds that binary replaced or unavailable, the daemon
continues serving active connections and work, then exits at the first quiet
check without waiting for the normal 30-minute timeout. A later client
connection autostarts the upgraded binary.

Operationally, autostarted daemons detach into their own session, so ending the
launching process group (or pressing Ctrl-C in the launching terminal) no longer
stops them. A detached daemon exits on its own via the idle timeout, or at the
first quiet moment after a binary upgrade. To stop one immediately, send SIGTERM
to the PID recorded in `<state root>/agentbus.pid`.

State storage requirements:

| Item | Requirement |
| --- | --- |
| State root | `$XDG_STATE_HOME/agentbus`, falling back to `~/.local/state/agentbus` when `XDG_STATE_HOME` is unset. |
| Directory modes | State directories MUST be created with mode `0700`. |
| File and log modes | State files and logs MUST be created with mode `0600`. |
| Workspace namespace | Per-workspace state MUST be keyed by the full 64-hex SHA-256 of the canonicalized absolute `cwd`. The digest MUST NOT be truncated. |

Every foreground turn and background job MUST have a job record. A job record
MUST include, when applicable:

- supervisor PID, PGID, and process start time
- worker PID, PGID, and process start time
- heartbeat lease
- backend session id
- backend child PID

`job.status`, `job.result`, `job.cancel`, and equivalent CLI status operations
MUST detect expired heartbeat leases, stale queued jobs, foreground crashes,
orphaned records, and PID reuse. PID reuse detection MUST compare the observed
process start time with the start time recorded for that PID. Corrupted job
records MUST be moved to a quarantine directory with diagnostics describing the
record path and validation or parse failure. A single-job status or result read
MUST reconcile only the requested record while holding that record's lock. Full
workspace reconciliation runs independently on daemon start and on a periodic
per-workspace daemon tick; enumerating all jobs also requests a full pass. Full
passes are debounced and single-flighted per workspace (the default minimum
interval is two seconds), so concurrent readers do not stampede the store.

Job-record writes MUST be atomic: write a temporary file in the same directory,
fsync that file, rename it over the target, then fsync the containing
directory. Each backend invocation MUST run in a new process group. Canceling a
running job or interrupting a foreground turn MUST send `SIGTERM` to the
process group, wait a grace period whose default is 10 seconds, then send
`SIGKILL` to remaining processes in that group.

Implementations MUST read process start time portably. On Linux this can use
`/proc/<pid>/stat` field 22; on macOS this can use `ps -o lstart=` or `sysctl`.
Backend stdout and stderr MUST be captured to state log files from process
start. Log files MUST be size-capped, with a default cap of 10 MB, and capped
logs MUST include a truncation marker.

Terminal job records and logs MUST be retained for a default of 14 days.
Spilled result files MUST be retained for a default of 14 days. Orphaned
job-input files MUST be swept when their job is terminal. Garbage collection
MUST piggyback on full reaper passes, reuse the records already read by that
pass, and be throttled per workspace (the default minimum interval is 30
seconds). Retention settings MUST be configurable.

## Identity model

In protocol v1, every foreground turn is also a job record:

```text
turnId == jobId
```

`turn.start` returns both identifiers for clarity and forward compatibility:

```json
{
  "turnId": "job_01J00000000000000000000001",
  "jobId": "job_01J00000000000000000000001",
  "sessionId": "ses_01J00000000000000000000001"
}
```

If a client disconnects during a foreground turn, the turn continues as its job.
The client recovers the outcome with `job.result` using the returned `jobId`.

## Correlation and streams

Every server notification about a turn or job MUST carry:

- `sessionId`
- `turnId`
- `jobId`

For v1 turn notifications, `turnId` and `jobId` MUST be equal.

The response to `turn.start` is not the event stream. It only acknowledges that
the turn job exists. Runtime output arrives as `turn.event` notifications. The
terminal foreground notification is `turn.result`, which is distinct from the
`job.result` request method used to fetch persisted outcomes.

Example event notification:

```json
{
  "jsonrpc": "2.0",
  "method": "turn.event",
  "params": {
    "sessionId": "ses_01J00000000000000000000001",
    "turnId": "job_01J00000000000000000000001",
    "jobId": "job_01J00000000000000000000001",
    "sequence": 3,
    "event": {
      "type": "AgentText",
      "text": "Working through the patch.",
      "truncated": false
    }
  }
}
```

Example terminal notification:

```json
{
  "jsonrpc": "2.0",
  "method": "turn.result",
  "params": {
    "sessionId": "ses_01J00000000000000000000001",
    "turnId": "job_01J00000000000000000000001",
    "jobId": "job_01J00000000000000000000001",
    "state": "completed",
    "result": {
      "text": "Done.",
      "resultPath": "/home/me/.local/state/agentbus/results/job_01J00000000000000000000001.txt",
      "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      "bytes": 5
    }
  }
}
```

## Error-code namespace

JSON-RPC numeric error codes are implementation-defined and MUST remain
numeric. Structured errors MUST put the stable v1 error identifier in
`error.data.code`, using this namespace:

| Code | Meaning |
| --- | --- |
| `unauthorized` | The required `protocol.hello` token was missing or invalid. |
| `session_busy` | A session already has an active turn. |
| `name_conflict` | A policy name was re-registered with a different spec. |
| `version_mismatch` | The client and server protocol major versions differ. |
| `capability_missing` | A requested capability is not available. |
| `backend_unavailable` | The requested backend is not installed, unhealthy, or disabled. |
| `timeout` | A turn or job exceeded its timeout. |
| `interrupted` | A foreground turn was interrupted. |
| `quarantined` | A corrupt record was quarantined and cannot produce a normal result. |
| `result_too_large` | A requested inline result exceeds the supported inline limit. |
| `invalid_task_spec` | A `taskSpec` is malformed or contains unsupported fields. |
| `request_conflict` | A `requestId` was reused with a different task specification; `error.data.jobId` identifies the existing job. |

Example:

```json
{
  "jsonrpc": "2.0",
  "id": "8",
  "error": {
    "code": -32000,
    "message": "session already has an active turn",
    "data": {
      "code": "session_busy",
      "sessionId": "ses_01J00000000000000000000001"
    }
  }
}
```

## Methods

All method examples are JSON-RPC objects before newline framing.

### `protocol.hello`

`protocol.hello` negotiates protocol major version and advertises backends and
capabilities. It MUST be the first request on a connection.

Request params:

```json
{
  "clientProtocolVersion": 1,
  "token": "accident-prevention-token"
}
```

`token` is REQUIRED. The server MUST check it against the daemon token file
before advertising capabilities. The token check is accident-prevention only;
it is not a security boundary. If the token is missing or invalid, the server
MUST fail `protocol.hello` with `error.data.code` set to `unauthorized`.

Result:

```json
{
  "protocolVersion": 1,
  "backends": ["codex", "claude"],
  "backendMetadata": [
    {"backend": "codex", "models": ["gpt-5.4"], "efforts": ["low", "medium", "high", "xhigh"]},
    {"backend": "claude", "models": ["sonnet", "opus"], "efforts": ["low", "medium", "high", "xhigh", "max"]}
  ],
  "capabilities": {
    "policy.shape": true,
    "policy.jsonSchema": true,
    "policy.named": true,
    "policy.retry": true,
    "nativeStructuredOutput.codex": false,
    "nativeStructuredOutput.claude": false,
    "models.discovery": true,
    "models.reported": true
  }
}
```

`backendMetadata`, `models.discovery`, and `models.reported` are additive
protocol-v1 fields. Older clients may ignore them and continue using the stable
`backends` string array. Clients that need backend-confirmed model identity MUST
gate on `models.reported`.

### Model discovery, validation, and reporting

Runtime model and effort catalogs are best-effort discovery data, not an
entitlement guarantee. If a requested model or effort is absent from a current
discovered catalog, agentbus MUST emit a `Warning` event and pass the request to
the backend; the backend remains authoritative for rejecting an invalid value.
Only an adapter's explicitly configured static allow-list may cause agentbus to
reject a model or effort before starting the backend.

`modelReported`, when present on `turn.result`, `job.status`, or `job.result`,
is the non-empty model value reported by the backend while executing the turn.
It is best-effort and MAY be absent. It is also repeated in `result` when a
persisted final result exists. An empty backend event MUST NOT erase a model
reported earlier in the same job. When a backend reports more than one non-empty
model in the same job, the most recent value wins: later configuration events
(for example codex `session_configured` after `thread.started`) and later
attempts refine earlier ones.

Unauthorized error example:

```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "error": {
    "code": -32000,
    "message": "missing or invalid hello token",
    "data": {
      "code": "unauthorized"
    }
  }
}
```

Version mismatch error example:

```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "error": {
    "code": -32000,
    "message": "protocol major version mismatch",
    "data": {
      "code": "version_mismatch",
      "serverProtocolVersion": 1
    }
  }
}
```

`backends` is the list of backend names accepted by `session.start` and
`job.submit`. `capabilities` is a string-keyed object. Clients MUST gate optional
behavior on explicit capabilities rather than protocol major version alone.
`jobs.requestId: true` advertises support for idempotent `job.submit` request
identifiers.

### `session.start`

Starts a backend session.

Request params:

```json
{
  "backend": "codex",
  "cwd": "/absolute/workspace/path",
  "write": false,
  "model": "gpt-5",
  "effort": "medium",
  "tags": {
    "client": "convo-relay",
    "slot": "codex-a"
  }
}
```

Result:

```json
{
  "sessionId": "ses_01J00000000000000000000001",
  "backend": "codex"
}
```

`write` on `session.start` is the session default only. Effective write
permission is a per-turn property. If `turn.start.write` is absent, the turn
uses the session default.

### `session.resume`

Resumes a known backend session by id.

Request params:

```json
{
  "sessionId": "ses_01J00000000000000000000001"
}
```

Result:

```json
{
  "sessionId": "ses_01J00000000000000000000001",
  "backend": "codex"
}
```

### `session.list`

Lists known sessions, optionally filtered by exact tag matches.

Request params:

```json
{
  "tags": {
    "client": "convo-relay"
  }
}
```

Result:

```json
{
  "sessions": [
    {
      "sessionId": "ses_01J00000000000000000000001",
      "backend": "codex",
      "cwd": "/absolute/workspace/path",
      "write": false,
      "tags": {
        "client": "convo-relay",
        "slot": "codex-a"
      },
      "activeTurnId": null
    }
  ]
}
```

### `turn.start`

Starts one foreground turn on an existing session.

Request params:

```json
{
  "sessionId": "ses_01J00000000000000000000001",
  "prompt": "Inspect the working tree and report findings.",
  "write": false,
  "policy": {
    "prologue": "Return a structured report.",
    "contract": {
      "shape": {
        "firstLineEnum": ["PASS", "FAIL"],
        "requiredSections": ["Findings", "Tests"],
        "requiredAttestations": ["I inspected the diff."],
        "evidenceHeuristic": true
      }
    },
    "retry": {
      "max": 1,
      "template": "Your response missed: {{missing}}. Emit the corrected report only; make no further changes."
    }
  },
  "timeoutMs": 1800000
}
```

Result:

```json
{
  "turnId": "job_01J00000000000000000000001",
  "jobId": "job_01J00000000000000000000001",
  "sessionId": "ses_01J00000000000000000000001"
}
```

`write` is optional and defaults to the session default. A server MUST allow a
turn to downgrade a write-enabled session to `write:false`.

`timeoutMs` is optional and uses the same timeout semantics as
`taskSpec.timeoutMs`.

There is exactly one active turn per session. If `turn.start` is called while
the session has an active turn, the server MUST return `session_busy`. The
server MUST NOT queue or interleave turns on a session in v1.

There is no re-attach stream in v1. If the client disconnects mid-turn, events
are dropped and are not replayed. The outcome remains available through
`job.result`.

#### `turn.event` notification

`turn.event` notifications use this outer shape:

```json
{
  "sessionId": "ses_01J00000000000000000000001",
  "turnId": "job_01J00000000000000000000001",
  "jobId": "job_01J00000000000000000000001",
  "sequence": 1,
  "event": {
    "type": "ToolUse",
    "name": "shell",
    "input": "git status --short",
    "text": "git status --short",
    "truncated": false
  }
}
```

The `event.type` enum is:

| Type | Required fields | Meaning |
| --- | --- | --- |
| `AgentText` | `text`, `truncated` | Assistant text emitted during the turn. |
| `ToolUse` | `name`, `text`, `truncated` | Tool invocation or equivalent backend action. |
| `Warning` | `text`, `truncated` | Non-terminal warning from agentbus or the backend adapter. |

Adapters MAY include backend-specific metadata on events, but v1 clients MUST
only depend on the fields above.

#### `turn.result` notification

Terminal foreground notification:

```json
{
  "sessionId": "ses_01J00000000000000000000001",
  "turnId": "job_01J00000000000000000000001",
  "jobId": "job_01J00000000000000000000001",
  "state": "completed",
  "modelReported": "gpt-5.4",
  "result": {
    "text": "PASS\n\n## Findings\nNone.",
    "resultPath": "/home/me/.local/state/agentbus/results/job_01J00000000000000000000001.txt",
    "sha256": "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
    "bytes": 24,
    "modelReported": "gpt-5.4"
  },
  "contract": {
    "status": "compliant",
    "missing": [],
    "reason": "",
    "contractName": "delegate/delegate-report@1",
    "contractSha256": "sha256:7f83b1657ff1fc53b92dc18148a1d65dfa1352f3aabbccddeeff001122334455",
    "attempts": 1,
    "retryUsed": false,
    "validatedAt": "2026-07-09T12:00:00Z"
  }
}
```

### `turn.interrupt`

Interrupts a foreground turn. In v1 this kills the backend process group.

Request params:

```json
{
  "turnId": "job_01J00000000000000000000001"
}
```

Result:

```json
{
  "turnId": "job_01J00000000000000000000001",
  "jobId": "job_01J00000000000000000000001",
  "state": "interrupted"
}
```

### `job.submit`

Submits a background job.

Request params:

```json
{
  "requestId": "delegate-run-20260713-001",
  "taskSpec": {
    "backend": "claude",
    "cwd": "/absolute/workspace/path",
    "write": false,
    "model": "sonnet",
    "effort": "medium",
    "prompt": "Summarize the current diff.",
    "policy": {
      "contract": {
        "named": "delegate/delegate-report@1"
      },
      "retry": {
        "max": 0,
        "template": "Your response missed: {{missing}}. Emit the corrected report only; make no further changes."
      }
    },
    "tags": {
      "client": "delegate",
      "kind": "task"
    },
    "timeoutMs": 1800000
  }
}
```

`requestId` is optional, opaque, and scoped to the canonical workspace. When
present it MUST contain 1-128 printable characters and is persisted with the
accepted job. The server hashes the submitted `taskSpec` as canonical JSON
(recursively sorted object keys, with no fuzzy normalization) and stores the
mapping `requestId -> { jobId, taskSpecSha256 }` durably.

Repeating a `requestId` with the same hash MUST return the existing job's
submit response, with additive `"deduplicated": true`. The existing job is
returned in its current state, including `failed`; relaunching a failed job is
caller policy and requires a new request identifier. Reusing the same
identifier with a different hash MUST return `request_conflict` and the
existing `jobId` in error data.

Before starting a worker, the server atomically creates an O_EXCL reservation
keyed by a SHA-256 hash of `requestId`, then creates the job record while
holding its request lock. A reservation with no job record can only represent
a crash between those steps; it is retained for a bounded five-minute grace
period and then reclaimed by a later submit. GC removes an index entry when it
removes the referenced job record.

Result:

```json
{
  "jobId": "job_01J00000000000000000000002",
  "state": "queued"
}
```

On a replay, the result additionally contains `"deduplicated": true`.

### `job.status`

Fetches one job status or all job statuses.

Each job entry MUST include `heartbeatAt`, `lease`, `workerPid`,
`workerStartTime`, `backendChildPid`, `backendChildStartTime`, `statePath`, and
`logPaths` when those fields are known for the current record.
`workerStartTime` and `backendChildStartTime` are opaque platform process
start-time tokens. Clients MUST compare them only for exact equality when
verifying that a PID has not been reused. `lease.expired` MUST be computed at
status-read time from `lease.expiresAt`. A running job whose heartbeat lease
has expired MUST still report its recorded `state` as `running` until the
reaper transitions it, and MUST report `lease.expired: true`. The daemon's
default lease duration is five minutes and its default heartbeat interval is
30 seconds. On an expired lease, the reaper MAY renew the lease and append a
stale-heartbeat warning only when every persisted process identity with both a
PID and start-time token still matches the observed process. If any required
identity is missing, dead, or reused, the reaper MUST move the record to
`orphaned`.

A backend child may legitimately exit at every turn boundary, before the worker
persists its authoritative `turn.result`. Its absence alone MUST NOT orphan a
`running` or `retrying` record. Outside the lease-expiry branch, reaping moves
such a record to `orphaned` only when the worker or supervisor is missing or
has been reused; an identity-confirmed worker owns reporting the turn result.

`lateFinalization` is an optional boolean on `job.status` and `job.result`.
When `true`, an authoritative terminal result arrived after the record had
entered `orphaned`; it is an informational lifecycle annotation, not a warning
condition or a different terminal state. It is additive and requires no
capability negotiation; an omitted value is false.

An orphaned record MUST remain non-terminal for an orphan grace period measured
from the transition to `orphaned`. The default orphan grace is the configured
lease duration and MUST NOT be shorter than that duration. After the grace
expires, the reaper MAY transition the record to `reaped`. This reconciliation
window permits an in-process backend completion already held by the supervising
daemon to persist its authoritative result.

Request params for one job:

```json
{
  "jobId": "job_01J00000000000000000000002"
}
```

Request params for all jobs:

```json
{
  "all": true
}
```

Without `jobId`, `all: false` (or omission) lists the daemon's configured
workspace; `all: true` lists every known workspace. Both listing forms accept
optional exact-match filters, combined with AND semantics:

```json
{
  "all": true,
  "requestId": "delegate-run-20260713-001",
  "tags": {
    "client": "delegate",
    "kind": "task"
  }
}
```

There are no glob or partial matches. Filter tag keys and values MUST be
printable and at most 256 characters. A filter `requestId` follows the same
1-128 printable-character validation as `job.submit`. `requestId` is echoed
on `job.status` and `job.result` when the job was submitted with one.

Result:

```json
{
  "jobs": [
    {
      "jobId": "job_01J00000000000000000000002",
      "requestId": "delegate-run-20260713-001",
      "sessionId": "ses_01J00000000000000000000002",
      "backend": "claude",
      "state": "running",
      "modelReported": "claude-opus-4",
      "tags": {
        "client": "delegate",
        "kind": "task"
      },
      "startedAt": "2026-07-09T12:00:00Z",
      "updatedAt": "2026-07-09T12:01:00Z",
      "heartbeatAt": "2026-07-09T12:01:00Z",
      "lease": {
        "expiresAt": "2026-07-09T12:02:00Z",
        "expired": true
      },
      "workerPid": 12345,
      "workerStartTime": "2026-07-09 12:00:00",
      "backendChildPid": 12346,
      "backendChildStartTime": "2026-07-09 12:00:00",
      "statePath": "/home/me/.local/state/agentbus/jobs/job_01J00000000000000000000002.json",
      "logPaths": {
        "stdout": "/home/me/.local/state/agentbus/logs/job_01J00000000000000000000002.stdout.log",
        "stderr": "/home/me/.local/state/agentbus/logs/job_01J00000000000000000000002.stderr.log"
      }
    }
  ]
}
```

### `job.result`

Fetches a persisted terminal result. `job.result` is a request method. It is not
the `turn.result` notification.

Request params:

```json
{
  "jobId": "job_01J00000000000000000000002"
}
```

Result:

```json
{
  "jobId": "job_01J00000000000000000000002",
  "requestId": "delegate-run-20260713-001",
  "sessionId": "ses_01J00000000000000000000002",
  "state": "completed_noncompliant",
  "lateFinalization": true,
  "modelReported": "claude-opus-4",
  "result": {
    "resultPath": "/home/me/.local/state/agentbus/results/job_01J00000000000000000000002.txt",
    "sha256": "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
    "bytes": 327680,
    "modelReported": "claude-opus-4"
  },
  "contract": {
    "status": "noncompliant",
    "missing": ["section: Tests"],
    "reason": "missing required section",
    "contractSha256": "sha256:7f83b1657ff1fc53b92dc18148a1d65dfa1352f3aabbccddeeff001122334455",
    "attempts": 1,
    "retryUsed": false,
    "validatedAt": "2026-07-09T12:00:00Z"
  }
}
```

### `job.cancel`

Cancels a queued or running background job.

Request params:

```json
{
  "jobId": "job_01J00000000000000000000002"
}
```

Result:

```json
{
  "jobId": "job_01J00000000000000000000002",
  "state": "canceled"
}
```

### `policy.validate`

Validates text against a contract without starting a backend turn.

Request params:

```json
{
  "text": "PASS\n\n## Findings\nNone.\n\nI inspected the diff.",
  "contract": {
    "shape": {
      "firstLineEnum": ["PASS", "FAIL"],
      "requiredSections": ["Findings"],
      "requiredAttestations": ["I inspected the diff."],
      "evidenceHeuristic": false
    }
  }
}
```

Result:

```json
{
  "valid": true,
  "missing": [],
  "contractSha256": "sha256:7f83b1657ff1fc53b92dc18148a1d65dfa1352f3aabbccddeeff001122334455"
}
```

### `policy.register`

Registers an optional named policy spec cache entry.

Request params:

```json
{
  "name": "delegate/delegate-report@1",
  "spec": {
    "shape": {
      "firstLineEnum": ["PASS", "FAIL"],
      "requiredSections": ["Findings", "Tests"],
      "requiredAttestations": ["I inspected the diff."],
      "evidenceHeuristic": true
    }
  }
}
```

Result:

```json
{
  "name": "delegate/delegate-report@1",
  "contractSha256": "sha256:7f83b1657ff1fc53b92dc18148a1d65dfa1352f3aabbccddeeff001122334455",
  "registered": true
}
```

If the same name is registered again with an identical spec, the operation is
idempotent and MUST succeed. If the same name is registered with a different
spec, the server MUST return `name_conflict`.

## `taskSpec`

`taskSpec` is exactly a "run one backend turn" object:

```json
{
  "backend": "codex",
  "cwd": "/absolute/workspace/path",
  "write": false,
  "model": "gpt-5",
  "effort": "medium",
  "prompt": "Do the task.",
  "policy": {
    "contract": {
      "shape": {
        "requiredSections": ["Summary"]
      }
    }
  },
  "tags": {
    "client": "delegate"
  },
  "timeoutMs": 1800000
}
```

Allowed fields:

| Field | Required | Type | Meaning |
| --- | --- | --- | --- |
| `backend` | yes | string | Backend name from `protocol.hello.backends`. |
| `cwd` | yes | string | Absolute working directory. |
| `write` | yes | boolean | Effective write permission for this job turn. |
| `model` | no | string | Backend model selector. |
| `effort` | no | string | Backend effort selector. |
| `prompt` | yes | string | User prompt. |
| `policy` | no | TurnPolicy | Generic structural policy. |
| `tags` | no | object | Client-supplied string tags for discovery. |
| `timeoutMs` | no | integer | Timeout in milliseconds. See timeout rules below. |

No other fields are permitted in v1. An implementation MUST reject unknown
fields with `invalid_task_spec`. A future protocol version MAY add a `kind`
discriminator. v1 intentionally does not include open-ended extensibility:
workflow composition belongs to clients, not agentbus.

When `timeoutMs` is omitted, the timeout defaults to 1800000 milliseconds
(30 minutes). Non-zero timeout values MUST NOT exceed 14400000 milliseconds
(4 hours). The explicit value `0` means unbounded. Unbounded timeout is allowed
only when `timeoutMs` is explicitly set to `0`; implementations MUST NOT infer
an unbounded timeout from an omitted field. These rules apply to foreground
turns and background jobs alike.

## State machine

Foreground turns and background jobs share the same job state machine.

```text
queued -> starting -> running -> [retrying ->] completed
                                      |          completed_noncompliant
                                      |          failed
                                      |          timed_out
                                      |          interrupted
                                      |          canceled
```

Supervision states:

```text
orphaned -> completed | completed_noncompliant | failed | reaped
reaped --authoritative in-process completion--> completed | completed_noncompliant
quarantined
```

An authoritative completion may normally transition an `orphaned` record to
`completed`, `completed_noncompliant`, or `failed`; a `lateFinalization: true`
annotation records that ordering. The `reaped` completion amendments are
intentionally narrow: only the daemon
instance that already holds the authoritative backend completion may salvage a
`reaped` record. Status clients, restarted daemons, and unauthenticated or
out-of-process writers MUST treat `reaped` as terminal and MUST NOT synthesize a
result. This exception closes the `running -> orphaned -> reaped -> finalize`
race without reopening arbitrary terminal records.

`retrying` is entered only from `running` when policy validation fails and
`retry.max == 1`. It may be entered at most once for a job.

Terminal states:

| State | Terminal | Meaning |
| --- | --- | --- |
| `queued` | no | Job record exists but execution has not started. |
| `starting` | no | Supervisor is launching the backend process. |
| `running` | no | Backend process is active. |
| `retrying` | no | One corrective resume is being launched after policy failure. |
| `completed` | yes | Backend completed and policy, if any, is compliant or skipped without noncompliance. |
| `completed_noncompliant` | yes | Backend produced a final result but structural policy validation failed after allowed retry. |
| `failed` | yes | Backend or agentbus failed before a successful final result. |
| `timed_out` | yes | Timeout expired. |
| `interrupted` | yes | Foreground turn was interrupted. |
| `canceled` | yes | Background job was canceled. |
| `orphaned` | no | Reaper found missing, stale, or identity-mismatched supervision; the orphan grace is still reconciling possible in-process completion. |
| `reaped` | yes | Reaper finalized an orphan after its grace elapsed; only an authoritative in-process completion already held by the daemon may amend it to a completed state. |
| `quarantined` | yes | Corrupt record was moved aside with diagnostics. |

CLI commands that surface a single job result MUST map terminal states to exit
codes as follows:

| State | Exit code |
| --- | ---: |
| `completed` | 0 |
| `completed_noncompliant` | 3 |
| `failed` | 4 |
| `timed_out` | 5 |
| `interrupted` | 6 |
| `canceled` | 7 |
| `reaped` | 8 |
| `quarantined` | 9 |

Non-terminal states returned by polling commands MUST use exit code `2` when
the command cannot return a terminal result yet.

## Result-size semantics

Each `turn.event` payload field containing backend text is subject to a
per-event truncation cap. The default cap is 64 KiB. When truncation occurs, the
event MUST set `truncated: true`. Event text is a streaming convenience and MUST
NOT be treated as the authoritative final result.

The terminal final assistant message is always spilled to a state file. The
spilled file is the authoritative final result. The SHA-256 hash is computed
over the raw final assistant message bytes exactly as written to the result
file. No JSON encoding, newline normalization, ANSI stripping, or Unicode
normalization is applied before hashing.

`turn.result` and `job.result` include inline text only when the final result is
under the inline cap. The default inline cap is 256 KiB.

Inline result shape:

```json
{
  "text": "Final answer.",
  "resultPath": "/home/me/.local/state/agentbus/results/job_01J00000000000000000000001.txt",
  "sha256": "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
  "bytes": 13,
  "modelReported": "gpt-5.4"
}
```

Spilled-only result shape:

```json
{
  "resultPath": "/home/me/.local/state/agentbus/results/job_01J00000000000000000000001.txt",
  "sha256": "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
  "bytes": 327680,
  "modelReported": "gpt-5.4"
}
```

`resultPath` is stable within a protocol major version. Its contents are the
authoritative API. The directory layout is not guaranteed across major
versions.

## TurnPolicy

TurnPolicy is a generic mechanism. It has no built-in policy opinions.

```json
{
  "prologue": "Optional text prepended before the user prompt.",
  "contract": {
    "shape": {
      "firstLineEnum": ["PASS", "FAIL"],
      "requiredSections": ["Findings", "Tests"],
      "requiredAttestations": ["I inspected the diff."],
      "evidenceHeuristic": true
    }
  },
  "retry": {
    "max": 1,
    "template": "Your response missed: {{missing}}. Emit the corrected report only; make no further changes."
  }
}
```

Fields:

| Field | Required | Meaning |
| --- | --- | --- |
| `prologue` | no | Text prepended to the backend prompt before execution. |
| `contract` | no | Contract spec: `jsonSchema`, `shape`, or `named`. |
| `retry` | no | Bounded corrective resume settings. |

Contract variants:

```json
{ "jsonSchema": { "type": "object" } }
```

```json
{
  "shape": {
    "firstLineEnum": ["PASS", "FAIL"],
    "requiredSections": ["Findings"],
    "requiredAttestations": ["I inspected the diff."],
    "evidenceHeuristic": true
  }
}
```

```json
{ "named": "delegate/delegate-report@1" }
```

`jsonSchema` contracts validate the final assistant message parsed as JSON. If
the final assistant message is not valid JSON, validation fails. The v1 schema
dialect is JSON Schema Draft 2020-12. Schema failures in `ContractStamp.missing`
and `{{missing}}` are leaf violations formatted as `<json-pointer>: <message>`.
The root uses the empty RFC 6901 pointer, so a root-level violation renders as
`: <message>`; `/` instead addresses an empty-named property. Entries are sorted
and de-duplicated. They include at most 20 entries, with a final
`+N more schema violations` summary line when needed (not a violation location).
Parser and schema-compilation failures are single `json: ...` and
`jsonSchema: ...` entries.
Individual messages and rendered retry violation text are bounded to keep retry
prompts safe.

`retry.max` MUST be `0` or `1`. `retry.template` MUST include the literal token
`{{missing}}` when `max == 1`. A corrective retry template MUST instruct the
backend to emit the corrected report only and make no further changes. The
corrective retry turn MUST run with `write:false`, even if the original session
or turn allowed writes.

No policy means no-op passthrough and no contract stamp. A disabled policy is a
client-level convention; if a client sends no policy, agentbus does not stamp.

### Registry semantics

Inline contracts are the primary v1 path. The registry is off the critical path.
`policy.register` is an optional cache and convenience mechanism.

Names MUST be either:

- namespaced version names: `<client>/<name>@<ver>`
- content-addressed names: `sha256:<hash>`

Specs are immutable per name. A changed spec MUST ship under a new versioned
name such as `@2`. Re-registering a different spec under an existing name MUST
fail with `name_conflict`. Re-registering an identical spec under the same name
MUST be idempotent.

Named references are resolved to concrete specs at submit time. The resolved
spec MUST be persisted in the job record. Daemon restarts and reaper-side
validation MUST NOT depend on resolving a name again later.

`agentbus validate --contract` MUST accept spec files. Names are best-effort for
that command and are resolvable only if registered in the current daemon
lifetime.

### Shape-spec validator semantics

The shape validator runs over raw result text after ANSI escape stripping. It
performs no other normalization.

Fenced code blocks are excluded entirely from section matching and attestation
matching. Fence detection follows CommonMark fence pairing for backtick and
tilde fences:

- An opening fence line begins with zero to three spaces, followed by a run of
  at least three backticks or at least three tildes.
- The opening fence character and run length are remembered.
- A closing fence line begins with zero to three spaces, uses the same fence
  character, and has a run length greater than or equal to the opening run.
- Lines indented four or more spaces do not open or close fences.
- Text after the opening fence run is treated as an info string and is ignored
  by the validator.

Text inside a fence MUST NOT satisfy a required section, attestation, or
evidence pattern except for the fenced-command evidence pattern defined below.

Section matching:

- Matching is case-insensitive.
- Required sections match Markdown headings from `#` through `####`.
- Required sections also match line-initial `Label:` labels.
- Duplicates are allowed; the first matching heading or label wins.
- An empty section satisfies presence. Content quality is out of scope.

Attestation matching:

- Each required attestation string MUST appear outside fenced code blocks.
- Matching is on the ANSI-stripped raw text with no other normalization.

`firstLineEnum`:

- The first line of ANSI-stripped raw text MUST exactly equal one of the listed
  strings.
- No trimming is performed beyond removing the line terminator.

`evidenceHeuristic`:

When `evidenceHeuristic` is true, evidence is required only when the message
claims findings. In v1, "claims findings" is a structural trigger: after ANSI
stripping and fenced-code exclusion, the message has a `Findings` section or
`Findings:` label whose body contains at least one non-empty line other than
`none`, `no findings`, `n/a`, or `not applicable`, matched case-insensitively.
The frozen v1 evidence pattern list is:

| Pattern | Definition |
| --- | --- |
| `path:line` occurrence | A non-whitespace path-like token followed by `:` and a decimal line number, for example `engine/run.go:42`. |
| fenced command with adjacent exit-code mention | A fenced command block with an adjacent line before or after the fence mentioning an exit code, such as `exit code 0` or `exit 1`. |
| diff hunk header | A unified diff hunk header beginning with `@@`. |

The validator MUST NOT infer correctness from these patterns. They only satisfy
the structural evidence requirement.

### Engine validation behavior

For a turn with policy, the engine behavior is:

1. Prepend `policy.prologue` to the backend prompt when present.
2. Run the turn.
3. Persist the complete final assistant message to the spilled result file.
4. Validate the contract, if present, against the complete persisted final
   result. The engine MUST NOT validate against wire-truncated `turn.event`
   text.
5. If validation fails and `retry.max == 1`, run one corrective resume with the
   rendered retry template.
6. Force the corrective resume to `write:false`.
7. Persist the retry final message and validate it.
8. Stamp the terminal result.

If validation is skipped because no usable final result exists, the stamp status
MUST be `skipped` with one of the documented skipped reasons.

TurnPolicy records structural compliance only. It does not verify correctness,
task completion, repository cleanliness, or instruction-following.

### Contract stamp

`turn.result.contract` and `job.result.contract`, when present, use this shape:

```json
{
  "status": "retried",
  "missing": [],
  "reason": "initial response missed required section; retry satisfied contract",
  "contractName": "delegate/delegate-report@1",
  "contractSha256": "sha256:7f83b1657ff1fc53b92dc18148a1d65dfa1352f3aabbccddeeff001122334455",
  "attempts": 2,
  "retryUsed": true,
  "validatedAt": "2026-07-09T12:00:00Z"
}
```

Stamp fields:

| Field | Required | Meaning |
| --- | --- | --- |
| `status` | yes | Compliance status. |
| `missing` | yes | Machine-readable missing structural requirements. Empty when none. |
| `reason` | yes | Human-readable reason or empty string. |
| `contractName` | no | Name used by the client, when any. |
| `contractSha256` | yes when validation ran | SHA-256 identifier of the resolved contract spec. |
| `attempts` | yes | Number of backend attempts whose final results were considered. |
| `retryUsed` | yes | Whether corrective retry ran. |
| `validatedAt` | yes when validation ran | RFC 3339 timestamp. |

Status enum:

| Status | Meaning |
| --- | --- |
| `compliant` | First attempt satisfied the contract. |
| `retried` | First attempt failed; one corrective retry satisfied the contract. |
| `noncompliant` | Contract remained invalid after allowed attempts. |
| `skipped` | Validation could not run for an allowed skipped reason. |
| `disabled` | Client explicitly disabled enforcement and chose to stamp that fact. |

Skipped reasons:

| Reason |
| --- |
| `timeout` |
| `interrupt` |
| `no_final_message` |
| `backend_error` |
| `result_unavailable` |
