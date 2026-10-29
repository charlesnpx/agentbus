# Agentbus wire protocol

Agentbus protocol version 3 is a JSON-RPC 2.0 service over a local,
newline-delimited Unix socket. Each frame is one JSON object followed by a
newline. The daemon accepts only protocol version 3.

The wire surface has nine methods:

| Method | Purpose |
| --- | --- |
| protocol.hello | Authenticate the connection and confirm the protocol version. |
| backend.inspect | Inspect configured backend executables and daemon defaults. |
| job.submit | Create or replay an identified job. |
| job.get | Read one identified job. |
| job.list | List compact job summaries with optional workspace, tag, and state filters. |
| job.transcript | Return a stateless digest and selected captured items for one job. |
| job.cancel | Request cancellation of one job. |
| artifacts.prune | Preview or remove eligible Agentbus-owned diagnostic artifacts. |
| job.steer | Submit one bounded follow-up instruction to an active steerable job. |

A client must call protocol.hello successfully before any other method on the
connection. A second hello on the same connection is invalid. Unknown methods
return JSON-RPC method-not-found.

All times are RFC 3339 UTC strings. Optional fields are omitted rather than
set to null.

## Envelopes and errors

A successful response has the normal JSON-RPC result member:

~~~json
{"jsonrpc":"2.0","id":"request-id","result":{}}
~~~

An error response has this shape:

~~~json
{
  "jsonrpc": "2.0",
  "id": "request-id",
  "error": {
    "code": -32000,
    "message": "human-readable explanation",
    "data": {
      "code": "stable_error_code",
      "jobId": "optional job id",
      "serverProtocolVersion": 3
    }
  }
}
~~~

The numeric code is -32601 for method_not_found and -32000 for the other
Agentbus errors. The stable data.code values are:

| data.code | Meaning |
| --- | --- |
| unauthorized | Hello was omitted or its token was invalid. |
| protocol_version_mismatch | The hello request used another major version. serverProtocolVersion identifies the server version. |
| method_not_found | The requested method is not part of the version-3 surface. |
| backend_unavailable | The daemon cannot use the requested backend or its job store. |
| invalid_task_spec | Parameters, identity, task specification, or inline schema are invalid. |
| queue_full | The new submission would exceed the daemon's bounded waiting queue. |
| unknown_job | No job has the requested jobId. |

jobId is included when the error relates to a known or requested job. Clients
should branch on data.code, not on message text.

## protocol.hello

~~~json
{
  "clientProtocolVersion": 3,
  "token": "state-root token"
}
~~~

The result confirms the version and reports configured backend names with any
available model and effort metadata:

~~~json
{
  "protocolVersion": 3,
  "backends": [
    {
      "backend": "codex",
      "models": ["string"],
      "efforts": ["string"]
    }
  ]
}
~~~

## backend.inspect

backend.inspect takes an empty parameter object and inspects each configured
backend independently:

~~~json
{}
~~~

~~~json
{
  "backends": [
    {
      "backend": "codex",
      "executablePath": "/usr/local/bin/codex",
      "version": "codex-cli 1.2.3",
      "defaultModel": "gpt-5-codex",
      "defaultEffort": "high"
    },
    {
      "backend": "claude",
      "error": "--version failed: exit status 1"
    }
  ]
}
~~~

Each entry contains the resolved executable path, the first line of its
`--version` output, and configured default model/effort when present. A
per-backend `error` reports path resolution or version-probe failure and does
not block the other entries. The daemon makes at most one bounded `--version`
call per executable path and modification time during its lifetime, caching
both successful results and failures. Inspection does not query provider
accounts, billing, or model catalogues.

## job.submit

Submission has a compound identity. A matching replay returns the existing job
without launching it again. Reusing the same identity with a different task
specification is rejected. A new submission that exceeds the daemon's bounded
waiting queue returns `queue_full`; matching deduplicated submissions are not
new queue entries. Execution timeouts begin when the daemon reserves an
execution slot, not when the job is submitted.

~~~json
{
  "workspaceKey": "string",
  "requestId": "string",
  "taskSpec": {
    "backend": "codex",
    "cwd": "/absolute/path",
    "write": true,
    "prompt": "string",
    "inputMode": "steerable",
    "localIPC": true,
    "resumeJobId": "job_previous",
    "retainSession": true,
    "model": "string",
    "effort": "string",
    "outputSchema": {},
    "tags": {"key": "value"},
    "timeoutMs": 1800000
  }
}
~~~

backend, cwd, write, and prompt are required task fields. inputMode, localIPC,
resumeJobId,
retainSession, model, effort, outputSchema, tags, and timeoutMs are optional.
inputMode accepts `fixed` or `steerable`. Omission means `fixed`; this value is
stored with the immutable task specification and participates in identified
replay hashing. Because it uses `omitempty`, omission preserves the existing
hash for every prior request. A daemon that does not recognize the field
rejects it as `invalid_task_spec` through strict unknown-field decoding, so no
protocol major-version bump is needed. The field is independent of `write` and
does not infer review intent from prompt text.
`localIPC` is also immutable request data; omitting it preserves existing replay
hashes. Set it to true only for a Codex write job on macOS. It
allows Unix sockets under the job's managed scratch root and TCP loopback. The
job shares loopback with the host; the profile adds no other Unix socket paths,
Docker socket access, or outbound domains. Claude and Cursor requests are
rejected. Linux requests are rejected because Codex's current Linux sandbox
blocks Unix sockets before applying its allowlist. A resumed job follows its
new TaskSpec value and does not inherit IPC permission from the target job.
outputSchema is one inline Draft 2020-12 JSON Schema value: an object or a
boolean. timeoutMs is a non-negative integer no greater than four hours; zero
means no deadline.

workspaceKey is an opaque, submitter-chosen namespace. Agentbus cannot derive
another tool's workspaceKey, even when that tool submits work for the same
directory.

resumeJobId names a prior job, never a backend thread ID. For a new submission,
the target must be terminal, use the same backend, and have a recorded backend
session ID at turn retirement. A target without that ID returns
`invalid_task_spec` with its jobId; Agentbus never falls back to a fresh thread.
A completed target is resumable only when its original submission included
`retainSession: true`; other terminal states retain their existing resume
rules. With retention, the managed Codex session home remains in place and is
never cleaned automatically. An operator may remove
`<state-root>/workspaces/<workspace-hash>/codex/<job-id>` after no further
resume is needed.

A resume creates a new job with a new id, record, transcript sidecar, result,
and fresh deadline. It replays the prior backend thread as history; it does not
continue the old job or extend its deadline. Under the normal managed Codex
configuration, the new job uses the retained CODEX_HOME at the root of the
resume lineage so the backend can see that thread. Different resumeJobId values
are different task specifications for identified replay, even when workspaceKey
and requestId are the same.

The result always includes the resolved timeout:

~~~json
{
  "jobId": "string",
  "state": "queued",
  "deduplicated": false,
  "timeout": {
    "requested": 1800000,
    "effective": 1800000,
    "source": "client"
  }
}
~~~

timeout.effective is milliseconds. timeout.source is client when timeoutMs was
provided, otherwise daemon_default. timeout.requested is omitted when the
daemon default is used.

## job.list

All job.list parameters are optional:

~~~json
{
  "workspaceKey": "string",
  "tags": {"key": "value"},
  "states": ["queued", "running"],
  "limit": 50,
  "cursor": "opaque-next-page-token"
}
~~~

An empty workspaceKey applies no workspace filter and returns matching jobs from
every workspace. The server never invents a default workspace. tags is a map:
a job matches only when it has every requested key with its requested value.
states contains public states; a job matches when its public state is one of
them. Empty or omitted tags and states do not filter. All supplied filter
dimensions combine with AND.

The result contains compact summaries:

~~~json
{
  "jobs": [
    {
      "jobId": "string",
      "backend": "string",
      "state": "running",
      "tags": {"team": "core"},
      "cleanup": "clean",
      "createdAt": "2026-08-20T12:00:00Z",
      "queuedAt": "2026-08-20T12:00:00Z",
      "updatedAt": "2026-08-20T12:00:01Z",
      "failureClass": "backend_error",
      "contract": {
        "evaluated": true,
        "compliant": false
      },
      "itemCount": 12,
      "lastItemAt": "2026-08-20T12:00:01Z",
      "lastActivityAt": "2026-08-20T12:00:02Z",
      "liveness": "alive"
    }
  ]
}
~~~

Each summary always contains jobId, backend, state, cleanup, createdAt,
queuedAt, and updatedAt. queuedAt is the time the job entered the queue;
startedAt on job.get shows when the durable running transition was recorded.
tags, modelReported, effortReported, failureClass, and contract appear when
applicable. modelReported is the latest model the backend reported;
effortReported is included when the backend reports effort. Neither is filled
from the task request. tags is the submitted tag map, allowing a caller to see
why a tag filter matched.

itemCount and liveness appear only while this daemon has an active execution
for that job. lastItemAt appears after that active execution has assembled an
item; it is absent before the first item. lastActivityAt advances whenever the
backend does anything observable, including a contentless progress event;
lastItemAt advances only when a transcript item is assembled. An orchestrator
watching for a stall should use lastActivityAt. A terminal job, or a job that
came from before this daemon started, has no activity or liveness projection.
liveness is alive when the recorded claim exactly matches a live process,
gone when the recorded claim is absent from the process table or mismatches a
recycled process, and unknown when the daemon cannot establish the identity.
The summary never exposes a PID, process-group ID, start token, or any process
claim.

The list contains summaries only. Other than tags, it does not expose request
identity, task details, prompt, current working directory, result text, failure
reason, log paths, or process claims.

Workspace filtering is scoping, not ownership. The local Unix socket has no
stable authenticated caller identity, so Agentbus cannot enforce who owns a
job. workspaceKey is an opaque, submitter-chosen namespace, and Agentbus
cannot derive another tool's key. Therefore, status without --job sends no
workspace filter and lists every workspace; a caller that knows a key can use
--workspace-key <key> to filter the list.

The wire limit is optional. Omitting it preserves the existing unbounded
client.JobList behavior; a positive limit returns newest jobs first, ordered by
createdAt descending and jobId descending for ties. cursor continues strictly
after the last summary in that ordering. A truncated result contains
`"truncated": true` and a `nextCursor` that can be sent as `cursor` on the next
request. When there is no further page, those fields are omitted. The CLI sends
a limit of 50 by default, supports `--before <nextCursor>` for continuation,
and uses `--all` to omit the wire limit.

## job.transcript

job.transcript requires jobId and reads the append-only item sidecar for that
job. It is deliberately stateless: there is no opaque cursor to retain across
an orchestrator's context compaction.

~~~json
{
  "jobId": "string",
  "kinds": ["message", "error"],
  "since": "2026-08-20T12:00:00Z",
  "sinceOrdinal": 141,
  "last": 10,
  "limit": 20
}
~~~

Kinds may contain message, reasoning, tool, toolResult, fileChange, warning,
or error. An absent kinds field and an explicit empty kinds array both mean no
kind filter. since includes items strictly after its RFC 3339 timestamp;
sinceOrdinal includes items whose ordinal is strictly greater than its value.
last selects the final N matching items and limit bounds the selected response.
kinds, since, sinceOrdinal, and last combine with AND; when last and limit are
both present, the response keeps the most recent minimum of the two bounds. An
ordinal is a readable handle: a caller can record that it last saw ordinal 141
and later use sinceOrdinal: 141 without retaining server-issued state.

~~~json
{
  "state": "running",
  "liveness": "alive",
  "itemCount": 258,
  "counts": {
    "message": 6,
    "reasoning": 0,
    "tool": 156,
    "toolResult": 0,
    "fileChange": 0,
    "warning": 0,
    "error": 1
  },
  "firstAt": "2026-08-20T12:00:00Z",
  "lastAt": "2026-08-20T12:27:00Z",
  "items": [
    {
      "ordinal": 141,
      "at": "2026-08-20T12:26:00Z",
      "kind": "message",
      "text": "working on the requested change",
      "truncated": false
    }
  ],
  "gap": true
}
~~~

itemCount, counts, firstAt, lastAt, and gap describe the captured sidecar, not
only the selected items. counts always has all seven kinds, including zero
values. liveness appears only while this daemon has an active execution for the
job and follows the same alive, gone, or unknown rule as job.list. A terminal
job remains readable because terminal handling does not remove its sidecar.
While a job runs, its transcript is not yet proven complete, so gap is true;
callers distinguish that from real loss using state.

With no kinds, since, sinceOrdinal, last, or limit, the response is a digest,
not the whole stream: it returns counts and timestamps plus the last few
message items and every captured error item. Any explicit selector returns its
matching items, subject to limit. A missing sidecar returns a valid empty
transcript with itemCount zero, seven zero counts, no timestamps, and items:
[]; gap is true when capture or reading could not establish continuity, so the
returned prefix may be incomplete rather than a claim that the job emitted no
more activity. A sidecar captured before completion receipts existed has no
receipt and is therefore reported as a gapped prefix, because its completeness
cannot be established.

When Agentbus has deliberately removed a job's log sidecars, the result also
includes `pruned: true` and an empty transcript. This distinguishes retention
cleanup from a sidecar that was never created or is unexpectedly missing.

## artifacts.prune

This method previews eligible artifact cleanup by default. `dryRun` may be
omitted (equivalent to `true`) or set to `false` to apply the cleanup.
Thresholds use Go duration syntax and default to `168h` for completed jobs and
`720h` for failed, canceled, or unknown jobs.

~~~json
{
  "dryRun": true,
  "successfulOlderThan": "168h",
  "failedOlderThan": "720h"
}
~~~

The result contains `wouldRemove` and `wouldRemoveBytes` for a dry-run,
`removed` and `removedBytes` after an apply pass, and `skipped` entries with a
job ID, optional path, and reason. The method removes only clean terminal jobs'
raw log sidecars and Agentbus-created Codex `cache`/`tmp` directories after the
age threshold for their state. Retained sessions, resumable sessions, resume
targets of non-terminal jobs, and all other unsafe or ineligible paths are
skipped. Job records, request identity, and result artifacts are preserved.

## job.get

job.get requires a non-empty jobId. An empty parameter object is an
invalid_task_spec parameter error rather than a list request.

~~~json
{"jobId":"string"}
~~~

~~~json
{
  "jobId": "string",
  "workspaceKey": "string",
  "requestId": "string",
  "backend": "string",
  "inputMode": "steerable",
  "steers": [],
  "state": "running",
  "cleanup": "clean",
  "createdAt": "2026-08-20T12:00:00Z",
  "queuedAt": "2026-08-20T12:00:00Z",
  "startedAt": "2026-08-20T12:00:01Z",
  "execution": {
    "intent": "edit",
    "sourceWritable": true,
    "scratch": "/state-root/workspaces/key/codex/job_id",
    "caches": {
      "GOCACHE": "/state-root/workspaces/key/codex/job_id/cache/go-build",
      "GOMODCACHE": "/home/user/go/pkg/mod"
    },
    "shell": "sandboxed",
    "network": "denied",
    "localIPC": "scratch",
    "gitMetadata": "read-only",
    "enforcement": "os-sandbox"
  },
  "timeout": {
    "effective": 1800000,
    "source": "daemon_default"
  },
  "deadline": "2026-08-20T12:30:01Z",
  "remainingMs": 1799500,
  "resumable": {
    "eligible": false,
    "reason": "resume target is running rather than terminal"
  },
  "modelRequested": "gpt-5-codex",
  "effortRequested": "high",
  "modelReported": "gpt-5-codex",
  "effortReported": "high",
  "tags": {"key": "value"},
  "logPaths": {
    "stdout": "string",
    "stdoutTruncated": false,
    "stderr": "string",
    "stderrTruncated": false
  }
}
~~~

jobId, workspaceKey, requestId, backend, inputMode, state, cleanup, createdAt,
queuedAt, timeout, and steers are always present. Older stored jobs without
inputMode project as `fixed`; steers is an empty array when there are no
steering receipts. Each steer summary contains messageId, status, mechanism,
and time; mechanism is empty unless delivery was confirmed. `execution` appears
after the job starts and is recorded atomically with its starting transition.
It contains intent (`inspect` or
`edit`), the selected source write mode, optional scratch and cache paths, shell
policy, local IPC, network policy, Git metadata access, and enforcement
strength. `localIPC` is `denied` for Codex jobs using the ordinary sandbox,
`scratch` for Codex jobs using the local IPC profile, and `unenforced` for
trusted full-access Codex writes and Claude/Cursor. The `shell` field is
`sandboxed` for Codex jobs using the OS sandbox, `restricted` for Claude
read-only jobs (the adapter's allowed/denied shell tool lists), and
`unenforced` for trusted full-access Codex writes, Claude write, and Cursor
jobs. The posture is descriptive:
`sourceWritable` reports the selected mode, while `enforcement` identifies
whether the backend uses an OS sandbox, agent permissions, or no filesystem
boundary. Before any non-trusted Codex job launches app-server, Agentbus runs a
bounded `codex sandbox -- true` check. A failure records `backend_unavailable`
with the sandbox diagnostic and does not fall back to a weaker mode. For Codex
edit jobs using the `workspaceWrite` sandbox,
`gitMetadata` is `read-only` because the granted writable roots do not include
the Git metadata directory. `logsPruned` is true after the daemon removes this
job's raw log sidecars. diagnostics appears when the job has recorded
operational diagnostics.

startedAt, finishedAt, deadline, remainingMs, tags, result, contract, failure,
cancellationReason, logPaths, modelRequested, effortRequested, modelReported,
and effortReported appear only when they apply. resumable is a derived
projection of the same session and retention rules used when validating resume
submissions. Its `eligible` value describes local admission only; it is not a
promise that the provider will accept the resume. deadline and remainingMs are
present for a running job with a finite effective timeout. deadline is derived
from startedAt plus timeout.effective; remainingMs is the current time left,
clamped at zero.

modelRequested and effortRequested preserve only values in the submitted
TaskSpec, so they may be absent when a configured daemon default was applied.
`requested is not executed`: these fields record intent, while modelReported
and effortReported record the latest values observed in provider telemetry.
modelReported retains its name for compatibility and now means latest
observation; observed values are never filled from requested values.

result exists only for a completed job with an authoritative result. Its
resultPath, sha256, and bytes are required; text may be omitted when the result
is kept only in the artifact. The sha256 field is a bare lowercase 64-character
hexadecimal digest with no algorithm prefix.

failure exists only for a failed job. contract exists only when an outputSchema
was submitted. attempts is one after initial evaluation and two only when the
single permitted correction was attempted.

When supplied by a provider, failure also includes its providerCode,
providerMessage (bounded to 512 Unicode code points), stage, and absolute
retryAfter time. providerCode is bounded to 128 code points. Existing class
and reason values keep their prior meaning. Current stages are `startup`,
`turn`, and `finalize`; schema-correction errors remain contract diagnostics
under the existing correction behavior. A transient failure can leave partial
workspace edits. Agentbus does not retry automatically; replay is the caller's
decision.

~~~json
{
  "failure": {
    "class": "usage_limit",
    "reason": "provider reported a usage limit",
    "providerCode": "rate_limit_error",
    "providerMessage": "weekly usage limit reached",
    "stage": "turn",
    "retryAfter": "2026-08-20T12:15:00Z"
  }
}
~~~

For each present log path, its paired `stdoutTruncated` or `stderrTruncated`
boolean is explicitly present as `true` or `false` when the daemon can
determine the state; the paired boolean is absent when the answer is unknown,
such as when the log file cannot be read.

## job.cancel

~~~json
{"jobId":"string"}
~~~

The result reports the durable public state after the cancellation request:

~~~json
{"jobId":"string","state":"canceled"}
~~~

If the job is already terminal, cancellation returns that existing state. A
race with completion also returns the first terminal state recorded by the
daemon. Read job.get or `agentbus status --job <id>` for the persisted
cancellationReason.

## job.steer

~~~json
{"jobId":"string","messageId":"caller-generated-id","text":"follow-up instruction"}
~~~

messageId must contain 1 to 256 bytes. text must be non-empty and at most
65,536 UTF-8 bytes (64 KiB). A steerable job accepts instructions only while a
provider Turn is active. Fixed-input jobs return `not_steerable` before any
provider I/O. Queued, completed, failed, canceled, or otherwise inactive jobs
return `not_running`; job.steer never creates a job. A schema-correction Turn
returns `schema_correction_active`.

The receipt shape is:

~~~json
{"jobId":"string","messageId":"caller-generated-id","status":"admitted","mechanism":"","reason":"","deduplicated":false}
~~~

status is `admitted`, `delivered`, `rejected`, or `unknown`. An admitted receipt
is immediate queue acceptance; job.get later shows its delivery outcome.
mechanism is `native` or `interrupt_continue` only for confirmed delivery and
is empty otherwise. reason is empty for admitted/delivered and one of
`not_steerable`, `not_running`, `duplicate_conflict`, `queue_full`,
`schema_correction_active`, `backend_unsupported`, or
`delivery_unconfirmed` for rejection or uncertainty.

The daemon orders steering against cancellation, schema correction, and job
completion. Repeating a messageId with the same text returns its original
receipt with `deduplicated: true`; a different text returns
`duplicate_conflict`. The per-job pending queue is ordered and capped at eight
messages. Each durable steer record stores messageId, the text SHA-256 and byte
size, status, mechanism, reason, and timestamps. The text itself is not added to
the original prompt or task hash. Cancel rejects queued steers and remains
authoritative.

The adapter contract keeps one logical Turn and one final result. An internal
interrupt used to steer is not cancellation, and an interrupted segment's
partial result is not the job result. Steering never changes deadline,
permissions, model, effort, or schema. Sessions without steering support
return `backend_unsupported`.

`unknown` means provider delivery could not be confirmed. After a daemon
restart, an admitted steer without a recorded outcome becomes
`unknown`/`delivery_unconfirmed`; it is never sent again automatically.
Deduplication is client-request identity, not provider exactly-once delivery.
The daemon does not promise simultaneous interruption across backends, provider
compliance with the new instruction, a stable provider turn ID across an
interrupt-and-continue, rollback of already-started tools, a fresh-session
fallback, or an alternative-backend fallback.

## States, cleanup, and failures

The public state vocabulary is closed:

| State | Meaning |
| --- | --- |
| queued | Accepted and waiting to start. |
| running | Work is active. |
| completed | A terminal result was recorded. It may be noncompliant with an inline schema. |
| failed | A terminal failure was recorded. failure.class gives its category. |
| canceled | Cancellation won the terminal transition. |
| unknown | The daemon cannot state the execution outcome. |

The store has a private launch marker that projects as running. It is never
returned by the protocol. There is no retry state.

cleanup is independent of state:

| Cleanup | Meaning |
| --- | --- |
| clean | No cleanup uncertainty is recorded. |
| uncertain | Cleanup could not be established under the process-claim rule. |

For example, a completed job can have cleanup set to uncertain and still retain
its result and schema verdict.

Failure classes are data on failed jobs, not more states:

| Failure class | Meaning |
| --- | --- |
| backend_unavailable | The selected backend could not be used. |
| provider_overloaded | The provider reported capacity pressure. |
| usage_limit | The provider reported a usage or rate limit. |
| model_unavailable | The requested model was unavailable. |
| content_policy | The provider refused the request on content-policy grounds. |
| authentication | The provider explicitly rejected authentication. |
| backend_error | A launched backend returned another error. |
| timeout | The job deadline expired. |
| interrupted | The backend stopped without a successful terminal result. |
| internal | Agentbus could not make a more specific classification. |
| storage | Agentbus could not persist an authoritative result or terminal record. |

The Codex app-server adapter recognizes structured completion `errorInfo` or
`errorCode` signals for usage/rate limits, authentication, unavailable models,
content policy, and overload. The Claude stream-json adapter recognizes
structured error code/type/status signals for these classes, plus explicit
usage-limit phrases in provider error messages. Its authentication mapping
requires an authentication code or status 401. Other adapter errors retain
their existing classification when they do not carry a distinguishable
provider signal. Cursor ACP preserves its structured JSON-RPC error code and
message, but does not assign `usage_limit` or `authentication` from generic
terminal errors. Provider reset and retry-after values are normalized to the
absolute `retryAfter` timestamp when present.

`cancellationReason` is independent of public state and can be `operator`,
`deadline`, `daemon_shutdown`, or `provider_interruption`. `operator` is
recorded for job.cancel, including a queued job; the other values are recorded
on their corresponding timeout, daemon-shutdown, and provider-interruption
terminal paths. An operator cancel still wins the existing canceled terminal
transition; deadlines and provider interruptions retain their existing failed
classifications and states. Internal steering interrupts are not cancellation
reasons.

## Inline schema validation

A task has zero or one inline JSON Schema. Agentbus evaluates the final result
against it. If the first result is noncompliant, Agentbus may make one
read-only correction attempt with its own fixed prompt. A successful correction
becomes the result. A failed correction preserves the original completed result
and records a noncompliant contract verdict.
