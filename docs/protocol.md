# Agentbus wire protocol

Agentbus protocol version 3 is a JSON-RPC 2.0 service over a local,
newline-delimited Unix socket. Each frame is one JSON object followed by a
newline. The daemon accepts only protocol version 3.

The wire surface has six methods:

| Method | Purpose |
| --- | --- |
| protocol.hello | Authenticate the connection and confirm the protocol version. |
| job.submit | Create or replay an identified job. |
| job.get | Read one identified job. |
| job.list | List compact job summaries with optional workspace, tag, and state filters. |
| job.transcript | Return a stateless digest and selected captured items for one job. |
| job.cancel | Request cancellation of one job. |

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

## job.submit

Submission has a compound identity. A matching replay returns the existing job
without launching it again. Reusing the same identity with a different task
specification is rejected.

~~~json
{
  "workspaceKey": "string",
  "requestId": "string",
  "taskSpec": {
    "backend": "string",
    "cwd": "/absolute/path",
    "write": true,
    "prompt": "string",
    "resumeJobId": "job_previous",
    "model": "string",
    "effort": "string",
    "outputSchema": {},
    "tags": {"key": "value"},
    "timeoutMs": 1800000
  }
}
~~~

backend, cwd, write, and prompt are required task fields. resumeJobId, model,
effort, outputSchema, tags, and timeoutMs are optional. outputSchema is one
inline Draft 2020-12 JSON Schema value: an object or a boolean. timeoutMs is a
non-negative integer no greater than four hours; zero means no deadline.

workspaceKey is an opaque, submitter-chosen namespace. Agentbus cannot derive
another tool's workspaceKey, even when that tool submits work for the same
directory.

resumeJobId names a prior job, never a backend thread ID. For a new submission,
the target must be a terminal non-completed job for the same backend and must
have recorded a backend session ID at turn retirement. A target without that ID
returns `invalid_task_spec` with its jobId; Agentbus never falls back to a fresh
thread. Completed jobs are not resumable because completion is final. Normal successful
Codex cleanup also removes the private CODEX_HOME, but a home retained after
uncertain cleanup is a recovery artifact rather than permission to reopen
completed work.

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
  "states": ["queued", "running"]
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

Each summary always contains jobId, backend, state, cleanup, createdAt, and
updatedAt. tags, modelReported, failureClass, and contract appear when
applicable. tags is the submitted tag map, allowing a caller to see why a tag
filter matched.

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
  "state": "completed",
  "cleanup": "clean",
  "createdAt": "2026-08-20T12:00:00Z",
  "startedAt": "2026-08-20T12:00:01Z",
  "finishedAt": "2026-08-20T12:00:05Z",
  "timeout": {
    "effective": 1800000,
    "source": "daemon_default"
  },
  "tags": {"key": "value"},
  "result": {
    "text": "optional inline result",
    "resultPath": "/state-root/artifacts/results/job_x.txt",
    "sha256": "64 lowercase hexadecimal characters",
    "bytes": 123
  },
  "contract": {
    "schemaSha256": "schema digest",
    "evaluated": true,
    "compliant": true,
    "attempts": 1,
    "violations": []
  },
  "failure": {
    "class": "backend_error",
    "reason": "string"
  },
  "logPaths": {
    "stdout": "string",
    "stdoutTruncated": false,
    "stderr": "string",
    "stderrTruncated": false
  }
}
~~~

jobId, workspaceKey, requestId, backend, state, cleanup, createdAt, and timeout
are always present. startedAt, finishedAt, tags, result, contract, failure,
and logPaths appear only when they apply.

result exists only for a completed job with an authoritative result. Its
resultPath, sha256, and bytes are required; text may be omitted when the result
is kept only in the artifact. The sha256 field is a bare lowercase 64-character
hexadecimal digest with no algorithm prefix.

failure exists only for a failed job. contract exists only when an outputSchema
was submitted. attempts is one after initial evaluation and two only when the
single permitted correction was attempted.

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
daemon.

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
| model_unavailable | The requested model was unavailable. |
| content_policy | The provider refused the request on content-policy grounds. |
| authentication | Authentication was rejected. This class is reserved; no current producer assigns it. |
| backend_error | A launched backend returned another error. |
| timeout | The job deadline expired. |
| interrupted | The backend stopped without a successful terminal result. |
| internal | Agentbus could not make a more specific classification. |

## Inline schema validation

A task has zero or one inline JSON Schema. Agentbus evaluates the final result
against it. If the first result is noncompliant, Agentbus may make one
read-only correction attempt with its own fixed prompt. A successful correction
becomes the result. A failed correction preserves the original completed result
and records a noncompliant contract verdict.
