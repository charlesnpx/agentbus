# Agentbus wire protocol

Agentbus protocol version 3 is a JSON-RPC 2.0 service over a local,
newline-delimited Unix socket. Each frame is one JSON object followed by a
newline. The daemon accepts only protocol version 3.

The wire surface has four methods:

| Method | Purpose |
| --- | --- |
| protocol.hello | Authenticate the connection and confirm the protocol version. |
| job.submit | Create or replay an identified job. |
| job.get | Read one job or list compact job summaries. |
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
    "model": "string",
    "effort": "string",
    "outputSchema": {},
    "tags": {"key": "value"},
    "timeoutMs": 1800000
  }
}
~~~

backend, cwd, write, and prompt are required task fields. model, effort,
outputSchema, tags, and timeoutMs are optional. outputSchema is one inline
Draft 2020-12 JSON Schema value: an object or a boolean. timeoutMs is a
non-negative integer no greater than four hours; zero means no deadline.

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

## job.get

An empty parameter object lists jobs:

~~~json
{}
~~~

~~~json
{
  "jobs": [
    {
      "jobId": "string",
      "backend": "string",
      "state": "running",
      "cleanup": "clean",
      "createdAt": "2026-08-20T12:00:00Z",
      "updatedAt": "2026-08-20T12:00:01Z",
      "failureClass": "backend_error",
      "contract": {
        "evaluated": true,
        "compliant": false
      }
    }
  ]
}
~~~

The list contains summaries only. It does not expose request identity, task
details, prompt, current working directory, result text, failure reason, log
paths, or process claims.

Passing a jobId returns the full job record:

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
    "stderr": "string"
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
