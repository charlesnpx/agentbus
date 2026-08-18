# agentbus wire protocol v2

This document is the public wire contract for agentbus protocol major version
2. ADR-12 is normative for the strict-only contract, and this document is
written to match the served implementation.

agentbus is a local, same-user execution daemon. Protocol v2 exposes identified
background jobs plus policy validation. It does not expose foreground sessions,
turn streams, orchestration, delegation, review, or rescue workflows.

## Transport and framing

The daemon listens on a Unix domain socket named `agentbus.sock` under the
agentbus state root. The CLI uses `AGENTBUS_STATE_ROOT` as the state-root
override when it is set. Go clients can pass `client.Options.StateRoot` to use a
specific state root, or `client.Options.SocketPath` to connect to a specific
socket path. When `SocketPath` is absent, the socket path is
`<state root>/agentbus.sock`.

When no state-root override is provided, the default state root resolution is:

| Environment | State root |
| --- | --- |
| `XDG_STATE_HOME` set | `$XDG_STATE_HOME/agentbus` |
| `XDG_STATE_HOME` unset | `~/.local/state/agentbus` |

The daemon also maintains a token file named `token` in the same state root.
State directories are created with mode `0700`; the socket and token file are
created or adjusted to mode `0600` where supported.

The wire protocol is JSON-RPC 2.0 over newline-delimited JSON:

- Each frame is one UTF-8 JSON object followed by `\n`.
- Request and response IDs are JSON-RPC `id` values.
- Every supported protocol method requires an `id`.
- JSON-RPC numeric `error.code` remains numeric. Stable protocol identifiers
  are carried in `error.data.code`.

Protocol v2 does not define server notifications.

## Trust model

agentbus trusts code running as the same OS user. Socket permissions and the
hello token prevent accidental cross-user access; they are not a security
boundary against same-user code that can read the token or state files.

State files can contain prompts, tool output, backend logs, and model output.
Clients should treat all paths returned by agentbus as private same-user state.

## Method surface

The protocol v2 socket methods are:

| Method | Purpose |
| --- | --- |
| `protocol.hello` | Authenticate the connection and negotiate protocol v2. |
| `job.submit` | Submit an identified strict job. |
| `job.status` | Read one job or list jobs with `{"all":true}`. |
| `job.result` | Read one job result from authority state. |
| `job.cancel` | Request authority cancellation for one job. |
| `policy.validate` | Validate text against a contract without execution. |
| `policy.register` | Register an immutable daemon-local named contract. |

Job listing is `job.status` with `{"all":true}`. There is no separate list
method.

Removed or unknown requests with a JSON-RPC `id` return
`error.code = -32601` and `error.data.code = "method_not_found"` after a
successful hello. This includes the v1 session and turn surface:

```text
session.start
session.resume
session.list
turn.start
turn.interrupt
turn.event
turn.result
```

Other `session.*` and `turn.*` request names are also outside v2 and receive
the same `method_not_found` error. This remains true while the daemon is
fail-stopped: unknown methods are rejected by dispatch before fail-stop
admission checks.

Unidentified `job.submit` is also removed. Because it uses the still-supported
`job.submit` method name, it is rejected as an admission error rather than as
`method_not_found`: a serving strict daemon returns `missing_identity` unless a
higher-priority root or shutdown condition applies first.

## Hello

`protocol.hello` must be the first request on a connection. Any other method
before hello returns:

```json
{"jsonrpc":"2.0","id":"1","error":{"code":-32000,"message":"protocol.hello is required before other methods","data":{"code":"unauthorized"}}}
```

Hello params:

| Field | Required | Type | Meaning |
| --- | --- | --- | --- |
| `clientProtocolVersion` | yes | integer | Must equal `2`. |
| `token` | yes | string | Must match the state-root `token` file. |

If the token is absent or wrong, the server returns `unauthorized`. If the
version is not `2`, the server returns `protocol_version_mismatch` and includes
`serverProtocolVersion: 2` in `error.data`.

A successful hello result has this shape:

| Field | Type | Meaning |
| --- | --- | --- |
| `protocolVersion` | integer | Always `2`. |
| `backends` | string array | Backend names known to the daemon, sorted by name. |
| `backendMetadata` | array | Optional backend model and effort metadata. |
| `capabilities` | object | Boolean feature flags advertised by the daemon. |

The Go client sends `clientProtocolVersion = protocol.Version`. If a server
returns a successful hello result whose `protocolVersion` is not `2`, the client
returns `client.ErrProtocolVersionMismatch`; the error matches that sentinel
with `errors.Is` and includes the expected and received versions.

Calling `protocol.hello` a second time on the same connection returns
`invalid_task_spec`.

## Error envelope

All protocol errors use the JSON-RPC error object:

```json
{
  "code": -32000,
  "message": "human-readable detail",
  "data": {
    "code": "stable_protocol_code"
  }
}
```

`method_not_found` is the only current stable code that maps to JSON-RPC
numeric `-32601`. Other stable codes map to `-32000`.

`error.data` fields:

| Field | Meaning |
| --- | --- |
| `code` | Stable protocol error code. Always present. |
| `jobId` | Job ID related to the error, when known. |
| `sessionId` | Backend/admission session ID related to the error, when known. |
| `backend` | Backend name related to the error, when known. |
| `admissionCause` | ADR-12 strict rejection cause. |
| `runtimeSupport` | Native runtime diagnostic for strict runtime failures. |
| `serverProtocolVersion` | Server protocol version on hello mismatch. |

`runtimeSupport`, when present, has:

```json
{"class":"unsupported","cause":"...","attempts":1,"cleanupSafe":true}
```

Stable `error.data.code` values in v2 include:

| Code | Meaning |
| --- | --- |
| `unauthorized` | Missing or invalid hello token, or method sent before hello. |
| `name_conflict` | A policy name was registered with a different spec. |
| `protocol_version_mismatch` | Client and server protocol major versions differ. |
| `method_not_found` | Unknown or removed method. |
| `capability_missing` | Required daemon, backend, or admission capability is unavailable. |
| `backend_unavailable` | Backend or authority root is unavailable. |
| `timeout` | A job timed out. |
| `interrupted` | A job was interrupted. |
| `quarantined` | Corruption prevents a normal result. |
| `result_too_large` | Inline result text exceeds the inline cap. |
| `invalid_task_spec` | Submitted params or task spec are malformed or incompatible. |
| `unknown_job` | The authority has no job for the requested `jobId`. |

## Strict rejection causes

Admission rejections carry the stable cause in `error.data.admissionCause`.
The protocol error code remains in `error.data.code`; clients should classify
admission errors by `admissionCause` first, then by `code`.

ADR-12 defines this priority when multiple causes apply:

```text
root_corrupt > root_identity_mismatch > root_fail_stopped > root_sealed >
admission_closing > unavailable_native_runtime > missing_identity >
request_fingerprint_unsupported > replay_conflict > request_expired >
unsupported_backend > unfenceable_backend > invalid_strict_config
```

The served mapper checks repository corruption before fail-stop, so an error
that still carries both corruption and fail-stop identity is reported as
`root_corrupt`. After the safety latch is tripped, later admission attempts
normally see `root_fail_stopped`.

| Cause | `error.data.code` | Meaning |
| --- | --- | --- |
| `missing_identity` | `invalid_task_spec` | `job.submit` lacks a valid top-level `workspaceKey` and `requestId`. |
| `replay_conflict` | `invalid_task_spec` | A live binding or tombstone exists for the key but the raw task identity differs, or the live binding is for a different admission mode. |
| `request_expired` | `invalid_task_spec` | The key matches a tombstone whose raw task identity matches the request. |
| `request_fingerprint_unsupported` | `invalid_task_spec` | The recorded binding or tombstone uses a fingerprint algorithm or version the daemon cannot compare. |
| `unsupported_backend` | `backend_unavailable` | The requested backend name is not available in strict admission. |
| `unfenceable_backend` | `capability_missing` | The backend exists but cannot satisfy strict fencing or containment. |
| `invalid_strict_config` | `invalid_task_spec` | The strict task configuration is malformed or incompatible. |
| `unavailable_native_runtime` | `capability_missing` | The host cannot provide basic controlled process supervision: process groups, identity/start-token observation, TERM/KILL/wait, and a controlled runner. Lack of Linux cgroup v2 alone is not this cause. |
| `root_corrupt` | `backend_unavailable` | The authority root has detected repository, anchor, or integrity corruption. |
| `root_identity_mismatch` | `backend_unavailable` | Repository and anchor identities disagree. In current served startup this is a pre-socket failure surfaced through the launcher, not a normal JSON-RPC response. |
| `root_fail_stopped` | `backend_unavailable` | The authority root or served safety latch is fail-stopped. |
| `root_sealed` | `capability_missing` | The authority root is sealed and cannot serve admission. Current production startup normally reports this before socket readiness. |
| `admission_closing` | `capability_missing` | The daemon is gracefully shutting down and no longer accepts admission. |

These 13 cause strings are stable. Under contract version 2,
`unavailable_native_runtime` is intentionally narrow: it means no basic
supervision substrate is available, not that Linux cgroup v2 is absent or
undelegated.

## `job.submit`

Protocol v2 accepts only identified strict submissions.

Params:

```json
{
  "workspaceKey": "workspace-opaque-token",
  "requestId": "request-opaque-token",
  "taskSpec": {
    "backend": "codex",
    "cwd": "/absolute/workspace/path",
    "write": false,
    "model": "gpt-5",
    "effort": "medium",
    "prompt": "Do the work.",
    "policy": {
      "prologue": "Optional text prepended to the backend prompt.",
      "contract": {
        "jsonSchema": {
          "type": "object",
          "required": ["status"],
          "properties": {
            "status": {"const": "ok"}
          }
        }
      },
      "retry": {
        "max": 1,
        "template": "Your response missed: {{missing}}."
      }
    },
    "tags": {
      "suite": "architecture"
    },
    "timeoutMs": 1800000
  }
}
```

Top-level fields:

| Field | Required | Type | Rules |
| --- | --- | --- | --- |
| `workspaceKey` | yes | string | Opaque client/delegate token. Non-empty, at most 256 bytes, valid UTF-8, no whitespace or control characters. |
| `requestId` | yes | string | Opaque request token with the same token rules. Stable across retries of the same logical request. |
| `taskSpec` | yes | object | Raw task object used for fingerprinting and later typed validation. |

`taskSpec` fields:

| Field | Required | Type | Rules |
| --- | --- | --- | --- |
| `backend` | yes | string | Must be non-empty and available to strict admission. |
| `cwd` | yes | string | Must be non-empty, absolute, and resolvable by `filepath.EvalSymlinks` for a new admission. |
| `write` | yes | boolean | Effective write permission for the backend execution. `false` is valid. |
| `prompt` | yes | string | Must be non-empty. |
| `model` | no | string | Passed to the backend session when present. |
| `effort` | no | string | Passed to the backend session when present. |
| `policy` | no | object | Optional structural policy for the final backend text. |
| `tags` | no | object | Optional string-to-string metadata. |
| `timeoutMs` | no | integer | Omitted means 30 minutes. `0` disables the timeout. Negative values are invalid. Values above 4 hours are invalid. |

Unknown fields in `job.submit` params or `taskSpec` are rejected during typed
decode after replay lookup.

`policy.contract` must contain exactly one of:

| Contract field | Meaning |
| --- | --- |
| `jsonSchema` | JSON Schema used to validate final text as JSON. |
| `shape` | Opaque client-owned contract identity. Agentbus records and stamps the digest but does not validate markdown shape. |
| `named` | Name registered with `policy.register`. |

If `policy.retry.max` is `1`, `policy.retry.template` must include
`{{missing}}`. Other non-zero retry counts are invalid.

Result:

| Field | Type | Meaning |
| --- | --- | --- |
| `jobId` | string | Authority job ID. |
| `state` | string | Current public state. New non-replay submissions return `queued`. |
| `deduplicated` | boolean | Present and `true` only when replay returns an existing live job. Omitted otherwise. |
| `timeout` | object | Resolved deadline with `requested` (present only when the client sent `timeoutMs`), `effective` (milliseconds), and `source` (`client` or `daemon_default`). The deadline is measured separately from entry to each `runAttempt`, before launch work; time spent admitted or queued does not count. An effective value of `0` means no timeout. Also returned by `job.status` and `job.result`. |

Clients MUST treat `jobId` as opaque. Current strict authority allocation emits
IDs in the form `job-%020d`, but clients must not parse sequence numbers or
depend on that spelling.

### Request keys and workspace keys

The replay key is the pair `(workspaceKey, requestId)`. The daemon treats
`workspaceKey` as an opaque request namespace token. It does not recompute it
from `taskSpec.cwd` and does not require it to equal the storage workspace
layout key.

For new admissions only, the daemon separately derives the storage layout key
from `taskSpec.cwd` as:

```text
sha256(filepath.EvalSymlinks(filepath.Abs(taskSpec.cwd)))
```

Clients that derive `workspaceKey` from a workspace identity must compute and
persist it before the first send, then reuse the persisted value on every retry.
Do not derive a different `workspaceKey` from the current filesystem state when
retrying.

`requestId` is stable for one logical request. Reusing the same
`(workspaceKey, requestId)` with a different raw `taskSpec` is a protocol
conflict. New logical work must use a new `requestId`.

### Task fingerprints

Bindings and tombstones record a task fingerprint with algorithm `sha256` and
version `1`.

Fingerprint v1 is derived from the raw `taskSpec`, not from typed defaults:

1. The raw `taskSpec` must be one JSON object.
2. JSON must be valid UTF-8 and contain one top-level value.
3. Duplicate object keys are rejected by the fingerprint parser.
4. Whitespace outside strings is removed by canonical rendering.
5. Object keys and string values are decoded by the JSON parser, then rendered
   with Go `encoding/json` escaping.
6. Number lexemes are preserved verbatim; for example, `1`, `1.0`, and `1e0`
   fingerprint differently.
7. Object keys are sorted for canonical rendering.
8. Array order is preserved.
9. Omitted fields, explicit `null`, empty objects, and numeric zero remain
   distinct inputs.
10. The hash input is the domain prefix `agentbus/task-spec/sha256/v1\0`
   followed by the canonical JSON bytes.

Replay comparison uses the recorded fingerprint algorithm and version. A
recorded algorithm or version that cannot be compared returns
`request_fingerprint_unsupported`; the daemon does not rehash the old request
with the current algorithm.

## Replay semantics

For a serving strict route, `job.submit` processing is ordered as follows:

1. Decode enough top-level JSON to validate `workspaceKey` and `requestId` and
   extract raw `taskSpec`.
2. Run `LookupReplay(workspaceKey, requestId)`.
3. For a live binding, compare the current raw `taskSpec` with the recorded
   fingerprint.
4. For a tombstone, compare the current raw `taskSpec` with the recorded
   fingerprint.
5. Only when no live binding or tombstone exists, perform typed task validation,
   backend validation, workspace resolution, current fingerprinting, durable
   acceptance, and launch.

Replay lookup and recorded-version comparison happen before backend validation,
filesystem validation, workspace resolution, current-version fingerprinting, or
typed `taskSpec` decode. That means replay is independent of current filesystem
state, current symlink targets, and current backend composition.

| Existing authority record | Raw `taskSpec` comparison | Result |
| --- | --- | --- |
| none | not applicable | Validate and admit as a new job. |
| live binding, identified fenced | matches recorded fingerprint | Return the same `jobId`, current public state, and `deduplicated:true`. |
| live binding, different mode | not applicable | `replay_conflict`. |
| live binding | does not match | `replay_conflict`. |
| live binding | recorded fingerprint unsupported | `request_fingerprint_unsupported`. |
| tombstone | matches recorded fingerprint | `request_expired`. |
| tombstone | does not match | `replay_conflict`. |
| tombstone | recorded fingerprint unsupported | `request_fingerprint_unsupported`. |

Malformed typed fields inside `taskSpec` do not preempt replay for an existing
key. For example, if the original raw task differs from a retry whose
`taskSpec.backend` is a number, the response is `replay_conflict`, not a typed
decode error.

If the initial `job.submit` response is lost after durable acceptance, the
identified fenced obligation is retained and runs at most once. Replaying the
same `(workspaceKey, requestId, taskSpec)` returns the same job with
`deduplicated:true`.

## `job.status`

Params:

```json
{"jobId":"job_..."}
```

or:

```json
{"all":true}
```

If both `jobId` and `all` are omitted, the server treats the request as
`{"all":true}`. If `jobId` is present, the request is an exact authority lookup
and `all` is ignored.

Result:

```json
{"jobs":[{"jobId":"job_...","sessionId":"ses_...","state":"completed","cleanupDisposition":"verified_absent","timeout":{"effective":1800000,"source":"daemon_default"}}]}
```

The served v2 path is authority-only. It does not fall back to legacy JSON job
records. Listing reads authority jobs and sorts returned statuses by `jobId`.
While authority lookup is healthy, unknown or malformed job IDs that do not
resolve to an authority job return `unknown_job` with `jobId` in `error.data`.
During fail-stop, a syntactically valid absent ID can return
`root_fail_stopped` before absence can be established.

`cleanupDisposition` is an additive machine-readable field derived from the
authority terminal record plus per-launch quiescence. Values are
`no_execution_possible`, `verified_absent`, and `unresolved`. It is present for
authority statuses when the disposition can be derived; it is not physical proof
serialization.

`timeout` uses the same resolved-deadline shape as `job.submit`: `requested`
is present only when the client supplied `timeoutMs`, `effective` is in
milliseconds, and `source` is `client` or `daemon_default`. The deadline starts
on entry to each `runAttempt`, before launch work; admitted and queued time is
excluded. A retry receives a new deadline of the same effective duration.

## `job.result`

Params:

```json
{"jobId":"job_..."}
```

Result:

```json
{
  "jobId": "job_...",
  "sessionId": "ses_...",
  "state": "completed",
  "cleanupDisposition": "verified_absent",
  "timeout": {"effective": 1800000, "source": "daemon_default"},
  "result": {
    "text": "final text",
    "resultPath": "/home/me/.local/state/agentbus/workspaces/.../results/job_....txt",
    "sha256": "64 lowercase hex characters",
    "bytes": 10
  }
}
```

`job.result` is authority-only. The public state, result metadata, and
`cleanupDisposition` are derived from the authority terminal record. Physical
terminal proof serialization is not exposed in protocol v2.

`timeout` has the same semantics and shape as the `job.status` field.

For completed terminal outcomes, the authority terminal record contains a
certified final-result reference. The server returns `resultPath`, `sha256`,
and `bytes`. It includes inline `text` only when the result is below the inline
cap and a bounded read verifies both byte count and digest. If verification
fails, the path and digest metadata remain, and `text` is omitted.

Timed-out and interrupted terminal outcomes can also contain a recovered
transcript excerpt when no worker report was produced. Those results include
`"partial": true` and `"partialReason": "timeout"` or `"interrupted"`; they
are not worker final reports, and their artifact text begins with that warning.
The excerpt is a bounded tail of assistant-visible transcript messages.

Other non-completion terminal states and non-terminal states return no
`result`.
While authority lookup is healthy, unknown jobs return `unknown_job` with
`jobId` in `error.data`. During fail-stop, a syntactically valid absent ID can
return `root_fail_stopped` before absence can be established.

## `job.cancel`

Params:

```json
{"jobId":"job_..."}
```

Result:

```json
{"jobId":"job_...","state":"canceled"}
```

`job.cancel` is authority-only. If the job is already terminal, cancel returns
the existing authority terminal state and does not mutate the authority record.
If the job is active, served requests command interruption and records authority
cancellation through the coordinator. If the authority has no job for the ID,
the server returns `unknown_job`.

`job.cancel` is not allowed while the served safety latch is fail-stopped; in
that state it returns `root_fail_stopped`.

## Public states and CLI exit codes

Public job states:

```text
queued
starting
running
retrying
completed
completed_noncompliant
interrupted
quarantined
failed
timed_out
canceled
reaped
orphaned
```

Terminal states are:

```text
completed
completed_noncompliant
failed
timed_out
interrupted
canceled
reaped
quarantined
orphaned
```

The CLI maps single-job `status`, `result`, and `cancel` outcomes as follows.
In-band JSON-RPC errors from a running daemon are included where noted:

| Condition | Exit code |
| --- | ---: |
| `completed` | 0 |
| any non-terminal state, including `queued`, `starting`, `running`, and `retrying` | 2 |
| `completed_noncompliant` | 3 |
| `failed` | 4 |
| `timed_out` | 5 |
| `interrupted` | 6 |
| `canceled` | 7 |
| `reaped` | 8 |
| `quarantined` | 9 |
| `unknown_job` | 10 |
| in-band `unavailable_native_runtime` admission cause | 11 |
| in-band authority root condition: `root_fail_stopped`, `root_corrupt`, or `root_identity_mismatch` | 12 |
| `orphaned` | 14 |

Launcher, startup, and local shutdown failures use a separate classification:

| Condition | Exit code |
| --- | ---: |
| daemon startup failure, including launcher-side root corruption, launcher-side `root_identity_mismatch`, and `unavailable_native_runtime` | 11 |
| launcher-side authority fail-stop, including `root_fail_stopped` and served safety fail-stop | 12 |
| graceful shutdown deadline exceeded | 13 |

## Policy methods

`policy.validate` params:

```json
{
  "text": "{\"status\":\"ok\"}",
  "contract": {
    "jsonSchema": {
      "type": "object",
      "required": ["status"],
      "properties": {
        "status": {"const": "ok"}
      }
    }
  }
}
```

Result:

```json
{"valid":true,"missing":null,"contractSha256":"sha256:<elided>"}
```

`contractSha256` is the concrete `sha256:<64 lowercase hex>` contract digest;
examples elide the digest value. On the current valid path, the missing list is
nil and serializes as `null`.

Invalid result example:

```json
{
  "text": "{\"status\":\"bad\"}",
  "contract": {
    "jsonSchema": {
      "type": "object",
      "required": ["status"],
      "properties": {
        "status": {"const": "ok"}
      }
    }
  }
}
```

```json
{"valid":false,"missing":["/status: value must be 'ok'"],"contractSha256":"sha256:<elided>"}
```

`policy.register` params:

```json
{
  "name": "delegate/delegate-report@1",
  "spec": {
    "shape": {
      "delegateContract": "report-v1"
    }
  }
}
```

Result:

```json
{"name":"delegate/delegate-report@1","contractSha256":"sha256:...","registered":true}
```

Registered policy names are daemon-local and immutable. Re-registering the same
name with an identical spec is idempotent. Re-registering the same name with a
different spec returns `name_conflict`.

## Admission admin CLI

Admission admin commands are not JSON-RPC socket methods. They are local CLI
operations under:

```text
agentbus admission <inspect|recover|reset-empty-root|seal|clear-fail-stop>
```

Daemonless admin is intentionally outside the socket protocol.

Admin commands:

| Command | Mutates | Purpose |
| --- | --- | --- |
| `inspect --state-root <path> [--json]` | no | Reads activation metadata, contract version, generation, root counts, domain UUID, sealed state, successor fields, anchor phase, and fail-stop fields. |
| `recover --state-root <path> [--json]` | yes | Runs recovery-only strict admission without opening the protocol listener; reconciles durable nonterminal obligations. |
| `reset-empty-root --state-root <path> [--json]` | yes | Reinitializes only an empty root. It refuses non-empty jobs, bindings, tombstones, launch records, or recovery obligations. |
| `seal --state-root <path> --new-state-root <path> --start-new-authority-domain --acknowledge-replay-history-reset [--json]` | yes | Permanently seals the old root and initializes or verifies a successor authority domain. |
| `clear-fail-stop --state-root <path> --acknowledge-unsafe-diagnosis [--json]` | yes | Clears a persisted fail-stop only after explicit operator acknowledgement. |

`inspect` returns `RootInspection`. `seal` returns `SealReport`.
`clear-fail-stop` returns `ClearFailStopReport`. `recover` returns
`AdmissionRecoveryReport`, including `orphanedJobs`, `unresolvedLaunches`, and
`cleanupWarnings`.

## Startup, autostart, and shutdown

Production `agentbus serve` starts strict identified admission. macOS and Linux
serve under the same custody contract; Linux cgroup v2 is an optional cleanup
enhancement. Hosts without basic process supervision fail closed at startup.

Autostart is a client and launcher behavior, not a JSON-RPC method. The Go
client connects to the configured socket, sends hello, and only autostarts when
the connection error is autostartable. Protocol mismatch and bad-token hello
failures do not trigger autostart. During autostart, the client serializes
state-root startup attempts with an autostart lock, rechecks the daemon after
acquiring the lock, then launches `agentbus serve --foreground` if needed.

The launcher uses a private readiness pipe. Its `protocolVersion` is the
launcher-readiness protocol version, currently `1`; it is not the JSON-RPC wire
protocol version. A child reports either:

```json
{"ready":{"protocolVersion":1,"pid":1234,"canonicalStateRoot":"/...","socketPath":"/.../agentbus.sock"}}
```

or:

```json
{"failed":{"code":"strict admission support unavailable","message":"..."}}
```

Readiness failures surface to CLI/client code as `daemonlaunch.StartupError`
with a typed `Kind`, `Code`, `Message`, optional stderr tail, and optional
canonical state-root mismatch fields. These are not JSON-RPC error responses.
If a launched daemon reports `agentbus daemon already listening`, the launcher
verifies the existing socket with hello and converges to that daemon instead of
starting another one.

Graceful shutdown first marks admission closing. New `job.submit` requests on a
connection that reaches the handler during that phase receive:

```json
{"jsonrpc":"2.0","id":"submit","error":{"code":-32000,"message":"admission authority is shutting down","data":{"code":"capability_missing","admissionCause":"admission_closing"}}}
```

The server then closes the listener, removes its owned socket path, cancels
authority-owned jobs, waits for active work and result publication to drain,
shuts down the coordinator, closes runtime and repository resources, removes
its owned PID file, and stops the serve context. Connections already accepted
may drain during this sequence; fail-stop closes accepted connections through
the served safety latch.

If graceful shutdown exceeds its deadline, the CLI reports exit code `13`.
Recovery on the next startup is driven by the durable authority records.
Terminal `orphaned` and terminal unresolved-cleanup jobs are durable history,
not recovery obligations; startup does not relaunch them. Genuine authority
integrity, ownership ambiguity, corrupt root, and contract-version mismatch
cases remain fail-closed before readiness.

## Examples

### Hello

Request:

```json
{"jsonrpc":"2.0","id":"hello","method":"protocol.hello","params":{"clientProtocolVersion":2,"token":"0123456789abcdef"}}
```

Response:

```json
{"jsonrpc":"2.0","id":"hello","result":{"protocolVersion":2,"backends":["codex"],"backendMetadata":[{"backend":"codex","models":["gpt-5"],"efforts":["low","medium","high"]}],"capabilities":{"policy.shape":false,"policy.jsonSchema":true,"policy.named":true,"policy.retry":true,"nativeStructuredOutput.codex":false,"nativeStructuredOutput.claude":false,"models.discovery":true,"models.reported":true,"admission.strictContainment":true}}}
```

### Submit accept

Request:

```json
{"jsonrpc":"2.0","id":"submit-1","method":"job.submit","params":{"workspaceKey":"workspace-a","requestId":"request-1","taskSpec":{"backend":"codex","cwd":"/home/me/project","write":false,"model":"gpt-5","effort":"medium","prompt":"Run the requested task.","tags":{"suite":"protocol"},"timeoutMs":1800000}}}
```

Response:

```json
{"jsonrpc":"2.0","id":"submit-1","result":{"jobId":"job-00000000000000000001","state":"queued","timeout":{"requested":1800000,"effective":1800000,"source":"client"}}}
```

### Submit deduplicated replay

Request:

```json
{"jsonrpc":"2.0","id":"submit-replay","method":"job.submit","params":{"workspaceKey":"workspace-a","requestId":"request-1","taskSpec":{"backend":"codex","cwd":"/home/me/project","write":false,"model":"gpt-5","effort":"medium","prompt":"Run the requested task.","tags":{"suite":"protocol"},"timeoutMs":1800000}}}
```

Response:

```json
{"jsonrpc":"2.0","id":"submit-replay","result":{"jobId":"job-00000000000000000001","state":"completed","deduplicated":true,"timeout":{"requested":1800000,"effective":1800000,"source":"client"}}}
```

### Replay conflict

```json
{"jsonrpc":"2.0","id":"submit-conflict","error":{"code":-32000,"message":"task_identity.value: does not match raw taskSpec","data":{"code":"invalid_task_spec","admissionCause":"replay_conflict"}}}
```

### Unknown job

```json
{"jsonrpc":"2.0","id":"status-missing","error":{"code":-32000,"message":"job is not known","data":{"code":"unknown_job","jobId":"job_missing"}}}
```

### Root fail-stopped

```json
{"jsonrpc":"2.0","id":"submit-failstop","error":{"code":-32000,"message":"served safety fail-stop: authority fail-stopped: persisted unsafe stop","data":{"code":"backend_unavailable","admissionCause":"root_fail_stopped"}}}
```

### Admission closing

```json
{"jsonrpc":"2.0","id":"submit-closing","error":{"code":-32000,"message":"admission authority is shutting down","data":{"code":"capability_missing","admissionCause":"admission_closing"}}}
```
