# ADR-14: Simplified core

ADR-13 expressly leaves worker-launch removal for a separate deletion-oriented
review as a non-goal. This ADR is that review. It completes the de-escalation
that ADR-13 began.

## Decision

Agentbus becomes a small generic job service. It retains durable identified
submission, one job lifecycle, inline JSON Schema validation, and process-group
supervision. It deletes the durable admission-control, process-proof,
held-worker, and parallel-projection machinery. This ADR is NORMATIVE.

ADR-14 supersedes the ADRs identified in the ADR index. Their historical files
are retained under docs/history; their location does not preserve normative
force.

### Cross-repository boundary

| Component | Owns | Does not own |
| --- | --- | --- |
| Witness | Review workflows. | Generic job lifecycle or provider sandbox implementation. |
| Delegate | Task preparation, compound identity, and one typed job.submit call. | Review workflow orchestration or duplicate sandbox/cache code. |
| Agentbus | Generic job lifecycle after typed submission. | Witness review policy or Delegate task preparation. |
| Convo Relay | Its embedded adapters, unchanged. | A replacement adapter tree. |

Agentbus MUST retain provider sandbox policy, per-job write caches, and private
CODEX_HOME precisely because Delegate is deleting its copies. Agentbus MUST
retain the engine/adapter tree and engine/command because Convo Relay depends on
them.

### Protocol version 3

Protocol version is 3. Its entire method set is protocol.hello, job.submit,
job.get, and job.cancel. Any other method, including a former method, is absent
and MUST return the JSON-RPC method-not-found response. Each shape below is a
JSON-RPC params or result object, not its envelope. Optional fields are omitted,
never null. Times are RFC 3339 UTC strings.

protocol.hello:

    params
    {
      "clientProtocolVersion": 3,
      "token": "string"
    }

    result
    {
      "protocolVersion": 3,
      "backends": [
        {
          "backend": "string",
          "models": ["string"],
          "efforts": ["string"]
        }
      ]
    }

There is no capabilities map, capability negotiation, or capability-dependent
surface in version 3.

job.submit:

    params
    {
      "workspaceKey": "string",
      "requestId": "string",
      "taskSpec": {
        "backend": "string",
        "cwd": "string",
        "write": true,
        "model": "string",
        "effort": "string",
        "prompt": "string",
        "outputSchema": {},
        "tags": {"string": "string"},
        "timeoutMs": 1800000
      }
    }

backend, cwd, write, and prompt are required TaskSpec fields. model, effort,
outputSchema, tags, and timeoutMs are optional. outputSchema, if present, is
exactly one inline Draft 2020-12 JSON Schema value, either an object or a
boolean. It is not a name, registry key, or caller-supplied correction program.

    result
    {
      "jobId": "string",
      "state": "queued|running|completed|failed|canceled|unknown",
      "deduplicated": "false|true",
      "timeout": {
        "requested": 1800000,
        "effective": 1800000,
        "source": "client|daemon_default"
      }
    }

state is the current public projection. deduplicated and timeout are always
present. timeout always contains effective and source; requested is absent
unless taskSpec.timeoutMs was supplied. A new admission returns the queued
snapshot with deduplicated false; a matching replay returns the existing job's
current state, including a terminal state, with deduplicated true.

job.get with a jobId returns this full JobRecord:

    params
    {"jobId": "string"}

    result
    {
      "jobId": "string",
      "workspaceKey": "string",
      "requestId": "string",
      "backend": "string",
      "state": "queued|running|completed|failed|canceled|unknown",
      "tags": {"string": "string"},
      "createdAt": "RFC3339 UTC timestamp",
      "startedAt": "RFC3339 UTC timestamp",
      "finishedAt": "RFC3339 UTC timestamp",
      "timeout": {
        "requested": 1800000,
        "effective": 1800000,
        "source": "client|daemon_default"
      },
      "result": {
        "text": "string",
        "resultPath": "string",
        "sha256": "lowercase-64-character-hex",
        "bytes": 123
      },
      "contract": {
        "schemaSha256": "schema-digest",
        "evaluated": true,
        "compliant": true,
        "attempts": 1,
        "violations": ["string"]
      },
      "failure": {
        "class": "backend_unavailable|provider_overloaded|model_unavailable|content_policy|authentication|backend_error|timeout|interrupted|internal",
        "reason": "string"
      },
      "cleanup": "clean|uncertain",
      "logPaths": {
        "stdout": "string",
        "stderr": "string"
      }
    }

JobRecord requires jobId, workspaceKey, requestId, backend, state, cleanup,
createdAt, and timeout. timeout always contains effective and source; requested
is absent unless taskSpec.timeoutMs was supplied. tags, startedAt, and
finishedAt are optional. result, contract, failure, and logPaths are
pointer-valued groups and are absent when they do not apply. result exists only
for an authoritative terminal result. In a present result, text is optional;
resultPath, sha256, and bytes are required, retaining inline text or an
authoritative path with its digest and byte count. A present failure has class
and reason and exists only for failed jobs. contract exists only when
outputSchema was submitted. ContractResult is exactly the object named contract;
schemaSha256 exists only when a schema was submitted.

job.get with an empty object returns compact summaries:

    params
    {}

    result
    {
      "jobs": [
        {
          "jobId": "string",
          "backend": "string",
          "state": "queued|running|completed|failed|canceled|unknown",
          "cleanup": "clean|uncertain",
          "createdAt": "RFC3339 UTC timestamp",
          "updatedAt": "RFC3339 UTC timestamp",
          "failureClass": "backend_unavailable|provider_overloaded|model_unavailable|content_policy|authentication|backend_error|timeout|interrupted|internal",
          "contract": {
            "evaluated": true,
            "compliant": true
          }
        }
      ]
    }

Each JobSummary requires jobId, backend, state, cleanup, createdAt, and
updatedAt. failureClass and the compact contract verdict are optional. If
present, contract contains only evaluated and compliant. A summary never
contains workspaceKey, requestId, tags, timeout, result, failure reason,
logPaths, prompt, cwd, or a process claim.

job.cancel:

    params
    {"jobId": "string"}

    result
    {
      "jobId": "string",
      "state": "queued|running|completed|failed|canceled|unknown"
    }

Former extra job, schema, and capability operations are deleted. There is no
status-list flag; an empty job.get object is the only list request.

### CLI version 0.13.0

The complete CLI surface is version, serve, status, result, and cancel. There
is no submit command: Delegate makes the typed job.submit request after task and
identity preparation.

status and result MUST project the same job.get response differently. For a
selected job, their JSON modes write the byte-identical JobRecord, including
inline result text when it is present. The human-readable status projection uses
JobSummary for a list or JobRecord for one job, shows lifecycle, cleanup,
contract verdict, and failure class, and MUST NEVER print the result text.
result fetches the same JobRecord, writes authoritative terminal result text to
standard output, and writes the applicable failure reason to standard error. A
successful result is therefore pipeable. If no authoritative terminal result
exists, result writes no result text and reports why on standard error.

cancel invokes job.cancel. version reports application version 0.13.0 and
protocol version 3. serve owns daemon startup only. setup, validate, every
admission subcommand, and every internal-* subcommand are deleted.

### One state vocabulary

The public state set is exactly queued, running, completed, failed, canceled,
and unknown. starting is a private persisted launch marker. It MUST project as
running and MUST NOT appear in a protocol or CLI response. There is no retry
state, public or private.

This table maps the current twelve labels to version-3 public vocabulary for
review only. It is not a database migration and MUST NOT be implemented as one.

| Current label | Version-3 projection | Required interpretation |
| --- | --- | --- |
| queued | queued | Awaiting launch. |
| starting | running | Private durable no-relaunch marker for the initial turn. |
| running | running | Active work. |
| completed | completed | Terminal completion. |
| completed_noncompliant | completed | ContractResult is evaluated and not compliant. |
| failed | failed | FailureClass supplies the cause. |
| timed_out | failed | FailureClass is timeout. |
| interrupted | failed | FailureClass is interrupted. |
| canceled | canceled | Terminal cancellation. |
| orphaned | unknown | The execution outcome is unknown. |
| reaped | unknown | The former state also lacked a recorded execution outcome. |
| quarantined | failed | FailureClass is internal if such a legacy condition is represented. |

The current engine.JobState, execution/model Outcome, and execution/model
PublicState vocabularies are deleted together. Decision and dispatch
projections, with their proof-driven state machine, are deleted with the
admission-control subsystem. One job record supplies the only version-3
vocabulary.

### Cleanup is independent

Cleanup is an axis orthogonal to state. Its only values are clean and uncertain.
clean means Agentbus has no recorded cleanup uncertainty; it does not claim that
a nonterminal job has no live process. uncertain means cleanup could not be
established under the process-claim rule. A completed job MAY be completed plus
uncertain and MUST retain ResultInfo and any applicable ContractResult. Cleanup
uncertainty MUST NOT rewrite a known result or state to unknown.

### Failure classes

FailureClass is closed:

| FailureClass | Meaning |
| --- | --- |
| backend_unavailable | Agentbus could not use the selected backend. |
| provider_overloaded | The provider reported capacity or overload. |
| model_unavailable | The requested model is unavailable. |
| content_policy | The provider refused the request on content-policy grounds. |
| authentication | Authentication was rejected. This class is declared-but-unpopulated: no producer exists today. |
| backend_error | A launched backend returned an error not otherwise classified. |
| timeout | The job deadline expired. |
| interrupted | The backend stopped without a successful terminal result. |
| internal | Agentbus could not make a more specific classification. |

FailureClass is data on failed, not a second state vocabulary. Existing provider
observations and failure-classification behavior remain in place; this ADR fixes
the durable and public class set and removes timeout and interruption state
names.

### Inline contract validation

A TaskSpec has zero or one inline JSON Schema. Agentbus validates the
authoritative final result against it and makes at most one correction attempt.
The correction prompt belongs to Agentbus, not the caller, and its immutable text
is:

    The preceding final result did not satisfy the required JSON Schema. Return a
    replacement final result that satisfies it. This is the one permitted
    correction attempt. It is read-only: make no further changes. Do not edit
    files, write data, invoke tools, or alter the workspace or external systems.

Agentbus appends the canonical submitted schema and bounded validation violations
after that immutable text. The caller cannot replace, template, or extend it. A
correction therefore cannot continue task work.

When outputSchema is present, ContractResult is recorded in the optional
contract group as:

    ContractResult{
      schemaSha256,
      evaluated,
      compliant,
      attempts,
      violations,
    }

schemaSha256 is the sha256 digest of the canonical inline schema. attempts is
one after initial evaluation, and two only when the one correction attempt was
made. A successful correction becomes the authoritative final result and records
compliant true. A failed correction MUST preserve the original final result,
record compliant false with its violations, and MUST NOT replace completed with
failed merely because correction failed.

Named contracts, PolicyRegistry, shape contracts, client-supplied retry
templates, ContractStatus, and the skipped-reason vocabulary are deleted. There
is no standalone policy method.

### Identity and replay

The public compound identity (workspaceKey, requestId) is retained. Submission
MUST use this order:

1. Validate only workspaceKey and requestId syntax.
2. Canonically serialize and hash the complete TaskSpec as specified below,
   without touching the filesystem.
3. Look up the compound key in requests.
4. If a binding has the same TaskSpec hash, return its job with deduplicated
   true.
5. If a binding has a different TaskSpec hash, return conflict.
6. Only for a new key, validate TaskSpec, selected backend, optional schema,
   and cwd.
7. Persist the new job and its requests binding atomically, then return
   deduplicated false.

Failure in step 6 is a rejection. It creates neither a job record nor a
requests binding.

A same-hash replay does not inspect present backend availability or filesystem
state. It succeeds even if cwd has been deleted or changed. A different hash is
a conflict before backend or cwd validation.

The TaskSpec hash is SHA-256 over the UTF-8 bytes of the RFC 8785 JSON
Canonicalization Scheme (JCS) representation of the submitted TaskSpec. Its
input contains exactly the required fields backend, cwd, write, and prompt, plus
each supplied optional field among model, effort, outputSchema, tags, and
timeoutMs. It contains no derived value, default, workspaceKey, requestId, or
filesystem observation. cwd is the submitted string bytes; it is not resolved,
statted, or path-canonicalized.

An absent optional field is omitted from the canonical object; a present field
remains present even when its legal value is empty, including model: "", tags:
{}, an empty outputSchema object, or timeoutMs: 0. null is not a legal
substitute for absence. JCS sorts object members, preserves array order,
canonicalizes numbers and JSON string escaping, and performs no Unicode
normalization. Before hashing, the raw submitted outputSchema bytes MUST parse
as exactly one JSON value with duplicate object member names rejected, then be
rendered as that value by JCS; its original member order and insignificant
whitespace do not affect the hash.

There are no tombstones, request expiry, fingerprint-version negotiation,
binding index, job-metadata garbage collection, or migrations. The requests
binding lives for the version-3 store lifetime. There is no artifact garbage
collection: result and log artifacts remain until an operator removes them, so
the state root grows until it is cleaned up. Removing an artifact does not
remove jobs, bindings, identity hashes, terminal metadata, or ContractResult.

### Crash safety and no relaunch

Agentbus permits at most one initial process turn and, when required, at most
one correction process turn per job. The retained adapter tree remains
unchanged and continues to supervise one process per turn. The initial-turn
launch ordering is normative:

1. Commit the job as private starting durably.
2. If that commit did not cleanly succeed, DO NOT SPAWN.
3. Fork and exec the backend into a new process group.
4. In a separate transaction, record the process claim
   {pid, pgid, startToken}.

The initial process MUST have retired before Agentbus spawns the correction
turn. The correction turn's process claim, recorded by the same
OnProcessStart -> recordProcessClaim path as the initial turn's claim, is the
durable evidence that the correction turn exists. There is no retry state,
public or private, and no durable retry commit before correction spawn.

The claim transaction MUST be separate from the durable initial-turn state
transaction. It may fail after exec; that failure never licenses a second spawn
for that turn. Neither turn is ever relaunched after its launch path leaves its
exec outcome unknown.

On daemon restart, Agentbus MUST relaunch nothing. It terminalizes a recovered
queued job as failed with FailureClass internal. It terminalizes a recovered
starting or running job as unknown. It preserves every terminal
record.
Recovery and the orphan reaper run before new work is accepted.

This design CAN guarantee at most one initial launch and at most one correction
launch per job, and can guarantee that neither turn is relaunched. It CANNOT
distinguish a permitted initial or correction turn never spawned from one
spawned and lost between its durable turn-state write and exec. A false unknown
is possible and accepted. It CANNOT reap a process that died between exec and
the separate claim write.

The accepted cost is direct: after daemon restart, a provider CLI from the
previous incarnation may still run. The orphan reaper reduces but does not
eliminate that risk.

### Orphan reaper

At restart, Agentbus snapshots each recovered starting or running job before
terminalizing its public projection. For each such job with a claim, it reads
the live process start token and compares it for exact equality with startToken.
It signals the recorded process group only on exact equality.

Agentbus MUST NEVER signal a group on a mismatch, an unreadable token, or a
missing claim. A recycled PID MUST NEVER cause a stranger process group to be
killed. Reaper observation does not create a reaped state or allow unknown to
re-enter a different terminal state.

### Exit codes

CLI exit status is computed from state, failureClass, and contract.compliant,
not state alone. Contract evaluation is also considered, so unevaluated does not
look noncompliant.

| Exit code | Condition |
| --- | --- |
| 0 | completed and (contract not evaluated or contract compliant) |
| 2 | queued or running; also usage error |
| 3 | completed, contract evaluated, and not compliant |
| 4 | failed with a class other than timeout or interrupted; also the default when class is empty |
| 5 | failed with failureClass timeout |
| 6 | failed with failureClass interrupted |
| 7 | canceled |
| 14 | unknown |
| 10 | Unknown job ID, unchanged |
| 11 | Daemon startup failure, unchanged |
| 13 | Shutdown deadline, unchanged |
| 15 | completed, but the authoritative result artifact is missing, unreadable, or does not match its recorded digest |

Codes 8 (reaped), 9 (quarantined), and 12 (the former safety stop) are
permanently retired and MUST NEVER be reused. unknown uses 14 rather than 8
because orphaned meant “we lost process certainty and cannot say what
happened,” which is exactly unknown. Reaped meant the opposite: confirmed gone
and cleaned up. Reusing code 8 with an inverted meaning would break scripted
consumers.

### First terminal wins

The first recorded terminal wins. A terminal job MUST NEVER be overwritten by a
later result, cleanup observation, correction result, or recovery event. The one
correction attempt happens before its terminal record is committed.

LateFinalization and orphaned/reaped re-entry edges are deleted. Two
contradictory policies ship today: one treats terminal records as durable final
facts, while late-finalization and re-entry permit a later result to replace
them. ADR-14 selects the former.

### Storage

Agentbus uses one bbolt store with exactly three top-level buckets: meta,
requests, and jobs. meta holds format metadata. requests maps compound identity
to immutable TaskSpec hash and job ID. jobs holds job records. There is no
workspace fan-out store, admission root, anchor, binding index, proof record,
or projection store.

A corrupt or truncated database MUST produce a typed store error before socket
bind and MUST NEVER fault the daemon. bbolt memory-maps its database, so a naive
bbolt Open can fault rather than return an error. The implementation MUST use a
fault-isolated open preflight before the serving process opens the store. A
preflight signal, malformed page, truncation, or open failure becomes the typed
error and prevents socket bind. Preflight is a safety check, not another store
or a repair path.

### Required operator break

VERSION 0.13.0 AND PROTOCOL VERSION 3 ARE A CLEAN BREAK. There is no
compatibility shim and no state migration. Old state roots are abandoned. Before
using version 0.13.0, the operator MUST delete the old state root and start with
a new version-3 store. Agentbus MUST NOT open, transform, import, or silently
delete an old root.

### Explicitly retained and explicitly deleted

| Existing concern | ADR-14 decision | Replacement or retained scope |
| --- | --- | --- |
| Durable admission-control subsystem | Delete it. | One bbolt store and one generic job service. |
| Process-proof and held-worker protocol | Delete it. | Direct fork and exec into one process group plus a separate claim. |
| Linux resource-hierarchy subsystem | Delete it; it is not an optional enhancement. | Plain process-group supervision and token-equality reaping. |
| PolicyRegistry, named/shape contracts, retry templates, policy methods | Delete them. | One inline JSON Schema and one fixed correction attempt. |
| engine/store.go workspace-store implementation | Delete it. | Single bbolt meta/requests/jobs store; no job-metadata GC. |
| Parallel engine.JobState, model Outcome, and model PublicState vocabularies | Delete them together. | Six public states plus private starting marker. |
| LateFinalization and orphaned/reaped re-entry | Delete them. | First-terminal-wins. |
| engine/adapter tree and engine/command | Retain unchanged. | Convo Relay dependency remains supported. |
| Provider sandbox policy, per-job write cache, private CODEX_HOME | Retain unchanged. | Delegate is deleting duplicate copies, not protections. |
| Provider observation and failure classification | Retain. | The public result is the nine-class FailureClass set. |

## Invariant(s)

- Agentbus owns generic job lifecycle, not review workflows or durable admission
  control.
- Identified replay compares canonical TaskSpec hash before filesystem or backend
  validation.
- A job has at most one initial launch and at most one correction launch; neither
  turn is relaunched on restart or after an unknown exec outcome.
- A recorded terminal state is immutable.
- Cleanup uncertainty cannot erase an authoritative result.
- Restart reaping signals only exact start-token equality.
- The public protocol has exactly four methods and exactly six states.
- The version-3 store opens safely before socket bind or returns a typed error.
- Old roots are not compatible with version 0.13.0 and are not migrated.

## Rejected alternatives

- Keeping the Linux resource hierarchy as an optional process-cleanup enhancement.
- Retaining held workers or a release/ack protocol.
- Retaining a durable admission-control subsystem, anchor, safety latch, proof vocabulary, or
  binding index behind a simpler protocol.
- Relaunching queued, starting, or running jobs on restart.
- Signaling on a missing, unreadable, or unequal start token.
- Letting a late result overwrite terminal state.
- Retaining named contracts, a registry, caller correction templates, or more
  than one correction attempt.
- A compatibility shim, state migration, or automatic old-root deletion.

## Consequences

Each identified job has one small durable lifetime with at most one initial turn
and one correction turn. Agentbus remains safe against daemon-initiated duplicate
launch of either turn while expressly accepting false unknown states and possibly
leftover provider CLIs after a crash. That loss is visible rather than hidden
behind process-proof machinery.

Later deletion can remove the legacy admission-control, process-proof,
resource-hierarchy, validation, store, and parallel-state code against this
ADR. Convo Relay adapters, command execution, provider sandboxing, per-job
write isolation, private CODEX_HOME, and the underlying failure classifier
remain outside that deletion.

## Non-goals

This ADR does not change Witness review workflows, Delegate task preparation,
engine/adapter, engine/command, Convo Relay embedded adapters, provider sandbox
policy, per-job write cache, private CODEX_HOME, or underlying provider
failure-classification logic. It does not provide earlier-protocol
compatibility, state-root migration, resource-hierarchy retention, or a
stronger daemon-restart cleanup guarantee than exact-token process-group
signaling supports.

## Amendments

After ADR-14 was first written, its wire examples were corrected against the
authoritative specification. TaskSpec uses outputSchema, and JobRecord preserves
the required nested, operator-visible fields while the empty-object job.get
listing remains compact. These corrections remove shape drift without changing
the collapse's semantics.

The amended launch invariant distinguishes the one initial turn from the one
permitted correction turn, applies durable state-before-spawn and separate claims
to both, and preserves the unchanged one-process-per-turn adapter surface. The
job.submit result now admits every public replay state, the replay hash fixes its
TaskSpec inputs and canonical form, and the ADR index names ADR-14 as the sole
normative contract. These corrections remove contradictory review targets without
changing the collapse's retained boundaries.

The submit result includes the resolved timeout. New-record validation failure
is a rejection with no job record or requests binding. The state vocabulary was
reviewed to confirm that no retry state is implied.
