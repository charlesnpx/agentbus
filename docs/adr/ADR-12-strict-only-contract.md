# ADR-12: Strict-only contract

## Decision

AB-E freezes the strict-only protocol, replay, storage, rejection, shutdown, and result contract for
the following implementation units. This ADR is NORMATIVE.

### Protocol surface v2

`protocol.Version` is 2. The protocol v2 surface contains `protocol.hello`, identified
`job.submit`, `job.status`, `job.result`, `job.cancel`, `policy.validate`, and `policy.register`.
`policy.validate` and `policy.register` REMAIN in v2 as non-execution admin/policy surface. Job
listing means `job.status {all:true}`; there is no separate list method.

E1 removes `session.start`, `session.resume`, `session.list`, `turn.start`, `turn.interrupt`,
`turn.event`, `turn.result`, and unidentified `job.submit`.

There is no protocol-v1 compatibility layer. A v1 hello (`protocolVersion` mismatch) receives a
typed version-mismatch error with stable `error.data.code = "protocol_version_mismatch"`. Removed
or unknown methods return JSON-RPC `error.code = -32601` and stable
`error.data.code = "method_not_found"`. The v2 client rejects a hello result whose
`protocolVersion` differs from `protocol.Version` by returning a distinct exported sentinel/typed
error in package `client` named `ErrProtocolVersionMismatch`. The returned error MUST match
`ErrProtocolVersionMismatch` via `errors.Is`, report both the expected version (`protocol.Version`)
and the received version in its message, and be returned from `Connect`/`clientHello` during the
hello exchange before any caller request is sent. No request or response is translated, migrated, or
accepted through a v1 compatibility path.

### Replay ordering

After JSON framing and request-key validation, identified `job.submit` processing MUST follow this
ADR-0B-compliant order:

1. Extract the raw `taskSpec` without backend validation and without filesystem validation.
2. Run `LookupReplay(requestKey)`.
3. For a live binding, compare the raw task spec with the recorded identity using
   `model.TaskIdentityMatchesRawTaskSpec` and the recorded fingerprint version. On match, return the
   existing job with `deduplicated=true`. On mismatch, return the typed `replay_conflict` cause.
4. For a tombstone, perform the same recorded-version comparison. On match, return the typed
   `request_expired` cause. On mismatch, return the typed `replay_conflict` cause.
5. Only when neither a live binding nor a tombstone exists, validate the current task schema,
   validate backend support and fenceability, resolve the workspace, compute the new identity with
   the current fingerprint version, and admit the request.

Replay is independent of present filesystem state, including a deleted workspace, moved workspace,
or broken symlink. Replay is independent of current backend composition. Replay interprets the
request using the fingerprint version recorded with the original binding or tombstone. An unknown
recorded fingerprint version returns the typed `request_fingerprint_unsupported` cause.

### Storage schema v2 and root existence

Storage schema v2 adds `binding_index` and record-level validation semantics. E5A implements those
semantics.

Opening an older schema is a typed incompatible-schema fatal error. Existing databases are not
silently repaired during normal startup: no silent bucket creation, no silent index rebuild, and no
normal-startup repair of missing records or indexes.

Root existence classification is evaluated in this normative order: existence checks, zero-length
check, structural corruption check, schema version check, bucket/index presence, anchor
presence/match, then the remaining existence rows. Structural corruption applies only to an existing
non-zero-length file. The FIRST matching row classifies the typed error. Explicit reset-empty
eligibility is decided only after classification, and is eligible only for the `present | missing
anchor` row after proving the root empty. Each classified row has one unambiguous typed error.

| DB | Anchor | Result |
| --- | --- | --- |
| missing | missing | fresh creation only through an explicit create path |
| missing | present | fatal; never recreate |
| zero-length | any | fatal; never initialize implicitly |
| structurally corrupt | any | fatal before socket bind; file untouched |
| old schema | any | typed incompatible-schema fatal |
| missing buckets/index | any | fatal; no normal-startup repair |
| present | missing | fatal by default; explicit reset-empty may repair after proving root empty |
| present | mismatched | fatal |
| valid | matching | open, audit, verify identity |

Unsupported-platform first serve is NON-MUTATING. The support probe runs before creating the DB,
anchor, token, socket, or PID file. A failed support probe leaves nothing behind.

### Corruption classes

Repository point operations validate only touched records and referenced records. A touched or
referenced invalid record returns `ErrCorruptRecord`; unrelated records are physically preserved.

Any DETECTED safety-significant corruption trips the SafetyLatch: global fail-stop, listener closed,
and database preserved unchanged for diagnosis. Safety-significant corruption includes meta records,
bindings, safety records, tombstones, binding-index mismatch, terminal records, and structural
corruption. Projection-only corruption retains the existing quarantine and reconstruction policy.

| Class | Storage locality | Safety scope |
| --- | --- | --- |
| Meta corruption | authority metadata | SafetyLatch; global fail-stop |
| Binding corruption | request binding record | SafetyLatch; global fail-stop |
| Safety-record corruption | permit, containment, recovery, or terminal proof record | SafetyLatch; global fail-stop |
| Tombstone corruption | request tombstone record | SafetyLatch; global fail-stop |
| Binding-index mismatch | binding index disagrees with records | SafetyLatch; global fail-stop |
| Terminal-record corruption | terminal authority fact | SafetyLatch; global fail-stop |
| Structural corruption | database structure, pages, buckets, or envelope integrity | SafetyLatch before socket bind; file untouched |
| Projection-only corruption | derived projection record only | quarantine and reconstruct from valid proof records |
| Unrelated record corruption during point operation | record not touched or referenced by that operation | preserve physically; no point-operation error until detected |

### Stable rejection causes

Strict rejection error payloads carry stable machine-readable cause strings in
`error.data.admissionCause`. `error.data.code` remains the stable protocol error identifier, such as
`invalid_task_spec` or `capability_missing`; it is NOT replaced by the cause. Clients classify
admission rejections by `error.data.admissionCause` first, falling back to `error.data.code`.

When multiple causes apply, exactly ONE cause is emitted, chosen by this normative order
(fail-stop/root states before request states): `root_corrupt` > `root_identity_mismatch` >
`root_fail_stopped` > `root_sealed` > `admission_closing` > `unavailable_native_runtime` >
`missing_identity` > `request_fingerprint_unsupported` > `replay_conflict` > `request_expired` >
`unsupported_backend` > `unfenceable_backend` > `invalid_strict_config`.

This ADR amends ADR-0: the cause formerly named request_conflict is normatively replay_conflict.

Each rejection cause MUST carry exactly the `error.data.code` listed below.

| Cause | `error.data.code` | Meaning |
| --- | --- | --- |
| `missing_identity` | `invalid_task_spec` | The request lacks the strict `(workspaceKey, requestId)` identity required for identified admission. |
| `replay_conflict` | `invalid_task_spec` | The request key is already bound or tombstoned to a different raw task identity. |
| `request_expired` | `invalid_task_spec` | The request key matches a tombstone whose original job is no longer replayable. |
| `request_fingerprint_unsupported` | `invalid_task_spec` | The recorded binding or tombstone uses a fingerprint algorithm or version the daemon cannot compare. |
| `unsupported_backend` | `backend_unavailable` | The requested backend is not available to strict admission. |
| `unfenceable_backend` | `capability_missing` | The requested backend exists but cannot provide the required strict fencing or containment contract. |
| `invalid_strict_config` | `invalid_task_spec` | The strict task configuration is malformed or incompatible with strict admission. |
| `unavailable_native_runtime` | `capability_missing` | The native runtime support probe cannot satisfy the strict runtime requirements. |
| `root_corrupt` | `backend_unavailable` | The authority root has detected repository, anchor, or integrity corruption. |
| `root_identity_mismatch` | `backend_unavailable` | The repository identity and anchor identity do not match. |
| `root_fail_stopped` | `backend_unavailable` | The authority root has entered fail-stop and rejects admission. |
| `root_sealed` | `capability_missing` | The authority root is sealed and cannot accept service or admission. |
| `admission_closing` | `capability_missing` | The daemon is closing and rejects new admission. |

E1 and E3 wire these causes into strict rejection responses. E6 documents them in the public protocol
reference.

### Shutdown contract

Graceful shutdown reuses the authority's existing durable cancellation transition. It creates no new
intent record and no second cancellation state machine.

The sequence is: reject new admission; stop accepting connections; request cancellation for each
active authority-owned job; abort unreleased launches; contain released custodies; wait for physical
absence and normal terminalization; drain the coordinator; close runtime, repository, socket, and PID
with identity checks.

The invariant is: a daemon that reports successful graceful shutdown has no live custody and no
remaining recovery obligation. Forced-timeout fallback is monitor containment plus startup recovery,
fail-closed.

### Result semantics

`job.result` returns data derived from an authority-owned terminal record. Physical proof remains an
internal prerequisite for terminalization and is NOT added to the v2 public response.

## Invariant(s)

- Protocol v2 is strict-only and has no v1 compatibility path.
- Replay lookup and recorded-version identity comparison happen before backend validation,
  filesystem validation, workspace resolution, or current-version fingerprinting.
- Existing authority storage is never silently recreated or repaired on normal startup.
- Detected safety-significant corruption fail-stops the authority and preserves the database
  unchanged for diagnosis.
- Stable rejection causes are machine-readable protocol values.
- Successful graceful shutdown leaves no live custody and no recovery obligation.
- Public result data is derived from authority-owned terminal records without exposing terminal
  proof serialization.

## Rejected alternatives

- Keeping foreground sessions or unidentified jobs as protocol-v2 compatibility shims.
- Translating v1 requests into v2 requests.
- Recomputing replay identity with the current filesystem, backend set, or fingerprint version.
- Silently initializing zero-length databases or repairing existing roots during normal startup.
- Treating safety-significant corruption as local projection damage.
- Adding a shutdown-specific durable intent record or second cancellation state machine.
- Adding physical terminal proof to the public `job.result` response.

## Consequences

AB-E implementation units can make strict-only behavior observable without redefining the contract.
Operators see deterministic incompatibility and integrity failures instead of best-effort migration or
silent recreation. Clients receive stable rejection causes suitable for retry, operator intervention,
or fail-closed handling.

## Non-goals

AB-E does not include: interactive/foreground session redesign; protocol-v1 or storage-schema-v1 compatibility; storage migration; a sqlite implementation; authority mutex redesign; public terminal-proof serialization; a generalized daemon logging system; online backup or compaction; relocation of execution packages under internal/.
