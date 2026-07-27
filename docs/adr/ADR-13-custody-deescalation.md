# ADR-13: Custody de-escalation

**Supersession.** This ADR NORMATIVELY amends ADR-12 `Corruption classes` lines
81-102 for the safety scope of physical cleanup uncertainty, ADR-12 `Shutdown contract`
lines 139-151 and its invariant at line 167, ADR-12 `Result semantics` lines 153-156,
and the meaning of ADR-12 `unavailable_native_runtime` at line 129. All other ADR-12
provisions remain in force unchanged, including protocol surface v2, replay ordering,
storage schema v2 and the root-existence table, and stable rejection cause codes.

## Decision

Agentbus durably deduplicates identified requests and never automatically relaunches an
execution whose launch may already have occurred. During normal operation it terminates
and waits for the supervised process group on cancel, timeout, and graceful shutdown.
After abrupt daemon loss it performs bounded, identity-checked cleanup; if absence
cannot be established the job becomes terminal `orphaned` and is never relaunched, and
the daemon continues serving. Cleanup uncertainty is job-local, not authority
corruption.

This contract is identical on macOS and Linux.

### Outcome and cleanup axes

Execution outcome and cleanup disposition are INDEPENDENT axes. A known execution
outcome (`completed`, `failed`, `timed_out`, `interrupted`, or `canceled`) MUST NEVER be
overwritten because cleanup is uncertain. `OutcomeOrphaned` is reserved for an UNKNOWN
execution outcome only.

The following outcome and cleanup matrix is NORMATIVE.

| Execution status | Cleanup result | Terminal representation |
| --- | --- | --- |
| Backend execution definitely impossible | verified / irrelevant | keep canceled/failed; cleanup `no_execution_possible` |
| May have occurred; no execution outcome recorded | absence verified | `reaped`; cleanup `verified_absent` |
| May have occurred; no execution outcome recorded | absence unresolved | `OutcomeOrphaned`; cleanup `unresolved` |
| Any execution outcome recorded (completed/failed/timed_out/interrupted/canceled) | absence verified | keep the recorded outcome; cleanup `verified_absent` |
| Any execution outcome recorded (completed/failed/timed_out/interrupted/canceled) | absence unresolved | keep the recorded outcome AND its result; cleanup `unresolved` |

### Cleanup disposition

The cleanup disposition is one of `no_execution_possible`, `verified_absent`, or
`unresolved`. It is a DERIVED value, derived from the terminal record plus per-launch
quiescence. It MUST NOT be a separately mutable or independently persisted authority
fact. This prevents outcome, terminal proof, and cleanup disposition from contradicting
one another and requires no authority storage-schema change.

The cleanup disposition MUST be exposed to operators as an ADDITIVE, machine-readable
protocol-v2 field on job status and job result, for example
`"cleanupDisposition": "verified_absent"`. It is NOT physical-proof serialization and
MUST NOT be buried only in logs.

### Unresolved and fatal boundary

Only TYPED PHYSICAL uncertainty becomes job-local `unresolved`. Anything that could
threaten deduplication or ownership integrity remains FATAL and fail-stop.

| Condition | Handling |
| --- | --- |
| PID/PGID identity cannot be safely established | job-local `unresolved` |
| signal/probe cannot establish absence after the bounded attempt | job-local `unresolved` |
| absence deadline expires with identity still ambiguous | job-local `unresolved` |
| caller/startup context is canceled | abort recovery and retry later; do NOT terminalize |
| group record malformed or contradicts its job/ordinal | authority-integrity FATAL |
| durable binding or grant commit outcome unknown | FAIL-STOP (ownership/duplicate-launch risk) |
| repository mutation or finalization ambiguous | FAIL-STOP |
| cgroup directory cleanup fails AFTER absence already proven | record `verified_absent`; emit a cleanup WARNING, not an orphan or global failure |

### Graceful shutdown

This section amends ADR-12 `Shutdown contract` lines 139-151 and its invariant at line
167. Affected jobs MAY terminalize with `unresolved` cleanup. The graceful-shutdown
operation MUST NOT report the stronger "all custody gone / no remaining recovery
obligation" success condition when any affected job terminalized with unresolved
cleanup.

The amended invariant is: a daemon reporting successful graceful shutdown has no live
custody it can still act on; it may leave terminal jobs whose cleanup is `unresolved`,
and those are NOT recovery obligations.

### Recovery obligations

A terminal `unresolved` or `orphaned` job is NOT a recovery obligation. Startup MUST NOT
repeatedly retry it. It remains durable history and replays normally under its
`(workspaceKey, requestId)`. It still COUNTS as a job and binding, so `reset-empty-root`
MUST still refuse the non-empty root. It MUST NOT prevent sealing once there is no active
or retryable recovery work. No workspace-wide block is added after an orphan because
request replay already prevents repetition of the same logical job.

### Result semantics

This section amends ADR-12 `Result semantics` lines 153-156. Physical proof is NO LONGER
a universal prerequisite for terminalization. Terminalization without proven absence is
permitted ONLY for supervisor-loss or orphan causes. Normal outcomes and successfully
recovered outcomes MUST still carry proven quiescence.

The public `job.result` still MUST NOT serialize physical proof. The additive
`cleanupDisposition` field is the only new public surface.

### Corruption and fail-stop scope

This section amends ADR-12 `Corruption classes` lines 81-102. The SafetyLatch global
fail-stop remains JUSTIFIED for genuine authority-integrity failures: meta corruption,
binding corruption, safety-record corruption, tombstone corruption, terminal-record
corruption, binding-index mismatch, structural corruption, unknown durable mutation
outcome, unknown job/request ownership, and any condition permitting duplicate launch.
ADR-12 safety-significant corruption classes are otherwise UNCHANGED.

The SafetyLatch MUST NO LONGER trip merely because agentbus cannot prove an old process
is absent after the daemon itself was forcibly killed. Physical cleanup uncertainty is
reclassified from global authority corruption to job-local `unresolved`.

### Runtime and platform

This section amends ADR-12 and relates to ADR-11. The daemon serves on Linux with cgroup
v2, Linux without cgroups using process-group supervision, and macOS using process-group
or held-parent supervision. cgroup v2 is a PREFERRED Linux cleanup ENHANCEMENT, not a
serving prerequisite.

Containment strength is NOT part of a root's permanent identity. A single root MAY hold
mixed-history jobs, including cgroup-backed and process-group-backed jobs, and
containment is selected per launch.

The rejection cause `unavailable_native_runtime` from ADR-12 line 129 is REDEFINED to
mean this host cannot provide basic controlled process supervision: process groups,
identity/start-token observation, TERM/KILL/wait, and controlled runner. It MUST NOT
mean this host lacks Linux cgroups.

Baseline VERIFIED containment during NORMAL supervised operation remains MANDATORY on
every serving platform. Only crash-durable retained-object containment is no longer
required.

### Contract version

The admission contract version increments from 1 to 2 to mark this deliberate normative
change. Implementations MUST NOT silently reuse contract version 1. Root activation
itself is RETAINED.

Given the project's no-migration posture, old candidate roots recorded at contract
version 1 MUST fail typed and require reset or sealing. This ADR does NOT introduce any
storage-schema migration.

## Invariant(s)

- Identified requests are durably deduplicated and executions whose launch may already
  have occurred are never automatically relaunched.
- Execution outcome and cleanup disposition are independent; known outcomes are not
  overwritten by cleanup uncertainty.
- `OutcomeOrphaned` represents an unknown execution outcome only.
- Cleanup disposition is derived from terminal records and per-launch quiescence, not
  separately persisted mutable authority state.
- Typed physical cleanup uncertainty after daemon loss is job-local `unresolved`, not
  authority corruption.
- Genuine authority-integrity failures and duplicate-launch risks remain global
  fail-stop conditions.
- Successful graceful shutdown leaves no live custody the daemon can still act on; any
  remaining terminal `unresolved` jobs are durable history, not recovery obligations.
- cgroup v2 is a preferred Linux cleanup enhancement, not a serving prerequisite or root
  identity component.
- Admission contract version 2 marks this normative change without a storage-schema
  migration.

## Rejected alternatives

- Keeping a Linux-only formal "no orphan after crash" guarantee, which retains nearly
  all the proof machinery.
- Deleting the Linux cgroup backend, which is a valuable cleanup enhancement; only its
  authority-defining role is removed.
- Overwriting a known execution outcome with `orphaned` when only cleanup is uncertain.
- A separately persisted or mutable cleanup-disposition authority fact.
- Forging a quiescence certificate claiming absence that was not proven.
- Global fail-stop because physical absence is unprovable after a forced daemon kill.
- Building a launchd / Endpoint Security / macOS system-extension subsystem.
- A workspace-wide quarantine or block after an orphaned job.

## Consequences

The daemon-crash custody contract becomes operational rather than authority-corrupting.
Agentbus still prevents duplicate launch by replaying identified requests and by never
automatically relaunching an execution that may already have begun. Operators receive a
machine-readable cleanup disposition for each terminal job without exposing physical
proof serialization.

macOS and Linux share one public contract. Linux cgroup v2 remains useful where
available, but roots no longer encode cgroup-backed containment as permanent identity.
Old contract-version-1 candidate roots fail typed under the no-migration policy and
require reset or sealing.

## Non-goals

This ADR does not include: protocol-v1 migration; storage-schema migration; a sqlite
implementation; public physical-proof serialization; a new containment framework beyond
the existing native containment backend; relocation of execution packages; or
parked-worker deletion, which is deferred to a separate deletion-oriented review.
