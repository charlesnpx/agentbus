# ADR-13: Custody de-escalation

**Status:** Superseded by [ADR-14](../adr/ADR-14-simplified-core.md).

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
After abrupt daemon loss it performs bounded, identity-checked cleanup. If the execution
outcome is UNKNOWN and absence cannot be established, the job becomes terminal `orphaned`;
a job with a recorded execution outcome KEEPS that outcome and its result, with cleanup
marked `unresolved` (never rewritten to `orphaned`). Such jobs are never relaunched. The
daemon continues serving in this case because physical cleanup uncertainty is job-local;
genuine authority-integrity or ownership-ambiguity failures (below) remain global
fail-stop and are NOT covered by "continues serving".

This contract is identical on macOS and Linux.

### Outcome and cleanup axes

Execution outcome and cleanup disposition are INDEPENDENT axes. A known execution
outcome (`OutcomeCompleted`, `OutcomeCompletedNoncompliant`, `OutcomeFailed`,
`OutcomeTimedOut`, `OutcomeInterrupted`, or `OutcomeCanceled`) MUST NEVER be overwritten
because cleanup is uncertain. `OutcomeOrphaned` is reserved for an UNKNOWN execution
outcome only.

`OutcomeReaped` is the terminal representation of "execution may have occurred, no
outcome was recorded, and absence was VERIFIED" (matrix row 2); it always carries cleanup
`verified_absent`. `OutcomeQuarantined` belongs to the projection-corruption recovery
path (ADR-11 / ADR-12 projection-only corruption) and is out of scope for the crash-
custody axes here; its cleanup disposition is derived from its own proof records like any
other terminal outcome. Only `OutcomeOrphaned` carries the `unresolved`-absence terminal
basis for an unknown outcome.

The following outcome and cleanup matrix is NORMATIVE.

| Execution status | Cleanup result | Terminal representation |
| --- | --- | --- |
| Backend execution definitely impossible | verified / irrelevant | keep canceled/failed; cleanup `no_execution_possible` |
| May have occurred; no execution outcome recorded | absence verified | `reaped`; cleanup `verified_absent` |
| May have occurred; no execution outcome recorded | absence unresolved | `OutcomeOrphaned`; cleanup `unresolved` |
| Any execution outcome recorded (completed/completed_noncompliant/failed/timed_out/interrupted/canceled) | absence verified | keep the recorded outcome; cleanup `verified_absent` |
| Any execution outcome recorded (completed/completed_noncompliant/failed/timed_out/interrupted/canceled) | absence unresolved | keep the recorded outcome AND its result; cleanup `unresolved` |

### Cause and outcome compatibility

The durable `TerminalCause` (engine/execution/model/types.go) determines the OUTCOME axis:
whether execution was possible and which terminal outcome may be recorded. This table is
NORMATIVE. The CLEANUP axis is independent (see Result semantics): the
`ProofUnresolvedAbsence` basis may accompany ANY of these outcomes when physical absence
could not be established. What this table restricts is the OUTCOME — in particular,
`OutcomeOrphaned` (the only new outcome) is reachable ONLY from the marked rows, and the
execution-impossible causes may never be orphaned.

| TerminalCause | Execution possibility | Permitted terminal representation |
| --- | --- | --- |
| `CauseCompletedNormally` | outcome recorded | keep recorded outcome; requires proven quiescence in the normal path; if absence is unprovable at shutdown, keep the outcome with cleanup `unresolved` |
| `CauseResponseUndeliverable` | outcome recorded | keep recorded outcome; cleanup per proof |
| `CauseCanceledBeforeAuthorization`, `CauseDaemonRestartedBeforeAuthorization`, `CauseSupervisorLostBeforeAuthorization`, `CauseReleaseDefinitelyNotSent` | execution was impossible (pre-authorization / release never sent) | clean terminal (canceled/never-permitted); cleanup `no_execution_possible`; NEVER `OutcomeOrphaned` |
| `CauseCanceledAfterAuthorization` | canceled after authorization | `OutcomeCanceled`; quiescence required normally; unprovable absence -> keep `OutcomeCanceled` with cleanup `unresolved` |
| `CauseDaemonRestartedAfterAuthorization`, `CauseSupervisorLostAfterAuthorization`, `CauseReleaseOutcomeUnknown` | execution MAY have occurred | if a real outcome was recorded, keep it (cleanup `verified_absent` or `unresolved`); else absence verified -> `OutcomeReaped` (`verified_absent`); else **`OutcomeOrphaned` with `ProofUnresolvedAbsence` (`unresolved`)** |
| `CauseCorruptProjection` | integrity path | projection reconstructed from proof (ADR-11); not a crash-custody orphan path |

`OutcomeOrphaned` is therefore reachable ONLY from `CauseDaemonRestartedAfterAuthorization`,
`CauseSupervisorLostAfterAuthorization`, or `CauseReleaseOutcomeUnknown`, and ONLY when no
execution outcome was recorded and absence could not be verified. The `BeforeAuthorization`
causes and `CauseReleaseDefinitelyNotSent` are execution-impossible and MUST NOT produce
`OutcomeOrphaned`.

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

The decisive line is between DURABLE containment identity and RUNTIME physical
observation. A valid, trustworthy durable group identity (the recorded `GroupRef`) must
FIRST be established; only then is inconclusive runtime observation job-local. A missing,
corrupt, or otherwise untrustworthy durable containment identity remains FATAL and is NOT
amended by this ADR (see the ADR-11 note below).

| Condition | Handling |
| --- | --- |
| physical observation cannot resolve absence AFTER a valid, trustworthy durable `GroupRef` was established (identity established, runtime state inconclusive) | job-local `unresolved` |
| signal/probe cannot establish absence after the bounded attempt | job-local `unresolved` |
| absence deadline expires while runtime state (not durable identity) is still inconclusive | job-local `unresolved` |
| caller/startup context is canceled | abort recovery and retry later; do NOT terminalize |
| durable `GroupRef`/containment identity missing, corrupt, or untrustworthy | authority-integrity FATAL (ADR-11, unchanged) |
| group record malformed or contradicts its job/ordinal | authority-integrity FATAL |
| durable binding or grant commit outcome unknown | FAIL-STOP (ownership/duplicate-launch risk) |
| repository mutation or finalization ambiguous | FAIL-STOP |
| cgroup directory cleanup fails AFTER absence already proven | record `verified_absent`; emit a cleanup WARNING, not an orphan or global failure |

**ADR-11 note.** ADR-11 (lines 30-32) holds that missing/corrupt proof records or
untrustworthy containment identity are FATAL because permit certainty cannot be derived.
ADR-13 does NOT amend that rule. ADR-13 only reclassifies the DISTINCT case of a trustworthy
durable identity whose live process state cannot be physically observed after daemon loss:
that becomes job-local `unresolved`, not global fail-stop.

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
a universal prerequisite for terminalization.

The two axes are governed independently:

- **Cleanup axis (terminal basis).** The `ProofUnresolvedAbsence` terminal basis (terminal
  WITHOUT proven absence) is the cleanup-axis representation of "physical absence could not
  be established". It MAY accompany any terminal outcome whose absence can genuinely be
  unprovable, and is not restricted to orphan causes. (It does NOT apply to `OutcomeReaped`,
  which is verified-absent by definition, nor to the execution-impossible outcomes, whose
  cleanup is fixed to `no_execution_possible` by the matrix.) Proven quiescence is required
  to CLAIM `verified_absent` (basis
  `ProofCleanQuiescentOutcomeAndRetired` / `ProofContained`), NOT to terminalize at all. A
  job whose absence is unprovable terminalizes with `ProofUnresolvedAbsence` and cleanup
  `unresolved`, keeping whatever outcome axis applies.
- **Outcome axis.** `OutcomeOrphaned` (unknown outcome) is reserved for the after-
  authorization supervisor-loss / daemon-restart / unknown-release causes when NO outcome
  was recorded (per the Cause and outcome compatibility table). A recorded outcome
  (`completed`, `canceled`, etc.) keeps that outcome even when its cleanup basis is
  `ProofUnresolvedAbsence`.

Consequently a normal `completed` outcome that cannot prove absence at graceful shutdown
terminalizes as `completed` + cleanup `unresolved`; it does NOT require proven quiescence
to become terminal, and it is NOT rewritten to `orphaned`.

The public `job.result` still MUST NOT serialize physical proof. The additive
`cleanupDisposition` field is the only new public data surface.

Terminal `orphaned` is a TERMINAL job state and MUST carry its OWN CLI exit code, distinct
from the generic nonterminal `2` a client reads as "still running". The frozen exit code
for terminal `orphaned` is **14** (`ExitCodeForState` in engine/job.go). Codes 10-13 are
already assigned (unknown-job=10, daemon-startup-failure=11, fail-stop=12,
shutdown-deadline=13), so 14 is the next unused code; code `2` is retired from meaning
"orphaned". The full terminal map is: completed=0, completed_noncompliant=3, failed=4,
timed_out=5, interrupted=6, canceled=7, reaped=8, quarantined=9, orphaned=14. `IsTerminal`
MUST report `StateOrphaned` as terminal and client terminal-wait MUST stop on it. The
public protocol reference (docs/protocol.md, updated in U7) MUST reflect terminal
`orphaned` and exit code 14.

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

The daemon's current admission contract version becomes 2 to mark this deliberate
normative change. Activation stamps the daemon's current contract version. Root activation
itself is RETAINED, and `admission_root.contract_version` remains immutable once declared
(engine/execution/repository/records.go). A "candidate root" is an initialized root that
is not yet activated (`AdmissionRoot.Activated == false`).

Handling of a version-1 root, given the no-migration posture and the immutability rule:

- **Activated v1 root.** Opening it under a v2 daemon fails with a stable typed
  incompatible-contract-version fatal error (analogous to ADR-12's incompatible-schema
  fatal), fail-closed before socket bind, file untouched. There is NO in-place migration.
  Recovery is operator-driven `seal`, then serve the successor.
- **Seal successor stamping.** When a v2 daemon performs `seal`, the newly initialized
  successor root MUST be stamped at the DAEMON'S CURRENT contract version (2), NOT copied
  from the sealed root's version. This overrides the prior behavior where the seal
  successor inherited the old root's `ContractVersion` (engine/execution/authority/admin.go
  `initializeReservedAdmissionRoot`); implemented in U5.
- **Candidate (unactivated) v1 root.** Activation is refused typed; the root is eligible
  for `reset-empty` per the ADR-12 root-existence matrix (after proving it empty) or for
  `seal`. A freshly created root is stamped version 2.

This ADR does NOT introduce any storage-schema migration; only the activation contract
version and the seal-successor stamping change.

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
