# ADR-0: Reliability contract

**Status:** Superseded by [ADR-14](../adr/ADR-14-simplified-core.md).

## Decision

AB-D's target contract is exactly-once admission for `(workspaceKey, requestId)` and fail-closed
execution. A request key binds indefinitely to one `jobID` and one fingerprint. Replays are observational only:
matching bindings return the existing job, mismatches return `request_conflict`, matching tombstones
return `request_expired`, and mismatched tombstones return `request_conflict`.
Amendment: ADR-12 makes `replay_conflict` the normative name for the former `request_conflict` cause.

S1 implementation status: production keeps `jobs.requestId` disabled and unadvertised. The current
process custodian is `UnavailableCustodian`, so no production path claims completed fail-closed
execution until the real trusted computing base lands.

Identified fenced submissions do not treat response delivery as the commit boundary. Legacy fenced
and legacy unfenced submissions remain acknowledgement-gated. The model must define the internal
`Decision`, `Dispatch`, and `Outcome` enums and a total public projection before production storage.

## Invariant(s)

- One request key binds to one job and one fingerprint forever, including after GC through a tombstone.
- Replay has no execution side effects.
- Awaiting acknowledgement has no permit.
- a current-boot nonterminal job has a matching live CoordinatorObligation OR the daemon is fail-stopping.
- Permit implies durable supervisor identity; permit-maybe-sent implies containment is required.
- Terminal jobs carry a valid terminal proof.
- Owner or supervisor loss contains and terminalizes; it never auto-retries.
- Launch ordinal 2 requires ordinal 1 to be proven quiescent and at most one ordinal may be active.
- An initialized AdmissionStore is never silently recreated.

## Rejected alternatives

- Exactly-once external effects.
- Durable execution across daemon restart.
- Treating response delivery as the identified submission commit boundary.
- Recreating or rolling back the admission authority as part of the reliability guarantee.

## Consequences

Daemon loss terminalizes nonterminal jobs instead of rerunning them. Whole state-root loss or rollback
is outside the guarantee unless explicitly recovered by an operator.

## Non-goals

Durable resume, content-addressed execution, and exactly-once backend side effects are future work.
