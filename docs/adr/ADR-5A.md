# ADR-5A: Live supervisor-loss handling

## Decision

Unexpected worker exit or control EOF while the daemon is alive triggers reconciliation:
`BeginReconciliation`, revoke future subpermit, ask the custodian to reconcile the durable
`GroupRef`, verify quiescence, record one `QuiescenceCertificate`, then publish the terminal outcome.
If quiescence or terminalization cannot be proven, the daemon closes the listener and fails visibly.

Containment, launch quiescence, and retirement are one physical fact: the exact process group for the
launch ordinal has been observed absent. Signal sent, control closed, and worker exited are diagnostics
only; none is a terminal-proof predicate.

## Invariant(s)

- Live supervisor loss is never ignored.
- Live supervisor loss never auto-retries.
- Quiescence of every relevant launch group is proven before terminalization.
- No-regress principle: no code path may terminalize from projection state, signal attempt, or
  unverified worker/control observations.

## Rejected alternatives

- Waiting for startup reconciliation when the daemon is alive.
- Best-effort kill without verified quiescence.
- Continuing to accept requests after unprovable quiescence.
- Separate containment and retirement receipts.

## Consequences

The daemon must own current-boot supervisor-loss reconciliation as a normal lifecycle event, but the
custodian remains the trusted computing base for physical absence.

Until the real custodian exists, this path is not enabled in production. `UnavailableCustodian` refuses
bootstrap with `supervisor_unavailable`; it does not certify live supervisor-loss handling.

## Non-goals

This ADR does not define durable execution recovery.
