# ADR-5A: Live supervisor-loss handling

## Decision

Unexpected worker exit or control EOF while the daemon is alive triggers reconciliation:
`BeginReconciliation`, revoke future subpermit, signal the verified group, verify quiescence,
`RecordContained`, then `PublishTerminal(Contained)`. If containment or terminalization cannot be
proven, the daemon closes the listener and fails visibly.

## Invariant(s)

- Live supervisor loss is never ignored.
- Live supervisor loss never auto-retries.
- Containment is proven before terminalization.

## Rejected alternatives

- Waiting for startup reconciliation when the daemon is alive.
- Best-effort kill without verified containment.
- Continuing to accept requests after unprovable containment.

## Consequences

The daemon must own current-boot supervisor-loss reconciliation as a normal lifecycle event.

## Non-goals

This ADR does not define durable execution recovery.
