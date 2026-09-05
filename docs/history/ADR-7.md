# ADR-7: Startup reconciliation as hard socket barrier

**Status:** Superseded by [ADR-14](../adr/ADR-14-simplified-core.md).

## Decision

Startup opens and integrity-checks the DB, generates the current boot ID, and reconciles every prior
boot nonterminal aggregate before binding the socket. Permit definitely not granted terminalizes as
failed with `daemon_restarted_before_launch`. Permit maybe granted requires persisted group
inspection, containment, verified absence, and reaped terminalization. Unprovable cases fail startup.

## Invariant(s)

- Prior-boot nonterminal work is terminal before the daemon advertises service.
- Startup reconciliation never auto-reruns accepted work.
- Process inspection and signalling are coordinator actions, not opaque store operations.

## Rejected alternatives

- Opening the socket before reconciliation.
- Retrying prior-boot work.
- Hiding unprovable containment behind degraded service.

## Consequences

Startup latency includes reconciliation, and fatal diagnostics must identify the failing job and
attempt.

## ADR-11 amendment

Startup reconciliation produces a non-forgeable `Ready` capability only after the durable boot-ready
commit and startup-anchor advancement. Socket binding follows that capability.

## Non-goals

This ADR does not promise durable execution after restart.
