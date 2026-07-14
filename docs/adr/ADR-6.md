# ADR-6: Permit and cancel linearization

## Decision

`GrantPermit` and `RequestCancel` are mutually exclusive durable CAS operations. Permit commits
before the `PERMIT` message. A committed permit that may not have been delivered is execution
uncertain and requires containment. Completion versus cancel has a deterministic model winner.

## Invariant(s)

- Permit and cancel cannot both win the same unpermitted state.
- A permit is durable before launch authority is sent.
- Cancel after permit cannot terminalize until containment or clean quiescence is proven.

## Rejected alternatives

- Using an in-memory mutex as the authority boundary.
- Sending permit before durable commit.
- Terminalizing a permit-maybe job without proof.

## Consequences

Crash on either side of the permit send is reconciled through durable state and containment.

## Non-goals

This ADR does not implement backend cancellation mechanics.
