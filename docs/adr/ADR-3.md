# ADR-3: Post-acceptance response and error contract

## Decision

Before acceptance, validation, backend, capability, and store errors are definitive rejection. After
acceptance, the server returns a success-shaped result with the durable `jobID` and state, even when
preparation immediately fails. Response undeliverable after acceptance is transport-ambiguous and is
resolved by replay.

## Invariant(s)

- No application-level error crosses the acceptance point.
- Post-accept state-write failure keeps the coordinator obligation.
- Response loss after identified acceptance does not cancel execution.

## Rejected alternatives

- Returning rejection after a job was durably accepted.
- Cancelling identified jobs on response write failure.
- Dropping the obligation when a post-accept write fails.

## Consequences

Callers must replay ambiguous identified submissions with the same request key.

## Non-goals

This ADR does not claim backend execution succeeded.
