# ADR-5: Fenced attempt supervisor

## Decision

Fenced attempts use a parked exec worker plus monitor. The worker is pre-registered, its PID, PGID,
and high-resolution start token are persisted before permit, and its PID remains unchanged across
`exec` into the backend. The worker cannot exec until it receives a permit.

## Invariant(s)

- Process identity precedes execution.
- Permit is never sent if supervisor identity persistence fails.
- Parent/control-channel loss kills the process group.
- Unsupported backends are rejected before acceptance.

## Rejected alternatives

- Persisting child identity only after backend start.
- Ignoring identity persistence failures.
- Allowing the control descriptor to be inherited by the backend.

## Consequences

The durable store can identify the executing group even when failure occurs before `BACKEND_STARTED`.

## Non-goals

This ADR does not define adapter-specific command-line details.
