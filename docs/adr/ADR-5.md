# ADR-5: Fenced attempt custodian

## Decision

Fenced attempts use a process custodian as trusted computing base for physical process facts. Custody
is per launch ordinal, not per attempt: each launch slot records its own durable `GroupRef`, grant,
release fact, and quiescence certificate. The worker is pre-registered, its PID, PGID, host boot,
leader identity, monitor identity, custody id, and retained platform handle are persisted before
permit, and its PID remains unchanged across `exec` into the backend. The worker cannot exec until it
receives a permit.

## Invariant(s)

- Process identity precedes execution.
- Permit is never sent if the ordinal's `GroupRef` persistence fails.
- The custodian is the only component that can mint physical quiescence attestations.
- Logical authority stores logical certificates; it does not fabricate physical proof.
- Parent/control-channel loss kills the process group.
- Unsupported backends are rejected before acceptance.

## Rejected alternatives

- Persisting child identity only after backend start.
- Ignoring identity persistence failures.
- Allowing the control descriptor to be inherited by the backend.

## Consequences

The durable store can identify each executing group even when failure occurs before `BACKEND_STARTED`.
Admission authority holds only the attestation verifier; the custodian holds the issuer.

## Non-goals

This ADR does not define adapter-specific command-line details.
