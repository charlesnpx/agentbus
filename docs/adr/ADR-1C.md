# ADR-1C: Corrupt aggregate handling

## Decision

Individual aggregate decode, checksum, or invariant failures are distinct from whole-DB corruption.
The daemon preserves the request binding permanently, independently inspects attempt identity, and
quarantines with a terminal proof when safe. If containment identity is untrustworthy, startup is
fatal.

## Invariant(s)

- A corrupt aggregate never permits request-key reacceptance.
- Permit definitely absent can quarantine with `NeverPermittedAndRetired`.
- Permit maybe observed requires containment before quarantine.
- Untrustworthy containment identity prevents startup.

## Rejected alternatives

- Deleting corrupt values and accepting the request key again.
- Treating all per-value corruption as whole-DB corruption.
- Quarantining a permit-maybe job without containment.

## Consequences

Bindings outlive corrupt job records, and diagnostics must point operators to the corrupt aggregate.

## Non-goals

This ADR does not specify the diagnostic file format beyond requiring an external pointer.
