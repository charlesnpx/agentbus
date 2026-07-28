# ADR-2A: Coordinator-obligation linearization and fail-stop

## Decision

Submission materializes and validates an immutable launch spec, installs a non-runnable pending
`CoordinatorObligation { jobID, launchSpec, mode, committed }`, commits durable acceptance, marks the
obligation committed, and only then returns or allows preparation. The coordinator loop is fail-stop.

## Invariant(s)

- A post-commit, pre-runnable failure leaves either fail-stop or durable terminalization, not ownerless
  runnable work.
- A defensive current-boot scan terminalizes records with no matching obligation.
- Obligations become runnable only after durable commit.

## Rejected alternatives

- Committing before any coordinator obligation exists.
- Recovering the coordinator loop after panic and continuing.
- Running preparation before the identified acceptance point.

## Consequences

Failpoint tests must inject after commit and before the obligation is runnable.

## Non-goals

This ADR does not make execution restart-surviving.
