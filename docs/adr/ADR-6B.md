# ADR-6B: Supervisor retirement in terminal proof

## Decision

Before terminal commit, the coordinator closes or revokes the control channel, waits for the parked
worker to exit, and verifies the group is empty. This is required even when execution was never
permitted. Prepared legacy workers must be synchronously reaped if acceptance fails or the response is
rejected after `SUPERVISOR_READY`.

## Invariant(s)

- `NeverPermitted` and `CleanQuiescentOutcome` proofs are valid only when retired.
- Pre-acceptance store failure after supervisor preparation leaves zero backend launches.
- Response rejection after legacy supervisor preparation reaps the worker.

## Rejected alternatives

- Treating no permit as enough proof without worker retirement.
- Leaving prepared workers alive when no durable job exists.
- Publishing terminal before verifying the group is empty.

## Consequences

Terminalization requires a retirement step for every fenced supervisor.

## Non-goals

This ADR does not specify exact wait timing or signal escalation.
