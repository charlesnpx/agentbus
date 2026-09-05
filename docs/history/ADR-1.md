# ADR-1: Atomic root AdmissionStore

**Status:** Superseded by [ADR-14](../adr/ADR-14-simplified-core.md).

## Decision

All jobs admitted through the fenced coordinator use one small root AdmissionStore. Acceptance is one
atomic update that resolves replay, creates the aggregate, establishes current-boot ownership, and
commits the request binding and job together. GC deletes the job and binding and writes the tombstone
in the same transaction.

## Invariant(s)

- Binding and job creation are atomic.
- Job GC cannot remove a request key without writing the indefinite tombstone.
- Prompt data stays outside the store.
- The store interface exposes durable operations, not bucket, transaction, or encoding internals.

## Rejected alternatives

- Per-workspace authoritative stores.
- Reservation grace files or partial acceptance states.
- Letting prompt payloads become part of the admission DB.

## Consequences

Later bbolt implementation must model requests, tombstones, jobs, attempts, and quarantine as one
root authority for fenced coordinator jobs.

## ADR-11 amendment

`AdmissionAuthority` is the coordinator/server-facing durable-operation boundary. Its repository is
internal and record-oriented, not a public lifecycle-policy surface. Atomic acceptance still writes
the request binding, safety/proof record, and projection in one root transaction.

## Non-goals

The root store is not authoritative for every preexisting legacy JSON job.
