# ADR-4: Read routing, historical dual-read, and statePath

**Status:** Superseded by [ADR-14](../adr/ADR-14-simplified-core.md).

## Decision

Exact job reads check bbolt first and legacy JSON second. Global/current `job.status` scans bbolt and
merges JSON, diagnosing duplicate job IDs. `session.resume` consults both sources, while cancel and
result route to the owning source. `statePath` is the root DB path and is documented as opaque shared
storage.

## Invariant(s)

- Fenced jobs have no authoritative JSON record.
- Duplicate job IDs across sources are diagnosed or rejected.
- `job.status` remains globally scoped.

## Rejected alternatives

- JSON projections for fenced jobs.
- Workspace-scoped legacy `job.status`.
- Presenting the root DB path as a per-job state file.

## Consequences

Migration can read historical JSON while treating bbolt as authoritative for fenced jobs.

## Non-goals

This ADR does not perform historical migration of existing JSON jobs into bbolt.
