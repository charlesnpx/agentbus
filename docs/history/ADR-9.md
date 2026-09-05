# ADR-9: Restore global legacy job.status

**Status:** Superseded by [ADR-14](../adr/ADR-14-simplified-core.md).

## Decision

The legacy `job.status` scope change is reverted. `job.status` without an exact job ID is again a
global listing merged across bbolt fenced jobs and legacy JSON jobs.

## Invariant(s)

- Global status lists newly admitted fenced jobs after restart.
- Request ID and tag filters remain exact filters over the global listing.

## Rejected alternatives

- Workspace-scoped `job.status` as introduced in PR #28.
- Hiding fenced jobs from legacy status listing.

## Consequences

Read routing must diagnose duplicate job IDs across stores.

## Non-goals

This ADR does not define a new status protocol version.
