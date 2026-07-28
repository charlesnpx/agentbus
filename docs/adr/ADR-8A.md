# ADR-8A: Result-publication exclusion

## Decision

Orphan-result cleanup deletes only files unreferenced by a terminal aggregate and excludes jobs with a
live coordinator obligation, jobs in `result_publishing`, current-boot nonterminal jobs, and temp
result names belonging to an active publication. Cleanup is never age-based.

## Invariant(s)

- Active or publishing jobs cannot have their results cleaned up.
- Result cleanup uses durable references and coordinator obligations, not file age.
- Pause after result rename plus cleanup preserves the result.

## Rejected alternatives

- Age-based orphan cleanup.
- Ignoring `result_publishing` jobs.
- Cleaning temp names owned by active publication.

## Consequences

Cleanup must consult both durable state and live coordinator ownership.

## ADR-11 amendment

Cleanup consumes authority projections plus authority-owned runtime exclusions. It never reads
proof-adjacent coordinator maps.

## Non-goals

This ADR does not change the result file format.
