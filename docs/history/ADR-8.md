# ADR-8: Results, minimal migration, and terminal ordering

**Status:** Superseded by [ADR-14](../adr/ADR-14-simplified-core.md).

## Decision

Existing `<results>/<jobID>.txt` layout is preserved. Completed result publication orders as
`BeginResultPublication`, write temp, fsync temp, close, rename, fsync results directory, and
`PublishTerminal` with digest and byte count. Terminal commit failure keeps ownership and retries.
Downgrade after first v0.6 admission is unsupported.

## Invariant(s)

- Terminal completed jobs reference a result whose digest and byte count match.
- Ownership remains live while terminal publication can be retried.
- Result publication precedes terminal commit.

## Rejected alternatives

- Content-addressed results.
- Protocol-major bump for result storage.
- Historical result migration or global cutover.

## Consequences

Result files remain compatible while terminal ordering becomes stricter.

## Non-goals

This ADR does not define orphan cleanup; ADR-8A does.
