# ADR-1A: Admission-store integrity policy

## Decision

The admission DB uses restrictive permissions, normal bbolt syncing, and an integrity check before
socket bind. Structural or page corruption is fatal startup. Once initialized, the daemon never
renames, quarantines, deletes, or recreates the DB on its own.

## Invariant(s)

- Initialized admission storage is never silently recreated.
- Whole-DB corruption prevents socket bind.
- Store durability settings do not disable normal sync behavior.

## Rejected alternatives

- Auto-recreate-on-corruption behavior.
- Renaming or deleting a suspect DB and continuing.
- Running bbolt with `NoSync` for the admission authority.

## Consequences

Operators see a visible startup failure rather than a new empty authority that could reaccept old
request keys.

## Non-goals

This ADR does not handle single corrupt aggregate values; ADR-1C covers that case.
