# ADR-5B: GroupRef and containment proof

## Decision

`GroupRef` records PGID, leader PID with high-resolution start token, known child refs, and any
platform-retained handle. The parked worker leader PID is the eventual backend PID. Death injection
before fork, after fork before exec, after exec before `BACKEND_STARTED`, and after
`BACKEND_STARTED` before `RecordStarted` must yield verified containment or listener shutdown.

## Invariant(s)

- PID reuse is fenced by high-resolution process identity.
- `kill(-pgid)` attempted is not a containment proof.
- All four real-process death points are modeled.

## Rejected alternatives

- macOS second-resolution start tokens as sufficient PID-reuse fencing.
- Treating signal attempt as proof of absence.
- Persisting only child PID after backend start.

## Consequences

Production tests must inject process death at every listed point.

## Non-goals

This ADR does not choose the exact platform handle API.
