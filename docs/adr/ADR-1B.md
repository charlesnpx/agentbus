# ADR-1B: External admission-authority anchor

## Decision

A root `admission.anchor` file outside bbolt records `dbUUID`, `schemaMajor`,
`everInitialized`, and `highWaterGeneration`. Startup compares the anchor and DB: first
initialization is allowed only when both are absent, a missing DB with an existing anchor is fatal,
UUID mismatch is fatal, DB generation below the anchor is fatal, and a lagging anchor may be advanced
when the DB generation is higher.

Startup decision rows:

| DB | Anchor | Prior initialization | Decision |
| --- | --- | --- | --- |
| absent | absent | no | initialize first |
| absent | absent | yes | fatal |
| present and valid | absent | no | recover interrupted initialization |
| present and valid | absent | yes | recover missing anchor from initialized DB |
| absent | present and valid | any | fatal |
| present and valid | present and valid | yes | continue, advance lagging anchor, or fatal on UUID/schema/rollback mismatch |

## Invariant(s)

- DB deletion after initialization is detected and never recreated silently.
- DB-only rollback is detected by high-water generation.
- DB and anchor UUIDs must match.
- Init publishes the DB before the anchor and fsyncs the relevant directories.

## Rejected alternatives

- Keeping the sole authority marker inside the DB.
- Treating missing DB plus present anchor as first initialization.
- Detecting rollback when both the DB and anchor are restored together.

## Consequences

The admission authority is protected against DB-only loss or rollback, while whole-state-root rollback
remains documented unsupported.

## Non-goals

This ADR does not define production backup or operator recovery tooling.
