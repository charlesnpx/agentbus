# ADR-5B: GroupRef and containment proof

**Status:** Superseded by [ADR-14](../adr/ADR-14-simplified-core.md).

## Decision

`GroupRef` records schema version, custody id, launch key, host boot id, PGID, leader process identity,
monitor process identity, and any platform-retained handle. The parked worker leader PID is the
eventual backend PID. Death injection before fork, after fork before exec, after exec before
`BACKEND_STARTED`, and after `BACKEND_STARTED` before release recording must yield verified
quiescence or listener shutdown.

Recovery always starts from the durable per-ordinal `GroupRef`, never from an in-memory launch map.
The recovery matrix is:

- Different host boot id: old custody is absent; record `host_reboot` quiescence.
- Same boot, matching leader identity, group live: safe to signal, then poll until absent.
- Same boot, group absent: record `already_absent` quiescence.
- Same boot, leader reused or missing while group exists: unprovable; fail closed.
- Leader and monitor identity gone with unknown descendants: unprovable; fail closed.

## Invariant(s)

- PID reuse is fenced by high-resolution process identity.
- The process-group leader identity is bound to the group id (`Leader.PID == PGID` on POSIX).
- `kill(-pgid)` attempted is not a quiescence proof.
- Prior-boot durable refs normally recover to ready state; only genuinely unprovable evidence is fatal.
- All four real-process death points are modeled.

## Rejected alternatives

- macOS second-resolution start tokens as sufficient PID-reuse fencing.
- Treating signal attempt as proof of absence.
- Persisting only child PID after backend start.
- Depending on a live monitor as the correctness foundation.

## Consequences

Production tests must inject process death at every listed point and verify the recovery matrix.
S1 does not implement these probes or signals; the unavailable production custodian refuses enablement
until the platform-specific containment TCB can satisfy this matrix.

## Non-goals

This ADR does not choose the exact platform handle API.
