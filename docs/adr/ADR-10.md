# ADR-10: Coordinator ownership drives shutdown and replacement

## Decision

Idle shutdown and stale-binary replacement consult coordinator ownership. `HasOwnedWork()` and
`Shutdown(ctx)` block replacement until every owned job is terminal or every permitted process is
contained and terminal-committed.

## Invariant(s)

- Every internal nonterminal state blocks idle shutdown or replacement.
- Permitted processes must be contained and durably terminal before ownership is released.
- Shutdown cannot strand current-boot nonterminal work.

## Rejected alternatives

- Replacing the daemon while it owns nonterminal fenced work.
- Treating no active backend process as enough to drop ownership.
- Releasing ownership before terminal commit.

## Consequences

Operational replacement becomes lifecycle-aware and may be delayed by reconciliation.

## Non-goals

This ADR does not implement durable execution handoff to a replacement daemon.
