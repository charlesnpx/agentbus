# ADR-10: Coordinator ownership drives shutdown and replacement

**Status:** Superseded by [ADR-14](../adr/ADR-14-simplified-core.md).

## Decision

Idle shutdown and stale-binary replacement consult coordinator ownership. `HasOwnedWork()` and
`Shutdown(ctx)` block replacement until every owned job is terminal or every permitted process is
contained and terminal-committed.

## Invariant(s)

- Every internal nonterminal state blocks idle shutdown or replacement.
- Permitted processes must be contained and durably terminal before ownership is released.
- Shutdown cannot strand current-boot nonterminal work.
- Any error after supervisor preparation or after a successful permit CAS creates a post-authority
  recovery obligation: reload durable state, reconcile, signal containment, verify containment,
  record `Contained`, and publish a terminal proof, or fail-stop if proof cannot be established.
- Startup reconciliation must consume and persist the startup-anchor decision before entering
  `running`; fatal anchor decisions cannot be bypassed by an otherwise valid admission store.
- Terminal clean-completion proof is valid only for a consumed launch with durable exit,
  quiescence, and retirement evidence.

## Rejected alternatives

- Replacing the daemon while it owns nonterminal fenced work.
- Treating no active backend process as enough to drop ownership.
- Releasing ownership before terminal commit.

## Consequences

Operational replacement becomes lifecycle-aware and may be delayed by reconciliation.

## ADR-11 amendment

`AdmissionAuthority` owns `HasOwnedWork` and terminal-release semantics. Coordinator shutdown executes
authority plans rather than deriving release policy from private coordinator/store maps.

## Non-goals

This ADR does not implement durable execution handoff to a replacement daemon.
