# ADR-6A: Execution evidence and corrective launches

## Decision

Terminal proof is one of `NeverPermittedAndRetired`, `CleanQuiescentOutcomeAndRetired`, or
`Contained`. Corrective launch 2 uses a fresh one-use subpermit only after launch 1 child exit and
quiescence are proven. At most one launch ordinal may be active.

## Invariant(s)

- Launch ordinal 2 requires ordinal 1 quiescent.
- Corrective launch state persists attempt ID, owner epoch, launch ordinal, permit nonce, and permit
  state.
- Terminal states carry one of the approved proofs.

## Rejected alternatives

- Reusing a launch permit.
- Starting corrective launch 2 while launch 1 may still be active.
- Terminal outcomes without retirement or containment proof.

## Consequences

The model must track launch ordinals explicitly, even though production execution remains
fail-closed.

## Non-goals

This ADR does not make retries survive daemon restart.
