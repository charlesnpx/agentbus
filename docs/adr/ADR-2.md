# ADR-2: Current-boot coordinator

## Decision

The coordinator serializes admission by request key, owns OS side effects, and retains current-boot
ownership until terminalization completes. The store owns durable CAS transitions with
preconditions. Stale attempt, epoch, or launch-ordinal messages cannot mutate a newer attempt.

## Invariant(s)

- Store transitions are preconditioned CAS operations.
- The coordinator never drops active ownership after a failed state update.
- Foreground `turn.start` stays outside this coordinator.
- Current-boot nonterminal fenced jobs have a live obligation unless fail-stopping.

## Rejected alternatives

- Letting store methods perform opaque process inspection or signalling.
- Recovering from coordinator failure by continuing with stamped but ownerless work.
- Unconditional removal from active job tracking after update failure.

## Consequences

Production code must keep durable state and OS effects separated and make failure to terminalize a
daemon-level problem.

## Non-goals

This ADR does not define the bbolt schema; ADR-1 does.
