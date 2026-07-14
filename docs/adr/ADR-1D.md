# ADR-1D: Expanded execution command surface

## Decision

The execution model accepts the expanded durable command surface beyond the originally frozen
AdmissionStore sketch. The additional commands are refinements that make recovery boundaries explicit:
supervisor identity recording, permit send receipts, exec/start phase receipts, containment evidence,
retirement evidence, result publication receipts, terminal publication with reason, expiry, and corrupt
quarantine.

Harness-only phase tracking remains separate from the production-facing command contract. It may be used
to prove failpoint coverage and recovery edge distinctness, but it is not a public storage API guarantee.

## Consequences

The frozen surface is treated as the minimum contract, not the complete storage command inventory. New
durable commands must still be single-boundary transitions with immediate invariant checks and distinct
failpoints where they represent a crash-sensitive mutation or side effect.
