# ADR-1D: Execution command surface boundaries

## Decision

The execution model accepts an expanded durable command surface beyond the originally frozen
AdmissionStore sketch, but keeps coordinator-only phase tracking out of the AdmissionStore interface.
The store contract contains durable state transitions: supervisor identity recording, permit receipts,
containment evidence, retirement evidence, result publication intent, terminal publication with reason,
expiry, and corrupt quarantine.

Exec fork/exec/backend-start phase receipts and result-file side-effect phases remain coordinator or
harness concerns. They may be modeled to prove failpoint coverage and recovery edge distinctness, but
they are not public AdmissionStore API guarantees.

## Consequences

The frozen surface is treated as the minimum durable storage contract, not the complete coordinator
implementation inventory. New durable commands must still be single-boundary transitions with immediate
invariant checks and distinct failpoints where they represent a crash-sensitive mutation. Side-effect
phase tracking stays outside the store interface unless it becomes durable state.
