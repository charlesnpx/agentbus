# ADR-1D: Execution command surface boundaries

**Status:** Superseded by [ADR-11](../adr/ADR-11-admission-authority.md).

## Decision

The execution model preserves the frozen AdmissionStore interface. The public store contract remains
limited to admission/replay, lookup/listing, acknowledgement/rejection, supervisor identity recording,
permit grant, cancellation, started/outcome recording, result publication intent, terminal publication,
and expiry.

Begin-reject, permit-send receipt, launch-exit evidence, containment phases, retirement phases,
exec fork/exec/backend-start phase receipts, corrupt quarantine, and result-file side-effect phases are
coordinator or harness internal model steps. They may be represented by concrete memory-store helpers to
prove failpoint coverage and recovery edges, but they are not AdmissionStore interface methods.

## Consequences

The frozen surface is the durable storage contract, not the complete coordinator implementation
inventory. Internal phase helpers must remain typed to the concrete harness/store and must still be
single-boundary transitions with immediate invariant checks and distinct failpoints where they represent
a crash-sensitive mutation.

## Supersession

ADR-11 replaces this boundary. Concrete-only phase helpers are forbidden as an architecture boundary.
Crash-sensitive intermediate phases remain effect diagnostics, while final proof-bearing receipts are
commands to `AdmissionAuthority`.
