# ADR-11: AdmissionAuthority boundary

**Status:** Superseded by [ADR-14](../adr/ADR-14-simplified-core.md).

## Decision

The coordinator/server-facing durable-operation contract is `AdmissionAuthority`, not
`AdmissionStore`. `AdmissionAuthority` is the single public policy boundary for admission, replay,
permit/cancel linearization, terminalization, readiness, cleanup ownership, and shutdown release.

Persistence is an internal transactional repository. The repository is record-oriented and policy-free:
it loads and commits immutable identity, monotonic safety facts, projections, runtime exclusions, and
diagnostics under authority commands, but callers cannot use repository methods as lifecycle policy.

Only proof-bearing final facts are durable. Crash-sensitive intermediate phases are effect
diagnostics, not externally replayed lifecycle commands. Final receipts that prove acceptance, grant,
containment, retirement, result publication, or terminalization are commands to `AdmissionAuthority`.

Terminal proof is authority-derived. Callers request terminalization with observed facts; the authority
derives the terminal certificate from committed safety records and rejects fabricated or unsupported
proof labels.

Safety history is monotonic. Grant history, permit-send/consume evidence, containment identity,
retirement evidence, and terminal certificates are append-only or no-op duplicate facts. Missing or
corrupt proof evidence never means "never permitted."

Readiness is both a capability and a durable boot precondition. Startup reconciliation must advance
the startup anchor, commit boot readiness, and return a non-forgeable `Ready` capability before
admission or socket binding can occur. Every admission transaction checks the durable boot-ready
precondition.

Projection corruption and safety-record corruption have different policies. A corrupt projection can
be recovered from valid proof records. Missing or corrupt proof records, or untrustworthy containment
identity, are fatal because permit certainty cannot be derived.

## Invariant(s)

- Coordinators and servers depend on `AdmissionAuthority`, not a memory-store implementation.
- Atomic acceptance commits request binding, proof/safety record, and projection in one root
  transaction.
- Rejected authority commands leave no partial durable mutation.
- Terminal proof is derived once from authority-owned facts.
- Safety facts are never cleared to make a later state easier to represent.
- No admission exists before `Ready`.

## Rejected alternatives

- Expanding the old `AdmissionStore` until it matches the coordinator implementation inventory.
- Treating concrete memory-store helpers as the durable architecture boundary.
- Letting callers supply terminal proof kinds.
- Inferring no permit from absent or corrupt safety evidence.
- Making one package responsible for persistence, process effects, and lifecycle policy.

## Consequences

"Single owner" means one public policy boundary backed by pure modules, not one package that performs
persistence and process effects. The coordinator remains responsible for OS/process side effects, but
interprets them through authority plans and commits final proof-bearing receipts back to the authority.

ADR-1D is superseded. Its frozen `AdmissionStore` surface and concrete-only phase-helper allowance are
no longer the target execution architecture.

## Non-goals

This ADR does not define the bbolt schema or wire the production server to the authority.
