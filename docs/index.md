# agentbus docs

- [Wire protocol v2](protocol.md) - JSON-RPC framing, strict-only methods,
  result semantics, trust model, and policy-validation contract.
- [Operations runbook](operations.md) - first serve, restart/recovery,
  fail-stop clearing, seal/reset-empty, backup, rollback, and CI gates.
- [Backend adapter contract v1](adapters.md) - Go adapter interfaces, supported
  backend argv profiles, config hermeticity, drift guard, and capability flags.
