# agentbus

A local agent-execution service: one daemon multiplexing concurrent
sessions/turns/jobs across backend CLIs (Claude Code, Codex), plus an
importable engine. agentbus is agent-agnostic — it has zero delegation
opinions. It publishes three consumable surfaces:

1. **Wire protocol** (`docs/protocol.md`) — JSON-RPC 2.0, newline-delimited,
   over a Unix socket. Versioned, with a documented state machine, result-size
   semantics, and trust model.
2. **Go client package** (`github.com/charlesnpx/agentbus/client`) — typed
   wrapper over the socket, with daemon autostart and reconnect.
3. **Go engine package** (`github.com/charlesnpx/agentbus/engine`) — the same
   core, importable for embedded (daemonless) use and tests.

agentbus also defines a generic `TurnPolicy` mechanism (prologue, contract
validation, bounded retry, compliance stamping) with no built-in policy
opinions — clients such as [`delegate`](https://github.com/charlesnpx/delegate)
supply the concrete contracts.

This repo is currently private and pre-release. See
`~/tmp/agent-server-delegate-plan.md` (or the equivalent planning doc) for the
full design and delivery plan.

## Roadmap / v0.2

- **stop-review-gate**: a Claude Code Stop-hook client of agentbus that gates
  a turn via `turn.start` with an ALLOW/BLOCK shape contract plus
  `policy.validate`, replacing the vendor `openai-codex` plugin's stop-review
  gate (not replicated in v1). Until this lands, `delegate setup` reports
  `stop-review-gate: not available (planned v0.2)`.
