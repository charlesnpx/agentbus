# Deferred feature: session forking (fork_turns-equivalent) for delegated jobs

STATUS: DEFERRED — not scheduled. Do not implement before the AB-D strict-native ledger completes
(R7B; see `docs/history/abd-roadmap.md`). Recorded 2026-07-21 so the design intent is not lost.

## What

A `delegate fork` verb that spawns a new supervised job whose backend session begins with a COPY of a
parent job's conversation history up to a chosen turn boundary, then diverges:

```
delegate fork --job <parent-job-id> --at-turn <N> --message "<divergence instruction>" [usual task flags]
```

Equivalent in shape to Codex multi-agent v2's `spawn_agent` + `fork_turns` (spawn-time history
inheritance), but implemented at the harness boundary instead of inside the agent loop, so every forked
child remains a full agentbus job with its own custody, admission, and lifecycle guarantees.

## Why

- Tightly-coupled parallel exploration (N children diverging from one shared reasoning prefix) is the
  one composition pattern the current cold-start handoff-packet flow cannot express.
- Doing it at the delegate layer keeps the property the in-agent version lacks: verifiable lifecycle
  (exactly-once admission, contain-and-verify termination, crash recovery) plus auditable lineage.

## Mechanism sketch

Forking is a PAYLOAD concern, orthogonal to custody. Both supported backends already persist and resume
the required artifact:

- codex: session rollout JSONL under `~/.codex/sessions/...`; resume support exists (delegate already
  exposes `--resume-session`). Fork = snapshot rollout at a turn boundary -> rewrite session identity ->
  launch a normal job resuming from the copy. N children = N copies. Prefer a backend resume/fork API
  where offered; file splice is the fallback.
- claude: `claude --fork-session` provides resume-under-new-session-id natively.

Turn boundaries are clean cut points in both formats. A still-running parent can be forked by
snapshotting its transcript mid-flight (the fork sees a prefix; the parent continues unaffected).

## Provenance (required if implemented)

Forked lineage must be first-class in job metadata, extending the existing origin/depth tags:
`fork.parent_job`, `fork.at_turn`, `fork.parent_transcript_sha256`. A fork whose parent transcript
cannot be hashed/identified at spawn time must fail closed, not launch with unknown inheritance.

## Known risks / why deferred

1. **Format coupling (primary).** Backend transcript formats are undocumented internals; version bumps
   can break file-level splicing, and any move toward encrypted session content kills it for that
   backend entirely. Mitigation: capability-probe per backend; refuse (typed error) rather than degrade.
2. **Token cost.** Each child replays the inherited prefix on every API call. Provider prefix caching
   blunts this for identical prefixes, but wide fan-outs of long parents are expensive and the cost is
   visible at our layer.
3. **Secret hygiene.** Parent history flows to every child. The verb needs redact/cut-above-turn
   controls; handoff packets currently provide this by construction and forks give it up.
4. **Not live sharing.** Children cannot see each other's ongoing reasoning (same limitation as the
   in-agent equivalent). Cross-child communication is a separate message-bus problem and explicitly out
   of scope for this feature.

## Non-goals (now)

- No implementation, no CLI stub, no schema reservation before R7B.
- No live shared context between concurrent children.
- No cross-backend forking (fork a codex session into a claude child or vice versa).
