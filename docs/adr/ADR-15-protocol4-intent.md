# ADR-15: Protocol 4, explicit intent, and owned conversations

## Decision

Agentbus 0.18.0 and its coordinated consumers use protocol 4 with exact version
matching and fresh store format 2. There is no compatibility, downgrade,
historical replay, migration, or data-import layer for earlier releases. A
canonical state-root owner and boot, directory, and PID-namespace scoped process
claims guard recovery against accidentally copied or stale claims. Current
format-2 requests retain their durable idempotent replay behavior.

Every task and turn supplies inspect or edit intent. Both intents use one host
execution policy, with an instruction prefixed once at the adapter boundary.
Inspect is a work instruction, not an enforcement or credential boundary.
The caller owns workspace isolation for concurrent editing.

Agentbus inherits normal tool caches, temporary paths, and networking, with
deliberate session overlays applied last. It does not create private build
caches, frozen workspaces, or IPC profiles. The daemon creates an owned Codex
home for each root conversation and reuses it for serialized continuations.
Initial credentials and configuration come from the daemon's source Codex
home; earlier conversation history is not imported.

Completed root conversations are retained when requested and when a provider
session ID is available; unsuccessful roots with a session ID and roots with
uncertain cleanup are retained under the lifecycle rules. Retained homes are
not pruned by age. An operator can explicitly discard an inactive, clean
conversation, revoking continuation for its entire lineage before removing
the owned home. Automatic retention applies only to diagnostic logs and
transcripts, not results, records, request bindings, or provider assets.

Invocation selection is committed with the starting marker before launch.
Requested values remain in the immutable task; reported values are independent
provider observations.

## Supersession

This decision supersedes the conflicting protocol, sandbox, cache, home, and
retention requirements in [ADR-14](ADR-14-simplified-core.md) and earlier
historical ADRs. No additional release decision is delegated to an earlier ADR.
The [protocol](../protocol.md), [operations](../operations.md), and
[deployment](../deployment-0.18.md) documentation describe the active contract.
