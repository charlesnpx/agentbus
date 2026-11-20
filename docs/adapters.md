# Agentbus backend adapters

The `engine` package defines `Backend`, `Session`, `SessionOpts`, and
`TurnInput`. Every turn requires `engine.IntentInspect` or `engine.IntentEdit`.
The built-in common turn boundary prepends the corresponding instruction once
on a copy of the prompt. Resume and schema correction use the same boundary;
correction uses inspect intent, the same session, selection, and deadline.
Inspect is an instruction, not an enforced permission boundary: both intents
use the same host policy and can access ordinary caches, temporary files, and
local services. Host and provider restrictions may still apply.

A session's environment overlays inherited daemon values in this order:
adapter values, then `SessionOpts.EnvOverlay`. The overlay is copied when the
session is created. Embedded callers may set `CODEX_HOME` through it; they own
and serialize their session directories.

| Backend | Launch and turns |
| --- | --- |
| Codex | app-server with approval policy `never`, thread sandbox `danger-full-access`, and turn policy `dangerFullAccess` for both intents. |
| Claude | stream-json with `--dangerously-skip-permissions` for both intents; resume uses the observed session ID. |
| Cursor | ACP agent mode for both intents; an offered `allow_once` permission is selected while the turn is active. |

Model and effort are passed without alias rewriting. The provider decides
availability. Cursor requires the exact provider-advertised ACP `modelId` and
has no supported effort control. Agentbus does not infer reported values from a
request or from an empty selection acknowledgement. Provider preflight remains
in place; the declared minimum versions are Codex 0.143.0, Claude 2.1.205, and
Cursor 2026.08.04. Qualification requires actual provider binaries.

## Codex app-server event mapping

For `codex-appserver-v1`, the Codex adapter uses app-server JSON-RPC frames,
not one-shot CLI JSON events. The driver treats request responses and
notifications separately and maps only verified app-server methods into
agentbus events.

The codex adapter maps:

| Codex app-server frame | agentbus event | Basis |
| --- | --- | --- |
| `thread/start` or `thread/resume` response thread id | session ID | stable app-server resume identifier |
| `item/agentMessage/delta` | `AgentText` | incremental assistant text |
| `item/completed` with an agent or assistant message item | `AgentText` | completed assistant text |
| `item/started` with command, file-change, MCP, or dynamic-tool items | `Progress` | a tool that has begun advances the liveness clock and is retained pending by its item id; it does not add a second transcript item |
| `item/completed` with command, file-change, MCP, or dynamic-tool items | `ToolUse` | the matching item id clears its pending lifecycle and produces one transcript item; a completed frame without an observed start is still recorded |
| turn end with a pending command, file-change, MCP, or dynamic-tool item | `ToolUse` | pending items flush once before the completion outcome or any other turn return, including failed, empty-status, interrupted, and cancellation paths; a file-change flush remains a text-free `ObservedWorkspaceWriteItem` |
| `warning`, `error`, `config/warning`, or `guardian/warning` notification text | `Warning` | backend warning surfaces |
| `turn/completed` with status `completed` or empty status | `ResultMessage` | an explicit turn-level final text wins; otherwise the latest observed assistant item supplies its completion snapshot or accumulated deltas |
| `turn/completed` with status `failed`, unexpected interruption, or unsupported status | terminal error | the shared duplex runtime emits the driver error as a terminal error |

## Cursor ACP event mapping

For `cursor-acp-v1`, Cursor emits ACP `session/update` notifications while a
`session/prompt` request is active. Tool-call notifications are lifecycle
frames, not independently countable tool operations.

The pinned Cursor fixture establishes `in_progress` as a nonterminal status and
`completed` as a terminal one. It does not establish another terminal status,
so the adapter records only `completed` immediately and retains every other
status until prompt processing ends. That fallback preserves a tool observation
when Cursor finishes a prompt without a terminal tool-call frame.

| Cursor ACP frame | agentbus event | Basis |
| --- | --- | --- |
| `session/update` `agent_message_chunk` | `AgentText` | incremental assistant text |
| `session/update` `tool_call` or `tool_call_update` with a nonterminal status | `Progress` | a lifecycle frame advances the liveness clock without adding a second tool item |
| `session/update` `tool_call` or `tool_call_update` with the same `toolCallId` and status `completed` | `ToolUse` | fields from the lifecycle are correlated by id and produce exactly one item |
| a correlated tool-call frame whose `content` includes a `diff` block | `ToolUse` with `ObservedWorkspaceWriteItem` | the emitted event has no text; the persistence boundary retains no provider-supplied name, path, contents, or metadata |
| turn end, whether or not it ended successfully, after a correlated call has no recognized terminal frame | `ToolUse` | the pending call is flushed once before the prompt outcome is handled, preventing an unterminated lifecycle from being lost |
| `session/prompt` response with `stopReason: "end_turn"` | `ResultMessage` | result text is the concatenated assistant chunks |

## Implementing an adapter

Implement `Backend` and `Session`, preserve cancellation and final observations,
and validate required turn intent before launch. Keep stdout consumption and
stderr capture consistent with the command runner contract. Report provider
model and effort only from actual applicable telemetry. Transcript observations
are evidence of supported captured events, not proof that all provider actions
were reported.

Embedded command consumers must read stdout and stderr concurrently while the
process runs; pipe backpressure can block a child until both streams are read.
Embedded session consumers must drain the event channel through closure, including
after cancellation. Raw log paths are single-owner and must not be shared by
concurrent sessions.
