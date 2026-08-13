# agentbus backend adapter contract v1

This document defines the v1 contract between the agentbus engine and backend
CLI adapters. It is a contract for adapter authors and tests; it is not the
wire protocol. See `docs/protocol.md` for the socket API.

Exact vendor flag names are verified in merge unit A5. If the installed vendor
CLIs differ, this document is amended in A5 and tests are pinned to the amended
contract.

## Go interfaces

The public engine adapter surface is:

```go
type Backend interface {
	Name() string
	Preflight(ctx context.Context) (Health, error)
	Start(ctx context.Context, opts SessionOpts) (Session, error)
	Resume(ctx context.Context, id string, opts SessionOpts) (Session, error)
}

type Session interface {
	ID() string
	Turn(ctx context.Context, input TurnInput) (<-chan Event, error)
	Interrupt(ctx context.Context) error
}
```

`TurnInput` carries the effective per-turn `Write` value. `SessionOpts.Write`
is only the session default. A corrective resume MUST be represented as a turn
with `TurnInput.Write == false`, even when the session default is writable.

The engine owns job state, process supervision, result spilling, validation,
and policy retry decisions. Adapters own CLI argv construction, process launch,
stream parsing, session-id extraction, and interruption of their active backend
process.

CLI-backed adapters run through the shared duplex runtime. A backend with a
bidirectional provider protocol supplies a `duplex.Driver`; the older
build-argv/parse-JSONL shape is wrapped as a trivial one-shot driver.

## Supported argv profiles

The following profiles are the only supported v1 CLI shapes. Adapters MUST NOT
invent additional permission modes under protocol v1.

| Backend | Profile | Argv/process shape |
| --- | --- | --- |
| codex | write | spawn `codex app-server`; start or resume an app-server thread and send a writable turn request |
| codex | read-only | spawn `codex app-server`; start or resume an app-server thread and send a read-only turn request |
| codex | corrective-resume | spawn `codex app-server`; `thread/resume <session-id>` followed by a read-only `turn/start` request |
| cursor | write | spawn `cursor-agent [--model <id>] acp`; create or load an ACP session, set mode `agent`, then prompt |
| cursor | read-only | spawn `cursor-agent [--model <id>] acp`; create or load an ACP session, set mode `plan`, then prompt |
| cursor | corrective-resume | cursor read-only profile plus ACP `session/load <session-id>` before setting mode `plan` |
| claude | write | `claude -p --input-format stream-json --output-format stream-json --verbose --dangerously-skip-permissions` plus `--resume <session-id>` when resuming |
| claude | read-only | `claude -p --input-format stream-json --output-format stream-json --verbose --strict-mcp-config --mcp-config '{"mcpServers":{}}' --permission-mode dontAsk --allowedTools <claude-read-only-allow-list> --disallowedTools <claude-read-only-deny-list>` |
| claude | corrective-resume | claude read-only profile plus `--resume <session-id>` |

### codex write

```text
codex app-server
```

Codex turns use JSON-RPC over JSONL stdio. The driver sends `initialize`,
then `initialized`, then `thread/start` for a fresh session or `thread/resume`
for a resumed session, then `turn/start`. Progress arrives as `item/*`
notifications and semantic completion arrives as `turn/completed`.

Write turns set the app-server turn request's sandbox to workspace-write with
approval policy `never` and network access disabled. When a CWD is available,
it is also used as the writable root.

### codex read-only

```text
codex app-server
```

Read-only turns use the same app-server lifecycle, but the turn request's
sandbox is read-only, approval policy is `never`, and network access is
disabled. Agentbus does not pass legacy exec-mode sandbox or user-config flags
to Codex; isolation is represented in the app-server thread and turn requests.

### codex corrective-resume

```text
codex app-server
```

Corrective resume is always read-only. It is used for policy repair after an
invalid final result and MUST NOT inherit writable permissions from the original
turn. The app-server driver resumes the existing thread id and sends a
read-only `turn/start` request.

All Codex profiles spawn the app-server with `SessionOpts.CWD` as the process
working directory when one is supplied. The app-server thread and turn requests
carry the requested working directory and model when provided; the turn request
also carries the prompt, reasoning effort, sandbox, and approval policy.
`Interrupt` sends `turn/interrupt` for the active thread and turn before the
shared runtime falls back to process interruption.

When served by the daemon, Codex additionally receives a job-private
`CODEX_HOME` under the workspace state namespace. Agentbus links only the
operator home’s `auth.json` and `config.toml` into it; it never copies
credentials. `AGENTBUS_CODEX_HOME` selects a fixed absolute replacement home
and `AGENTBUS_CODEX_HOME_INHERIT=1` disables isolation. The opt-out wins over
the fixed override.

### Backend JSONL frame limit

Backend stdout uses newline-delimited JSON with a default maximum frame size of
32 MiB. This accommodates normal Codex `fileChange` diffs and command-output
events while preserving a hard bound. Set `AGENTBUS_BACKEND_JSON_LINE_BYTES` to
a positive integer byte count to override it. An absent, invalid, zero, or
negative value uses the 32 MiB default; it never disables the limit.

When a backend frame exceeds the limit, agentbus discards that frame through
its newline and resumes at the next frame. It retains at most 4 KiB in memory
only to classify the envelope; it does not persist that payload. The terminal
record keeps only the discarded-frame count, total bytes, and a short redacted
`method=` or `type=` summary when the bounded prefix establishes one. Codex
turn-terminal frames (`turn/completed` and `task_complete`) and unclassified
or unknown oversized frames fail the turn rather than being skipped, because a
prefix is evidence of a frame type, not proof that no result was carried.

### codex effort values

When `SessionOpts.Effort` is provided, the codex adapter includes it in the
app-server turn request's reasoning effort field.

The default codex effort allow-list is `none`, `minimal`, `low`, `medium`,
`high`, and `xhigh`.

### cursor ACP

```text
cursor-agent [--model <id>] acp
```

The Cursor adapter resolves `cursor-agent` first and falls back to `agent` when
the former is unavailable. It never passes `--mode`: ACP mode is selected per
session. Each process performs ACP v1 `initialize` with file-system and
terminal client capabilities disabled, authenticates with `cursor_login`, then
uses `session/new` or `session/load`. A loaded session's replay updates are
discarded before the load response; they are never treated as output for the
new turn.

Writable turns select Cursor mode `agent`; read-only and corrective-resume
turns select `plan`, including when the resumed session was previously
writable. The adapter verifies `session/set_mode` before prompting. It answers
only ACP's qualified `session/request_permission` reverse request: writable
turns choose an offered `allow_once` option and read-only turns choose an
offered `reject_once` option. It never selects an `*_always` option. Other
server requests fail explicitly as JSON-RPC method-not-found.

`Interrupt` sends ACP `session/cancel` for the active session. A resulting
`stopReason: "cancelled"` is clean only after that interrupt was requested.
Assistant chunks are emitted live and concatenated into the final result on an
`end_turn` completion. Cursor does not expose an effort control through this
adapter; specifying one is rejected before launch.

The adapter reports the resolved ACP `models.currentModelId`, rather than
echoing a requested model flag. Setup uses a no-prompt ACP qualification
(`initialize`, `authenticate`, `session/new`, and verified `session/set_mode`)
in a temporary working directory. Model discovery additionally parses the
small `cursor-agent models` listing after its `Available models` header; an
empty parsed catalog is valid and explicit model IDs pass through.

### claude write

```text
claude -p --input-format stream-json --output-format stream-json --verbose --dangerously-skip-permissions
```

When resuming:

```text
claude -p --input-format stream-json --output-format stream-json --verbose --dangerously-skip-permissions --resume <session-id>
```

Write turns run under the user's normal Claude configuration. The driver uses
Claude's bidirectional stream-json input protocol: it sends a control-protocol
`initialize` request, sends one user-message envelope, answers supported
`control_request` messages, and treats the `result` message as authoritative
turn completion. A `result` with subtype `success` and no error flag emits the
result text; other result subtypes or `is_error` values emit terminal errors.

### claude effort values

When `SessionOpts.Effort` is provided, the claude adapter passes it as:

```text
--effort <effort>
```

The default claude effort allow-list is `low`, `medium`, `high`, and `max`.

### claude read-only

Base argv:

```text
claude -p --input-format stream-json --output-format stream-json --verbose --strict-mcp-config --mcp-config '{"mcpServers":{}}' --permission-mode dontAsk --allowedTools <allow-list> --disallowedTools <deny-list>
```

The read-only profile MUST NOT include `--dangerously-skip-permissions`.
The installed Claude CLI exposes tool allow/deny controls as
`--allowedTools`/`--allowed-tools` and
`--disallowedTools`/`--disallowed-tools`; agentbus uses the camelCase spellings
shown above. It also exposes `--permission-mode dontAsk`, which agentbus uses
as the print-mode fail-closed permission mode. Claude 2.1.x `--bare` is not
used because live verification showed that it strips API authentication and
ends the turn with `terminal_reason=api_error` and exit status 1.
Claude accepts MCP configuration as JSON strings, and the installed CLI's
schema requires the top-level `mcpServers` record even when no servers are
configured.

The read-only profile MUST allow only these first-party tools:

| Tool | Status |
| --- | --- |
| `Read` | allowed |
| `Grep` | allowed |
| `Glob` | allowed |
| `Bash` | allowed, scoped by the command patterns below |

The read-only profile MUST deny these tool families:

| Tool or pattern | Status |
| --- | --- |
| `Edit` | denied |
| `Write` | denied |
| `NotebookEdit` | denied |
| `mcp__*` | denied |

Scoped Bash allow patterns:

| Pattern |
| --- |
| `git diff*` |
| `git log*` |
| `git show*` |
| `git status*` |
| `cat*` |
| `rg*` |
| `grep*` |
| `ls*` |
| `head*` |
| `tail*` |
| `wc*` |

`find*` is intentionally not allowed. `find` can mutate the workspace with
flags such as `-delete` and `-exec`, while `rg`, `ls`, `Glob`, and `Grep`
cover read-only discovery.

Scoped Bash deny patterns:

| Pattern |
| --- |
| `Bash(*&&*)` |
| `Bash(*&*)` |
| `Bash(*;*)` |
| `Bash(*\|*)` |
| `Bash(*$(*)` |
| ``Bash(*`*)`` |
| `Bash(*<(*)` |
| `Bash(*>*)` |
| `Bash(*>>*)` |
| `Bash(sed -i*)` |
| `Bash(tee*)` |
| `Bash(find*)` |
| `Bash(rm*)` |
| `Bash(mv*)` |
| `Bash(cp*)` |
| `Bash(git -c*)` |
| `Bash(git --config-env*)` |
| `Bash(git --paginate*)` |
| `Bash(git -p*)` |
| `Bash(git *--help*)` |
| `Bash(*--output*)` |
| `Bash(*--ext-diff*)` |
| `Bash(*--textconv*)` |
| `Bash(*--pre*)` |
| `Bash(*--hostname-bin*)` |
| `Bash(*--search-zip*)` |
| `Bash(* -z*)` |
| `Bash(git commit*)` |
| `Bash(git push*)` |
| `Bash(git checkout*)` |
| `Bash(chmod*)` |
| `Bash(curl*)` |
| `Bash(wget*)` |

Claude help documents allow/deny entries using the `Bash(...)` command pattern
form with wildcard examples such as `Bash(git *)`, but does not formally define
anchoring. agentbus therefore uses leading and trailing wildcards for redirect
and shell composition denies so the deny pattern is expressed as a
contains-style match in the same CLI pattern language. The `Bash(*\|*)` deny
subsumes the older pipe-to-`tee` special cases. `Bash(*&*)` closes the matching
single-ampersand command terminator/backgrounding case.

The Git deny patterns intentionally over-block options that can write output
files or execute configured helpers even when attached to otherwise read-only
commands. `--output` writes files for `git diff`, `git show`, and `git log`;
`--ext-diff` and `--textconv` can execute external diff or conversion helpers;
`git -c` and `git --config-env` can inject values such as `diff.external` or
`core.pager`; and `git --paginate`, `git -p`, and `git *--help*` can execute a
pager or help viewer. These are denied as contains-style Bash patterns rather
than parsed per subcommand so later allow-list edits do not reopen the same
class of hole.

The ripgrep deny patterns block options that execute helper commands:
`--pre`, `--hostname-bin`, and compressed-search helpers via `--search-zip` or
its short `-z` form. Standard `cat`, `grep`, `ls`, `head`, `tail`, and `wc`
options do not expose comparable write-or-exec switches in the audited command
families; they remain covered by the shell composition, redirect, and mutating
command denies above.

Default permission mode in `-p` / `--print` mode MUST fail closed. A command
that is not allowed MUST be denied rather than prompting interactively.

Honesty note: this read-only profile blocks known first-party write tools and
known unsafe Bash patterns. It is not an OS sandbox. Shell-level bypasses may
exist, and same-user processes retain normal OS access unless the backend CLI
itself enforces stronger isolation.

### claude corrective-resume

Corrective resume uses the full claude read-only profile plus:

```text
--resume <session-id>
```

Corrective resume is always read-only. It is used for policy repair after an
invalid final result and MUST NOT inherit writable permissions from the original
turn.

## Config hermeticity

Read-only turns MUST apply the strongest backend-supported read-only profile:

| Backend | Required read-only config behavior |
| --- | --- |
| codex | Use the app-server read-only sandbox, approval policy `never`, and disabled network access in the turn request's sandbox policy. No legacy exec-mode config-isolation flag is passed. |
| cursor | Use ACP mode `plan`, which permits read tools and blocks edits. Cursor user configuration still loads for both write and read-only ACP turns. |
| claude | Pass `--strict-mcp-config` with an empty `mcpServers` record. MCP servers are excluded, but full settings isolation is unavailable because Claude 2.1.x `--bare` strips API authentication; user settings and hooks may still load on read-only turns. |

Write turns run under user configuration because real work may legitimately
need the user's MCP servers, hooks, plugins, credentials, and skills.

`agentbus setup --json` MUST report these fields per backend:

```json
{
  "backend": "codex",
  "binaryPath": "/Users/me/.local/bin/codex",
  "version": "1.2.3",
  "configMode": {
    "write": "user",
    "readOnly": "hermetic"
  },
  "sandboxModes": ["workspace-write", "read-only"],
  "jsonEventsProbe": {
    "ran": true,
    "version": "1.2.3",
    "streamSchema": "codex-appserver-v1"
  }
}
```

## Drift guard

Adapters MUST run setup-time stream qualification only during
`agentbus setup`. Generic adapters use a live trivial-turn stream probe, which
may spend real API quota. Codex app-server uses a driver-backed setup
qualification instead: it starts `codex app-server`, completes the
`initialize`/`initialized` handshake, and calls `model/list` without sending a
user prompt. Setup records the detected backend version and stream schema in
agentbus state.

Routine `Preflight` MUST NOT run a network turn. It checks:

- backend binary exists and is executable
- current backend version matches the setup cache
- current backend version is at least the adapter's minimum known-good version
- cached stream schema exists for the backend

If the version differs from the setup cache, `Preflight` MUST fail loudly with:

```text
backend version changed since setup; re-run agentbus setup, then retry the launch so the running daemon can re-probe the refreshed cache; restart the daemon if it is running an older agentbus binary
```

Each adapter MUST declare a minimum known-good version. The exact values are
pinned by adapter tests:

| Backend | Minimum known-good version |
| --- | --- |
| codex | `0.143.0` |
| cursor | `2026.08.04` |
| claude | `2.1.205` |

Current stream schema identifiers are:

| Backend | Stream schema |
| --- | --- |
| codex | `codex-appserver-v1` |
| cursor | `cursor-acp-v1` |
| claude | `claude-streamjson-v2` |

The setup probe cache consumed by `Preflight` is internal agentbus state and has
this shape:

```json
{
  "version": 3,
  "backends": [
    {
      "backend": "codex",
      "binaryPath": "/Users/me/.local/bin/codex",
      "version": "0.143.0",
      "streamSchema": "codex-appserver-v1",
      "configMode": {
        "write": "user",
        "readOnly": "hermetic"
      },
      "sandboxModes": ["workspace-write", "read-only"],
      "jsonEventsProbed": true,
      "discoveredModels": ["gpt-5.4"],
      "discoveredEfforts": ["low", "medium", "high", "xhigh"],
      "discoverySource": "app-server",
      "discoveryFetchedAt": "2026-07-11T12:00:00Z",
      "discoveryClientVersion": "0.143.0"
    }
  ]
}
```

Model and effort discovery is captured into the setup probe cache. Codex
discovers models during app-server setup qualification by calling `model/list`
with hidden models excluded and following cursors until the listing is complete.
The adapter caches non-hidden model identifiers and the union of supported
reasoning efforts, ordered as `none`, `minimal`, `low`, `medium`, `high`,
`xhigh`, `max`, `ultra`, then first-seen unknown levels. A repeated cursor, an
empty usable model list, or a `model/list` error fails Codex setup
qualification.

Claude discovery remains local help scraping: the adapter runs `claude --help`
and parses documented model aliases/examples and effort choices. Help-scraped
values are not an account-entitlement query.

For generic discovery hooks, discovery failure does not by itself make a
backend unavailable. A requested model or effort outside a discovered catalog
emits a warning and is passed through to the backend. Only explicitly
configured static allow-lists reject a selection before the backend starts.
Missing discovery data falls back to those static known-good lists with a
warning.

Verified discovery surfaces for the installed CLIs:

| Backend | Verified source | Discovery status |
| --- | --- | --- |
| codex | app-server `model/list` during setup qualification | Non-hidden model identifiers and supported reasoning efforts are cached from the app-server response. |
| cursor | ACP `session/new` plus best-effort `cursor-agent models` parsing | ACP's available/current model identifiers are cached during no-prompt qualification; the CLI listing is a small best-effort catalog and may be empty. |
| claude | `claude --help` lists effort choices and documents model aliases/examples | Efforts and documented model aliases/examples are cached. These are help-advertised values, not an account-entitlement query. |
| gemini | `gemini --help` exposes `--model` but no model or effort listing | B1-ready discovery interface returns no listing; static fallback applies when the B2 adapter is added. |

`agentbus setup --json` exposes `discoveredModels`, `discoveredEfforts`,
`discoveryFetchedAt`, `discoveryClientVersion`, and `warnings` per backend.
`protocol.hello.backendMetadata` exposes the cached arrays with capability
`models.discovery`; the protocol major remains 1. When a driver-backed backend
has empty live discovery during serve-time probing, validation and metadata are
hydrated from the matching setup cache entry if the binary path, version, and
stream schema still match.

## A5 flag verification amendments

The installed CLIs verified for A5 reported:

| Backend | Documented flag/profile item | Installed CLI result | Amendment |
| --- | --- | --- | --- |
| codex | `app-server` process | adapter launches `codex app-server` and speaks JSON-RPC over JSONL stdio | use the duplex app-server driver |
| codex | app-server handshake | `initialize` response followed by `initialized` notification | required before thread, turn, or discovery requests |
| codex | app-server turn lifecycle | `thread/start` or `thread/resume`, then `turn/start`; `turn/interrupt` cancels the active turn | map session id, prompt, CWD, model, effort, sandbox, and approval policy through app-server requests |
| codex | setup model discovery | app-server `model/list` is called during `SetupQualify` | cache discovery source `app-server`; do not read an external model cache file |
| cursor | ACP process | `cursor-agent [--model <id>] acp` is a hidden ACP subcommand | use ACP JSON-RPC over JSONL stdio; do not pass `--mode` |
| cursor | ACP lifecycle | v1 `initialize`, `authenticate(cursor_login)`, `session/new` or `session/load`, verified `session/set_mode`, then `session/prompt` | map write to `agent`, read-only to `plan`, and discard load replay updates before collecting the new turn |
| cursor | setup qualification | no-prompt ACP handshake and session mode verification | run in a temporary working directory; require a usable mode and clean process retirement |
| claude | `-p` stream-json input mode | present in `claude --help` as print mode with `--input-format stream-json` | send control-protocol initialization and one user-message envelope on stdin |
| claude | `--output-format stream-json` | present in `claude --help`; the installed CLI requires `--verbose` with print-mode streaming | add `--verbose` |
| claude | `--dangerously-skip-permissions` | present in `claude --help` | none |
| claude | `--resume <session-id>` | present in `claude --help` | none |
| claude | read-only tool allow/deny flags | real flags are `--allowedTools`/`--allowed-tools` and `--disallowedTools`/`--disallowed-tools` | use `--allowedTools` and `--disallowedTools` |
| claude | fail-closed print-mode permission behavior | real flag is `--permission-mode dontAsk` | add `--permission-mode dontAsk` |
| claude | hermetic customization minimization | `--strict-mcp-config` exists; `--mcp-config` accepts JSON strings; live Claude 2.1.x runs with `--bare` fail authentication, while the same profile without it succeeds | exclude MCP servers with `--strict-mcp-config --mcp-config '{"mcpServers":{}}'`; do not use `--bare`, and document that user settings/hooks can still load |

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
| `item/started` or `item/completed` with command, file-change, MCP, or dynamic-tool items | `ToolUse` | app-server tool/progress item types |
| `warning`, `error`, `config/warning`, or `guardian/warning` notification text | `Warning` | backend warning surfaces |
| `turn/completed` with status `completed` or empty status | `ResultMessage` | result text is the last completed agent message, a turn-level last-agent message, or the last agent delta |
| `turn/completed` with status `failed`, unexpected interruption, or unsupported status | terminal error | the shared duplex runtime emits the driver error as a terminal error |

## Adding a backend

This is the normative recipe for adding a backend adapter. A backend profile
MUST ship only when its argv and stream behavior have been verified against an
installed binary. Documentation, remembered flags, or another project's
integration are not sufficient evidence. If no candidate CLI is installed,
document the recipe or discovery surface instead of shipping a speculative
profile.

### 1. Implement the engine surfaces

Implement `Backend` and `Session` as described in [Go interfaces](#go-interfaces).
The implementation MUST keep responsibilities at the existing boundary:

- `Backend.Name` returns the stable protocol name. `Preflight` performs only
  local availability and drift checks. `Start` and `Resume` validate options
  and create sessions; `Resume` MUST reject an empty backend session id.
- `Session.ID` returns the backend id extracted from the stream. `Turn` launches
  exactly one backend process turn and emits normalized events. `Interrupt`
  sends the backend-native interrupt request when supported, falls back to
  process interruption, and is a no-op after the process has exited.
- argv construction MUST use the effective `TurnInput.Write`, not only the
  `SessionOpts.Write` default. A corrective resume MUST therefore downgrade to
  the complete read-only profile even when the original session was writable.
- The adapter MUST NOT take over job state, policy validation or retry
  decisions, result spilling, or workflow semantics owned by the engine.

Implement the B1 discovery or setup-qualification hooks in addition to
`Backend`: expose `ModelDiscoverer.DiscoverModels(context.Context)` for local
discovery surfaces, or a driver-backed setup qualifier when the backend
protocol itself is the evidenced discovery surface. Return `ModelDiscovery`
with models, efforts, provenance, and warnings as available, and cache the
result in `BackendSetupProbe`. For generic discovery hooks, missing or failed
discovery MUST leave empty lists plus an explicit warning and MUST NOT make the
backend unavailable. A driver-backed setup qualifier MAY fail setup when it
cannot produce the required setup facts. Selections outside a discovered set
MUST warn and pass through; only an explicit static allow-list may reject them.
Metadata advertised by `protocol.hello` comes from setup cache or matching
cache hydration and MUST NOT launch a network turn.

### 2. Verify and pin argv profiles

Record the installed binary version and preserve receipts from its `--help`,
subcommand help, protocol fixtures, offline strings, and hermetic fake runs as
applicable. Pin golden process argv and protocol-input tests for write,
read-only, fresh, and resume forms. Verify option placement for every
subcommand rather than assuming top-level flags remain valid after a resume
verb.

Every read-only profile MUST be fail-closed and MUST satisfy this hardening
checklist:

- default-deny unknown commands or tools; a denied action MUST NOT fall back to
  an interactive prompt
- allow only audited read-only tool families and command prefixes, while
  explicitly denying first-party write tools and untrusted extension/MCP tools
- express shell separators and substitution as contains-style denies, including
  `&&`, `;`, `|`, `$()`, backticks, and `<()`; also deny backgrounding and input
  or output redirection where the CLI's pattern language can express them
- deny commands and options that can write or execute helpers even when their
  base command appears read-only; examples include `find -delete`/`-exec`,
  output-file flags, external diff or text-conversion hooks, injected config,
  pagers/help viewers, preprocessors, and compressed-search helpers
- test deny-pattern matching against the installed CLI's actual pattern
  semantics, using leading and trailing wildcards when anchoring is not
  specified
- include an honesty note stating that the profile blocks known tools and
  patterns but is not an OS sandbox, and that shell bypasses or same-user OS
  access may remain

These requirements are minimums, not a reusable Claude-specific allow-list.
Each vendor's tool and option surface MUST be audited independently.

### 3. Make read-only configuration hermetic

Read-only and corrective turns MUST exclude user configuration, hooks, plugins,
MCP servers, auto-memory, and other customization sources to the strongest
extent supported by verified vendor flags. Supply an empty explicit config when
the CLI otherwise discovers user services. If the CLI cannot provide a
credible hermetic mode, do not advertise the profile as read-only; document the
limitation and stop the adapter from shipping. Writable turns MAY use normal
user configuration when their profile explicitly permits it.

### 4. Normalize the stream and terminal result

The adapter MUST parse the CLI's real streaming format incrementally, reject
malformed records, and map backend events to `AgentText`, `ToolUse`, `Warning`,
`ResultMessage`, and `TerminalError`. It MUST extract a stable backend session
id from any documented start or event envelope, retain the first valid id, and
use it for resume. When a backend init/configuration event carries a model, the
adapter MUST capture that non-empty model as best-effort reported-model data.
It MUST distinguish progress text from the authoritative
terminal result and map the latter to exactly the `ResultMessage` consumed by
engine result selection. Parse errors, unsupported terminal shapes, and
non-zero process exits MUST surface as terminal errors rather than successful
empty results.

Do not infer schemas from an old adapter or generic JSON mode. The
[Codex app-server event mapping](#codex-app-server-event-mapping) is the drift
lesson: provider request/response frames, notifications, item payloads, and
terminal completion semantics are backend-specific. Capture representative
installed-binary events without API spend where possible, parse defensively
only around verified shapes, and pin those fixtures in tests.

### 5. Wire setup, versions, and drift guards

Declare and test a minimum known-good CLI version and a version normalizer.
Wire the backend into `agentbus setup` so its setup qualification records the
resolved binary path, normalized version, stream schema, configuration modes,
sandbox modes, JSON-event success, and discovery result or warning. Generic
adapters may use a live trivial-turn probe; driver-backed adapters may provide
their own setup qualification when that is the evidenced discovery surface.
Setup is the only place a live probe may spend API quota.

Routine `Preflight` MUST remain local: resolve the executable, reject versions
below the minimum, and compare binary path, version, and exact stream-schema
identifier with the versioned setup cache. Missing cache data or any drift MUST
fail with an actionable instruction to rerun setup. Register the adapter in the
CLI/daemon backend list only after setup reporting, cache migration behavior,
and `protocol.hello` metadata are wired and tested.

### 6. Build a hermetic fake-backend test suite

Use a temporary executable and isolated setup cache; tests MUST NOT require the
real vendor CLI, user configuration, network, or API quota. At minimum, cover:

- golden process argv and protocol input for write, read-only, resume, and corrective-resume;
  assert that corrective resume downgrades a writable session to the complete
  read-only and hermetic profile
- representative stream fixtures, session-id extraction, authoritative
  terminal-result mapping, malformed JSON, unsupported terminal shapes, and
  non-zero exits
- timeout behavior and explicit interrupt, including termination of the whole
  process group (PGID) rather than only the direct child
- event and log truncation with the truncation marker and flag preserved
- unsupported model and effort rejection, discovered-cache validation, missing
  discovery fallback, and stale-cache warnings
- minimum-version rejection, binary/version/schema drift, setup-probe output,
  and discovery parsing from captured fake help or backend-protocol output

Run `go test -race ./...` and `go vet ./...` after registration. A backend is
not complete until the tests can falsify its permission profile, process
supervision, parser, resume behavior, and drift guard.

## Native structured output capability

Adapters MAY detect backend-native structured output support:

| Backend | Candidate vendor flag | agentbus capability |
| --- | --- | --- |
| codex | `--output-schema` | `nativeStructuredOutput.codex` |
| claude | `--json-schema` | `nativeStructuredOutput.claude` |

These capabilities are surfaced in `protocol.hello.capabilities`. They are a
future optimization only. Protocol v1 structural compliance uses post-hoc
validation of the persisted final result as defined in `docs/protocol.md`.

## Adapter responsibilities

Adapters MUST:

- construct argv using only the supported profiles above
- parse backend JSON event streams into agentbus `Event` values
- extract and preserve backend session ids for resume
- honor per-turn `Write`
- launch corrective resumes with the read-only profile
- surface backend unavailability as `backend_unavailable`
- surface malformed or unsupported backend streams as adapter errors
- support `Interrupt` for the active backend process

Adapters MUST NOT:

- perform policy validation
- decide whether policy retry is allowed
- hash or truncate final results
- mutate job state directly outside engine APIs
- add workflow-specific semantics
