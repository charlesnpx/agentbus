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

## Supported argv profiles

The following profiles are the only supported v1 CLI shapes. Adapters MUST NOT
invent additional permission modes under protocol v1.

| Backend | Profile | Argv shape |
| --- | --- | --- |
| codex | write | `codex exec --json --sandbox workspace-write` |
| codex | read-only | `codex exec --json --sandbox read-only --ignore-user-config` |
| codex | corrective-resume | `codex exec --json --sandbox read-only --ignore-user-config resume <session-id> -` |
| claude | write | `claude --print --output-format stream-json --dangerously-skip-permissions` plus `--resume <session-id>` when resuming |
| claude | read-only | `claude --print --output-format stream-json --bare --strict-mcp-config --mcp-config {} --permission-mode dontAsk --allowedTools <claude-read-only-allow-list> --disallowedTools <claude-read-only-deny-list>` |
| claude | corrective-resume | claude read-only profile plus `--resume <session-id>` |

### codex write

```text
codex exec --json --sandbox workspace-write
```

Write turns run under the user's normal Codex configuration.

### codex read-only

```text
codex exec --json --sandbox read-only --ignore-user-config
```

Read-only turns MUST be hermetic and MUST ignore user configuration.

### codex corrective-resume

```text
codex exec --json --sandbox read-only --ignore-user-config resume <session-id> -
```

Corrective resume is always read-only. It is used for policy repair after an
invalid final result and MUST NOT inherit writable permissions from the original
turn. The trailing `-` is the resume subcommand's prompt argument and makes the
CLI read the corrective prompt from stdin.

### codex effort values

When `SessionOpts.Effort` is provided, the codex adapter passes it as:

```text
--config model_reasoning_effort="<effort>"
```

The default codex effort allow-list is `none`, `minimal`, `low`, `medium`,
`high`, and `xhigh`.

### claude write

```text
claude --print --output-format stream-json --dangerously-skip-permissions
```

When resuming:

```text
claude --print --output-format stream-json --dangerously-skip-permissions --resume <session-id>
```

Write turns run under the user's normal Claude configuration.

### claude effort values

When `SessionOpts.Effort` is provided, the claude adapter passes it as:

```text
--effort <effort>
```

The default claude effort allow-list is `low`, `medium`, `high`, and `max`.

### claude read-only

Base argv:

```text
claude --print --output-format stream-json --bare --strict-mcp-config --mcp-config {} --permission-mode dontAsk --allowedTools <allow-list> --disallowedTools <deny-list>
```

The read-only profile MUST NOT include `--dangerously-skip-permissions`.
The installed Claude CLI exposes tool allow/deny controls as
`--allowedTools`/`--allowed-tools` and
`--disallowedTools`/`--disallowed-tools`; agentbus uses the camelCase spellings
shown above. It also exposes `--permission-mode dontAsk`, which agentbus uses
as the print-mode fail-closed permission mode, and `--bare`, which minimizes
hooks, plugin sync, auto-memory, background prefetches, and similar
customization sources.

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

Read-only turns MUST be hermetic:

| Backend | Required read-only config behavior |
| --- | --- |
| codex | Pass `--ignore-user-config`. |
| claude | Pass `--strict-mcp-config` and exclude user hooks/plugins to the extent vendor flags allow. |

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
    "streamSchema": "codex-json-v1"
  }
}
```

## Drift guard

Adapters MUST run a live trivial-turn stream probe only during
`agentbus setup`. That probe may spend real API quota. It records the detected
backend version and stream schema in agentbus state.

Routine `Preflight` MUST NOT run a network turn. It checks:

- backend binary exists and is executable
- current backend version matches the setup cache
- current backend version is at least the adapter's minimum known-good version
- cached stream schema exists for the backend

If the version differs from the setup cache, `Preflight` MUST fail loudly with:

```text
backend version changed since setup; re-run agentbus setup
```

Each adapter MUST declare a minimum known-good version. The exact values are
pinned by adapter tests:

| Backend | Minimum known-good version |
| --- | --- |
| codex | `0.143.0` |
| claude | `2.1.205` |

The setup probe cache consumed by `Preflight` is internal agentbus state and has
this shape:

```json
{
  "version": 2,
  "backends": [
    {
      "backend": "codex",
      "binaryPath": "/Users/me/.local/bin/codex",
      "version": "0.143.0",
      "streamSchema": "codex-json-v1",
      "configMode": {
        "write": "user",
        "readOnly": "hermetic"
      },
      "sandboxModes": ["workspace-write", "read-only"],
      "jsonEventsProbed": true,
      "discoveredModels": ["gpt-5.4"],
      "discoveredEfforts": ["low", "medium", "high", "xhigh"],
      "discoverySource": "codex --help (listing syntax when exposed)"
    }
  ]
}
```

Model and effort discovery runs only inside `agentbus setup`, alongside the
live stream probe. Routine `Preflight` reads the versioned cache and never runs
a discovery command or network turn. A current non-empty discovery list wins
for adapter option validation. Missing or legacy discovery data falls back to
the adapter's static known-good list and emits a loud warning in setup output
and `Health.Warning`; discovery data alone never causes a hard failure.

Verified discovery surfaces for the installed CLIs:

| Backend | Verified source | Discovery status |
| --- | --- | --- |
| codex | `codex --help`; user config contains selected model and effort but help exposes no available-model or effort listing | Parser accepts future help listing syntax; current installed CLI returns no discovery, so static fallback is used. |
| claude | `claude --help` lists effort choices and documents model aliases/examples | Efforts and documented model aliases/examples are cached. These are help-advertised values, not an account-entitlement query. |
| gemini | `gemini --help` exposes `--model` but no model or effort listing | B1-ready discovery interface returns no listing; static fallback applies when the B2 adapter is added. |

`agentbus setup --json` exposes `discoveredModels`, `discoveredEfforts`, and
`warnings` per backend. `protocol.hello.backendMetadata` exposes the cached
arrays with capability `models.discovery`; the protocol major remains 1.

## A5 flag verification amendments

The installed CLIs verified for A5 reported:

| Backend | Documented flag/profile item | Installed CLI result | Amendment |
| --- | --- | --- | --- |
| codex | `exec --json` | present in `codex exec --help` | none |
| codex | `--sandbox read-only|workspace-write` | present in `codex exec --help` | none |
| codex | `--ignore-user-config` | present in `codex exec --help` and `codex exec resume --help` | none |
| codex | `exec resume <session-id>` | present as `codex exec resume [SESSION_ID] [PROMPT]`; `PROMPT` may be `-` to read stdin | pass `-` after `<session-id>` |
| codex | resume sandbox profile | `--sandbox` is present in `codex exec --help` but absent from `codex exec resume --help` | pass exec options before `resume` |
| claude | `--print` | present in `claude --help` | none |
| claude | `--output-format stream-json` | present in `claude --help` | none |
| claude | `--dangerously-skip-permissions` | present in `claude --help` | none |
| claude | `--resume <session-id>` | present in `claude --help` | none |
| claude | read-only tool allow/deny flags | real flags are `--allowedTools`/`--allowed-tools` and `--disallowedTools`/`--disallowed-tools` | use `--allowedTools` and `--disallowedTools` |
| claude | fail-closed print-mode permission behavior | real flag is `--permission-mode dontAsk` | add `--permission-mode dontAsk` |
| claude | hermetic customization minimization | real flag `--bare` exists; `--strict-mcp-config` exists | add `--bare --mcp-config {}` alongside `--strict-mcp-config` |

## Codex 0.144.1 JSON event mapping

For Codex 0.144.1, `codex --help`, `codex exec --help`, and
`codex exec resume --help` verify only the JSONL mode and argv shape. The
installed package's offline binary strings additionally expose the current event
names `agent_message`, `item_completed`, and `task_complete`, and show
`task_complete` / `TurnCompleteEvent` carrying `last_agent_message`.

The codex adapter maps:

| Codex event | agentbus event | Basis |
| --- | --- | --- |
| `agent_message` | `AgentText` | verified event name; text field names are parsed defensively |
| `item_completed` with a tool-call item | `ToolUse` | verified event name; nested item payload shape is defensive |
| `task_complete.last_agent_message` | `ResultMessage` | verified event name and `last_agent_message` field |

Older aliases such as `message`, `assistant_message`, `tool_use`, and `result`
remain accepted for compatibility.

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
