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
| codex | corrective-resume | `codex exec resume <session-id> --json --sandbox read-only --ignore-user-config` |
| claude | write | `claude --print --output-format stream-json --dangerously-skip-permissions` plus `--resume <session-id>` when resuming |
| claude | read-only | `claude --print --output-format stream-json --strict-mcp-config --tools Read,Grep,Glob,Bash` plus first-party write-tool denies and scoped Bash allow/deny rules |
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
codex exec resume <session-id> --json --sandbox read-only --ignore-user-config
```

Corrective resume is always read-only. It is used for policy repair after an
invalid final result and MUST NOT inherit writable permissions from the original
turn.

### claude write

```text
claude --print --output-format stream-json --dangerously-skip-permissions
```

When resuming:

```text
claude --print --output-format stream-json --dangerously-skip-permissions --resume <session-id>
```

Write turns run under the user's normal Claude configuration.

### claude read-only

Base argv:

```text
claude --print --output-format stream-json --strict-mcp-config --tools Read,Grep,Glob,Bash
```

The read-only profile MUST NOT include `--dangerously-skip-permissions`.

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
| `find*` |

Scoped Bash deny patterns:

| Pattern |
| --- |
| output redirects: `>` and `>>` |
| `sed -i*` |
| `tee*` |
| `rm*` |
| `mv*` |
| `cp*` |
| `git commit*` |
| `git push*` |
| `git checkout*` |
| `chmod*` |
| `curl*` |
| `wget*` |

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
pinned by adapter tests in merge unit A5 after the installed CLI surfaces are
verified.

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
