# agentbus

Agentbus is a local generic job service for backend CLIs. It stores identified
jobs, supervises their process groups, and exposes a small JSON-RPC interface.

## Depends on / Depended on by

Agentbus depends on nothing in the delegate/convo-relay/witness set.

`delegate` imports `github.com/charlesnpx/agentbus/client`, pins a released
Agentbus version, and its installer invokes `agentbus configure-codex-sandbox`;
changes to those surfaces can break Delegate's build or installer.

`convo-relay` embeds `github.com/charlesnpx/agentbus/engine` and its adapters;
changes to those packages or their APIs can break its build.

Version 0.14.0 serves protocol version 3. The published contract is
[docs/protocol.md](docs/protocol.md). Operator details, including the required
state-root break for this release, are in
[docs/operations.md](docs/operations.md).

## CLI

~~~text
agentbus version [--json]
agentbus --help
agentbus backends [--json]
agentbus serve [--foreground]
agentbus status [--job <id>] [--tag <key=value>] [--state <state>] [--workspace-key <key>] [--limit <n>] [--before <cursor> | --all] [--json]
agentbus transcript --job <id> [--follow] [--kind <kind>] [--since <timestamp>] [--since-ordinal <n>] [--last <n>] [--limit <n>] [--json]
agentbus result --job <id> [--wait [--wait-timeout <duration>]] [--json]
agentbus cancel --job <id> [--json]
agentbus configure-codex-sandbox [--writable-root <absolute-path>]... [--json]
~~~

Help exits successfully without starting or contacting the daemon. Use
`agentbus -h`, `agentbus help`, or `agentbus status --help` for usage.

Status without `--job` prints the 50 newest matching jobs. JSON output includes
`truncated: true` and `nextCursor` when another page exists; text output prints
the continuation cursor on stderr. Continue with that cursor, or request full
history explicitly:

~~~sh
agentbus status --limit 50 --json
agentbus status --limit 50 --before '<nextCursor>' --json
agentbus status --all
~~~

`job.list` keeps its existing unbounded behavior for client callers that omit
`limit`; the CLI sends a limit by default. `--all` explicitly omits that limit.

`result --wait` observes the job with `job.get` until it becomes terminal or
the observation timeout expires. The default timeout is 30m. Expiry prints the
current record and exits 16; it does not cancel the job. Durations need explicit
units: `2400` is ambiguous, so use `2400s` or `40m`.

~~~sh
agentbus result --job "$JOB_ID" --wait --wait-timeout 30m
agentbus result --job "$JOB_ID" --wait --wait-timeout 2400s --json
~~~

`transcript --follow` pages by ordinal until the job is terminal and its
transcript pages are drained. `--json` prints one JSON object per item followed
by a summary object containing `gap`. Ctrl-C stops observation without
cancelling the job.

~~~sh
agentbus transcript --job "$JOB_ID" --follow --kind message
agentbus transcript --job "$JOB_ID" --follow --json
~~~

Singleton flags cannot be repeated (`--job a --job b` is a usage error).
Repeatable filters such as `--tag` and `--state` keep accepting multiple values.

Job submission uses the typed job.submit protocol method. There is no CLI submit
command.

Set AGENTBUS_STATE_ROOT to select daemon state. Otherwise Agentbus uses
$XDG_STATE_HOME/agentbus or ~/.local/state/agentbus.

`configure-codex-sandbox` adds Agentbus's state and cache roots to
`$CODEX_HOME/config.toml` (or `~/.codex/config.toml`), preserving existing TOML
comments and formatting. Repeat `--writable-root` to add another absolute root.

`agentbus backends` calls the read-only `backend.inspect` method. It reports
each configured backend's resolved executable path, version, and configured
default model/effort. JSON entries use `backend`, `executablePath`, `version`,
`defaultModel`, `defaultEffort`, and `error` when applicable. Each version
probe is one bounded `--version` call per
executable path and modification time for the daemon's lifetime; a failed
backend is reported in its own entry. It does not query accounts, billing, or
model catalogues. Use `--json` for the protocol response shape.

The daemon accepts intentional per-backend defaults through
`AGENTBUS_CODEX_DEFAULT_MODEL`, `AGENTBUS_CODEX_DEFAULT_EFFORT`,
`AGENTBUS_CLAUDE_DEFAULT_MODEL`, `AGENTBUS_CLAUDE_DEFAULT_EFFORT`,
`AGENTBUS_CURSOR_DEFAULT_MODEL`, and `AGENTBUS_CURSOR_DEFAULT_EFFORT`. An
explicit TaskSpec model or effort takes precedence. `modelRequested` and
`effortRequested` in job.get record only values submitted in the TaskSpec, so
they are omitted when the daemon supplied a default. `modelReported` and
`effortReported` contain the latest values the provider reported; they are not
filled from the request. **Requested is not executed**: provider telemetry is
the evidence of what was observed, and it may be absent.

Detailed job.get responses and `agentbus status --job <id>` expose `deadline`
and `remainingMs` for running jobs with a finite timeout, plus
`resumable: {eligible, reason}` derived from the existing session and retention
rules. **Eligible is not guaranteed**: the provider may still reject a resume.
`cancellationReason` records `operator`, `deadline`, `daemon_shutdown`, or
`provider_interruption` independently of the existing terminal state.

Failed job records keep their existing `failure.class` and `failure.reason` and
may add `providerCode`, bounded `providerMessage`, `stage`, and absolute
`retryAfter`. Current stages are `startup`, `turn`, and `finalize`. The failure
classes include `usage_limit` and `authentication`; Claude stream-json and
Codex app-server provide these only when their parsed provider error contains
a distinguishable signal. Cursor ACP preserves structured JSON-RPC error code
and message, but does not classify generic errors as usage or authentication
failures. Existing `provider_overloaded`, `model_unavailable`, and
`content_policy` classes remain. A transient failure may leave partial
workspace edits, so replay is the caller's decision and Agentbus does not
retry automatically. See [docs/protocol.md](docs/protocol.md) for field shapes,
provider signals, and the full method contract.

## Packages

The Go client package is github.com/charlesnpx/agentbus/client. The engine
package is github.com/charlesnpx/agentbus/engine. Backend adapter requirements
are documented in [docs/adapters.md](docs/adapters.md).

## Development

~~~sh
go build ./...
go test ./... -count=1
~~~
