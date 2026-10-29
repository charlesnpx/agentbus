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
agentbus serve [--foreground]
agentbus status [--job <id>] [--tag <key=value>] [--state <state>] [--workspace-key <key>] [--json]
agentbus transcript --job <id> [--kind <kind>] [--since <timestamp>] [--since-ordinal <n>] [--last <n>] [--limit <n>] [--json]
agentbus result --job <id> [--json]
agentbus cancel --job <id> [--json]
agentbus configure-codex-sandbox [--writable-root <absolute-path>]... [--json]
~~~

Job submission uses the typed job.submit protocol method. There is no CLI submit
command.

Set AGENTBUS_STATE_ROOT to select daemon state. Otherwise Agentbus uses
$XDG_STATE_HOME/agentbus or ~/.local/state/agentbus.

Writable jobs get private Go build, temporary, npm, Corepack, pnpm, and Yarn
caches under their managed state directory, outside the repository. Codex
workspace-write jobs can write there through the OS sandbox's managed writable
root. Agentbus resolves the daemon's `GOMODCACHE` once (from the daemon
environment, or `go env GOMODCACHE` with a short timeout) and leaves jobs
pointing at that populated host cache for reads; it never replaces it with an
empty per-job directory. The Codex sandbox allows reading the host module cache
on macOS and Linux while keeping it outside the writable roots. If no cache
path resolves, Agentbus leaves `GOMODCACHE` at its default. Codex jobs set
`GOPROXY=off`, so a missing module fails with Go's own offline lookup error
instead of repeated DNS retries. `-mod=readonly` only restricts module-file
changes; it is not a filesystem sandbox.

Inspect jobs receive no job-private scratch or writable caches: current
read-only backend modes do not enforce a writable root outside the submitted
workspace. `job.get` and `agentbus status --job <id>` expose the execution
posture recorded at start, including intent, source write mode, scratch/cache
paths, network and Git metadata policy, and enforcement strength. Codex uses
its OS sandbox, Cursor uses agent-mode permissions, and Claude has no filesystem
or network sandbox. `sourceWritable` reflects the selected inspect/edit mode;
`enforcement` identifies whether that mode has an OS sandbox, agent permissions,
or no filesystem boundary.

`configure-codex-sandbox` adds Agentbus's state and cache roots to
`$CODEX_HOME/config.toml` (or `~/.codex/config.toml`), preserving existing TOML
comments and formatting. Repeat `--writable-root` to add another absolute root.

## Packages

The Go client package is github.com/charlesnpx/agentbus/client. The engine
package is github.com/charlesnpx/agentbus/engine. Backend adapter requirements
are documented in [docs/adapters.md](docs/adapters.md).

## Development

~~~sh
go build ./...
go test ./... -count=1
~~~
