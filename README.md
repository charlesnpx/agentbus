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
agentbus steer --job <id> --request-id <messageId> --message-file <path|-> [--json]
agentbus configure-codex-sandbox [--writable-root <absolute-path>]... [--json]
~~~

Job submission uses the typed job.submit protocol method. There is no CLI submit
command.

TaskSpec `inputMode` is `fixed` when omitted. Set it to `steerable` only when
the running task may receive follow-up instructions. `agentbus steer` submits
one caller-identified message from a file or stdin; `status --job` shows the
mode and text-free steer receipts. A steerable job retains one public job ID,
deadline, permission envelope, model, effort, and schema. Delivery may be
native or interrupt-and-continue depending on the adapter. Delivery uncertainty
is reported without automatic redelivery; a steerable task does not guarantee
that a provider follows the instruction or rolls back tools already started.
`agentbus steer` exits 16 for a rejected receipt and 17 when delivery is unknown.
The wire details and receipt reasons are in [docs/protocol.md](docs/protocol.md).

Set AGENTBUS_STATE_ROOT to select daemon state. Otherwise Agentbus uses
$XDG_STATE_HOME/agentbus or ~/.local/state/agentbus.

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
