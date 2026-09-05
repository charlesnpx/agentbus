# agentbus

Agentbus is a local generic job service for backend CLIs. It stores identified
jobs, supervises their process groups, and exposes a small JSON-RPC interface.

Version 0.13.1 serves protocol version 3. The published contract is
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
~~~

Job submission uses the typed job.submit protocol method. There is no CLI submit
command.

Set AGENTBUS_STATE_ROOT to select daemon state. Otherwise Agentbus uses
$XDG_STATE_HOME/agentbus or ~/.local/state/agentbus.

## Packages

The Go client package is github.com/charlesnpx/agentbus/client. The engine
package is github.com/charlesnpx/agentbus/engine. Backend adapter requirements
are documented in [docs/adapters.md](docs/adapters.md).

## Development

~~~sh
go build ./...
go test ./... -count=1
~~~
