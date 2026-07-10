# agentbus

agentbus is a local agent-execution service. One daemon multiplexes concurrent
sessions, turns, and jobs across backend CLIs such as Claude Code and Codex,
while the same core engine remains importable for embedded use.

agentbus is deliberately agent-agnostic. It owns process supervision, state,
leases, JSON-RPC framing, result storage, and policy enforcement hooks; clients
such as `delegate` supply concrete delegation workflows and contract text.

## Surfaces

- Wire protocol: [docs/protocol.md](docs/protocol.md) is the published JSON-RPC
  2.0 contract over a newline-delimited Unix socket, including state machine,
  exit-code, result-size, trust, and contract-validation semantics.
- Go client: `github.com/charlesnpx/agentbus/client` provides a typed socket
  client with daemon autostart and reconnect behavior.
- Go engine: `github.com/charlesnpx/agentbus/engine` provides the same job
  store, session lifecycle, leases, process supervision, and `TurnPolicy`
  machinery for daemonless embedding and tests.
- CLI/daemon: `cmd/agentbus` exposes setup, daemon, state inspection, result,
  cancellation, and validation commands.

## Install

agentbus is a delegated `mise-en-place` entry. In the registry it is expected to
start under `experimental:` as a private optional delegated repo:

```yaml
experimental:
  agentbus:
    repo: github.com/charlesnpx/agentbus
    channel: latest-release
    fallback_ref: main
    visibility: private
    optional: true
```

Install the tools target through mise-en-place:

```sh
mise-en-place install agentbus --target tools
```

The delegated installer stages and reports `~/.local/bin/agentbus`. It builds
the binary from source with Go, reports SHA-256 hashes on real installs, and
does not install Claude or Codex skills in v1. The release tag should be
`v$(cat VERSION)`; release tagging updates `VERSION`.

For local installer checks:

```sh
./install-skill.sh --plan --target all --json
./install-skill.sh --install --target tools --json --install-root /tmp/agentbus-stage
./install-skill.sh --uninstall --target tools --json --install-root /tmp/agentbus-stage
```

## CLI

```sh
agentbus version [--json]
agentbus setup [--json]
agentbus serve [--foreground]
agentbus sessions [--tags k=v] [--json]
agentbus status [--job <id>] [--json]
agentbus result --job <id> [--json]
agentbus cancel --job <id> [--json]
agentbus validate --contract <file|name> [--text-file <f>] [--json]
```

`agentbus setup` probes backend CLIs and writes a setup cache. Adapter preflight
later fails loudly if a backend binary path, version, or stream schema drifts
from that cache.

`agentbus serve --foreground` runs the JSON-RPC daemon in the current process.
`agentbus serve` starts it in the background. `AGENTBUS_STATE_ROOT` may be set
to isolate daemon state for tests or local development.

The status/result/cancel commands map terminal job states to the exit codes
defined in [docs/protocol.md](docs/protocol.md).

## Packages

Use the client package when a process should talk to the daemon:

```go
import "github.com/charlesnpx/agentbus/client"
```

Use the engine package when a process needs embedded execution without the
daemon boundary:

```go
import "github.com/charlesnpx/agentbus/engine"
```

Backend adapters live under `github.com/charlesnpx/agentbus/engine/adapter`.
The Claude Code and Codex CLI adapters implement the argv profiles and setup
drift checks described in [docs/adapters.md](docs/adapters.md).

## Development

```sh
go test ./...
go test -race ./...
scripts/release-check.sh v0.1.0
```

Set `GOCACHE` under `/private/tmp` or another writable cache root when running
inside restricted sandboxes.

## Roadmap / v0.2

- `stop-review-gate`: a Claude Code Stop-hook client of agentbus that gates a
  turn via `turn.start` with an ALLOW/BLOCK shape contract plus
  `policy.validate`, replacing the vendor `openai-codex` plugin's stop-review
  gate. Until this lands, `delegate setup` reports
  `stop-review-gate: not available (planned v0.2)`.
