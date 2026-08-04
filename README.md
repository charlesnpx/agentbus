# agentbus

agentbus is a local agent-execution service. One daemon runs strict identified
background jobs across backend CLIs such as Claude Code and Codex, while the
same core engine remains importable for embedded use.

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
start under `experimental:` as an optional delegated repo:

```yaml
experimental:
  agentbus:
    repo: github.com/charlesnpx/agentbus
    channel: latest-release
    fallback_ref: main
    visibility: public
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

## Release notes

- v0.4.0: a real direct tools install stops a matching `agentbus serve` daemon after replacing the binary. For staged/mise upgrades, the running daemon detects that its on-disk binary changed and exits at its next quiet moment; the next client autostarts the upgraded binary.

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
agentbus admission <inspect|recover|reset-empty-root|seal|clear-fail-stop> --state-root <path>
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

`agentbus serve` always starts strict identified admission under the shared
custody contract. macOS serves with process-group/held-parent supervision.
Linux serves with process-group supervision when cgroup v2 is unavailable, and
uses cgroup v2 as a preferred cleanup enhancement when a delegated writable
root is available. A host fails closed only when it cannot provide basic
controlled supervision: process groups, identity/start-token observation,
TERM/KILL/wait, and the controlled runner. Strict activation is sticky and
one-way for a state root: an activated root must keep serving under the current
admission contract version or be handled with `agentbus admission recover`,
`seal`, or `reset-empty-root`. The first strict release supports one active
state root; seal rotation starts a new root and does not route read, cancel,
result, or replay requests across old and new roots.

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
Backend authors can follow the normative adding-a-backend recipe in that guide.
Packages under `github.com/charlesnpx/agentbus/engine/execution/...` are
unstable implementation APIs for AB-E and are not an import-stability promise.

## Development

```sh
go test ./...
go test -race ./...
scripts/ci/release-check.sh
```

Set `GOCACHE` under `/private/tmp` or another writable cache root when running
inside restricted sandboxes.

## Roadmap

Planned work not yet shipped as of v0.6.0:

- `stop-review-gate`: a Claude Code Stop-hook client of agentbus that gates a
  review run submitted as an identified `job.submit` (protocol v2 removed the
  session/turn surface) with delegate-owned ALLOW/BLOCK report validation while
  agentbus records contract identity, replacing the vendor `openai-codex`
  plugin's stop-review gate. Until this lands, `delegate setup` reports
  `stop-review-gate: not available (planned v0.2)`.
