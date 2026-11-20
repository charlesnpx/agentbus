# Deletion review: Agentbus 0.18

The review covered production code, tests, scripts, active documentation, and
the historical ADR index. Searches included sandbox, legacy, format 1,
protocol 3, read-only, verify, local IPC, GOPROXY, frozen, compatibility,
replay, migration, caches, aliases, and execution posture. The release uses a
fresh format-2 store; no historical job import or replay is required.

## Removed in this review

- The old `appendStopped` sidecar marker, its decoder branch, and its dedicated
  historical fixture. Incomplete current captures still report a gap when
  their completion receipt is absent; malformed records also report a gap.
- The old `--version`, `-version`, and `-V` command aliases. `agentbus
  version` is the canonical version command.
- The installer's fallback to vendored dependencies in older source trees; a
  failed readonly module build now reports the original build failure.
- Stale active documentation and misleading legacy labels on the retained
  subprocess and probe runners. ADR-14's conflicting instructions are marked
  historical and superseded by ADR-15.

The feature head already removed the older task decoder, old canonicalization
and replay exceptions, compatibility home modes, private build-cache and
migration traversal, execution-posture object, buffered command output, and
the sandbox-selection, verification-copy, and local-IPC features and their
feature-only tests. Format and request-shape checks reject unsupported input
rather than translate it.

## Retained and why

- Format-2 request replay and canonical hashing are required for idempotent
  admission, including after a workspace or daemon default changes. They do not
  decode or import earlier jobs.
- The public `client` aliases expose the current protocol types to consumers;
  they are the current API, not alternate old request shapes. Omitted
  `inputMode` means fixed under protocol 4.
- Owned-directory helpers and `WorkspaceLayout` provide path-safe Codex home,
  log, and result handling. They do not traverse old cache layouts. The public
  engine artifact helpers accept safe caller-provided opaque names with `job_`
  or `job-` prefixes; this performs no historical lookup or translation.
- Codex's provider fields named `sandbox` and `sandboxPolicy` explicitly
  request its full-access policy for both intents. Current adapter tests check
  those required provider messages, unrestricted Claude/Cursor behavior, and
  rejection of retired request fields.
- Verification of store ownership, process identities, result digests, socket
  locks, and Cursor's selected agent mode protects current operations; it is
  unrelated to the removed verification-copy feature. Read-only bbolt access in
  a test checks store contents.
- `GOPROXY=off` occurs in hermetic installer tests and CI helper-build setup,
  not the daemon's job environment. Test-only socket-sandbox detection reports
  host restrictions and does not select a provider execution policy.
- Direct command and probe runners implement current subprocess supervision and
  provider inspection. Protocol-mismatch and unsupported-format tests check
  refusal rather than compatibility. The `dev` version identifies an unversioned
  development build; release installation embeds the value from `VERSION`.
- Historical documents remain labeled as history, with ADR-15 as the current
  release decision. They are not a reason to retain executable old behavior.

## Checkout validation

On macOS arm64 with Go 1.27.3, the uncommitted Agentbus changes based on
`053fb02406bdf5964a77d28660d7cb058cbcefd8` produced these results:

| Command | Exit | Evidence |
| --- | --- | --- |
| `gofmt -l .` | 0 | No output. |
| `go build ./...` | 0 | All packages built. |
| `go vet ./...` | 0 | No diagnostics. |
| `go test ./...` | 1 | The environment's long default temporary path exceeded Unix socket path limits. |
| `TMPDIR=/private/tmp go test -count=1 ./...` | 1 | Short paths exposed sandbox `bind: operation not permitted` failures in client, daemon-launcher, and service socket tests; other packages passed. |

A focused service run executed and passed `TestJobTranscriptReportsGapAndMissingSidecar`,
`TestItemSidecarWriterStopsAtSmallFileCap`, and
`TestJobSubmitOmittedSessionRetentionPreservesReplayHash`. A separate run of
`./client ./internal/daemonlaunch` with `AGENTBUS_TEST_SANDBOX_BIND_DENIED=1`
passed the runnable tests and skipped ten blocked socket tests; an additional
helper test skips itself outside its subprocess. Four service socket tests
also failed in the full run. The opt-in skips are not evidence that those
socket checks passed. Consumer qualification, supported-host exercises, and a
fresh deployment remain separate release checks.

With the existing module cache supplied as `AGENTBUS_OFFLINE_MODCACHE`, a
focused installer run executed and passed plan output, staged install/uninstall,
staged non-restart, live-install warning, and malformed-flag tests. The staged
binary reported release 0.18.0 and protocol 4.
