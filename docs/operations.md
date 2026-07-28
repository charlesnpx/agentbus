# Agentbus Operations Runbook

This runbook covers the AB-E strict-only daemon at protocol v2, storage schema
v2, and admission contract version 2. Production serving is supported on macOS
and Linux under the shared custody contract. macOS serves with
process-group/held-parent supervision. Linux serves with process-group
supervision when cgroup v2 is unavailable, and uses cgroup v2 as a preferred
cleanup enhancement when a delegated writable root is available.

## 1. First Serve On A Supported Host

Use `agentbus serve` for the normal background daemon or `agentbus serve
--foreground` when a supervisor should own the process. Production serve always
starts strict identified admission; there is no `--admission` mode switch.

State root resolution is:

1. The CLI uses `AGENTBUS_STATE_ROOT` when it is set.
2. Go clients use `client.Options.StateRoot`, which controls token lookup,
   PID/autostart coordination, and launcher state, and defaults independently
   through the standard resolution below. `client.Options.SocketPath` overrides
   only the Unix socket endpoint — it does not change the state root, so a
   socket in another root still authenticates and coordinates under
   `StateRoot`.
3. Otherwise the default root is `$XDG_STATE_HOME/agentbus`, or
   `~/.local/state/agentbus` when `XDG_STATE_HOME` is unset.

On a supported first serve, agentbus creates the state root with owner-only
permissions, creates or reuses the token file `token`, initializes the authority
database `admission.bbolt`, writes the external authority anchor
`admission-anchor.json`, and binds `agentbus.sock`. A background launch through
`agentbus serve` or client autostart writes `agentbus.pid` only after the
readiness handshake succeeds. A direct `serve --foreground` process does not
itself promise a PID file.

Client autostart is a launcher behavior. A client first attempts to connect and
hello the configured socket. If the connection failure is autostartable, it
serializes startup for that state root, launches the configured `agentbus`
binary as `serve --foreground`, waits for a private readiness pipe, verifies any
already-listening daemon by hello, and then reconnects.

## 2. Restart And Recovery

A normal restart is to stop the serving daemon cleanly and let the next client
or `agentbus serve` start it again against the same state root. Startup is a
hard readiness barrier: the daemon opens the authority root, verifies the
anchor, begins the boot, reads metadata, runs startup recovery before it reports
ready, and only then installs the authority/coordinator for new work.

Crash recovery is driven by durable authority records. Recovery work consists of
nonterminal safety records and launch records that must be quiesced,
terminalized, or finalized before the daemon becomes ready. If an execution may
have occurred and absence cannot be established, the job becomes terminal
`orphaned` when no outcome was recorded, or keeps its recorded terminal outcome
with `cleanupDisposition=unresolved`. Those terminal unresolved jobs are not
recovery obligations and startup must not relaunch them. They remain durable
history and replay normally for the same `(workspaceKey, requestId)`.

A successful graceful shutdown means there is no live custody the daemon can
still act on and no retryable recovery obligation. It may still leave terminal
jobs whose cleanup disposition is `unresolved`. Inspection must show
`recoveryObligations=0`; historical jobs, bindings, and launch records may
still be present.

Use `agentbus admission recover --state-root <root>` when a root needs
daemonless recovery without opening a listener. It builds a recovery-only server
with strict native containment, requires an existing initialized DB and matching
anchor, refuses missing, nonregular, zero-length, mismatched, or incompatible
authority files, and exits without serving clients. JSON recovery reports
include `orphanedJobs`, `unresolvedLaunches`, and `cleanupWarnings`; a nonzero
`orphanedJobs` count is job-local cleanup uncertainty, not a global fail-stop by
itself.

## 3. Clear-Fail-Stop

A persisted fail-stop is an anchor record with phase `fail_stopped`, a boot
reference, and a reason. It is sticky: normal Begin, SealReady, Advance, and
VerifyReady paths refuse to move it back to ready or reconciling.

Protocol client commands (`status`, `result`, `cancel`, and other protocol
clients) return `11` for launcher-based daemon startup failures, except
launcher-side authority fail-stop, which returns `12`. In-band root fail-stop,
corruption, and identity-mismatch conditions also return `12`. Direct
`agentbus serve` and `serve --foreground` startup errors return `1`; a serve
shutdown-deadline overrun returns `13`.

`agentbus admission clear-fail-stop --state-root <root>` requires
`--acknowledge-unsafe-diagnosis`. It opens the repository writable, audits it,
loads the required anchor snapshot using the repository identity, clears only an
anchor whose phase is `fail_stopped`, and returns the post-clear inspection. It
does not repair corruption and it must not be used as a substitute for diagnosis.

## 4. Seal And Reset-Empty

Seal permanently closes the old authority domain for audit and starts a new
state root. It requires all acknowledgement flags and a new state-root path. It
refuses roots with live or retryable recovery obligations and refuses unsafe or
foreign destinations. Terminal `orphaned` or unresolved-cleanup jobs do not
block sealing once there is no active recovery work. A sealed root is not a
multi-root router: read, cancel, result, and replay are not routed across old
and new roots.

`reset-empty-root` is only for an empty authority root. The empty proof is the
authority count set: jobs, bindings, tombstones, launch records, and recovery
obligations must all be zero. Terminal `orphaned` and unresolved-cleanup jobs
still count as jobs, bindings, and launch history, so reset refuses a root that
contains them even though they are not recovery obligations. If a DB exists,
reset opens it, verifies any existing initialized anchor's identity (an absent
anchor is permitted for an empty DB), inspects counts, and refuses non-empty
roots. If the anchor exists without the DB, reset refuses instead of creating a
new root over it.

## 5. Supported And Unsupported Environments

macOS and Linux are supported serving environments. Linux cgroup v2 is a
preferred cleanup enhancement, not a serving prerequisite or a permanent root
identity component. A single root may contain mixed cgroup-backed and
process-group-backed launch history.

Unsupported strict environments fail closed only when the host cannot provide
basic controlled process supervision: process groups, identity/start-token
observation, TERM/KILL/wait, and a controlled runner. The production operator
path through client/launcher autostart reports daemon startup failure as exit
code `11`.

## 6. Corruption Response

The SafetyLatch is a first-wins, one-way in-memory fail-stop signal. Safety
significant repository corruption trips it. Safety significant record classes
are `db_uuid`, `meta`, `safety`, `binding_index`, `binding`, and `tombstone`;
unknown corruption kinds are treated as safety significant. Projection and
quarantine findings are not safety significant by class.

When the SafetyLatch trips in a running daemon, the listener is closed, the
owned socket path is removed, active connections/work are drained for the safety
deadline, and the daemon exits fail-stopped. Clients may see `root_corrupt` or
`root_fail_stopped` depending on where the failure is observed. Startup audit or
anchor corruption is detected before binding readiness; launcher-side startup
failures are reported as daemon startup failure unless the failure is an
authority fail-stop.

Operator response: stop using the root, inspect it read-only with
`agentbus admission inspect --state-root <root>`, preserve both authority files,
and do not hand-edit either file. Restarting does not erase a fail-stop; the
anchor keeps the fail-stop sticky until explicit clear-fail-stop succeeds.

Projection-only corruption is different. Startup can quarantine a broken
projection and reconstruct it from the safety record. That self-repair is
limited to projection state derived from committed safety records; safety,
binding, tombstone, metadata, DB UUID, and binding-index corruption remain fatal.

## 7. Contract Version And Rollback

Admission contract version 2 marks the ADR-13 custody contract change. There is
no in-place migration from activated contract-version-1 roots. A v2 daemon
opening an activated v1 root fails closed before socket bind with a typed
incompatible-contract-version error and leaves the files untouched. Operator
recovery is to seal the old root and serve the successor root stamped at the
daemon's current contract version.

Candidate, unactivated v1 roots refuse activation typed. If they are empty,
`reset-empty-root` can recreate them as fresh contract-version-2 candidates; a
seal operation also stamps the successor at version 2.

Binary rollback is safe only in the refusal sense: do not edit authority files
to make an older binary run. Current binaries opening an existing authority DB
reject schema versions other than v2 with typed incompatible-schema errors
before serving. A historical v1 binary from `4e0bd50` was checked against a v2
empty root: `admission inspect` exited `1` with
`agentbus: repository invalid record: meta is corrupt`, not with the newer typed
schema wrapper. That is still a refusal, not a migration path.

Do not start an old v1 binary against a missing production root during rollback.
That code had open-or-initialize behavior and can create schema-v1 state. The
rollback procedure is binary-only: restore the intended binary and let the
normal startup audit decide whether the existing v2 root is usable.

## 8. Backup Policy

Offline-only, minimal policy:

1. Shut the daemon down cleanly.
2. Confirm `recoveryObligations=0` with `agentbus admission inspect`.
3. Copy `admission.bbolt` and `admission-anchor.json` as one unit.
4. Restore both files together.
5. Start normally and let the usual audit plus identity verification run on
   next serve.

Hashes are recommended operationally for the copied pair, but hashes are not a
protocol artifact. Manual or live compaction is unsupported in AB-E. Do not
modify or independently replace either authority file.

There is no online backup command, no backup bundle format, and no manifest.

## 9. CI And Release Gates

The committed CI scripts are under `scripts/ci/`:

- `solo-battery.sh` checks gofmt, build, vet, strict-tag vet, full tests, and
  Linux amd64/arm64 cross-builds; it can add full race tests with
  `SOLO_BATTERY_RACE=1`.
- `docker-cgroup-v2.sh` runs the privileged Linux cgroup-v2 lane in the pinned
  Go container. It performs strict preflight, partitioned builds/tests, race,
  cgroup conformance, and strict E2E.
- `product-e2e.sh` builds one CLI binary, smokes that exact binary, then runs
  strict product E2E against it.
- `release-check.sh` combines the strict-capable container, full tests, strict
  preflight, exact release binary build, smoke, startup status round-trip, and
  strict production tests.
- `fail-closed.sh` runs the macOS supported process-group custody lane, the
  restricted-Linux process-group fallback serving lane, and typed fail-closed
  checks for genuine no-basic-supervision and incompatible contract cases.
- `vuln.sh` installs the pinned `govulncheck` version and scans `./...`.

The merge criterion is remote-green on the exact candidate SHA once GitHub
Actions billing is restored. The privileged strict-lane artifact is retained by
the staged workflow and is required evidence for the candidate SHA. Proposed
workflows are staged in `scripts/ci/github-workflows-proposed/`; installing them
means copying the selected YAML files into `.github/workflows/` after explicit
user approval.

## 10. Install Caveat

The installer builds `cmd/agentbus` from source with `go build -mod=readonly`.
It reports a Go executable prerequisite for tool installs, writes the tool
binary, and reports its SHA-256 on install. It only falls back to `-mod=vendor`
when a local `vendor/` directory exists.

No vendor tree is tracked in this checkout, and the module requires Go 1.26 plus
external modules. A clean install therefore needs network access or a
pre-populated Go module cache containing the required modules.
