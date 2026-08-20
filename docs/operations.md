# Agentbus operations

This runbook covers Agentbus 0.13.0 and protocol version 3 on macOS and Linux.
The daemon supervises backend process groups and records cleanup as a separate
clean-or-uncertain value.

## Required break before upgrading

0.13.0 cannot read any pre-0.13.0 state root. There is no compatibility shim
and no state migration. Before using 0.13.0, stop any daemon using the old root,
preserve anything you need outside it, and delete that pre-0.13.0 state root.
Start 0.13.0 with a new empty root.

Do not point 0.13.0 at an old root expecting inspection, import, repair, or
conversion. Old records are unreadable to this release.

## State root

Set AGENTBUS_STATE_ROOT to select a root. Without it, the default is
$XDG_STATE_HOME/agentbus, or ~/.local/state/agentbus when XDG_STATE_HOME is
unset. Go clients can instead set client.Options.StateRoot.

The core layout is:

~~~text
<state-root>/
  token
  agentbus.sock
  agentbus.pid
  jobs.db
  artifacts/
    logs/
    results/
~~~

token authenticates protocol.hello. agentbus.sock exists while the daemon is
serving. agentbus.pid is written for a successful background launch or
client-autostart; do not rely on it for a foreground daemon. jobs.db is the
single bbolt store. It has exactly the meta, requests, and jobs buckets.
artifacts contains log and result sidecars. There is no automatic artifact
retention sweep: result and log artifacts remain until an operator removes
them, so the state root grows until it is cleaned up.

A Codex job may also create an implementation-managed private home below
workspaces/<workspace-hash>/codex/<job-id>. It is separate from the shared job
store and may contain per-job caches.

The root and token are owner-only. Do not hand-edit jobs.db, token, or
artifacts while a daemon is using the root.

## Starting and selecting backends

Use agentbus serve to start a background daemon. Use agentbus serve --foreground
when a supervisor owns the process. The public CLI commands are version, serve,
status, result, and cancel.

There is no standalone setup command or backend probe. Admission checks only
that the requested backend name is registered in the daemon's backend map. A
registered backend that cannot run fails when the daemon starts its session,
with the applicable failure class. A hello response describes configured
backends; it does not establish that a backend can start a session.

Submitting work is a typed job.submit request. The CLI intentionally has no
submit subcommand. Use a compatible client to prepare compound identity and
send that request.

## Restart and recovery

Startup opens the job store and reconciles it before the socket becomes ready.
Recovered work is never launched again:

- A recovered queued job becomes failed with failure class internal.
- A recovered running job becomes unknown.
- A terminal record is preserved.

For a recovered process claim, the daemon signals a process group only after
the recorded start token exactly matches the live process. It records cleanup
as uncertain when cleanup cannot be established. Cleanup uncertainty does not
erase a recorded completed result.

## Reading jobs and results

agentbus status lists compact job summaries. With --job, its human-readable
projection omits result text, while `--json` emits the full job.get record.
That JSON is byte-identical to `agentbus result --json` and deliberately
includes inline result text when the record has it.

agentbus result --job <id> reads the same full record. For a completed job it
writes the authoritative result to standard output. It writes failure detail or
the absence of a result to standard error for other states. When it must read a
result artifact, it verifies both its byte count and recorded digest.

agentbus cancel --job <id> sends job.cancel, then reads the full record to
choose the appropriate exit status.

## Exit codes

For a selected job, the documented exit status is computed from state, failure
class, and schema compliance:

| Code | Condition |
| --- | --- |
| 0 | completed and no evaluated noncompliant schema verdict |
| 2 | queued, running, or CLI usage error |
| 3 | completed with an evaluated noncompliant schema verdict |
| 4 | failed other than timeout or interrupted, including an empty failure class |
| 5 | failed with timeout |
| 6 | failed with interrupted |
| 7 | canceled |
| 10 | unknown job id |
| 11 | daemon startup failure |
| 13 | shutdown deadline exceeded |
| 14 | unknown |
| 15 | completed but the authoritative result artifact is missing, unreadable, or disagrees with its recorded digest |

Codes 8, 9, and 12 are permanently retired. Do not reuse them.

## Store failures

The daemon opens an existing database through a fault-isolated preflight before
binding its socket. A corrupt, truncated, incompatible, or busy database stops
startup rather than being repaired in place. Preserve the root for diagnosis;
do not create a replacement database at the same path unless you have first
performed the required version-break cleanup above.

## Current footprint and residual risks

At the final sweep, excluding vendor, Agentbus has 15,272 production Go lines
and 13,819 test Go lines: reductions of 40,166 and 50,875 from the
55,438-production-line and 64,694-test-line baseline. It has 17 Go packages.
The largest production file is `engine/adapter/codexcli/appserver_driver.go`
at 1,032 lines, and the largest test file is
`engine/adapter/internal/duplex/session_test.go` at 1,880 lines.

If the daemon dies mid-run, a provider process can remain orphaned. Recovery
marks recovered running work unknown and never relaunches it; the reaper signals
a recorded process group only when its live start token exactly matches the
recorded token. This is the deliberate trade-off for preventing duplicate work.

Result and log artifacts remain until an operator removes them. Nothing reclaims
that disk automatically.

Downstream compatibility remains incomplete: Delegate will not build against
0.13.0 without edits, and Convo Relay needs two line edits to drop a removed
option field.

Admission validates only backend registration. An unusable backend binary is
therefore discovered when a job runs, not when it is submitted.
