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
  setup-probes.json
  artifacts/
    prompts/
    logs/
    results/
~~~

token authenticates protocol.hello. agentbus.sock exists while the daemon is
serving. agentbus.pid is written for a successful background launch or
client-autostart; do not rely on it for a foreground daemon. jobs.db is the
single bbolt store. It has exactly the meta, requests, and jobs buckets.
artifacts contains prompt, log, and result sidecars. Records and request
bindings are retained even when an artifact is later removed.

A Codex job may also create an implementation-managed private home below
workspaces/<workspace-hash>/codex/<job-id>. It is separate from the shared job
store and may contain per-job caches.

The root and token are owner-only. Do not hand-edit jobs.db, token, artifacts,
or the probe cache while a daemon is using the root.

## Starting and probing backends

Use agentbus serve to start a background daemon. Use agentbus serve --foreground
when a supervisor owns the process. The public CLI commands are version, serve,
status, result, and cancel.

There is no standalone setup command. Backend checking is lazy: the first
submitted job for a backend triggers its probe, and the daemon caches the probe
result under the state root. A hello response can describe configured backends;
it does not prove that every backend has already been checked.

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

agentbus status lists compact job summaries. With --job, it projects a full
job.get record without printing result text.

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
