# Agentbus operations

This runbook covers Agentbus 0.18.0, protocol 4, and format 2 on macOS and
Linux. Stop the previous daemon and select a fresh state root before upgrading;
there is no migration or import of old jobs, bindings, results, or sessions.
The default root is `$XDG_STATE_HOME/agentbus/v2`, or
`~/.local/state/agentbus/v2` when XDG_STATE_HOME is unset. An explicit
`AGENTBUS_STATE_ROOT` selects the exact root.

The root contains the owner-only token, socket, `jobs.db`, diagnostic logs,
results under `artifacts`, and Codex homes under `sessions/codex/<root-job-id>`.
The bbolt store has meta, requests, and jobs buckets. Its immutable `ownerRoot`
records the canonical root path. A copied or moved database, unsupported or
corrupt format, missing ownership metadata, or missing database alongside owned
data stops startup without rebinding or deleting the data. Do not hand-edit a
live store; inspect a stopped copy with offline tooling.

Agentbus inherits ordinary environment settings and does not create job-private
build caches or temporary trees. It uses the caller's selected workspace.
Codex homes are private conversation state; `auth.json` and `config.toml` are
linked from the daemon's `CODEX_HOME` or `~/.codex` when present. Provider
state may grow while a conversation is retained.

Automatic diagnostic pruning runs once after readiness and every 24 hours by
default. `serve --prune-interval 0` disables both automatic passes. The
`agentbus prune` command previews eligible cleanup, and `--apply` removes eligible logs and
transcripts; job records, request bindings, results, and conversation homes are
preserved. Completed-job diagnostics expire after seven days and failed,
canceled, or unknown-job diagnostics after 30 days by default. Retained sessions
and uncertain process cleanup do not protect diagnostics after local writers
have retired. Prune marks them unavailable before unlinking, and a later pass
retries any incomplete removal.

To reclaim an inactive owned Codex conversation, run
`agentbus session discard <job-id>`. It durably revokes continuation for the entire lineage before
removing the root home. Active or cleanup-uncertain lineages are refused.
Repeated calls can retry a failed removal; job history, results, and observed
provider IDs remain. Retained provider data has no automatic expiry.

## Starting and selecting backends

Use agentbus serve to start a background daemon. Use agentbus serve --foreground
when a supervisor owns the process. Both forms accept `--max-jobs`,
`--max-jobs-per-backend`, and `--max-queued`. Their defaults are 12 total jobs,
12 jobs per backend, and 64 waiting jobs. These are starting values, not
measured optima. Each value must be a positive integer; zero does not mean
unlimited. The values apply when a daemon starts and do not reconfigure an
already serving daemon.

Jobs beyond the execution limits remain queued and start in FIFO order within
their backend when a slot opens. A queued job for a saturated backend does not
block a later job for another backend with a free slot. The queue bound counts
waiting jobs; a matching deduplicated submission does not consume another
entry. When the bound is reached, a new job.submit returns the typed
`queue_full` error and CLI exit code 19. A queued cancellation removes the job
before it can start a provider. Job lookup, listing, transcript, and queued
cancellation remain available while all execution slots are occupied.
For a queued job still in the daemon's in-memory queue, `job.submit` and
`status <job-id>` report its one-based position among jobs for the same
backend, along with occupied backend and total slots and their limits. Human
status prints a line such as `queue: position 3 for backend claude (12/12
running for claude; 12/12 total)`. The projection disappears when the job
starts or is no longer in the daemon's queue.

The timeout is execution time: it begins when the daemon reserves a slot, not
when job.submit is accepted. `queuedAt` records queue entry and `startedAt`
records the start transition, so status shows both the waiting age and whether
the job is queued or running. Both intents default to two hours; explicit zero
means no deadline, and positive values are limited to four hours. Fixed and
steerable jobs with a finite effective timeout and backend steering support may
receive one best-effort warning at 90%, measured from slot acquisition. The warning uses the reserved message ID
`agentbus-deadline-warning` and is visible in the job's steer records.

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

For a recovered process claim, the daemon checks boot ID, state-root device
and inode, Linux PID namespace, and the exact live PID, start token, and
process group before signaling. It repeats the checks before escalation. It
records cleanup as uncertain when cleanup cannot be established. Cleanup uncertainty does not
erase a recorded completed result.

Replacing the Agentbus binary makes a stale daemon finish accepted work and
exit after active work and admission handling settle; installs do not signal it.
The next ordinary client can start its selected executable. Set
`AGENTBUS_NO_AUTOSTART=1` to prevent client autostart, including reconnects;
empty or `0` enables it. An explicit `serve` is unaffected. During drain the
endpoint returns `daemon_draining` until admitted store users retire and the
store closes, then the listener and owned socket retire. A manual SIGTERM initiates shutdown
and then cancels the service context; any jobs still running at cancellation
are interrupted and recorded as failed with
`cancellationReason: daemon_shutdown`.

## Capture and storage failures

Raw stdout and stderr logs are optional diagnostics. If a raw-log sink fails,
Agentbus stops writing that log and keeps draining provider output. `job.get`
includes a bounded `diagnostics` entry with the captured byte count and sink
error; `agentbus transcript` includes the warning and reports `gap: true`.
The missing raw-log suffix cannot be reconstructed.

Before a job launches, Agentbus checks available space on the state-root
filesystem. Below 256 MiB it records a low-space diagnostic and still attempts
the job because this estimate cannot predict the job's exact storage needs.
There is no automatic cleanup on a write failure. If Agentbus cannot persist a
final result artifact or its completed job record, the job is failed with
failure class `storage` and the operating-system error. It does not publish an
authoritative result; any captured transcript and known backend session remain
available when their writes succeeded.

## Reading jobs and results

agentbus status lists compact job summaries. With a job ID, its human-readable
projection omits result text, while `--json` emits the full job.get record.
That JSON is byte-identical to `agentbus result --json` and deliberately
includes inline result text when the record has it.

The full record contains the task intent and, after the starting transition,
an immutable `selection` of model and effort. Requested, selected, and reported
values have distinct meanings; provider observations may be absent.

agentbus result <job-id> reads the same full record. For a completed job it
writes the authoritative result to standard output. It writes failure detail or
the absence of a result to standard error for other states. When it must read a
result artifact, it verifies both its byte count and recorded digest.

agentbus cancel <job-id> sends job.cancel, then reads the full record to
choose the appropriate exit status.

## Exit codes

Successful status and transcript reads exit zero regardless of stored state.
Result and cancellation status is computed from state, failure class, and
schema compliance:

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
| 19 | submission rejected because the daemon's waiting queue is full |

Code 16 is observation timeout; 17 and 18 are rejected and unknown steering
receipts. Codes 8, 9, and 12 are not assigned.

`agentbus steer` exits 17 for a rejected receipt and 18 for an unknown-delivery receipt.

## Store failures

The daemon opens an existing database through a fault-isolated preflight before
binding its socket. A corrupt, truncated, incompatible, or busy database stops
startup rather than being repaired in place. Preserve the root for diagnosis;
do not create a replacement database at the same path unless you have first
performed the required version-break cleanup above.

Autostart errors distinguish a busy bbolt store, an incompatible or corrupt
store, a process that exited before readiness, and a readiness timeout. The
error names the startup phase and the `startup-diagnostic.log` path in the
state root. It also includes the bounded stderr tail when available.
If the daemon process cannot be started, the error uses code
`process_start_failed` and phase `launching daemon process`.

## Footprint and residual risks

The code footprint is measured from the checked-out tree at release review and
is not a stable operational property. Agentbus has 17 Go packages.

If the daemon dies mid-run, a provider process can remain orphaned. Recovery
marks recovered running work unknown and never relaunches it; the reaper signals
a recorded process group only when its live start token exactly matches the
recorded token. This is the deliberate trade-off for preventing duplicate work.

Result artifacts and conversation homes remain until deliberately removed;
diagnostic pruning does not reclaim provider or tool caches.

Downstream pins are recorded in each consumer's own go.mod and are not restated
here, because a version written into this runbook goes stale without warning.

Admission validates only backend registration. An unusable backend binary is
therefore discovered when a job runs, not when it is submitted.
