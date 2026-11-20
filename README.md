# agentbus

Agentbus is a local generic job service for backend CLIs. It stores identified
jobs, supervises their process groups, and exposes a small JSON-RPC interface.

## Depends on / Depended on by

Agentbus depends on nothing in the delegate/convo-relay/witness set.

`delegate` imports `github.com/charlesnpx/agentbus/client`, pins a released
Agentbus version; changes to the client API require coordinated consumer updates.

`convo-relay` embeds `github.com/charlesnpx/agentbus/engine` and its adapters;
changes to those packages or their APIs can break its build.

Version 0.18.0 serves protocol version 4 and uses store format 2. The published
contract is [docs/protocol.md](docs/protocol.md). See
[docs/deployment-0.18.md](docs/deployment-0.18.md) for fresh deployment and
[docs/operations.md](docs/operations.md) for ongoing operation.

## CLI

~~~text
agentbus version [--json]
agentbus --help
agentbus backends [--json]
agentbus serve [--foreground] [--max-jobs <n>] [--max-jobs-per-backend <n>] [--max-queued <n>] [--prune-interval 24h]
agentbus status [<job-id>] [--tag <key=value>] [--state <state>] [--workspace-key <key>] [--limit <n>] [--before <cursor> | --all] [--json]
agentbus transcript <job-id> [--follow] [--kind <kind>] [--since <timestamp>] [--since-ordinal <n>] [--last <n>] [--limit <n>] [--json]
agentbus result <job-id> [--wait [--wait-timeout <duration>]] [--json]
agentbus cancel <job-id> [--json]
agentbus prune [--apply] [--successful-older-than 168h] [--failed-older-than 720h] [--json]
agentbus steer <job-id> --request-id <messageId> --message-file <path|-> [--json]
agentbus session discard <job-id> [--json]
~~~

Help exits successfully without starting or contacting the daemon. Use
`agentbus -h`, `agentbus help`, or `agentbus status --help` for usage.

Status without a job ID prints the 50 newest matching jobs. JSON output includes
`truncated: true` and `nextCursor` when another page exists; text output prints
the continuation cursor on stderr. Continue with that cursor, or request full
history explicitly:

~~~sh
agentbus status --limit 50 --json
agentbus status --limit 50 --before '<nextCursor>' --json
agentbus status --all
~~~

`job.list` keeps its existing unbounded behavior for client callers that omit
`limit`; the CLI sends a limit by default. `--all` explicitly omits that limit.

`result --wait` observes the job with `job.get` until it becomes terminal or
the observation timeout expires. The default timeout is 30m. Expiry exits 16;
JSON output includes the latest record when available. It does not cancel or
extend the job. Durations need explicit units: `2400` is ambiguous, so use
`2400s` or `40m`. Plain output is exactly the verified result artifact bytes,
including an empty result or any trailing newlines.

~~~sh
agentbus result "$JOB_ID" --wait --wait-timeout 30m
agentbus result "$JOB_ID" --wait --wait-timeout 2400s --json
~~~

`transcript --follow` pages by ordinal until the job is terminal and its
transcript pages are drained. `--json` prints one JSON object per item followed
by a summary object containing `gap`. Ctrl-C stops observation without
cancelling the job.

~~~sh
agentbus transcript "$JOB_ID" --follow --kind message
agentbus transcript "$JOB_ID" --follow --json
~~~

Options may appear before or after the job ID. Singleton flags cannot be repeated.
Repeatable filters such as `--tag` and `--state` keep accepting multiple values.
Successful status and transcript reads exit zero for every stored job state;
result and cancellation commands use the outcome codes in the operations guide.

Delegate submits tasks and reviews through the typed `job.submit` method;
Agentbus provides status, result, cancel, and steer commands. There is no
Agentbus CLI submit command.

The daemon runs at most 12 jobs at once and 12 per backend by default, and keeps
up to 64 additional jobs waiting. These are starting values, not measured
optima. Set positive integer limits with `agentbus serve`; zero is rejected.
The values apply when a daemon starts and do not change an already serving daemon.
When the waiting queue is full, a new job.submit is rejected with
`queue_full` (CLI exit code 19). A matching deduplicated request does not use
another queue entry. Job execution timeouts begin when a job gets a slot;
`queuedAt` and `startedAt` show its wait and execution start.
For a queued job, `status <job-id>` prints its position among jobs for the
same backend and the current backend and total running limits. `status --json`
with a job ID includes the same optional `queue` projection as `job.get`.

TaskSpec `inputMode` is `fixed` when omitted. Set it to `steerable` only when
the running task may receive follow-up instructions. `agentbus steer` submits
one caller-identified message from a file or stdin; `status <job-id>` shows the
mode and text-free steer receipts. A steerable job retains one public job ID,
deadline, permission envelope, model, effort, and schema. Delivery may be
native or interrupt-and-continue depending on the adapter. Delivery uncertainty
is reported without automatic redelivery; a steerable task does not guarantee
that a provider follows the instruction or rolls back tools already started.
Codex uses native app-server `turn/steer`; confirmation means its correlated
JSON-RPC success response arrived before the steer context ended. No response
before that deadline is reported as unconfirmed.
`agentbus steer` exits 17 for a rejected receipt and 18 when delivery is unknown.
Claude Code uses `interrupt_continue` on its existing stream-json connection:
it waits for the interrupt acknowledgement, drains the interrupted result, and
writes the follow-up user message before reporting confirmed delivery. This is
not same-turn injection; confirmed delivery does not mean Claude followed the
instruction.
The wire details and receipt reasons are in [docs/protocol.md](docs/protocol.md).

Cursor steering uses ACP interrupt-and-continue on the same session. Its
`Confirmed` receipt means the follow-up prompt was written after the cancelled
prompt drained; ACP provides no separate acceptance acknowledgement before the
new prompt's response.

Set `AGENTBUS_STATE_ROOT` to select daemon state. Otherwise Agentbus uses
`$XDG_STATE_HOME/agentbus/v2` or `~/.local/state/agentbus/v2`. Format 2 starts
with fresh state and does not import older databases.
The new default root is nested under the old default root; see the deployment
guide before archiving or removing any earlier state.
Set `AGENTBUS_NO_AUTOSTART=1` to prevent client commands from starting a daemon;
empty or `0` enables normal autostart, and other values are invalid. An explicit
`agentbus serve` still works.

Automatic diagnostic pruning runs after readiness and every 24 hours by
default; `serve --prune-interval 0` disables it. `agentbus prune` previews
eligible logs and transcript files, and `--apply` removes them. Completed-job
diagnostics default to seven days and other terminal diagnostics to 30 days.
Prune preserves results, request bindings, job records, and conversation homes.

Codex conversation homes are private to the root job and reused by its
continuations. A completed job requires `retainSession` and a provider session
ID for continuation; unsuccessful jobs with a session ID are retained. Agentbus
refuses a continuation while any job in the lineage is active or has uncertain
cleanup. `agentbus session discard <job-id>` permanently revokes continuation
for the whole inactive Codex conversation, including retained descendants, then
removes its owned home. It leaves job history and results intact. Retained homes
can contain substantial provider-managed data and are never pruned by age.

Every task supplies an `inspect` or `edit` intent. Inspect asks the provider
not to edit project source or configuration; it may use ordinary build and
test artifacts, caches, temporary files, and local services. Edit asks the
provider to make only the requested changes. Both use the same host execution
policy. Intent is an instruction, not a filesystem, network, credential, or
tool-permission boundary; host and provider restrictions can still apply.

Agentbus inherits the daemon environment, applies adapter values, then the
session's `EnvOverlay`. It does not create per-job build caches or override
normal cache, temp, or Go proxy settings. The caller selects the workspace and
must provide separate mutable workspaces when concurrent edits could conflict.

`agentbus backends` calls the read-only `backend.inspect` method. It reports
each configured backend's resolved executable path, version, and configured
default model/effort. The JSON response also reports the connected daemon's
release, executable path, and protocol version. Entries use `backend`,
`executablePath`, `version`, `defaultModel`, `defaultEffort`, and `error` when
applicable. Each version probe is one bounded `--version` call per executable
path and modification time for the daemon's lifetime; a failed backend is
reported in its own entry. It does not query accounts, billing, or model
catalogues. Use `--json` for the protocol response shape.

The daemon accepts intentional per-backend defaults through
`AGENTBUS_CODEX_DEFAULT_MODEL`, `AGENTBUS_CODEX_DEFAULT_EFFORT`,
`AGENTBUS_CLAUDE_DEFAULT_MODEL`, `AGENTBUS_CLAUDE_DEFAULT_EFFORT`,
`AGENTBUS_CURSOR_DEFAULT_MODEL`, and `AGENTBUS_CURSOR_DEFAULT_EFFORT`. An
explicit TaskSpec model or effort takes precedence. `modelRequested` and
`effortRequested` in job.get record only values submitted in the TaskSpec, so
they are omitted when the daemon supplied a default. `modelReported` and
`effortReported` contain the latest values the provider reported; they are not
filled from the request. `selection` is the immutable model/effort chosen at
start, and is distinct from those observations. **Requested is not executed**: provider telemetry is
the evidence of what was observed, and it may be absent.

Detailed job.get responses and `agentbus status <job-id>` expose `deadline`
and `remainingMs` for running jobs with a finite timeout, plus
`resumable: {eligible, reason}` derived from the existing session and retention
rules. Failed, canceled, and unknown terminal jobs keep their backend session
without `--retain-session`; completed jobs require session retention. Resume
eligibility requires a recorded backend session ID, which is not exposed on the
job record. The human-readable `reason` names the retention rule that applied.
**Eligible is not guaranteed**: the provider may still reject a resume.
The default execution deadline is two hours for both intents; an explicit
`timeoutMs: 0` is unlimited. A finite job may receive one best-effort deadline
notice at 90% of its execution budget, including in fixed input mode.
`cancellationReason` records `operator`, `deadline`, `daemon_shutdown`, or
`provider_interruption` independently of the existing terminal state.

Failed job records keep their existing `failure.class` and `failure.reason` and
may add `providerCode`, bounded `providerMessage`, `stage`, and absolute
`retryAfter`. Current stages are `startup`, `turn`, and `finalize`. The failure
classes include `usage_limit` and `authentication`; Claude stream-json and
Codex app-server provide these only when their parsed provider error contains
a distinguishable signal. Cursor ACP preserves structured JSON-RPC error code
and message, but does not classify generic errors as usage or authentication
failures. Existing `provider_overloaded`, `model_unavailable`, and
`content_policy` classes remain. A transient failure may leave partial
workspace edits, so replay is the caller's decision and Agentbus does not
retry automatically. The `storage` class identifies failed result or terminal
record persistence. See [docs/protocol.md](docs/protocol.md) for field shapes,
provider signals, and the full method contract.

## Packages

The Go client package is github.com/charlesnpx/agentbus/client. The engine
package is github.com/charlesnpx/agentbus/engine. Backend adapter requirements
are documented in [docs/adapters.md](docs/adapters.md).

## Development

~~~sh
go build ./...
go test ./... -count=1
~~~
