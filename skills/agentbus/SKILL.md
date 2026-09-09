---
name: agentbus
description: Observe and control asynchronous jobs with the agentbus operator CLI.
---

# agentbus

Use Agentbus to observe jobs submitted by another tool. It has no submit command.
`serve` is an operator command: start the daemon with `serve`; add `--foreground` to
keep it attached. Agents normally use the observation commands below.

```sh
agentbus version [--json]
agentbus serve [--foreground]
agentbus status [--job <id>] [--workspace-key <key>] [--tag <key=value>] [--state <state>] [--json]
agentbus transcript --job <id> [--kind <kind>]... [--last <n>] [--since <rfc3339>] [--since-ordinal <n>] [--limit <n>] [--json]
agentbus result --job <id> [--json]
agentbus cancel --job <id> [--json]
```

`status` without `--job` lists compact summaries from every workspace unless filtered by
`--workspace-key`, `--tag`, or `--state`; a summary has no result, so select a job and
then use `status --job` or `result`. Filters combine with AND. Repeat `--tag` only for
distinct keys; every tag must match and duplicate keys are rejected. Repeat `--state`
to match any listed state. Valid states: `queued`, `running`, `completed`, `failed`,
`canceled`, `unknown`.

`transcript` without a selector (`--kind`, `--last`, `--since`, `--since-ordinal`, or
`--limit`) returns a digest of counts/timestamps plus recent messages and captured
errors. Any selector replaces that digest with the matching items themselves, which
may be only tool items; `--last` is the only selector that returns a tail.

Follow a queued/running job forward with
`--kind message --since-ordinal <n> --limit <n>`, starting at `0` and feeding back the
highest returned ordinal; ordinal `0` is valid and the first assigned ordinal is `1`.
`--last` returns only the latest matching items in that one response (for ordinals
`[1, 9, 14, 16, 17]`, `--last 2` returns `[16, 17]`). Every request re-reads the sidecar,
so the earlier items stay reachable by asking again from a lower ordinal; what does not
exist is paging backwards from a tail. Treat `--last` as a one-time view, never a
polling cursor.

The transcript response includes `state` and `gap`, and they answer different questions.
Drainage is a property of your paging: with a positive `--limit`, keep requesting pages
until one comes back short or empty, and keep doing so after the job turns terminal.
`gap` is a property of the capture, not of your page: `false` proves capture and reading
were continuous, `true` means completeness cannot be claimed even after you have drained
every readable matching item. A running job is always gapped, so `gap` says nothing
about exhaustion while one is in flight.

For a selected job, exit status is the job outcome, not command success: `0` completed,
`2` queued/running, `3` completed-noncompliant, `4` failed, `5` timeout, `6` interrupted,
`7` canceled, `14` unknown, `15` result-artifact-unavailable. CLI/daemon failures are
`10` unknown job, `11` daemon-startup failure, and `13` shutdown-deadline; `2` also
denotes a usage error. A listing exits `0` once printed.
