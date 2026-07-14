# ADR-0B: Stable replay key and fingerprint-version ordering

## Decision

Identified `job.submit` carries a top-level `workspaceKey` computed and persisted by delegate before
the first send. Replay lookup by `(workspaceKey, requestId)` happens before filesystem resolution or
algorithm choice. Existing bindings and tombstones are compared with the recorded fingerprint
algorithm and version; unknown recorded algorithms fail closed with `request_fingerprint_unsupported`.

## Invariant(s)

- Replay never depends on `filepath.EvalSymlinks` of a workspace that may have moved or disappeared.
- Existing bindings are resolved before current validation or current fingerprint selection.
- Bindings and tombstones store fingerprint algorithm, version, and value.

## Rejected alternatives

- Deriving replay identity from the current filesystem path on every retry.
- Treating an unknown stored fingerprint algorithm as an absent binding.
- Rehashing old requests with only the latest fingerprint algorithm.

## Consequences

Workspace deletion after acceptance does not make replay ambiguous. Fingerprint migrations must retain
old comparison behavior.

## Non-goals

This ADR does not define delegate receipt storage beyond requiring the stable key to be sent.
