# ADR-2B: Submission modes

## Decision

There are three submission modes. `IdentifiedFenced` resolves replay first, accepts atomically, and
prepares after acceptance. `LegacyFenced` completes fallible parked preparation before acceptance,
commits `awaiting_ack`, acknowledges on response success, and rejects without permit on response
failure. `LegacyUnfenced` keeps the v0.5.1 JSON lifecycle or rejects pre-acceptance and never enters
the fenced store.

## Invariant(s)

- Legacy fenced jobs cannot receive a permit until acknowledgement.
- Legacy unfenced submissions do not partially enter bbolt or supervisor state.
- Built-in Codex and Claude are fenced; unfenced custom backends remain legacy-only.

## Rejected alternatives

- Moving all legacy jobs into fenced admission immediately.
- Launching legacy fenced work before response acknowledgement.
- Turning legacy unfenced start failures into durable fenced jobs.

## Consequences

Compatibility behavior is preserved while identified and built-in fenced modes gain reliable
admission.

## Non-goals

This ADR does not enable or advertise `jobs.requestId` capability by itself. S1 keeps it gated off
until a verified platform custodian is available.
