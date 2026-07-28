# macOS containment under the shared custody contract

**Status:** Current contract, admission contract version 2.

macOS is a supported serving platform for agentbus strict identified admission.
It serves under the same public custody contract as Linux: identified requests
are durably deduplicated, a launch that may already have occurred is never
automatically relaunched, normal cancel/timeout/shutdown terminates and waits
for the supervised process group, and unresolved physical cleanup after daemon
loss is job-local history rather than a global fail-stop.

## Shared contract

The cross-platform contract is intentionally weaker than the earlier retained
cgroup-only design:

- During normal operation, the daemon must supervise the controlled runner and
  its process group, send TERM/KILL as needed, and wait for quiescence before
  claiming `verified_absent`.
- After abrupt daemon loss, startup performs bounded, identity-checked cleanup.
  If no execution outcome was recorded and absence cannot be established, the
  job terminalizes as `orphaned` with `cleanupDisposition=unresolved`.
- If an execution outcome was recorded, cleanup uncertainty does not rewrite
  that outcome. The job keeps `completed`, `failed`, `canceled`, or the other
  recorded terminal state and exposes `cleanupDisposition=unresolved`.
- Terminal `orphaned` and terminal unresolved-cleanup jobs are not recovery
  obligations. They replay normally for the same `(workspaceKey, requestId)`,
  still count as durable history for reset-empty-root, and do not block sealing
  once no active recovery work remains.

## macOS implementation shape

macOS uses process-group and held-parent supervision. While the daemon is alive,
the held parent prevents PGID reuse for the supervised leader and supports the
normal TERM -> grace -> KILL -> wait path. If the daemon is abruptly killed,
Darwin does not provide a cgroup-like retained kernel object that can prove
membership after the parent relationship is lost. Under ADR-13, that limitation
is handled as job-local unresolved cleanup when the durable group identity is
otherwise trustworthy.

This is why macOS serves: absence uncertainty is no longer treated as authority
corruption. It is surfaced through terminal state and `cleanupDisposition`.

## Linux comparison

Linux may use cgroup v2 as a preferred cleanup enhancement when a delegated
writable cgroup root is available. That can prove absence by retained object
membership after daemon loss. Linux without cgroups uses the same process-group
fallback contract as macOS. Cgroup availability is selected per launch and is
not part of a root's permanent identity.

## Fail-closed boundary

The daemon still fails closed for authority-integrity and ownership ambiguity:
corrupt or contradictory durable `GroupRef` identity, unknown binding or grant
commit outcome, repository mutation ambiguity, incompatible contract version,
root corruption, sealed roots, and hosts without basic process supervision.

`unavailable_native_runtime` means the host cannot provide process groups,
identity/start-token observation, TERM/KILL/wait, and the controlled runner. It
does not mean "Linux cgroup v2 is absent."
