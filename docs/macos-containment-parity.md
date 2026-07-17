# macOS containment parity — future work

**Status:** Future work. Not implemented. This document records *why* macOS cannot currently match the
Linux containment guarantee and *what* it would take to close the gap, so the decision is deliberate rather
than accidental.

**Audience:** anyone extending the AB-D admission/containment subsystem (`internal/containment`,
`engine/execution/custodian`, `internal/parklaunch`, `internal/procgroup`).

---

## 1. Context: the guarantee we are targeting

AB-D must launch backend agent processes with **exactly-once launch** and **guaranteed no-orphan
process-group containment**, holding *even across daemon crash / restart / PID reuse / reboot*, and it must
**fail closed** (refuse / report `Unprovable` / trip the `SafetyLatch`) rather than ever double-launch or
falsely certify absence.

The accepted design (see the containment design review, 2026-07) is **platform-asymmetric**:

- **Linux:** authoritative containment via a **cgroup-v2** object per launched group. A cgroup is a kernel
  object whose lifetime is independent of any process: membership (`cgroup.events` `populated`) and atomic
  teardown (`cgroup.kill`) are immune to PID/PGID reuse and survive the daemon's death. This delivers the
  full guarantee on Linux (in a correctly delegated environment).
- **macOS:** **no equivalent kernel object exists.** We accept a weaker, still-safe guarantee (see §3).

This document is about eliminating that asymmetry on macOS *in the future*. It is deliberately not on the
critical path: the asymmetric posture is safe (never a false certificate, never a double-launch), it just
is not maximally *available* on macOS after a daemon crash.

---

## 2. Why macOS is hard (the kernel facts)

The Linux guarantee rests on **a durable, kernel-owned group-lifetime object with a membership oracle, an
absence oracle, and atomic kill** (cgroup-v2). macOS has no such primitive. Concretely:

1. **No process-group / job lifetime object.** Darwin has process groups and sessions, but a numeric PGID
   is *not* a durable handle — it is recycled with its leader PID. There is nothing analogous to a cgroup
   or to a Windows Job Object that owns a whole descendant tree and outlives its members.

2. **PID/PGID reuse is the core hazard, and only non-reaping *parenthood* prevents it.** The one mechanism
   that pins a numeric PID/PGID is: *be the real parent and never `wait()` the leader.* An unreaped zombie
   keeps its PID allocated; since the PGID equals the leader PID, the group identity cannot be recycled.
   This is exactly what the current custodian does while it is alive (`internal/parklaunch` deliberately
   withholds `Wait`; `engine/execution/custodian/native_leader_darwin.go`).

3. **`kqueue`/`EVFILT_PROC`/`NOTE_EXIT` is notification, not ownership.** A kqueue exit-notifier held by a
   *non-parent* does **not** prevent reuse — once the real parent (or `launchd`, after reparenting) reaps
   the leader, the PID is freed and can be recycled. `kqueue` tells you *that* a PID exited; it does not
   keep the identity reserved. (This is the direct analogue of the Linux finding that a non-parent `pidfd`
   does not prevent reuse.)

4. **Daemon death destroys the only durable fact we have.** When the agentbus daemon dies, the backend
   group is reparented to `launchd` (PID 1), which becomes the reaper. The one-shot monitor is a *sibling*,
   not the parent, so it cannot hold the non-reaping invariant. A restarted daemon has no in-memory
   retention either. From that moment, if the group leader has exited (e.g. killed by TERM) while a
   TERM-ignoring descendant survives, **no macOS primitive lets us prove the surviving processes are still
   the original group** — the PGID may already have been recycled. The correct, safe response is
   `Unprovable` + fail-stop, which leaves a *possible physical orphan*.

5. **`setsid` escape is out of scope even on the happy path.** Process-group scope does not follow a
   descendant that creates a new session. On Linux a cgroup would still contain it; on macOS there is no
   containing object.

**Conclusion:** the macOS gap is *fundamental to a numeric-PGID design*. It cannot be closed by more
`kqueue`/handle retention. Closing it requires a component that either (a) remains the true, durable parent
of every launched group across daemon crashes, or (b) supplies an out-of-band durable membership+absence
oracle. Neither exists for free on macOS.

---

## 3. Current accepted macOS behavior (the baseline this doc improves on)

- **While the daemon is alive and is the non-reaping parent:** containment is fully sound. TERM → grace →
  KILL → independent stable zero-membership observation; reuse is impossible because the leader is held
  unreaped.
- **After daemon death / reparenting:** the monitor (or a restarted daemon) may KILL **only while the exact
  leader is still observably alive** (a live leader cannot have been recycled). Once the leader is
  missing/reaped/zombie, containment returns **`Unprovable`** and the system fails closed (SafetyLatch,
  stop admission, no relaunch, no false absence certificate).
- **Net:** safety (no double-launch, no false certificate) is preserved on macOS. **Liveness is not** — a
  TERM-ignoring descendant can survive a daemon crash as a physical orphan that we refuse to blindly kill.
- **Additional Darwin caveat:** the process start-token incorporates the PPID, so reparenting after daemon
  death can flip a *surviving* process to a conservative "reused" classification even before leader exit.
  This is safe (it only ever *withholds* a signal) but it further reduces post-crash cleanup ability.

The rest of this document describes how a future effort could upgrade the after-daemon-death case from
"`Unprovable` orphan" toward the Linux-equivalent "provably contained."

---

## 4. Requirements any macOS solution must meet

To match the Linux cgroup guarantee, a macOS mechanism must provide **all** of:

1. **Reuse-proof group identity** — a handle/object bound to *this specific launch* that cannot be
   satisfied by a recycled PID/PGID, and that survives the agentbus daemon's death.
2. **Membership oracle** — enumerate the live members of the launched group (including `setsid`
   descendants) without racing PID reuse.
3. **Absence oracle** — positively prove *zero live members* (the analogue of `cgroup.events populated=0`),
   distinguishing "empty" from "cannot tell."
4. **Atomic teardown** — kill the entire group in a way that is not defeated by fork/exec races (the
   analogue of `cgroup.kill`).
5. **Durable identity binding** — the object's identity is persistable (into `GroupRef.RetainedID`) and
   re-verifiable by a *restarted* daemon or a separate monitor, and is invalidated by reboot (distinct
   `KernelDomainID`/`HostBootID`).
6. **Fail-closed unavailability** — when the mechanism is not available/trustworthy, advertise no fenced
   runtime, reject before acceptance, and `Unprovable` + SafetyLatch for existing obligations.

The current macOS mechanism satisfies (2)–(4) *only while the daemon is the live parent* and fails (1) and
(5) after daemon death. That is the precise gap.

---

## 5. The significant architectural change: a launchd-supervised durable custodian

The only credible macOS direction is to **move the point of durable ownership out of the ephemeral agentbus
daemon and into a `launchd`-supervised, long-lived, privileged custodian process** — because `launchd` is
the one component guaranteed to survive, restart, and reap on macOS.

### 5.1 Shape

- Install a small **custodian helper** as a `launchd` `LaunchDaemon` (system domain) or a per-user
  `LaunchAgent`, with `KeepAlive` so `launchd` restarts it if it dies.
- **The custodian — not the main agentbus daemon — is the true parent** of every launched backend group.
  The main daemon requests a launch over an IPC channel; the custodian `fork`/`posix_spawn`s the parked
  worker, keeps the leader handle, and withholds `wait()` (the non-reaping invariant) *for the whole
  lifetime of the obligation*.
- Because the custodian is `launchd`-supervised and separate from the agentbus daemon, **an agentbus daemon
  crash no longer reparents the group** — the custodian is still its parent and still holds the reuse-proof
  invariant. The restarted daemon reconnects to the custodian and asks it about liveness / to contain.
- The custodian maintains a **membership ledger** for each launched group, updated from an authoritative
  process-lifecycle event source so it knows the full descendant set even across `setsid` (see §5.2).

This converts "the daemon must stay alive to keep containment sound" into "the `launchd`-supervised
custodian must stay alive," which is a far stronger property because `launchd` restarts it and it does
nothing but custody.

### 5.2 Membership + absence oracle: Endpoint Security

macOS has no `cgroup.procs`. The closest way to maintain an accurate descendant-membership set is the
**Endpoint Security (ES)** framework: subscribe to `ES_EVENT_TYPE_NOTIFY_FORK` / `EXEC` / `EXIT` and track
the process subtree rooted at the launched leader, *independent of PID reuse* (ES events carry audit tokens
/ `pid` + `pidversion`, which disambiguate reused PIDs). The custodian maintains, per group:

- the live-member set (updated on fork/exec/exit),
- an absence signal when the set becomes empty,
- reuse-safe identity via audit token / `pidversion`, not bare PID.

ES requires: the `com.apple.developer.endpoint-security.client` entitlement, deployment as a **System
Extension** (or a signed, approved daemon), root, and user approval. It is an **observation** API — it
does *not* itself kill or own processes — so it must be paired with the non-reaping custodian (for identity
pinning) and explicit signalling (for teardown).

### 5.3 Teardown

There is no `cgroup.kill` on macOS. Atomic-enough teardown is approximated by the custodian:

1. Freeze new work: the custodian stops the group from being extended (best-effort; macOS has no freezer).
2. Enumerate the ES-maintained member set.
3. Signal each member (TERM → grace → KILL) using reuse-safe identity (verify audit token/`pidversion`
   immediately before signalling).
4. Consume ES `EXIT` events until the member set is empty; that empty transition is the **absence proof**.
5. Only then release the leader (reap) and retire the group.

Because the custodian is the parent and holds the leader unreaped throughout, PID reuse cannot occur inside
the group during teardown — closing the hole that exists today.

### 5.4 Honest residual limitations (why this is "close," not "equal")

- **The custodian is still a single point of continuity.** A cgroup is a *kernel object* independent of any
  process; the macOS custodian is a *process*. If the custodian itself is `SIGKILL`ed (not caught) and the
  leader is reaped by `launchd` before restart, the reuse hole briefly reappears. `launchd` `KeepAlive`
  restart does **not** re-establish parenthood over the already-reparented group. Mitigations (ES ledger
  persisted + reconciled on restart; reduce the custodian to the smallest possible trusted core) narrow but
  do not fully eliminate this.
- **Entitlements/deployment burden.** ES + System Extension means Apple-granted entitlement, notarization,
  user approval, and privileged install — a real product/operational cost, not just code.
- **Not a drop-in backend.** Per-group custody via a launchd-supervised helper changes launch, identity,
  exactly-once, and recovery flows. It is a subsystem, not a `Signaler`/`Observer` swap.

If a *fully* kernel-equivalent guarantee is required, the only complete answer would be an Apple-provided
group-lifetime primitive (does not exist) — so the launchd+ES custodian is the pragmatic ceiling.

---

## 6. Alternatives considered and rejected

| Mechanism | Why it does not meet the requirements |
|---|---|
| `kqueue` / `NOTE_EXIT` retention | Notification only; a non-parent handle does not prevent PID/PGID reuse. Already used for exit detection; cannot be the ownership/absence object. |
| `proc_listchildpids` / process-group sysctls | Point-in-time snapshots; race reparenting, exit, and PID reuse; lose ancestry after reparent. No durable membership. |
| A dedicated monitor that is the worker's parent | Better than today, but only moves the single point of continuity from daemon to monitor; monitor `SIGKILL` recreates the hole and parenthood cannot be reacquired after restart. This is essentially §5 done ad hoc — do it properly via launchd. |
| Kauth / KEXT process monitoring | Deprecated/unsupported on modern macOS; Apple directs clients to Endpoint Security. Inappropriate for a general-purpose user-space service. |
| Endpoint Security alone | Observation only — cannot own or atomically kill a group. Necessary (as the membership oracle) but not sufficient; must be paired with the non-reaping custodian + explicit teardown (§5). |
| `launchd` per-launch jobs (submit a job per backend) | `launchd` becomes the durable supervisor, but its documented job model exposes no immutable membership set + independent absence oracle equivalent to `cgroup.events`. Would be a new architecture; worth prototyping but not assumed sound. |

---

## 7. Interaction with the existing design

Most AB-D structure is unaffected and is reused:

- **`GroupRef.RetainedID`** — already exists as the durable identity hook. A macOS custodian would populate
  it with the custodian-scoped group identity (and `SamePhysicalIdentity` must be updated to honor it, as
  it must for the Linux cgroup work).
- **`KernelDomainID` / `HostBootID`** — reboot fences the guarantee identically on both platforms (a new
  boot ⇒ old group is provably quiescent). A macOS custodian identity must also be bound to the boot
  session.
- **Containment engine (`internal/containment`)** — the TERM → grace → KILL → poll sequencing is reused,
  but (as with cgroups) the **authorization** must gain a *retained-object* branch: "membership proven by
  the durable custodian ledger" authorizes teardown without requiring a live matching leader. Today the
  engine only starts authority from a matching live leader — that is the contract that must be generalized
  for *both* Linux cgroups and a macOS custodian.
- **Parked launcher (`internal/parklaunch`)** — the bind ordering must place the group into its durable
  object *before* the `GroupRef` digest / release (same ordering change the Linux cgroup work needs); on
  macOS the "durable object" is registration with the custodian.
- **Authority / attestation** — unchanged: the custodian returns a *physical* outcome; only the authority
  mints the quiescence certificate, and only after independently confirmed absence.

The upshot: the **retained-object authorization branch** and the **earlier launcher bind ordering** are
shared prerequisites for *both* the Linux cgroup work and a future macOS custodian. Designing them
generically (not cgroup-specifically) keeps the macOS door open at low cost.

---

## 8. Suggested prototype plan (when this is picked up)

1. **Spike the launchd custodian shell:** a `KeepAlive` LaunchDaemon that spawns a parked helper, holds it
   unreaped, and survives a simulated agentbus-daemon kill (assert the group is *not* reparented to
   `launchd`).
2. **Spike Endpoint Security membership tracking:** subscribe to fork/exec/exit, maintain a reuse-safe
   (audit-token/`pidversion`) member set for a subtree, and prove an accurate empty-transition on teardown.
3. **Wire the retained-object authorization branch** in the containment engine/model (shared with Linux).
4. **Persist + reconcile** the custodian ledger across custodian restart; measure the residual
   custodian-`SIGKILL` window and decide whether it is acceptable or needs further mitigation.
5. **Adversarial conformance (must pass before claiming parity):**
   - daemon killed mid-run; TERM-ignoring grandchild; assert the custodian still contains + proves absence
     (not `Unprovable`).
   - custodian killed and restarted; assert the residual behavior is *documented and fail-closed*, never a
     false certificate.
   - `setsid`-escaping descendant; assert it is still tracked + contained.
   - PID reuse during teardown; assert reuse-safe identity prevents signalling an unrelated process.
   - ES/entitlement unavailable ⇒ fenced runtime not advertised, admission refused, obligations fail-stop.
   - reboot ⇒ old group provably quiescent via `HostBootID`.

Only when the daemon-death + grandchild case is provably contained on macOS (matching the Linux cgroup
conformance case) may S5B advertise a symmetric guarantee on macOS.

---

## 9. Summary

- **The gap:** macOS has no kernel group-lifetime object, so after an agentbus-daemon crash the current
  design can only *safely refuse* (`Unprovable`) to contain a TERM-ignoring orphan, whereas Linux cgroups
  contain it definitively.
- **The significant architectural change:** relocate durable custody from the ephemeral daemon to a
  **`launchd`-supervised, non-reaping custodian process** that owns every launched group and maintains a
  reuse-safe membership/absence ledger via **Endpoint Security**, paired with explicit reuse-safe teardown.
- **The honest ceiling:** this closes the daemon-crash hole but retains a narrow custodian-`SIGKILL`
  window, because a process is not a kernel object. Full kernel-equivalent parity would require a macOS
  primitive that does not exist.
- **Cheap insurance now:** design the **retained-object authorization branch** and the **earlier launcher
  bind ordering** generically during the Linux cgroup work, so a future macOS custodian plugs into the same
  contracts.
