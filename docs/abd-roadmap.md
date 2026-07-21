# AB-D strict-native roadmap (LOCKED 2026-07-20)

Canonical, durable run-state + roadmap for completing AB-D native containment in production.
Doctrine (read-only): `~/tmp/orchestrator.md`. Packets: `~/tmp/delegate-packets/`. Scratch ledger:
`~/tmp/agent-server-delegate-progress.md`. This file is the source of truth for scope + sequence + status.

STATUS: EXECUTING. R0..R3B CLOSED. R3C committed (single-lease self-tested runtime, SupportClass,
typed lease contention; real cgroup-v2 conformance tier fully green incl. monitor-EOF). TWO sol reviews
launched on the committed SHA. Then R4A → R4B → R5 → R6/R7A → R7B. Production still UnavailableRuntime;
R0T expected-RED green.

## Repo / branch
- Working branch `abd-authority` reset to `4a8f59d` (S5A capability-off checkpoint; reviewed clean).
- Inert S5B `d957878` preserved on `abd-s5b-inert-d957878` (salvage: hidden monitor cmd + production
  containment params — port in R3B, NOT retained as semantic base).
- `main` untouched. Nothing pushed.

## Delegation discipline
- Implement: gpt-5.5 xhigh (`delegate task --backend codex --model gpt-5.5 --effort xhigh --write`).
- Review: gpt-5.6-sol high (`--model gpt-5.6-sol --effort high`, read-only, refute-first, until zero
  Critical/High). R2/R3 use TWO reviewers: protocol/concurrency AND OS/cgroup, separate packets.
- Workers never spawn subagents. Orchestrator owns all VCS/commits. Verify on exact SHA; gate on exit
  status (never `grep FAIL`). Every unit: gpt-5.6-sol review before commit.

## Decision & ownership rules
Option A adopted (remove the authority's logical grant from the parked-worker physical frame; real
park-now/release-later custodian). Binding invariants:
1. AUTHORITY owns authorization (durable logical grant/nonce).
2. CUSTODIAN owns the physical release capability (channel-scoped release secret).
3. SUBMISSION COORDINATOR owns commitment timing.
4. ACTIVATED STATE ROOT owns whether the daemon may downgrade.

## Product scope (first strict release)
- **NO LEGACY / NO BACKWARD-COMPAT (binding, user-directed 2026-07-20).** There are no deployed users and no
  old on-disk/on-wire artifacts. Do NOT preserve backward-compat decode paths, do NOT build
  upgrade/migration/"old-worker-survives" scaffolding or tests, and do NOT default missing fields for
  compatibility — require explicit current-version fields and fail closed on anything else. Version/old-frame
  rejection is retained ONLY as cheap fail-closed defense (a single reject test), never as a compat feature.
  This does not by itself delete the unactivated non-strict default runtime (that stays until strict is the
  only path); it means: spend zero effort making anything compatible with a prior version.
- The ONLY shipped AB-D submission mode is **IdentifiedFenced**.
- No-ID and unfenceable/custom submissions are **rejected before any mutation** (before Backend.Start /
  session construction that can exec / bbolt accept / JSON job creation / parked prep). NO fallback from
  strict admission into LegacyUnfenced.
- Distinct-mode machinery may remain in the model to avoid a large proof-kernel deletion, but has **no
  reachable strict-production route**. (LegacyFenced feature work is CUT.)
- Strict activation is **explicitly configured** (e.g. `--admission=strict`), not auto-enabled by a passing
  probe. Fresh root + strict-not-configured → existing legacy behavior may run (unactivated).
- Post-accept response-loss: an accepted identified obligation is NOT canceled; client resolves by replay.

## Contracts
### C1 Physical/logical separation (secret seam)
Remove `LogicalGrant` from the worker-validated frame. The physical release secret is custodian-owned
(crypto-random, channel-scoped, generated in Prepare, private to prepared process + worker) and is removed
from `model.ReleaseSecret`, `launch.LaunchRequest`, `launch.RunnerBinding`, `custodian.GrantToken`,
`internal/served`. Never derived from logical identity. **Seam sequencing (critical): never a checkpoint
with two secrets per launch or a `Release` that ignores a token** — see R1/R2B/R3A1/R3A2.

### C2 Explicit two-phase held-launch (not a goroutine blocked in BeforeRelease)
```
type PrepareSpec struct { Exec ExecSpec; LaunchKey LaunchKey; ReleaseSecret ReleaseSecret } // secret external THROUGH R2B only
type HeldLaunch interface {
    Ref() GroupRef
    Release(ctx, ReleaseSecret) (RunningHandle, ReleaseOutcome, error) // R2B: validates supplied==park-time expectation; one-use
    AbortAndVerify(ctx) (PhysicalQuiescence, error)
}
```
Atomic `parklaunch.Launch` becomes a wrapper: `Prepare(spec) → Release(spec.ReleaseSecret)` (one impl path).

### C3 NativeCustodian explicit prepared-state machine
States: preparing→prepared→releasing→running→finalized; prepared→aborting→finalized;
releasing→release_accepted→running; releasing→release_unknown→containing→finalized.
`ActiveCustodyCount` includes prepared+releasing. Idle/stale-binary shutdown must not treat the daemon idle
while a worker is parked. `Close` aborts-and-verifies prepared launches or returns a typed refusal.
Release/Abort/Close serialized + one-use with defined duplicate outcomes. Race table:
| Race | Required result |
| Release vs Release | exactly one sends; second gets already-consumed |
| Release vs Abort before send | one wins; if abort wins, NO backend execution |
| Release vs Abort after send | execution possible; no second release; contain |
| Release vs Close | same ambiguity rule; contain |
| ctx canceled before frame write | DefinitelyNotSent |
| frame write began, ack lost | Unknown → contain |
| worker acked then exec failed | Accepted; observe exit + prove group quiescent |
| daemon dies while prepared | monitor/control-loss aborts; restart reconciles by durable GroupRef if bound |
| daemon dies after grant, before ack | execution possible; contain; never resend |

### C4 Typed release ambiguity
`ReleaseOutcome { DefinitelyNotSent, Accepted, Unknown }`. DefinitelyNotSent→abort ok. Accepted→record
release. Unknown→NEVER resend; assume execution possible; contain-and-verify. A bare error is insufficient.

### C5 Sticky one-way activation (minimal)
```
type AdmissionRootMetadata struct { Activated bool; ContractVersion uint16; ActivatedAtGen uint64 }
```
Lives inside anchored authority state; one-way during normal operation. An activated root may never silently
downgrade to legacy. NOT a broad feature-state machine.

Escape hatch is recover / seal-rotate / reset-empty — NEVER clear-a-nonempty-root (terminal jobs still leave
request-key tombstones enforcing exactly-once; clearing + serving legacy would silently violate replay):
- `agentbus admission inspect --state-root X`
- `agentbus admission recover --state-root X` (needs strict support env; reconciles all durable nonterminal
  obligations without opening a listener)
- `agentbus admission reset-empty-root --state-root X` (only when zero jobs/bindings/tombstones/launch
  records/recovery obligations)
- `agentbus admission seal --state-root X` (after all physical obligations quiescent; marks old domain
  permanently closed, retained read-only for audit; service continues on a NEW state root/UUID). Requires
  explicit `--start-new-authority-domain --acknowledge-replay-history-reset`. Rotation forfeits BOTH live
  job-API continuity AND cross-root request-replay continuity (same key could be accepted as new work in the
  new domain). Multi-root read/cancel/result routing deferred (no-user scope) — documented first-release
  limitation.

### C6 Platform
Linux + delegated cgroup-v2 + public-path self-test → `CrashDurableContainment=true`. Darwin → `false`,
strict capability NOT advertised/activated (no weaker same-name promise). Restricted/unsupported Linux →
`false`. Strict production composition + daemon-kill/restart gate run in Linux Docker cgroup-v2. macOS is for
compile/unit/protocol/state-machine/race tests only. (Amends the old symmetric Linux+macOS gate.)

### C8 One immutable per-Serve policy snapshot
```
type AdmissionServeMode uint8 // AdmissionLegacy, AdmissionStrictIdentified, AdmissionRecoveryOnly, AdmissionFatal
type ServeAdmissionPolicy struct {
    Mode; CrashDurableContainment bool; AcceptIdentified bool; AdvertiseRequestID bool;
    RejectLegacySubmissions bool; Reason error
}
```
Inputs (config, root activation metadata, runtime self-test, request-ID availability, authority
bootstrap/recovery) resolved ONCE during Serve → one immutable object installed with the per-Serve instance.
`Hello`, `job.submit`, status/cancel/recovery all read the SAME object. No request re-probes or
reconstructs capability. Admission activation is INDEPENDENT of `jobs.requestId` (an activated daemon may
need recovery-only even when new identified submits are disabled; request-ID stays frozen per PR #28).
`Hello` uses the same dynamic policy as request handling (no advertise-then-reject skew) — not static
`protocol.DefaultCapabilities()`.

### C9 The probe IS the runtime (single lease) + typed support
`NewNativeRuntime` acquires the one exclusive cgroup-root lease; `SelfTest` uses the same manager+lease; the
same runtime becomes production. Self-test exercises the PUBLIC physical path: Prepare hidden same-binary
fixture (not PATH) in a unique probe cgroup with monotonic deadlines/fixed inputs → independently prove NOT
exec'd → Release or Abort → ContainAndVerify → independently prove absence + verified cleanup before return.
```
type SupportClass uint8 // SupportAvailable, SupportRetryable, SupportUnsupported, SupportUnsafe
type SupportAssessment struct { Class SupportClass; Cause error; Attempts int; CleanupSafe bool }
```
Retry ONLY when the attempt's process was never created OR every object it created is contained + verified
absent. NOT retryable (→ Unsafe): uncertain release + unproven cleanup; exec-before-release; can't prove
probe cgroup empty; leaked FDs preventing monitor EOF; contradictory identity; attestation mismatch.
Activated-root behavior: Available→continue; Retryable→bounded retry on same runtime/lease;
Retryable-exhausted→fail THIS startup (retryable classification; do NOT clear activation, do NOT persist
permanent fail-stop; next restart retries); Unsupported→fail startup, never downgrade; Unsafe→fail-stop
visibly, require diagnosis. No cached probe success in first release.
Startup ordering (activated root): acquire runtime+lease → static prereq checks → read activation metadata →
reconcile recovery obligations FIRST → active launch self-test → build immutable policy → listen.
Fresh strict root: acquire → static → self-test(bounded retry) → atomically activate → bootstrap → listen.

### C10 One per-Serve runtime owner
```
type admissionInstance struct { runtime; repository; authority; launch; recovery }
```
Build locally → complete recovery → publish immediately before listener creation. On failure: close the
unpublished instance (runtime + lease released). On shutdown: unpublish + close exactly once. A second Serve
builds a fresh instance or is explicitly rejected; never reuse closed fields.

### R4A exec-seam boundary (no hidden exec before acceptance)
Reject unsupported requests before ANY backend method that can exec. Move backend setup/version probes to
Serve bootstrap → immutable `BackendDescriptor`; request path does pure option validation against it. Guards:
`Backend.Start`, `Backend.Resume`, `Session.TurnWithRunner`, request handlers do NOT call `exec.Command*`;
only explicit direct/probe runner impls may call `os/exec`. Black-box test: submit missing-ID/unfenceable
with a fixture whose `--version` writes a marker; assert rejection AND marker never created.
RESOLVED 2026-07-20 — global structural guard + strict-only ordering guarantee (two separated concerns):
- **Structural guard is GLOBAL.** No adapter lifecycle or validation method may launch a subprocess
  directly; ALL backend execution AND executable probing go through explicit neutral runner interfaces
  (`ProbeRunner`, `CommandRunner`). Applies to strict AND legacy. Rationale: hidden execution is a bad module
  boundary (callers can't know "construct a session" spawns; tests can't substitute/observe; cancellation
  ownership splits; a later refactor could reuse a legacy exec helper from the strict path and bypass custody
  without a package-level violation). P0A already introduced the neutral seam, so this is mostly moving
  executable probing behind an interface — not a legacy redesign.
- **Fail-closed ordering guarantee is STRICT-PATH-ONLY.** Only strict identified admission must reject
  (missing identity / unsupported backend / no controlled-runner support / invalid strict config /
  unavailable strict runtime) before any request-triggered probe, session construction, or durable
  acceptance. Legacy execution stays UNFENCED via explicit `DirectProbeRunner`/`DirectCommandRunner` — explicit
  + testable, but outside the AB-D guarantee. R4A does NOT implement legacy-fenced admission.

R4A structure:
1. Two-layer validation: pure `ValidateStaticOptions(StaticBackendDescriptor, BackendOptions)` (runs before
   admission) + explicit `ProbeBackend(ctx, ProbeRunner, descriptor) -> ProbedBackendDescriptor`.
2. Process-free session construction GLOBALLY: `Backend.Start`, `Backend.Resume`, `NewSession`, turn setup
   allocate/validate in-memory only — no `binary --version`, no helper spawn.
3. Strict backends probed at Serve bootstrap (construct native runtime → qualify → probe configured strict
   backends → immutable descriptors → recover → derive policy → listen). Avoids accept-then-discover-
   unsupported.
4. Legacy preserved via `DirectProbeRunner`/`DirectCommandRunner` (mechanical boundary cleanup, not a safety
   redesign).
Global architecture checks: `cliadapter` may import `engine/command`, may NOT import `os/exec` or
`engine/execution/*`; only `engine/command/direct_*.go` + explicit native-custodian + explicit helper/fixture
packages may import `os/exec`; `Backend.Start`/`Resume` process-free; `Turn`/`TurnWithRunner` don't directly
spawn. Behavioral tests: (1) Backend.Start never execs the binary; (2) Backend.Resume never execs;
(3) invalid strict request creates no version-probe marker; (4) legacy version probe still works via
DirectProbeRunner; (5) strict backend probe uses the bootstrap probe path; (6) TurnWithRunner launches only
via its supplied runner; (7) default legacy Turn delegates to DirectCommandRunner (no second exec impl).

## Per-unit ledger protocol (definition of done)
No unit is complete until its Docker-Linux run is recorded (append to `~/tmp/agent-server-delegate-progress.md`
AND summarize here). Each entry: unit · commit SHA · tree-clean · Docker image/kernel · cgroup
mode+delegation · exact command · expected checkpoint/sentinel · actual exit · actual checkpoint/sentinel ·
independent leaked-PID/cgroup check · log/artifact path · review disposition. Use `-count=1` (defeat test
cache). Opt-in gate: `AGENTBUS_RUN_STRICT_E2E=1 go test -tags=abd_strict_e2e ./internal/served -run
TestProductionStrictServe`. Ordinary suite stays green at every checkpoint. Review FAILS a unit if: the
sentinel moves earlier, a different error masks the intended boundary, a fixture process/cgroup leaks, the
test reaches an unsafe later state, the command wasn't run, or the ledger omits actual output.

## Unit sequence (dependency-ordered; production stays UnavailableRuntime until R7B)
- **R0**  decisions & contracts (this doc + ~/tmp R0 record). No behavior change.
- **R0T** real-Serve Docker-Linux harness + ledger protocol; opt-in gate expected-RED, sentinel
  `strict_admission_unavailable`. No production injection hooks. HONEST SCOPE (per sol review): pre-R4B the
  rejection fires at the jobs.requestId capability gate, UPSTREAM of native-runtime construction — so the
  gate proves "strict admission unavailable in the default composition", NOT anything about the native
  runtime yet. R4B strengthens it (see R4B).
- **R1**  park-protocol logical purity: remove LogicalGrant from release binding/frame/validation/equality/
  worker bootstrap expectation; bump `parkproto.Version`; reject old+mixed frames; keep release-secret
  threading (do NOT remove model.ReleaseSecret; do NOT touch LaunchController; do NOT implement
  NativeCustodian.Prepare). R1 gate: prove no bbolt/anchor record contains ReleaseSecret; no recovery path
  decodes parkproto frames; no restart path resends a release frame. Tests (NO legacy/upgrade scaffolding —
  see Product scope): (a) a restarted daemon recovering a BOUND current-version obligation performs ONLY
  ContainAndVerify (no frame decode/replay) — behavioral, via a recording fake custodian; (b) a single
  fail-closed test that a non-current/malformed frame is rejected. Strict frame decode requires EXPLICIT
  current-version fields (no backward-compat defaulting). Expected sentinel: `strict_admission_unavailable`
  (unchanged; production still rejects strict submits at the jobs.requestId gate).
- **R2A** held-launch contracts + race tests only (state machine, external-secret semantics, ReleaseOutcome,
  FD matrix, race table). No subprocess impl.
- **R2B** held-launch impl in parklaunch: Prepare/Release(secret, validated)/AbortAndVerify; one-use;
  serialized abort/release/close; atomic Launch → wrapper(Prepare→Release). Secret still EXTERNALLY supplied
  + validated. NativeCustodian.Prepare stays unavailable. Production unavailable.
- **R3A1** additive private native prepared path: crypto-random secret generated internally, passed to R2B
  primitive, retained privately, tokenless package-private release, physical result → typed ReleaseOutcome.
  Old public token path still validates. New path NOT selected by production/LaunchController. (Two impls
  temporarily; never two secrets/launch; never ignored token.)
- **R3A2** atomic public cutover: `PreparedProcess.Release(ctx) (RunningProcess, ReleaseOutcome, error)`
  tokenless; NativeCustodian.Prepare returns the new impl; migrate LaunchController/adapters/unavailable/
  fakes/tests; remove ReleaseSecret from model/launch/served/submission; delete old externally-credentialed
  path; secret lives ONLY in the native park protocol. Architecture test enforces the removals.
- **R3B** MUST-ADDRESS (from R2B OS/cgroup review, deferred here as containment-engine scope): (F-C) the
  retained-cgroup absence PROOF must be process-group emptiness `kill(-pgid,0)==ESRCH`, NOT cgroup-leaf-empty
  alone; explicit cgroup-migration/`setsid` escape is out-of-scope per P0C#2 but the proof must be sound and
  the boundary documented. (F-D) PID-reuse-before-cgroup-placement: place the worker into the retained cgroup
  before it can be reaped / add a PID-reuse fence, so numeric-PID placement cannot bind the wrong process.
- **R3B** native monitor + custody lifecycle: port hidden `internal-monitor` cmd + hidden parked-worker
  dispatch + native defaults at a LOWER package (`engine/execution/custodian/native_defaults_linux.go` or
  `internal/nativecustody/config.go`); monitor spawning + FD ownership + daemon-control-EOF; finish
  prepared/releasing/running/finalized ownership. Dependency direction: `cmd/agentbus → thin dispatch →
  parklaunch/nativecustody`; `custodian → physical impl+typed options`; `served → receives a completed
  Runtime`. NEVER `cmd/agentbus → internal/served`. Production unavailable.
- **R3C** single-lease runtime qualification: same-runtime SelfTest, bounded classified retry
  (SupportClass), cleanup requirements, Linux-only strict support, Darwin strict-unavailable. Production
  unavailable. Expected sentinel: self-test passes; strict activation still disabled.
- **R4A** GLOBAL no-hidden-exec structural guard (all adapter lifecycle/validation process-free; execution +
  probing via explicit runner/probe interfaces; global arch checks + 7 behavioral tests) + STRICT-ONLY
  fail-closed admission ordering (reject missing-ID/unsupported/no-runner/invalid-config/unavailable before
  any request-triggered probe or durable acceptance; marker test). Two-layer validation (pure static +
  explicit ProbeBackend); strict backends probed at Serve bootstrap → immutable descriptors; legacy via
  DirectProbeRunner/DirectCommandRunner (unfenced, outside AB-D). Does NOT implement legacy-fenced admission.
- **R4B** identified production composition: per-Serve admissionInstance; immutable ServeAdmissionPolicy;
  bootstrap independent of jobs.requestId; strict identified route; reject no-ID/unfenceable before mutation;
  response-loss replay; no legacy fallback; default NOT auto-activated. R0T-GATE STRENGTHENING (owed here):
  once admission is decoupled from jobs.requestId, upgrade the R0T gate to assert the strict-submit rejection
  cause is the UNAVAILABLE NATIVE RUNTIME (not the jobs.requestId gate), and only then track a
  native-runtime-specific sentinel. This retires the review's "different error masks the boundary" concern.
- **R5**  sticky activation + administration: anchored activation metadata; no-downgrade rule; inspect;
  recovery-only; reset-empty-root; seal + start-new-authority-domain rotation; support-loss diagnostics.
- **R6/R7A** mandatory production conformance: turn R0T GREEN under Linux cgroup-v2 with NO factory
  injection / NO manual field set: default runtime construction; real identified submit; no exec before
  grant; release-ack loss; cancellation; descendant containment; daemon SIGKILL; restart+recovery;
  activated-root support-loss; probe/runtime lease reuse; second Serve; no-ID rejection; unfenceable
  rejection; independent OS absence oracle.
- **R7B** tiny enablement: expose `--admission=strict`; select native runtime; publish derived capabilities;
  remove the temporary unavailable expectation from the E2E gate; version/docs. NO lifecycle code.

## Verification per unit
`CGO_ENABLED=0 go build ./...`=0; `GOOS=linux go build ./...`=0; `gofmt -l`=empty; `go vet ./...`=0;
`go test ./...`=0 (macOS); targeted `-race -count=N` where concurrency changed. Orchestrator re-verifies
Linux cgroup-v2 privileged Docker `-p 1` + the opt-in strict E2E (expected sentinel per unit). R6/R7A: real
`agentbus serve` + real job through default composition.

## Open sub-decisions
(none — R4A exec-guard scope RESOLVED 2026-07-20: global structural guard + strict-only ordering guarantee;
see the R4A contract block.)

## Log
- 2026-07-21 R3C-fix4 (this commit): closed the last High — the inherited-cleanup unlink TOCTOU.
  Worker delivered the serialized design (scoped transient flock: acquire NB → re-verify identity
  UNDER the flock → unlink → release; contender holds ⇒ typed skip); worker was Linux-blind, and the
  orchestrator diagnosed + repaired four Linux integration bugs (each flagged for the closing review):
  (1) inherited rootfd was O_PATH — flock(2) on O_PATH = EBADF ⇒ O_RDONLY; (2) inheritedLeafCapability.
  Remove ran the held-verify membership gate BEFORE the flock, pre-empting typed-skip/tombstone ⇒
  reordered, emptiness checked UNDER the flock via direct leaf-fd read (parsePopulatedEvents);
  (3) RealContainment failed containment on ANY post-absence cleanup error ⇒ typed unleased-skip
  tolerated ONLY behind TolerateUnleasedCleanupSkip, set solely by the monitor composition (daemon-side
  lease regressions still surface) + propagated through platformBindContainmentTarget; (4) the test
  helper monitor duplicates the production composition and needed the same flag (exit-3 root cause) —
  drift between duplicated composition points noted for review. Monitor-EOF test now encodes the full
  lifetime-lease contract: monitor proves absence + typed-skips the leaf; daemon-side leased
  ContainAndVerify reaps; leaf gone after. Verified: macOS static/full/race=0; Docker cgroup-v2:
  conformance=0 (serialized dead-owner removal, contender skip, replacement tombstone, monitor-EOF,
  two-launch overlap all green), full -p 1=0, race=0, R0T RED sentinel=0. Closing sol review next —
  SHIP closes R3C.
- 2026-07-21 R3C-fix3 (50b357d): closed fix2-review's 2H+2M. F1 inherited monitor cleanup no
  longer encodes leased:false as held — explicit identity-guarded inheritedLeafCapability cleanup path
  distinct from lease-required realFS.Remove (acquireHeldRoot now requires leased; ENOENT/ESTALE
  proven-gone only; EPERM/EIO error). F2 two-launch overlap test uses a FIFO entered/release handshake
  asserting B blocked before A's cleanup (no discarded wait errors). F3 StartMonitorProcess owns
  InheritedLeaf with defer-close on every non-transferred return + fault-injection tests. F4 dead
  Prepare-contention branch deleted; post-construction ErrRootLeaseUnavailable ⇒ SupportUnsafe
  invariant. Worker job_...000082 completed CLEANLY with report-first (first clean cycle). Verified:
  macOS static/full/race=0 (one parklaunch JSON-read flake under full-suite parallelism, isolated x5
  green, papercut); Docker cgroup-v2: conformance=0 (unleased-cleanup + contender fail-closed +
  deterministic overlap all proven), full -p 1=0, race=0, R0T RED sentinel=0. sol verification next —
  SHIP closes R3C.
- 2026-07-21 R3C-fix2 (2bb9a1a): closed both R3C reviews' 4 High + 1 Med. F1/F2 (lease design, per
  OS reviewer's prescription): delegated-root flock is now a TRUE custodian-lifetime owner lease —
  ReleaseRootLease-at-bind DELETED; the monitor receives an INHERITED leaf-capability FD
  (MonitorLeafFD=6, arg-strict --leaf-fd, identity re-verified; FD-ownership contract updated;
  ExtraFiles); the monitor never touches the root flock; Remove no longer re-acquires (custodian
  asserts its held root); contention is construction-time-only. Second-daemon qualification beside a
  live launch is now impossible by construction, and cleanup can no longer defeat another launch's
  monitor bind (deterministic two-launch conformance test added). F3: evidence-based self-test
  classification (nativePrepareFailureEvidence — Prepare failures retry ONLY with proof of
  no-creation-or-verified-cleanup, else SupportUnsafe). F4: contradictory Available (non-nil cause or
  CleanupSafe=false) ⇒ SupportUnsafe with evidence joined. F5: flockRoot bounded EINTR retry;
  EMFILE/ENFILE/ENOMEM/EINTR on root open classify retryable (typed), not permanent Unsupported.
  Worker job_...000078 timed out at its final race rerun (acceptance green in its own log);
  orchestrator audited all diffs + verified independently: macOS static/full/race=0; Docker cgroup-v2
  conformance tier=0 (incl. two-launch handoff determinism + flagship self-test + monitor-EOF under
  lifetime lease), full -p 1=0, race=0, R0T RED sentinel=0. 20 files +712/-103. sol verification next.
- 2026-07-21 R3C (f439151): single-lease runtime qualification per C9/C6. NewNativeRuntime
  constructs ONE custodian, runs SelfTest on that instance through the PUBLIC path (Prepare hidden
  same-binary fixture internal-native-self-test-fixture via os.Executable → marker-absence not-exec'd
  proof → AbortAndVerify → attestation verify → verifySelfTestClean pgid+leaf absence), and the SAME
  instance becomes production; failure closes it + releases the platform lease → UnavailableCustodian.
  SupportClass{Available,Retryable,Unsupported,Unsafe} + SupportAssessment{Class,Cause,Attempts,
  CleanupSafe}; classified bounded retry (max 3; unverified cleanup ⇒ Unsafe escalation + stop; valid
  proof + CleanupStatus.Err only retryable if leftover proven removed). Lease contention: flock
  EAGAIN/EWOULDBLOCK ⇒ typed cgroup.ErrRootLeaseUnavailable ⇒ Retryable Attempts=1 stop-on-contention
  (fix worker round). Darwin ⇒ SupportUnsupported typed. DELETED ad-hoc probes (probeNativeRuntime/
  probeNativeCgroupRuntime/probeNativeLeaderContainment/native_cgroup_probe_unix.go). Delivery: R3C
  worker orphaned WITH full report (report-first worked); R3C-fix worker clean; orchestrator made FOUR
  Linux-test-only repairs the workers could not run (documented in-code, flagged to both reviews):
  (a) verification helpers via custodian's own manager — second in-process cgroup.New managers EAGAIN
  by C9 design; (b) bare test custodians attach an attestation channel (issuer wiring moved to
  NewNativeRuntime); (c) fake-based CloseRefuses test injects a fake retained factory — its refused-
  close containment previously fell through to the REAL platform manager and leaked the real root
  flock process-wide (the conformance-run poisoner); (d) monitor-EOF test drops the mid-test membership
  assertion that re-established the root flock beforeMonitorBind releases (production lease shape:
  daemon holds no root flock between monitor readiness and EOF containment). Discovered + recorded for
  review: the delegated-root flock lifecycle TOGGLES (held at manager use → released at bind → re-held
  at cleanup) — contention-window questions assigned to the OS/cgroup reviewer. Verified: macOS
  static/full/race=0; Docker cgroup-v2: conformance tier (AGENTBUS_CGROUP_CONFORMANCE=1)=0 — flagship
  single-lease self-test GREEN on real cgroup-v2 (Available, Attempts=1, CleanupSafe, typed contention
  on second acquisition, reacquire after Close), monitor-EOF containment GREEN — full -p 1=0, race=0,
  R0T RED sentinel=0. Gate harness note: conformance tier requires module pre-cache (go test sets
  GOPROXY=off for subprocess builds).
- 2026-07-21 R3B CLOSED. Final sol verification of fix4 (69f6dab): SHIP, 0 findings, criteria 1a-1d,
  2a-2c, 3a-3c, 4, 5 all PASS. Reviewer proved: abort-path proof/cleanup separation (attestation failure
  = proof error; post-proof failures = CleanupStatus only); fail-stop ownership chain intact after the
  contain/retire refactor (outer recover → c.failStop → servedAdmissionAuthority.FailStop anchors then
  trips SafetyLatch; startup recovery records → failClosed latch); zero shim survivors; 2-value wrapper
  has zero call sites; hand-finished authority fake consistent; macOS idle-shutdown race flake not
  plausibly caused by fix4 (no semantic path; known flake class). R3B totals: 5 commits (39c2315 R3B,
  8517171 fix, 3434063 fix2, 8102773 fix3, 69f6dab fix4), 4 refute-first review rounds, every round's
  findings strictly downstream of the prior fix. Launching R3C.
- 2026-07-21 R3B-fix4 (69f6dab): closed fix3-review's 1 High + 2 Medium. F1 PreparedProcess.
  AbortAndVerify now structurally (VerifiedQuiescence, CleanupStatus, error) through custodian + launch
  + all implementers/fakes; served abortLegacyPrepared/rejectAuthority record proven quiescence exactly
  once then surface cleanup.Err (no more discarded absence proof on cleanup failure). F2 coordinator
  LaunchContainment contract carries CleanupStatus; RecordQuiescence happens BEFORE cleanup failure
  surfaces/fail-stops; latch trip moved out of the pre-record wrapper; event-order test asserts
  contain→record_quiescence→fail_stop. F3 CleanupAware* extension interfaces + type-assert adapters
  DELETED (launch + served mirrors); primary launch interfaces 3-value structural. Delivery: worker
  attempt 1 backend_error apply_patch (partial tree stashed), attempt 2 empty completion in 61s,
  attempt 3 (job_...000066) completed 95% then timed out on a hung acceptance run — orchestrator
  finished the one unmigrated authority test fake (3 signatures, mechanical) and verified all findings
  present in the diff. Verified: build/linux-build/gofmt/vet=0; macOS full=0; macOS race: one
  TestIdleShutdownWaitsForAuthorityOwnedWork timing flake under multi-package race parallelism
  (isolated -race -count=5 green, package rerun green — known served-shutdown flake class, papercut);
  Docker cgroup-v2 full -p 1=0, race -count=2=0, R0T RED sentinel strict_admission_unavailable=0.
  sol verification (final R3B round) next.
- 2026-07-20 R3B-fix3 (8102773): closed fix2-review's 2 High. F1 Darwin PID/PGID reuse window:
  leaderRetention now acquired at beforeMonitorBind (matching Linux fence-acquisition phase);
  beforeRelease only verifies the exact retained group; witness nil until acquired — post-placement
  failure paths are fenced on both platforms; ordering regression extended through the REAL
  waitBeforeProbeSignaler. F2 attestation/cleanup separation made STRUCTURAL: custody interfaces
  (ProcessCustodian/RunningProcess) now return (VerifiedQuiescence, CleanupStatus, error) — error means
  NO valid proof, CleanupStatus.Err carries post-proof cleanup failure beside a VALID attestation.
  Launch controller records the proven quiescence exactly once then surfaces cleanup.Err
  (eagerWait/finalizeWithVerified/containFinalResult); served coordinator containment records the
  attestation and trips the SafetyLatch on cleanup failure (fail-stop visible, proof not lost);
  recovery path consistent. launch.CustodianPort kept its 2-value shape with CleanupAware* extension
  interfaces (fakes unaffected) — flagged for reviewer assessment vs no-legacy. Worker job_...000058
  timed out 1 message before its report (3rd occurrence; acceptance green in its log); orchestrator
  audited all diffs + verified independently: build/linux/gofmt/vet=0; macOS full=0, race -count=2
  parklaunch/custodian/launch/served=0; Docker cgroup-v2 full -p 1=0, race=0, R0T RED sentinel
  strict_admission_unavailable=0. 17 files +622/-133. sol verification next.
- 2026-07-20 R3B-fix2 (3434063): closed the fix-verification review's 2 residual High. F1 unarmed
  Prepare failures no longer contain-before-reap: phase-aware cleanup closures (failBeforeVerifiedIdentity/
  failBeforeVerifiedPlacement/failAfterVerifiedPlacement) route every Prepare failure site; new
  waitBeforeAbsenceProofContainment seam — waitBeforeProbeSignaler starts the parent Wait via sync.Once at
  the FIRST ProbeGroup (identity fence preserved through the signaling phase, leader reapable exactly when
  the ESRCH proof needs it); preIdentityAbort passes the wait into terminateStartedProcess. F2
  finalAttestationLocked joins process.finalErr into the memoized finalAttestationErr (valid attestation
  preserved alongside non-nil cleanup error; repeated calls consistent); CleanupErr maps into
  PhysicalOutcome.Err so RealContainment surfaces cleanup failure on absent outcomes. 3 injected-failure
  ordering tests + retained-Remove-failure attestation regression added. Worker job_...000054 ended
  "orphaned" but wrote its FULL report first (report-before-final-acceptance mitigation worked). Verified:
  build/linux-build/gofmt/vet=0; macOS full=0 (24 pkgs), race -count=2 parklaunch+custodian=0; Docker
  cgroup-v2 full -p 1=0, race -count=2=0, R0T RED sentinel strict_admission_unavailable=0. sol
  verification round next.
- 2026-07-20 R3B-fix (8517171): closed both R3B reviews (protocol FIX-REQUIRED 1 Medium; OS/cgroup
  FIX-REQUIRED 2 High; F-C proof paths + F-D fence verified sound by reviewer). H1 prepared abort/EOF
  proof-vs-reaping cycle: startWaitBeforeContainment() (documented F-D-fence invariant) now precedes
  containment in containAndVerifyLocked/abortPreparedLocked/failArmedLocked + armed-failure path in
  Prepare — killed retained leader is reapable so kill(-pgid,0) can reach ESRCH. H2 fail-closed retained
  cleanup: Capability.Membership propagates ReadEvents errors; realFS.Remove requires held root lease
  (typed ErrRootLeaseUnavailable) and tombstones only proven ENOENT/ESTALE or durable-identity mismatch;
  backend close fail-closes on Unknown/Present membership (absence re-proof fallback); new
  Outcome.CleanupErr keeps cleanup status separate from absence proof; recovery containment
  (containPhysicalWithRetainedCleanup) removes leaked leaves under the root lease. H3 composition
  boundary realized: agentbusserve.Config = served.Config (alias, no field mirror); agentbusserve
  constructs NewUnavailableRuntime and injects via served.Config.Runtime; served uses injected runtime
  with fail-closed nil check (nil Process → unavailable). Worker job_...000050 timed out mid-final-race
  rerun (papercut pattern; prior full acceptance pass logged); orchestrator verified tree independently:
  build/linux-build/gofmt/vet=0; macOS full=0; macOS race -count=2 (parklaunch/containment/agentbusserve
  + earlier custodian/cgroup by worker)=0; Docker cgroup-v2 -p 1 full=0, race -count=2=0, R0T RED
  sentinel strict_admission_unavailable=0. sol fix-verification review next.
- 2026-07-20 Plan LOCKED after design dialogue. Branch reset abd-authority→4a8f59d (backup
  abd-s5b-inert-d957878). Awaiting user "go" before R0 launch.
- 2026-07-20 R4A exec-guard sub-decision RESOLVED: GLOBAL structural no-hidden-exec guard (strict+legacy) +
  STRICT-ONLY fail-closed admission ordering; legacy stays unfenced via explicit direct runners. Encoded.
  Still awaiting "go".
- 2026-07-20 GO given (goal: implement ledger, delegate gpt-5.5 xhigh impl / gpt-5.6-sol high review,
  continue until exhausted). R0 committed as 23275e1 (roadmap doc; no behavior change). R0T real-Serve
  strict-E2E harness launched: worker job_20260720T151723000000000Z_000002 (codex gpt-5.5 xhigh, --write).
  Packet: ~/tmp/delegate-packets/abd-R0T-real-serve-harness.md. Expected sentinel
  strict_native_runtime_unavailable; opt-in gate abd_strict_e2e + AGENTBUS_RUN_STRICT_E2E=1.
- 2026-07-20 R3B (this commit): native monitor + custody lifecycle + F-C/F-D. Ported hidden internal-monitor
  cmd (cmd/agentbus/monitor.go + main dispatch + parklaunch run-from-FDs); shared native defaults
  (internal/nativecustody/config.go); F-C absence PROOF now process-group emptiness (containment engine:
  RetainedObjectProofEmpty defers to processGroupProbe/Signaler.ProbeGroup; cgroup-empty only supplementary;
  setsid/migration escape documented out-of-scope); F-D PID-reuse fence (verifyRetainedPlacementProcess checks
  PID+HighResStartToken before/around retained-cgroup placement); dependency guard "cmd/agentbus does not
  import served" via thin internal/agentbusserve facade. Worker TIMED OUT (redundant trailing patches; report
  lost) but work landed coherent+complete; ORCH FIX: monitor_test spawned internal-monitor without Setpgid ->
  PGID 1 in container PID-ns -> monitor never became group leader (Linux-only fail; macOS green) -> added
  SysProcAttr{Setpgid:true} matching StartMonitorProcess. Verify: macOS linux-build/gofmt/race-count=3
  (containment/custodian/parklaunch/cmd)=0 + full=0; Docker -p 1 full=0 (after clearing a low-freq custodian
  real-cgroup full-suite flake, green in isolation/count=3/re-run; papercut) + race=0 + R0T RED=0. NOTE for
  reviewers: agentbusserve is broader than "thin dispatch" — dep reviewer to assess. TWO reviews pending.
- 2026-07-20 R3A2-fix (489ec9a): closed BOTH R3A2 reviews (cutover itself confirmed correct). HA
  (protocol, fail-closed): served failReleasedByGroup + failReleased passed the possibly-canceled caller ctx
  to ContainAndVerify/RecordQuiescence -> now use detachedAdmissionCleanupContext = WithTimeout(WithoutCancel(
  ctx), admissionDetachedCleanupTimeout) so containment of a possibly-live group can't be aborted by the same
  cancellation that caused Unknown (matches controller.go:488 WithoutCancel). HB (secret, test): rewrote
  durability_secret_test to thread a live parkproto.ReleaseSecret sentinel through the real flow + scan all
  durable sinks (no longer vacuous). Verify: build/linux/gofmt/vet=0; race -count=3 served/authority/custodian/
  launch (macOS)=0; macOS suite=0; Docker -p 1 full=0 + race=0 + R0T RED=0. Both reviews CLOSED.
- 2026-07-20 R3A2 (ae4889a): ATOMIC public cutover (19 files). PreparedProcess.Release now tokenless
  Release(ctx)(RunningProcess,ReleaseOutcome,error) (custodian+launch); custodian.GrantToken + logical
  model.ReleaseSecret DELETED (certificate.go -22); NativeCustodian.Prepare returns real *nativePreparedProcess
  (integrated with heldPrepared registry); served release-<job>-<ordinal> minting removed; secret type moved
  to internal/parkproto.ReleaseSecret (park-protocol-internal only). LaunchController.Release tokenless with
  fail-closed outcome mapping (Unknown->contain no-retry, NotSent->abort, Accepted->record). architecture_guard
  test enforces no ReleaseSecret/GrantToken in logical layer. Production still UnavailableRuntime; R0T RED.
  Verify: build/linux/gofmt/vet=0; race -count=3 parklaunch/custodian/launch (macOS)=0; macOS suite=0; Docker
  -p 1 full=0 + race=0 + R0T RED=0. TWO reviews pending (secret-removal completeness + protocol/concurrency).
- 2026-07-20 R3A1-fix (93b8a9a): closed R3A1 sol review (3 High). F1 tests no longer print secret values
  (report properties only). F2 effects.AbortAndVerify always attempts custodian contain+close+delete after a
  parklaunch abort error (success if absence proven; else retryable via ContainAndVerify, no strand/false-
  finalize); held_launch.go HandleControlLoss now handles Aborting (contain-retry). F3 custodian tracks a
  heldPrepared registry (documented lock order, no cycle); Close refuses with running procs, aborts-and-verifies
  prepared held launches or returns typed ErrHeldLaunchCloseRefused, idempotent; ActiveCustodyCount includes
  prepared. ORCH FIX: the new Close-refusal test asserted a platform-specific inner cause (ErrNativeCustodian
  Unavailable) that holds on Darwin but not Linux cgroup-v2 — relaxed to the contract-level typed refusal
  ErrHeldLaunchCloseRefused (Docker gate caught it; macOS was green). Verify: build/linux/gofmt/vet=0;
  custodian -race -count=3 (macOS 192s)=0; macOS suite=0; Docker -p 1 full=0 + custodian -race=0 + R0T RED=0.
  sol review CLOSED.
- 2026-07-20 R3A1 (428a1ea): ADDITIVE private native prepared path (2 new files native_held_launch.go +
  test, build-tag darwin||linux). Crypto/rand 32-byte channel-scoped internal secret (zeroized, not
  identity-derived, caller secret ignored, one secret/launch); nativeHeldLaunchEffects implements R2A
  HeldLaunchEffects over parklaunch.Prepare + native containment backend (witness-acquisition checked);
  outcome map: chan-lost->DefinitelyNotSent, release-unknown->Unknown, default->Unknown (fail-closed);
  Accepted adopts a real NativeRunningProcess into custodian.running. Package-private prepareNativeHeldLaunch;
  public NativeCustodian.Prepare stub + Launch UNCHANGED; not production-selected. Verify: build/linux/gofmt/
  vet=0; custodian -race -count=3 (macOS 188s)=0; macOS go test ./...=0; Docker -p 1 full=0 + custodian
  -race=0 + R0T RED=0. sol review pending (protocol/concurrency + secret lens; dual review reserved for R3B).
- 2026-07-20 R2B-fix (523239a): closed BOTH R2B reviews. F-A: Launch now does state-aware cleanup on
  release error (cleanupLaunchReleaseError: prepared->AbortAndVerify, releasing/unknown->ContainAndVerify,
  typed GroupRef error), and new PUBLIC Prepared.ContainAndVerify/containAndVerifyLocked gives release_unknown
  a terminal proof-of-absence cleanup. F-B: monitorArmed set at BindTarget success (not after WaitReady);
  monitor.go recordBoundTarget sets stopArmed early; a WaitReady cancellation now routes to the VERIFYING
  cleanupArmedMonitorFailure/waitArmedMonitorCleanup (never bare-kill an armed monitor). New beforeMonitorWaitReady
  test hook forces readiness-before-cancel ordering. NOTE: worker ended in backend_error on a redundant final
  patch (report lost, papercut filed); tree independently verified coherent+complete. Verify: gofmt/linux/vet=0;
  parklaunch -race -count=3 (macOS 96s + Linux)=0; macOS go test ./...=0; Docker -p 1 full=0 + R0T RED=0.
  F-C (cgroup absence-proof soundness) + F-D (PID-reuse fence) deferred to R3B. Both reviews CLOSED.
- 2026-07-20 R2B (00b8a72): real two-phase parklaunch primitive. Prepared{opMu-serialized one-use state
  machine: prepared/releasing/released/aborting/finalized/release_unknown}; Prepare parks+verifies+arms+binds
  then blocks; Release sends once (secret validated at release), ctx-cancellation-aware (canceled ack ->
  ReleaseUnknown + ErrReleaseOutcomeUnknown), channel-loss -> failArmedLocked; AbortAndVerify/Close on
  unreleased worker (contain+prove), refuse once execution-possible; Launch = Prepare+Release (one path,
  BeforeRelease retained as the single gate). Secret still external; NativeCustodian.Prepare NOT implemented;
  production unavailable. Verify: build/linux/gofmt/vet=0; parklaunch -race -count=3 (macOS 87s) + Linux
  -race=0; macOS go test ./...=0; Docker -p 1 full=0 + R0T RED=0. TWO reviews pending (protocol + OS/cgroup).
  KEY review focus: post-send non-ctx ack failure (failArmedLocked/cleanupArmedMonitorFailure) must CONTAIN a
  possibly-exec'd backend group, not just tear down the monitor.
- 2026-07-20 R2A-fix (b86de29): closed sol review of R2A (3 High concurrency-design flaws). H1
  normalizeReleaseTuple: Accepted requires non-nil running + no err else contradiction->Unknown->contain; no
  lost live handle. H2 control-loss preemption: Release no longer holds opMu across SendRelease; cancelable
  releaseCtx stored; HandleControlLoss trips releaseCtrlLost + cancels ctx BEFORE opMu so a hung release-ack
  unblocks and exactly one containment runs (state!=Releasing guard prevents double-contain; Abort/Close
  refuse during Releasing). H3 failed ContainAndVerify stays retryable Containing (never false-finalize);
  HandleControlLoss handles Containing->retry. M9 FD constants marked doc placeholders + TODO(R2B/R3B).
  Verify: build/linux/gofmt/vet=0; custodian held_launch -race -count=5 (100 pass, 0 races, no hang); macOS
  go test ./...=0; Docker -p 1 full=0 + R0T RED=0. Reviewed diff (both release/control-loss interleavings);
  R2B dual review will further exercise. sol review CLOSED.
- 2026-07-20 R2A (ac3e880): held-launch contracts + race tests, ADDITIVE (3 new files, no existing files
  touched). engine/execution/custodian/held_launch.go: ReleaseOutcome{DefinitelyNotSent,Accepted,Unknown}
  (C4, invalid->Unknown fail-closed); PrepareSpec{Exec,LaunchKey,ReleaseSecret}+HeldLaunch{Ref,Release(ctx)
  tokenless,AbortAndVerify} (C2); HeldLaunchCore pure state machine over injected HeldLaunchEffects with
  opMu-serialized one-use Release/Abort/Close + HandleControlLoss (C3 states + race table). held_launch_test.go
  race table; docs/abd-fd-ownership.md (FD matrix + compile-time assertions). NativeCustodian.Prepare NOT
  implemented; production unavailable. Verify: build/linux/gofmt/vet=0; custodian held_launch -race -count=3
  (33 PASS, 0 races); macOS go test ./...=0; Docker cgroup-v2 -p 1 full=0 + R0T RED=0. sol review pending.
- 2026-07-20 R1-fix (9560a47): closed sol review of R1 (verdict was FIX-REQUIRED; core R1 confirmed
  correct). F2 strict frame decode now requires explicit current GroupRef fields (pointers; nil->ErrMalformed)
  + Validate(); removed backward-compat defaulting (no legacy). F1 deleted vacuous upgrade tests; added
  behavioral served recovery tests: bound obligation => exactly one ContainAndVerify(cause=Recovery) on the
  durable group (no frame decode/replay/prepare/release/abort/backend-start), unbound => finalized w/ no
  backend start. F3 arch guard now AST/import walk over all non-test authority+served recovery files. F4
  durable-secret oracle threads a live sentinel ReleaseSecret through the release path and scans all durable
  sinks (safety record/projection/binding/bbolt/anchor/raw file) for bytes+field names. Verified: macOS
  build/vet/gofmt=0, go test ./...=0 (1 pre-existing served socket flake under full-suite parallelism, isolated
  count=5/race=3 green, papercut filed), race -count=3=0; Docker cgroup-v2 -p 1 full suite=0 + R0T RED=0. sol
  review CLOSED.
- 2026-07-20 R1 (e360d8e): parkproto logical purity. Version 1->2; LogicalGrant removed from
  ReleaseBinding + validateExpectation/validateStatic/equal; parklaunch releaseExpectation grant-free; worker
  bootstrap grant-free. Added tests: old/mixed-frame rejection (codec), durable release-secret absence
  (authority), recovery no-frame-decode/no-replay guard (architecture), 2 upgrade scenarios (recovery_tokens).
  DEFERRED to R4 (intentional): parklaunch.Spec.LogicalGrant + parent-side validation retained; model.ReleaseSecret
  retained; LaunchController/NativeCustodian.Prepare untouched. Independent verify: macOS build/vet/gofmt=0,
  go test ./...=0, race -count=3 (parklaunch/parkproto/custodian)=0, vet -tags=abd_strict_e2e=0; Docker
  golang:1.26 --privileged --cgroupns=private -p 1 full suite=0, R0T gate RED (strict_admission_unavailable)=0.
  sol review pending on the committed SHA.
- 2026-07-20 R0T LEDGER: worker finished completed_noncompliant (report-SHAPE miss only: missing
  Criteria/Receipts/Verification/Scope sections; work itself in-scope — only internal/served/strict_e2e_test.go
  added, no forbidden files). Independent verification on tree @23275e1+file: CGO_ENABLED=0 go build ./...=0;
  GOOS=linux go build ./...=0; gofmt -l cmd internal engine=empty; go vet ./...=0; go vet -tags=abd_strict_e2e
  ./internal/served=0; macOS AGENTBUS_RUN_STRICT_E2E=1 go test -tags=abd_strict_e2e ...=SKIP(non-linux),PASS.
  Docker golang:1.26 --privileged --cgroupns=private cgroup-v2 (controllers incl memory/pids): gate PASS,
  sentinel strict_native_runtime_unavailable emitted, GATE_EXIT=0. Independent OS check n/a (no process
  launched — RED baseline rejects at capability gate). Committing then sol review on the SHA.
