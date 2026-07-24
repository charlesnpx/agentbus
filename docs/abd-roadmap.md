# AB-D strict-native roadmap (LOCKED 2026-07-20)

Canonical, durable run-state + roadmap for completing AB-D native containment in production.
Doctrine (read-only): `~/tmp/orchestrator.md`. Packets: `~/tmp/delegate-packets/`. Scratch ledger:
`~/tmp/agent-server-delegate-progress.md`. This file is the source of truth for scope + sequence + status.

STATUS (AB-E): E0-E7 CLOSED; E8 documentation in final review (2026-07-24). AB-E completes when the E8 review returns SHIP; criterion 13 (candidate-SHA remote green) stays deferred pending Actions billing and the approved .github/workflows installation. See "AB-E hardening
roadmap" section below — 11 sequential units (E0→E8) from the post-completion external evaluation.
AB-D itself remains COMPLETE as recorded.

STATUS (AB-D): COMPLETE (2026-07-22). ALL 14 UNITS CLOSED. R7B closed at e42aa5f (SHIP, no blocking
findings): strict admission became live — explicit, sticky, fail-closed; the R0T
gate runs a REAL identified job end-to-end through the production binary on Linux cgroup-v2
(sentinel `strict_admission_real_job_end_to_end`), with byte-identical replay, exactly-once
execution, persisted IdentifiedFenced proof, protocol-correct inline results, and
independent-oracle containment. Version 0.6.0. The R6/R7A+R7B gate arc found and fixed FIVE
production defects that no unit test caught. Darwin/unsupported platforms fail closed typed. AB-E
later removed the legacy/default admission split; current production `agentbus serve` is strict-only.

## Repo / branch
- Working branch `abd-authority` reset to `4a8f59d` (S5A capability-off checkpoint; reviewed clean).
- Inert S5B `d957878` preserved on `abd-s5b-inert-d957878` (salvage: hidden monitor cmd + production
  containment params — port in R3B, NOT retained as semantic base).
- `main` untouched. All AB-D work pushed to `origin/abd-authority` (PR #29); tip at AB-D close: 4e0bd50.

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
- 2026-07-22 R7B CLOSED (SHIP at e42aa5f) — ROADMAP COMPLETE (14/14). Arc: 8ed727a (landed:
  --admission=legacy|strict flag, default byte-compatible legacy, strict explicit per C5;
  native runtime via the R6/R7A-proven served.StrictAdmissionConfig; dynamic
  admission.strictContainment Hello capability (jobs.requestId stays frozen); R0T gate rebuilt to
  run the REAL cmd/agentbus binary end-to-end — new sentinel strict_admission_real_job_end_to_end;
  unavailable-branch defense retained as a unit test; VERSION 0.6.0 + docs. The gate found two
  more production defects before going green: CLI default backends used bare names where strict
  native exec requires absolute ExecSpec.Path (now resolved from PATH up front), and strict
  job.result omitted protocol-mandated inline text for small results (now hydrated from the
  certified artifact under the inline cap with digest verification)) → e42aa5f (fix, orchestrator
  repairs audited SHIP: explicit empty/whitespace --admission fails closed with the usage error;
  hydration reads bounded at inlineCap+1 before exact-length+digest verification). Unit total:
  3 worker turns on one thread + 2 orchestrator repairs, 2 review rounds, 3 Docker gates. NO
  lifecycle code, NO engine/** changes — the earlier units held. Five production defects were
  found by the R6/R7A+R7B real-kernel gate arc overall: quiescence-before-fail-stop;
  release-unknown over-fail-stop after proven containment; cancel-kill latch misclassification;
  C9 lease-order preflight; inline-result protocol violation (plus the absolute-path composition
  gap). Production posture at completion: strict admission opt-in, one-way activation, durable
  fail-closed custody, R0T GREEN.
- 2026-07-22 R6/R7A CLOSED (SHIP, 0 findings at 6faedf5). Arc: 6ef6ada (landed: ONE production
  strict composition — served.StrictAdmissionConfig, the exact function R7B wires to
  --admission=strict — driving 12 real-subprocess Linux cgroup-v2 conformance scenarios with zero
  injection and an independent OS oracle; conformance found+fixed FOUR production defects:
  release-unknown recorded no quiescence before fail-stop; release-unknown fail-stopped the daemon
  even after PROVEN containment (C4: fail-stop is for the unprovable — contained+proven now
  finalizes typed and the daemon keeps serving); our own cancel TERM->KILL tripped the safety
  latch; a second Serve blocked on the bbolt flock before surfacing lease unavailability (C9
  preflight); plus NativeCustodian.Close aborts provably-unexecuted prepared launches) → 872e413
  (fix: ContainmentIntent marked before signals travels on LaunchRequest/RunnerBinding — the
  cancel-vs-registration race can no longer spuriously fail-stop; deterministic pre-grant-hold
  ordering proof; LaunchReleaseOutcomeFact durable write-once not_sent/sent_unknown/acked with
  causes derived from record never shape; test-local /proc oracle) → bf6f233 (fix2: fixture exec
  evidence at first instruction; unpersistable release facts fail-stop on the legacy path too;
  barriered cancel proof) → 6faedf5 (fix3: entry tags via t.Setenv genuinely reach the backend
  exec env; entry-vs-stdin evidence distinguishable; propagation regressions fail loudly). Review
  trajectory 2H+2M → 1H+2M → 1H → 0 across 4 refute-first sol rounds (one reviewer backend failure
  resumed on-thread). Unit total: 6 worker rounds, 9 Docker gates. 12/12 scenarios: default
  construction+self-test; identified submit/replay; no-exec-before-grant (deterministically
  proven); release-ack loss (at-most-once); cancellation; descendant containment; daemon SIGKILL;
  restart+recovery (byte-identical replay); activated-root support-loss; lease reuse/exclusivity;
  no-ID + unfenceable rejections; independent OS absence oracle.
- 2026-07-21 R5 CLOSED (SHIP, 0 findings at 3bed997). Arc: 2f00fb0 (landed: anchored one-way
  AdmissionRootMetadata; no-downgrade policy; C9 support-loss table + AdmissionSupportDiagnostic;
  admin verbs inspect/recover/reset-empty-root/seal; legacy_downgrade + permanently_sealed causes)
  → 5019111 (fix: repo-level one-way invariants via shared ValidateAuthorityMetaPut; SelfTest split
  from construction, probe at the C9 point per Serve; bounded runtime close; sticky fail_stopped +
  clear-fail-stop verb; seal rotation with --new-state-root; flock-held reset with ErrRootBusy)
  → 37c8716 (fix2: seal init-new-first — sealed-without-replacement unreachable; runtime cleanup on
  every pre-publication path + Config.Runtime single-use ErrRuntimeConsumed; meta reconstruction
  only on provably-fresh store; ContractVersion declare-once; UnavailableRuntime self-test support
  verbatim) → 7347d0e (fix3: SuccessorDomainUUID pinned in the seal record, immutable; consumed at
  disposal-COMMIT across all five disposal shapes; mkdir reservation + ownership-manifest cleanup)
  → b37093b (fix4: startable-successor replay; verify-absent + O_EXCL + os.Link exclusive
  publication; typed parent-must-exist) → 3bed997 (fix5: probeBeginStartability factored FROM and
  composed BY Begin — replay/startup divergence impossible; equivalence pin drives real Begin
  outcomes). Review trajectory 6H → 3H+1M → 2H+1M → 1H+1M+1L → 1H → 0 across 6 refute-first sol
  rounds. Landing gates: five consecutive zero-flake closure batteries after the shared-helper
  deadline repair (waitJobState/waitBackendStarts 1s→5s, papercut'd audit of residual 1s waits).
- 2026-07-21 R4B CLOSED (SHIP, 0 findings at 9726222). Arc: 6b3f2cb (landed) → 03ab8f2 (fix: five
  findings — fail-stop outruns stalled admission work; snapshot-checkout guards) → 499c6c3 (fix3:
  bounded 5s repository close, clear-state-first, leak-on-timeout; close takes admissionSubmitMu —
  no acceptance lands after close begins; bbolt 10s open timeout) → 4b2df13 (fix4: the close bound
  covers submit serialization — close-epoch marker first, bounded TryLock under ONE deadline,
  wedged-submit leak with crash-window-equivalence contract; storage failures => backend_unavailable
  not invalid_task_spec) → a264773 (fix5: backend session metadata validated BEFORE authority via
  exported ValidateSessionID, nonempty-invalid => backend_unavailable pre-acceptance; recovery
  PRESERVES durably recorded outcomes — a completed job no longer fail-stops FatalUnprovable on
  restart; Reaped only for authorized-without-recorded-outcome; physical gates unchanged;
  orchestrator repair: empty session id at Start is the NORMAL cliadapter contract — served
  fallback restored, mutation-checked pin test) → 9726222 (fix6: pin test asserts durable
  ses_-prefixed projection id; seam doc scoped to provable cases). Review trajectory 2H+3M → 3H+3M
  → 1H+2M → 0H+2M → 1M+1L → 0 across 6 refute-first sol rounds; three orchestrator repairs audited
  (one packet error caught by orchestrator, two doc/test residues caught by review). Gate v2
  landed mid-unit (user-directed): Docker 20-30min → ~1-2min (persistent cache volumes, single
  partitioned conformance-superset pass, targeted -p1), tiered race sweeps — landing gate ~6min.
- 2026-07-21 R4B LANDED (this commit): identified production composition + R0T gate strengthening.
  Worker (codex runtime, 15min round): per-Serve `admissionInstance` (immutable, constructed at
  bootstrap before listen: qualified runtime + probed descriptors + authority + coordinator/launch
  ports); immutable `ServeAdmissionPolicy` derived once; admission bootstrap DECOUPLED from
  jobs.requestId (flag stays unadvertised, no longer gates admission); strict identified route with
  ordered typed rejection taxonomy — strict_route_disabled / missing_identity(invalid_task_spec) /
  unsupported_backend(backend_unavailable) / unfenceable_backend(capability_missing) /
  invalid_strict_config(invalid_task_spec) / unavailable_native_runtime(capability_missing +
  RuntimeSupport assessment); reject-before-mutation proven by marker tests; response-loss replay
  (accepted obligation survives failed response; same-request-key replay returns same job); legacy
  fallback routing DELETED (SubmitLegacyUnfenced removed); default composition unchanged.
  R0T GATE STRENGTHENED (retires the R0T review's "different error masks the boundary" concern):
  strict E2E now asserts typed AdmissionCause == unavailable_native_runtime + non-available
  RuntimeSupport in the error data + NEGATIVE assert that the old jobs.requestId message is gone;
  NEW sentinel `strict_admission_native_runtime_unavailable` (old sentinel retired with the old
  gate meaning). Gate harness installs a PROBEABLE fake codex on PATH (S5A masquerade precedent) so
  rejection reaches the runtime boundary, not a missing-binary artifact.
  Orchestrator repairs (flagged for review; Docker R0T tier caught #1-#2, solo-battery triage
  #3-#5): (1) backend-specific probe failure is NOT Serve-fatal — recorded unfenceable + rejected
  pre-accept (fix2 H1 semantics; one missing binary must not kill the daemon; ctx errors stay
  fatal) + regression test TestServeBootstrapRecordsProbeFailureUnfenceableWithoutFailingClosed;
  (2) the e2e fake-codex fixture above; (3) PRODUCTION: bboltrepo opens with a bounded 10s flock
  timeout — nil options block indefinitely and a replacement daemon in the stale-binary handover
  could hang silently in admission bootstrap while its predecessor drains; (4) stopTestServer join
  1s→5s; (5) waitForSocket 1s→5s (Serve now bootstraps admission before listening; positive waits).
  Verification: macOS build/vet/gofmt + 3x -race sweeps + full suite clean (solo battery; a
  cross-load 6-failure cluster was proven to be concurrent-Docker artifacts); Docker conformance/
  full/race green + NEW R0T sentinel held. Next: sol refute-first review bound to this commit.
- 2026-07-21 R4A CLOSED. Re-review on 2a18d37: SHIP, findings NONE, closure table all-CLOSED
  (H1..L3 + orchestrator repairs R1..R3, each with file:line evidence). R4A totals: 2 commits
  (aea857e, 2a18d37), 2 worker rounds (1 stall caught by doom-signal sentinel at ~11min +
  same-thread resume), 2 sol reviews (3H+2M+3L → 0). Durable outcomes: no adapter lifecycle or
  validation method can launch a subprocess (neutral seams + AST/import allowlist guard); strict
  admission rejects fail-closed BEFORE any probe/session/durable write with capability-interface
  classification only; DirectCommandRunner.Wait is bounded in every path; process identity is
  token-verified fail-closed on all platforms (Darwin via sysctl, no subprocess); stream overflow
  and identity ambiguity are typed errors, never silent. Reviewer settled the worker-receipt
  mystery: stale checkout / misattributed receipt, not test caching (H3 changed the engine build
  ID). Launching R4B.
- 2026-07-21 R4A-fix2 (2a18d37): closed ALL 8 findings from the sol review of aea857e
  (DO-NOT-SHIP → re-review pending). Worker (codex-plugin runtime, thread 019f84b8-f6b5; one
  mid-run stall caught by the new doom-signal sentinel at ~11min and recovered via cancel +
  resume-last on the same thread): H1 name-based fail-open admission removed (unprobeable strict
  backends recorded unfenceable; "codex"/"claude" name fallbacks deleted — capability interfaces
  only; TurnWithRunner verified pre-accept); H2 post-leader stdout drain bounded by cancelGrace +
  idempotent pipe close (Wait bounded even with Background ctx and timeoutMs=0); H3 empty process
  identity tokens fail closed (typed ErrProcessIdentityUnverifiable, never signal) + REAL Darwin
  start token via unix.SysctlKinfoProc (no subprocess); M1 ring overflow returns typed
  command.ErrOutputTruncated (turn fails, never silent splice); M2 DiscoverModels takes
  caller-supplied ProbeRunner; L1 invalid-strict marker test runs against enabled admission;
  L2 dead layers deleted (direct_runner.go, BackendOptions, CommandRunner aliases); L3 cgroup
  guard rationale corrected.
  Orchestrator repairs (flagged for review): fixture daemon PIDs get real start-time tokens
  (3 sites — H3 fail-closed supervisor identity orphaned running test jobs whose fixture daemon
  was unverifiable; two tests at aea857e passed only BECAUSE of the empty-token bug class);
  reap test now models a genuinely departed supervisor (stronger, deterministic); idle-shutdown
  positive wait 1s→5s (known pre-existing sweep-load flake; negative assertion untouched).
  WORKER RECEIPT DISCREPANCY flagged: worker reported go test ./...=0 but the cancel test failed
  deterministically on its tree — receipts now independently re-verified per gate, always.
  Verification: Docker (golang:1.26 privileged cgroupns=private) conformance=0, linux_full=0,
  linux_race=0, R0T sentinel strict_admission_unavailable HELD; macOS build/vet/gofmt + full suite
  + 3x -race -count=2 sweeps green on final tree. Production composition unchanged
  (UnavailableRuntime). Next: sol re-review bound to this commit.
- 2026-07-21 R4A LANDED (aea857e): global no-hidden-exec structural guard + strict-only
  fail-closed admission ordering + runner-semantics fix. Two worker rounds: R4A worker
  (job ...000092, clean) delivered the unit — neutral seams `ProbeRunner`/`CommandRunner`, explicit
  legacy `DirectProbeRunner`/`DirectCommandRunner`, pure `ValidateStaticOptions` + `ProbeBackend` as
  the sole probing site, process-free `Backend.Start`/`Resume`/`NewSession`, Serve-bootstrap probing
  (`probeAdmissionBackends`, before listen), os/exec AST/import allowlist guard with rationale
  comments + unused-entry detection, 7 behavioral tests. R4A-fix worker (job ...000094) fixed the
  race-sweep hang (P0C#14 class: StdoutPipe + io.Pipe stderr let TERM-ignoring grandchildren and
  undrained callers block Wait to the 10m package timeout) — runner-owned os.Pipe pairs drained into
  bounded ring buffers; exactly-once directTerminator (cancel ctx / Interrupt / Wait-ctx watcher)
  closes runner read ends after group TERM→grace→KILL so Wait is bounded regardless of grandchildren
  or non-reading callers; stdout is the stream boundary, stderr gets a bounded 200ms diagnostic
  drain; cliadapter joins Wait in parallel with the stdout scan. The fix worker timed out at the
  tail (apply_patch retried against a stale file view — 10th worker lifecycle failure; the fix was
  already complete in-tree, receipts recovered from the rollout).
  Orchestrator surgical repairs, ALL flagged for adversarial review: (1) codexcli/claudecli
  Interrupt tests: blind 50ms sleep → deterministic trap-armed readiness (poll fixture stdin log,
  written strictly after TERM trap install); (2) authority `defaultAnchorStoreFor` keyed by anchor
  identity (dbUUID#schemaMajor) instead of repository POINTER (worker-diagnosed pointer-reuse
  "db uuid mismatch" sweep flake; the anchor is a per-database fact); (3) served terminal-wait test
  helpers 1s→10s positive-wait deadlines (record visibly mid-flight at 1s under sweep load).
  Verification: macOS build/vet/gofmt + full suite green; `-race -count=20 TerminatesOnce` and
  codexcli `-race -count=10` clean; full `-race -count=2 ./engine/... ./internal/served/...` sweeps
  green on the final tree; Docker (golang:1.26, --privileged, --cgroupns=private): conformance=0,
  linux_full=0, linux_race=0, R0T sentinel `strict_admission_unavailable` HELD (recipe pinned:
  requires `-tags abd_strict_e2e` AND `AGENTBUS_RUN_STRICT_E2E=1`; without both, go prints a
  deceptive bare PASS with "no tests to run"). Known divergence for review: Darwin
  `NativeProcessTable` no longer shells out to `ps` (PID liveness without a Darwin start-time
  token). Next: sol refute-first review bound to this commit.
- 2026-07-21 R3C CLOSED. Ship-confirmation on c9470de: SHIP, findings none, all acceptance criteria
  MET (single composition point verified at both call sites; nothing else changed). R3C totals:
  6 commits (f439151, 2bb9a1a, 50b357d, 9aff4be, c9470de + fix worker rounds), 2 domain reviews +
  4 verification rounds, findings narrowed monotonically 4H+1M → 2H+2M → 1H → 1M → 0. Durable
  outcomes: C9 single lease is now REAL (custodian-lifetime owner flock, construction-only contention),
  monitor runs on an inherited leaf FD and never touches the root flock, unleased cleanup is
  serialized + identity-guarded, self-test classification is evidence-based, and the Linux cgroup-v2
  conformance tier (AGENTBUS_CGROUP_CONFORMANCE=1) proves all of it. Launching R4A.
- 2026-07-21 R3C-fix4 (9aff4be): closed the last High — the inherited-cleanup unlink TOCTOU.
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

---

# AB-E hardening roadmap (LOCKED 2026-07-22, REV 3)

Post-completion external evaluation of `abd-authority @ 4e0bd50` (all findings independently verified
CONFIRMED) → 11 sequential units. Full plan: `~/.claude/plans/moonlit-imagining-wadler.md` (REV 3,
user-approved). Same delegation discipline as AB-D. Sequence is FULLY SEQUENTIAL, no parallel units:

E0 → E1 → E2A → E2B → E3 → E4 → E5A → E5B → E6 → E7 → E8

## Binding directives (user)
- No legacy path, no backwards compat; explicit breaks: protocol v2, storage schema v2; nothing
  translates or migrates (v1 handshake → version mismatch; removed methods → method-not-found;
  old storage → incompatible-schema).
- Fail closed everywhere: strict authority-owned execution is the ONLY path; unsupported platforms
  fail typed at serve BEFORE creating any root state. Linux-cgroup-v2-only production.
- DELETE the foreground surface (session.*/turn.*): product = identified job.submit only.
- Persistence redesign behind the existing `engine/execution/repository` contract (sqlite-ready),
  no persistence framework.

## Non-goals (binding)
AB-E does NOT include: interactive/foreground session redesign; protocol-v1 or schema-v1
compatibility; storage migration; a sqlite implementation; authority mutex redesign; public
terminal-proof serialization; a generalized daemon logging system; online backup or compaction;
relocation of execution packages under `internal/` (E9 deferred to a later cleanup roadmap).

Because E9 relocation was deferred, packages under `engine/execution/*` remain unstable
implementation APIs. They are public Go package paths only as a repository-layout artifact, not an
import-stability promise; external callers should treat `github.com/charlesnpx/agentbus/client`,
`github.com/charlesnpx/agentbus/engine`, and documented adapter contracts as the stable surfaces
unless a later roadmap explicitly promotes a narrower execution API.

## Units
| Unit | Scope | Status |
|---|---|---|
| E0 | Contract freeze: ADR-12, protocol.Version=2, storage schema v2, stable rejection causes, root-existence matrix, normative ADR-0B replay ordering, shutdown contract, result semantics (derived from authority terminal record, no public proof) | CLOSED 64031ca |
| E1 | Delete foreground surface + unidentified submit + --admission plumbing; reimplement handleIdentifiedJobSubmit per E0 replay ordering (LookupReplay before backend/filesystem validation, recorded fingerprint version); negative + replay test battery; architecture guard: no path reaches session.Turn outside an authority launch | CLOSED 005a8ae |
| E2A | Shared daemon launcher: readiness pipe (ready{protocolVersion,pid,canonicalStateRoot,socketPath} / failed{code,message}), PID-after-ready, Setsid, kill+reap on parent failure, stderr preserved to handshake, concurrent-start winner verification | CLOSED c69c83c |
| E2B | Autostart: typed diagnostic on unsupported host, restart-after-exit restores service, race convergence; `admission recover` gets a dedicated strict-native recovery constructor; compiled-CLI recovery in Docker gate | CLOSED 86a5c31 |
| E3 | Authority-only RPC handlers (no JSON merge/fallback); CLI status/result/cancel/list become protocol clients; delete `sessions` cmd; stable exit codes; compiled-binary E2E across restart+recovery | CLOSED 67be1d5 |
| E4 | Graceful shutdown: signal.NotifyContext (daemon serving only) + Shutdown(ctx) reusing existing durable cancellation; successful graceful shutdown ⇒ no live custody, no recovery obligation; forced-timeout path remains recoverable fail-closed | CLOSED d74288c |
| E5A | Record-level bbolt behind repository contract; Auditor + AnchorIdentified; binding_index as derived locator; dirty-closure validation sharing invariant helpers with full audit; operation-count tests (bbolt pkg) prove O(touched); 1k/10k/100k benchmarks | CLOSED 7fb6153 |
| E5B | Create/OpenExisting/OpenExistingReadOnly split (no ambiguous open-or-create); AuditIntegrity (Tx.Check drained + envelope + cross-record + index) at every existing entry point; root-existence matrix test per cell; unsupported first serve non-mutating; corruption fixtures fatal pre-bind, file untouched | CLOSED c8d50df |
| E6 | docs/protocol.md v2 reconciliation (hand-written; full job.submit identity schema, replay tables, rejection-cause table verified vs implementation) | CLOSED d1ef34b |
| E7 | CI lanes: committed gate scripts (scripts/ci/), full -race, strict-tag compile, privileged cgroup lane, product black-box lane, fail-closed lanes, govulncheck; release-check runs tests + strict smoke. Merge needs remote-green on candidate SHA (billing must be restored). `.github/**` edits need explicit user approval | CLOSED 22b6a77 |
| E8 | Docs: ADR index (+11,+12), operations runbook, offline-only backup policy, install caveat; FINAL holistic review of the complete candidate SHA. Deferred pending explicit user approval: install `scripts/ci/github-workflows-proposed/` into `.github/workflows/`; update PR #29 title/body. | IN FINAL REVIEW (doc fixes applied; closes on SHIP) |

## Verified defects driving AB-E (evidence at 4e0bd50)
1. Foreground session/turn executes outside the authority (server.go:1113/1153/1236; runAttempt non-admission branch).
2. Replay ordering violates ADR-0B (admission.go:1990-2110 validates backend/schema/fingerprint/workspace/runtime BEFORE LookupReplay).
3. Client autostart hardcodes legacy `serve --foreground`, no diagnostics channel (client.go:307).
4. `admission recover` composes UnavailableRuntime — can never succeed (main.go:305-308).
5. CLI sessions/status/result/cancel bypass the authority via the legacy JSON store (main.go:476-617).
6. Background serve: PID pre-readiness, no Setsid, orphan on post-Start error (main.go:406-442); no signal handling (main.go:54-55).
7. bbolt whole-store rewrite per mutation (loadState/persistState, DeleteBucket+CreateBucket).
8. ADR-1A unimplemented: no Tx.Check(); zero-length DB silently re-initialized (bbolt.go:310-341); ambiguous NewRepository everywhere.
9. protocol.md omits the strict identity contract; rejection causes collapse to invalid_task_spec (admission.go:2465-2470).
10. CI: no strict lane, race excludes client/+cmd/, gate scripts unversioned; tip red = GitHub Actions BILLING block (jobs die ~4s pre-step), not code.

## AB-E Log
- 2026-07-24: E7 CLOSED at 22b6a77 (SHIP on review round 2 + apply pass). Arc: worker 89fea16
  (gate scripts committed to scripts/ci/ — solo-battery, docker-cgroup-v2, product-e2e,
  release-check, fail-closed, vuln; five Actions workflows STAGED in
  scripts/ci/github-workflows-proposed/ with README, .github/ untouched pending user approval;
  actions SHA-pinned) -> orchestrator caught by EXECUTING the gates (worker sandbox had no Docker):
  docker run missing -i so the heredoc payload never reached the container — the gate exited 0 in
  0.27s having run NOTHING (fail-open); + go mod download for hermetic GOPROXY=off builds; + race
  partition needs CGO_ENABLED=1 -> review1 FIX(7H fail-open defects + 3M: workflow race under
  CGO=0; conformance partition missing packages; skip-on-unsupported = vacuous green privileged
  gate; product lane never testing its own artifact; release-check accepting 5s-alive and testing
  a different binary; fail-closed not checking residue; skippable required check merging
  unverified SHAs) -> fix1 d6afd2d (non-skippable strict-cgroup-preflight; full conformance set;
  AGENTBUS_E2E_PREBUILT_BINARY override so lanes test the exact artifact; status round-trip
  readiness; residue assertion via find; strict-lane push+dispatch only with evidence artifact;
  digest pin; + orchestrator repairs: MULTI-ARCH manifest-list digest with no --platform — amd64
  emulation on arm64 broke pidfd syscalls; release-check module-cache warm-up + serialized -p 1
  full-test phase against cgroup-root-lease contention; ALL lanes executed locally to green) ->
  review2 SHIP (2 worthwhile M) -> apply pass 22b6a77 (anchored ^TestProductionStrict.*E2B$;
  socket-first, timeout-bounded, PID-verified readiness probe; both lanes re-verified green).
  The fail-open docker gate is the campaign thesis in miniature: the CI layer itself needed the
  same refute-first treatment as the product.
- 2026-07-24: E6 CLOSED at d1ef34b (SHIP on review round 2, zero findings). Arc: worker aa982ec
  (protocol.md rewritten for v2 with per-section code verification — hello/version, full submit
  identity schema, E1 replay tables, 13-cause table + error.data shape + root_corrupt precedence,
  removed methods, authority-only status/result/cancel + exit codes, admin = daemonless CLI,
  shutdown/autostart semantics; -1018 lines of stale v1 doc; PLUS the carried E3 decision
  implemented: unknown_job protocol code emitted by all three unknown-job handler paths, CLI
  classifier keyed on code not substring, exit 10 unchanged) -> review1 FIX(2H+4M, all doc
  falsehoods: impossible policy.validate example; exit-code table claiming 12 for launcher-side
  root_corrupt/root_identity_mismatch which exit 11; legacy job_<ts> example IDs vs job-%020d;
  unconditional unknown_job claims ignoring fail-stop precedence; fingerprint v1 canonicalization
  under-specified; state-root overrides missing) -> fix1 d1ef34b (doc-only, truth-over-aspiration:
  exit-code table split by in-band vs launcher surface; validating example; opaque jobId note;
  healthy-lookup qualification; full canonicalization rules incl. verbatim number lexemes;
  AGENTBUS_STATE_ROOT + client Options overrides) -> review2 SHIP zero findings, every correction
  re-verified against code. Reviewer confirmed round 1: all 13 causes correctly paired, replay
  tables match E1, no unknown-job path emits invalid_task_spec anymore.
- 2026-07-24: E5B CLOSED at c8d50df (SHIP on review round 1 — first unit closed in a single review
  round; the new loop policy's apply-without-re-review pass used for the first time). Arc: worker
  3f00a4b (Create/OpenExisting/OpenExistingReadOnly replacing ambiguous NewRepository, folding in
  the E2B no-init opener as planned; existing call sites rewired only — recovery/clear-fail-stop/
  seal-source always OpenExisting, reset+new-root the only Create callers; AuditIntegrity with
  structural Tx.Check wired pre-serve/pre-mutation at every entry point; root-existence matrix
  test per ADR-12 cell incl. structural byte-identity fixtures and record-level repeat-detection;
  darwin unsupported-first-serve non-mutating E2E; both gates green FIRST PASS) -> review1 SHIP
  (1M+2L, no High/Critical; reviewer verified: no residual open-or-create path, O_EXCL enforced,
  read-only truly read-only, reset-empty proves emptiness via audit+RootStats under exclusive
  flock, audits run writer-free, matrix ordering correct) -> apply pass c8d50df (M1 failed-Create
  inode cleanup; L2 projection/quarantine-only audit findings proceed into the normative bootstrap
  repair path, authoritative findings stay fatal; L3 Lstat anchor presence — dangling symlink =
  present, refused non-mutating). E2B's OpenExistingNoInit groundwork paid off: fastest unit of
  the campaign (~1.5h).
- 2026-07-24: E5A CLOSED at 7fb6153 (SHIP, zero findings, round 4 of the new max-4 loop policy).
  Arc: worker b429b7c (full rewrite in one pass: point txs replacing whole-store-rewrite-per-
  mutation, binding_index derived locator with canonical verification, dirty-closure commit
  validation sharing helpers with the audit, schema v2, Auditor+AnchorIdentified, complexity
  proofs + 1k/10k/100k benchmarks) -> review1 FIX(2H+3M: SafetyLatch optional/unwired in
  production — ready-state corruption left listener open; RootStats silently discarded corrupt
  safety records enabling fail-open seal; audit nil-deref on missing binding_index; PutMeta
  O(total records) + dead instrumentation counters; tombstones counted as live jobs) -> fix1
  f597b52 (+2 orchestrator repairs: latch trips with TYPED corruption error before persistence —
  string-typed anchor hook was claiming the once-only trip; test semantics for fail-stop transport
  close EOF/EPIPE and record-level-corruption restart contract) -> review2 FIX(2H residual:
  read-side job.status/runtime/recovery-plan paths observed corruption without tripping;
  binding_index VALUES never validated anywhere in production) -> fix2 09c194a (read-side
  corruption routed to fail-stop with root_corrupt before root_fail_stopped; index values
  verified at every consultation; startup matrix checks image binding; memory RootStats typed;
  string-fallback classifier tightened; + orchestrator repair: startup index-corruption test
  asserts never-repaired + repeat-detection, byte identity reserved for structural open-time
  corruption — reviewer confirmed this ADR-12 reading) -> review3 FIX(1H: coordinator Snapshot
  path consumed images unrouted, ignored binding state; +2 Lows) -> fix3 7fb6153 (Snapshot routes
  binding+safety corruption to durable fail-stop, both callers propagate; orphan same-pair index
  entry = typed corruption; audit aggregates past index corruption) -> review4 SHIP zero findings,
  H1 exploit trace refuted end-to-end. Reviewer refutations held across rounds: binding_index can
  never return a wrong binding; dirty-closure complete; single-tx atomicity; DNC byte-stability.
- 2026-07-24: E4 CLOSED at d74288c (SHIP, zero findings, round 7). Hardest unit of the campaign —
  6 fix rounds. Arc: worker 4a19fb0 (Shutdown(ctx) orchestration, foreground-only signals, SIGTERM/
  SIGINT E2Es green in Docker) -> review1 FIX(3M: close phase discarded caller deadline + logged-
  success on close timeout; concurrent Shutdown ignored own ctx; PID removal replacement race) ->
  fix1 3f56b2a (min-deadline close + typed errors, ctx-aware single-flight, O_NOFOLLOW+quarantine
  PID teardown) -> review2 FIX(2M residual: post-close teardown unbounded + success cacheable past
  deadline; single-flight state survived re-Serve generations) -> fix2 accbef2 (ctx rechecks, per-
  generation shutdown state, ErrShutdownNotServing) -> review3 FIX(3M: restore-on-expiry unbounded;
  check-then-act generation gate; bootstrap signal exited nonzero) -> fix3 09b2c93 (bounded abandon,
  generation snapshots, bootstrap-cancel mapping; gate flake of the E2A context-split test dismissed
  after isolated -race -count=3 + full gate rerun green) -> review4 FIX(4: error/deferred-path fs ops
  post-cancel; Server-global mutations between generation checks; select-order cancel classification;
  ready frame emittable post-cancel; +Low timing-marginal test) -> fix4 cb0fc89 (post-cancel fs
  abstinence, serialized generation registration + instance-aware mutations + PID inode identity,
  errors.Is done-branch, guarded linearized readiness, ReadyHook test sync) -> review5 FIX(1M:
  classification wrong both directions — preflight cancel exited 1; joined SafetyFailStopError+
  Canceled masked to 0) -> fix5 1563331 (pre-select pure-cancel clean; typed-failure precedence,
  21-type gate list) -> review6 FIX(2M: DNC+cancel reverse-masked to 1; persisted fail-stop returned
  as bare Canceled by postDurableFailStopError -> exit 0/EOF while root durably fail-stopped) ->
  fix6 d74288c (fail-stop sentinel joined at authority source, four bypassing callers fixed with
  call-site audit; DNC dropped from gate, ErrAmbiguousCommit deliberately kept fatal; deterministic
  quarantine test) -> review7 SHIP zero findings. Best catch: review6's persisted-fail-stop-as-exit-0
  masking. Non-blocking Lows accepted under calibration: exceptional PID-restore sub-step race,
  close-epoch check-then-act under direct Server reuse (both fail-closed).
- 2026-07-24: E3 CLOSED at 67be1d5 (SHIP, 1 advisory Low). Arc: worker (authority-only handlers;
  CLI status/result/cancel/list as protocol-v2 clients via launcher autostart; sessions deleted;
  exit-code table 0/2-12) -> Docker gate caught TestStartReaperRecoversCrashedJob (legacy
  engine.Store reaper test invisible to macOS receipts) -> fix1 35b0ed5 (served background reaper
  DELETED — legacy-JSON-store-only walker: config fields, idle tick, startup reapKnownStores,
  recovery defaults; storeForJob narrowed to in-memory authority mapping) -> review1 FIX(1M:
  persisted fail-stop reached CLI as daemonlaunch.StartupError and exited 11 not 12; non-
  ErrStartupFailed kinds fell to exit 1) -> fix2 67be1d5 (classifier inspects StartupError.Code:
  fail-stop readiness codes -> 12, all other StartupError -> 11; new Linux E2E
  TestProductionStrictCLIStatusPersistedFailStopAutostartExitE2B on the real autostart path) ->
  review2 SHIP. Reviewer confirmed authority recovery subsumes the deleted reaper for authority
  jobs; invalid_task_spec for unknown-job accepted for E3 with distinct unknown_job code
  recommended for E6; named-policy L1 deferral judged honest (no policy field on SafetyRecord).
  First unit under the calibrated review policy (realistic-precondition weighting): 4 findings
  self-classified non-blocking Low.
- 2026-07-23: E2B CLOSED at 86a5c31 (SHIP, zero findings). Arc: worker 3df1d4d-pre (recovery
  composition + 4-test real-process battery; Docker gate — widened mid-unit from
  -run TestProductionStrictServe to the whole TestProductionStrict family, which the old pin would
  have silently skipped — caught 3/4 battery tests red on real Linux) -> fix1 (+1 orchestrator
  repair, SIGKILL-tolerance second site): race losers now corroborate a dialable winner on
  retryable cgroup-root-lease contention -> typed already-listening convergence; 3df1d4d ->
  review1 FIX(1H+2M: recovery could initialize/repair via normal bootstrapper) -> fix2 d26eb26
  (read-only no-create preflight, runtime close on error paths, before/after root hashing tests) ->
  review2 FIX(H1 TOCTOU + 3M) -> fix3 b6cbe6a (dev+ino handle identity, require-initialized anchor,
  typed lock-contention corroboration, canonical bucket/meta exports) -> review3 FIX(H1 residual:
  initialize() ran before the gate) -> fix4 3c9128f (bbolt.OpenExistingNoInit: no-create/no-init/
  NoFreelistSync-bounded open, same-inode bucket-deletion killer test) -> review4 FIX(1M schema-vs-
  bucket precedence under TOCTOU) -> fix5 86a5c31 (schema-first verification inside the no-init
  opener, typed UnsupportedAuthorityMetaSchemaVersionError) -> review5 SHIP. Delivered: strict-native
  recovery composition that structurally cannot create/repair root state; launcher-level race
  convergence under cgroup-lease contention; Linux real-process conformance battery (compiled-CLI
  recover of activated root, recover-missing-root typed + nothing created, autostart restores after
  SIGKILL, 3-way race -> one daemon) + macOS typed unsupported-host tests. OpenExistingNoInit is
  deliberate groundwork for E5B's OpenExisting split. Closure gates: battery + Docker green at
  86a5c31 (known cliadapter load-flake verified -count=3 isolated).
- 2026-07-23: E2A CLOSED at c69c83c (SHIP, zero blocking). Arc: worker 716a1eb (+resume: linux/arm64
  Dup2 break caught by Docker gate; AGENTBUS_READY_FD stripped from worker/monitor envs) -> review1
  FIX(4H+1M) -> fix1 8cab55b (+resume: loser blocked on bbolt lock pre-bind -> typed already-listening;
  probe-first non-mutating startup) -> review2 FIX(H2 residual in-root start lock + M bbolt blanket
  mapping) -> fix2 aedcc1b (+resume: lock relocated to tmp... then) -> review3 FIX(1H tmp TOCTOU + 3M)
  -> fix3 493fb16 (user-cache namespace, dev:ino key, deadline-long convergence, root-busy code) ->
  review4 FIX(2H: STARTUP DEADLINE BECAME DAEMON LIFETIME — every launched daemon exited ~9.75s
  post-ready — and lock-dir fd-trust gap) -> fix4 c69c83c (+resume: Linux real-binary
  survive-past-deadline test; bootstrap/service context split; Openat fd-relative locks; 2s
  foreground bound) -> review5 SHIP. Docker gate hardened to fail-closed exit aggregation (worst=N)
  mid-unit after it passed a red run. Two load-flakes papercut-logged.
- 2026-07-23: E1 CLOSED at 005a8ae (SHIP, zero blocking). Arc: worker 54d52e5 (+1 on-thread resume
  after orchestrator gates caught 5 real failures its receipt missed — masking test helper
  strictIdentifiedTestParams removed; 4th false receipt of campaign) -> review1 FIX(2H+2M+2L) ->
  fix1 6c61f12 (L1 named-policy coverage deferred to E3, judged SOUND) -> review2 FIX(1M residual)
  -> fix2 005a8ae (helloTransportError autostart classification) -> review3 SHIP. Delivered:
  foreground surface deleted (-3400 lines), unidentified submit structurally rejected, --admission/
  StrictAdmissionRequested gone (strict-only composition, typed pre-listener failure on unsupported
  platforms), ADR-0B replay ordering (LookupReplay before ANY typed decode), -32601 method_not_found
  incl. fail-stopped, protocol_version_mismatch + client.ErrProtocolVersionMismatch, root_fail_stopped
  cause wired, architecture guard, replay battery. Closure gates: 3-sweep battery + Docker + R0T green.
- 2026-07-23: E0 CLOSED at 64031ca (SHIP, zero blocking). Arc: worker c89aecc -> review1 FIX(2H+2M)
  -> fix1 9899db5 -> review2 FIX(1H+2M+1L, M2 reopened) -> fix2 d16d431 -> review3 FIX(2 residual)
  -> fix3 64031ca -> review4 SHIP. Deliverables: ADR-12 (protocol.hello surface, replay ordering,
  root matrix + evaluation order, corruption classes, 13 causes + admissionCause wire contract +
  precedence + error.data.code column, method_not_found -32601 / protocol_version_mismatch /
  ErrProtocolVersionMismatch client contract, shutdown, result semantics, non-goals);
  protocol.Version=2; StrictAuthorityMetaSchemaVersion=2; cause constants. Orchestrator repair #1
  (autostart hello pin + usage text) audited CLOSED in review1.
- 2026-07-22: AB-E locked at REV 3 after two external assessment rounds (foreground deletion decided
  by user; E9 deferred; no public proof; outcome-based shutdown; sequential-only). Tasks #4-#14
  created in the session ledger with dependency chain. Starting E0.
