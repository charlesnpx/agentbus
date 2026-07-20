package custodian

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/parkproto"
)

// ReleaseOutcome reports whether the physical release frame crossed the
// custodian boundary. A bare error is insufficient here because the caller must
// know whether abort is still permitted or execution may already be possible.
type ReleaseOutcome uint8

const (
	// ReleaseDefinitelyNotSent means the release frame was not written to the
	// worker. Abort remains permitted, but the same held launch still must not
	// attempt a second release.
	ReleaseDefinitelyNotSent ReleaseOutcome = iota + 1

	// ReleaseAccepted means the worker accepted the one physical release. The
	// authority must record release; later backend failure is observed through
	// the running handle and quiescence verification.
	ReleaseAccepted

	// ReleaseOutcomeUnknown means the write or acknowledgement boundary was
	// crossed ambiguously. Never resend. The policy core treats backend
	// execution as possible and moves to contain-and-verify.
	ReleaseOutcomeUnknown
)

func (outcome ReleaseOutcome) String() string {
	switch outcome {
	case ReleaseDefinitelyNotSent:
		return "definitely_not_sent"
	case ReleaseAccepted:
		return "accepted"
	case ReleaseOutcomeUnknown:
		return "unknown"
	default:
		return ""
	}
}

func (outcome ReleaseOutcome) Valid() bool {
	switch outcome {
	case ReleaseDefinitelyNotSent, ReleaseAccepted, ReleaseOutcomeUnknown:
		return true
	default:
		return false
	}
}

// PrepareSpec binds the logical launch identity to the custodian-owned
// physical release secret. The release secret is generated before physical
// prepare and revalidated when the one release is attempted; Release takes no
// second secret so authority grant nonces cannot be confused with the physical
// channel secret at call sites.
type PrepareSpec struct {
	Exec          command.ExecSpec
	LaunchKey     model.LaunchKey
	ReleaseSecret parkproto.ReleaseSecret
}

func (spec PrepareSpec) Validate() error {
	if len(spec.Exec.Argv) == 0 || spec.Exec.Argv[0] == "" {
		return fmt.Errorf("%w: exec argv is required", ErrInvalidHeldLaunch)
	}
	if err := spec.LaunchKey.Validate(); err != nil {
		return fmt.Errorf("%w: launch_key: %v", ErrInvalidHeldLaunch, err)
	}
	if err := spec.ReleaseSecret.Validate(); err != nil {
		return fmt.Errorf("%w: release_secret: %v", ErrInvalidHeldLaunch, err)
	}
	return nil
}

// HeldLaunch is the two-phase park-now/release-later contract. Release is
// one-use and returns a ReleaseOutcome with every error so callers can choose
// abort, record-release, or contain-and-verify without guessing.
type HeldLaunch interface {
	Ref() model.GroupRef
	Release(context.Context) (RunningProcess, ReleaseOutcome, error)
	AbortAndVerify(context.Context) (VerifiedQuiescence, error)
}

// HeldLaunchEffects are the only side effects used by HeldLaunchCore. R2A tests
// inject fakes here; real process, fork, exec, pipe, and syscall work belongs to
// later implementation units. SendRelease must return promptly when ctx is
// canceled. If it cannot prove the release frame was not sent, it must return
// ReleaseOutcomeUnknown rather than ReleaseDefinitelyNotSent.
type HeldLaunchEffects interface {
	Prepare(context.Context, PrepareSpec) (model.GroupRef, error)
	SendRelease(context.Context, PrepareSpec, model.GroupRef) (RunningProcess, ReleaseOutcome, error)
	AbortAndVerify(context.Context, model.GroupRef) (VerifiedQuiescence, error)
	ContainAndVerify(context.Context, model.GroupRef, QuiescenceCause) (VerifiedQuiescence, error)
}

type HeldLaunchState string

const (
	HeldLaunchStatePreparing       HeldLaunchState = "preparing"
	HeldLaunchStatePrepared        HeldLaunchState = "prepared"
	HeldLaunchStateReleasing       HeldLaunchState = "releasing"
	HeldLaunchStateReleaseAccepted HeldLaunchState = "release_accepted"
	HeldLaunchStateRunning         HeldLaunchState = "running"
	HeldLaunchStateAborting        HeldLaunchState = "aborting"
	HeldLaunchStateReleaseUnknown  HeldLaunchState = "release_unknown"
	HeldLaunchStateContaining      HeldLaunchState = "containing"
	HeldLaunchStateFinalized       HeldLaunchState = "finalized"
)

func (state HeldLaunchState) String() string {
	return string(state)
}

func (state HeldLaunchState) ActiveCustody() bool {
	switch state {
	case HeldLaunchStatePreparing,
		HeldLaunchStatePrepared,
		HeldLaunchStateReleasing,
		HeldLaunchStateReleaseAccepted,
		HeldLaunchStateRunning,
		HeldLaunchStateAborting,
		HeldLaunchStateReleaseUnknown,
		HeldLaunchStateContaining:
		return true
	default:
		return false
	}
}

var (
	ErrInvalidHeldLaunch              = errors.New("invalid held launch")
	ErrHeldLaunchEffectsRequired      = errors.New("held launch effects are required")
	ErrHeldLaunchAlreadyConsumed      = errors.New("held launch already consumed")
	ErrHeldLaunchExecutionPossible    = errors.New("held launch execution is possible")
	ErrHeldLaunchCloseRefused         = errors.New("held launch close refused")
	ErrHeldLaunchOutcomeRequired      = errors.New("held launch release outcome is required")
	ErrHeldLaunchReleaseContradiction = errors.New("held launch release tuple is contradictory")
	ErrHeldLaunchControlLost          = errors.New("held launch control lost during release")
)

// R2A has no real descriptor construction yet. These constants are documentation
// placeholders that keep the held-launch FD ownership rule visible at compile
// time; real allowlist and descriptor-building enforcement belongs in R2B/R3B.
// TODO(R2B/R3B): attach these invariants to native FD inheritance tests.
const (
	heldLaunchBackendMayInheritControlWriteFD  = 0
	heldLaunchBackendMayInheritControlMetadata = 0
	heldLaunchStdoutEOFMustRemainObservable    = 1
	heldLaunchStderrEOFMustRemainObservable    = 1
)

var (
	_ [1]struct{} = [1 - heldLaunchBackendMayInheritControlWriteFD]struct{}{}
	_ [1]struct{} = [1 - heldLaunchBackendMayInheritControlMetadata]struct{}{}
	_ [1]struct{} = [heldLaunchStdoutEOFMustRemainObservable]struct{}{}
	_ [1]struct{} = [heldLaunchStderrEOFMustRemainObservable]struct{}{}
)

// HeldLaunchCore is a pure policy state machine over injected effects. It
// serializes Release, AbortAndVerify, Close, and control-loss handling while
// keeping the physical release secret separate from the authority grant nonce.
type HeldLaunchCore struct {
	spec    PrepareSpec
	effects HeldLaunchEffects

	opMu sync.Mutex
	mu   sync.Mutex

	ref             model.GroupRef
	state           HeldLaunchState
	releaseConsumed bool
	abortConsumed   bool
	closeConsumed   bool
	releaseCancel   context.CancelFunc
	releaseCtrlLost bool
}

var _ HeldLaunch = (*HeldLaunchCore)(nil)

// PrepareHeldLaunch constructs a held launch by moving preparing -> prepared
// through the injected Prepare effect. It performs no process work itself.
func PrepareHeldLaunch(ctx context.Context, spec PrepareSpec, effects HeldLaunchEffects) (*HeldLaunchCore, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	if effects == nil {
		return nil, ErrHeldLaunchEffectsRequired
	}
	launch := &HeldLaunchCore{
		spec:    spec,
		effects: effects,
		state:   HeldLaunchStatePreparing,
	}
	ref, err := effects.Prepare(ctx, spec)
	if err != nil {
		launch.setState(HeldLaunchStateFinalized)
		return nil, err
	}
	if err := ref.Validate(); err != nil {
		launch.setState(HeldLaunchStateFinalized)
		return nil, fmt.Errorf("%w: group_ref: %v", ErrInvalidHeldLaunch, err)
	}
	if !ref.Launch.Equal(spec.LaunchKey) {
		launch.setState(HeldLaunchStateFinalized)
		return nil, fmt.Errorf("%w: group_ref launch_key mismatch", ErrInvalidHeldLaunch)
	}
	launch.mu.Lock()
	launch.ref = ref
	launch.state = HeldLaunchStatePrepared
	launch.mu.Unlock()
	return launch, nil
}

func (launch *HeldLaunchCore) Ref() model.GroupRef {
	if launch == nil {
		return model.GroupRef{}
	}
	launch.mu.Lock()
	defer launch.mu.Unlock()
	return launch.ref
}

func (launch *HeldLaunchCore) State() HeldLaunchState {
	if launch == nil {
		return HeldLaunchStateFinalized
	}
	launch.mu.Lock()
	defer launch.mu.Unlock()
	return launch.state
}

func (launch *HeldLaunchCore) ActiveCustodyCount() int {
	if launch == nil {
		return 0
	}
	if launch.State().ActiveCustody() {
		return 1
	}
	return 0
}

func (launch *HeldLaunchCore) Release(ctx context.Context) (RunningProcess, ReleaseOutcome, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if launch == nil {
		return nil, ReleaseDefinitelyNotSent, ErrInvalidHeldLaunch
	}
	launch.opMu.Lock()
	releaseCtx, ref, err := launch.beginRelease(ctx)
	launch.opMu.Unlock()
	if err != nil {
		return nil, ReleaseDefinitelyNotSent, err
	}

	running, outcome, releaseErr := launch.effects.SendRelease(releaseCtx, launch.spec, ref)

	launch.opMu.Lock()
	defer launch.opMu.Unlock()
	return launch.finishRelease(ctx, ref, running, outcome, releaseErr)
}

func (launch *HeldLaunchCore) finishRelease(ctx context.Context, ref model.GroupRef, running RunningProcess, outcome ReleaseOutcome, releaseErr error) (RunningProcess, ReleaseOutcome, error) {
	running, outcome, releaseErr = normalizeReleaseTuple(running, outcome, releaseErr)
	state, controlLost := launch.releaseCompletionState()
	if controlLost {
		running = nil
		outcome = ReleaseOutcomeUnknown
		releaseErr = errors.Join(releaseErr, ErrHeldLaunchControlLost)
	}
	if state != HeldLaunchStateReleasing {
		return nil, ReleaseOutcomeUnknown, releaseErr
	}

	switch outcome {
	case ReleaseDefinitelyNotSent:
		launch.setState(HeldLaunchStatePrepared)
		return nil, outcome, releaseErr
	case ReleaseAccepted:
		launch.setState(HeldLaunchStateReleaseAccepted)
		launch.setState(HeldLaunchStateRunning)
		return running, outcome, releaseErr
	case ReleaseOutcomeUnknown:
		launch.setState(HeldLaunchStateReleaseUnknown)
		cause := QuiescenceCauseContain
		if controlLost {
			cause = QuiescenceCauseRecovery
		}
		containErr := launch.containAfterUnknown(ctx, ref, cause)
		return nil, outcome, errors.Join(releaseErr, containErr)
	default:
		panic("unreachable release outcome")
	}
}

func (launch *HeldLaunchCore) AbortAndVerify(ctx context.Context) (VerifiedQuiescence, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if launch == nil {
		return VerifiedQuiescence{}, ErrInvalidHeldLaunch
	}
	launch.opMu.Lock()
	defer launch.opMu.Unlock()
	return launch.abortPrepared(ctx)
}

// Close aborts a prepared launch. If execution is possible or already running,
// Close returns a typed refusal instead of silently dropping custody.
func (launch *HeldLaunchCore) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if launch == nil {
		return nil
	}
	launch.opMu.Lock()
	defer launch.opMu.Unlock()

	launch.mu.Lock()
	if launch.closeConsumed {
		launch.mu.Unlock()
		return ErrHeldLaunchAlreadyConsumed
	}
	launch.closeConsumed = true
	launch.mu.Unlock()

	_, err := launch.abortPrepared(ctx)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrHeldLaunchExecutionPossible) || errors.Is(err, ErrHeldLaunchAlreadyConsumed) {
		return errors.Join(ErrHeldLaunchCloseRefused, err)
	}
	return err
}

// HandleControlLoss models daemon death/recovery policy without process I/O.
// A prepared unbound launch is aborted through the local handle. A prepared
// durable binding is reconciled by the durable GroupRef, and any control loss
// once release may have crossed the boundary is contained and never resent.
func (launch *HeldLaunchCore) HandleControlLoss(ctx context.Context, groupDurablyBound bool) (VerifiedQuiescence, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if launch == nil {
		return VerifiedQuiescence{}, ErrInvalidHeldLaunch
	}
	launch.tripInFlightReleaseControlLoss()
	launch.opMu.Lock()
	defer launch.opMu.Unlock()

	launch.mu.Lock()
	state := launch.state
	ref := launch.ref
	launch.mu.Unlock()

	switch state {
	case HeldLaunchStatePrepared:
		if groupDurablyBound {
			return launch.containAndFinalize(ctx, ref, QuiescenceCauseRecovery)
		}
		return launch.abortPrepared(ctx)
	case HeldLaunchStateReleasing, HeldLaunchStateAborting, HeldLaunchStateReleaseUnknown, HeldLaunchStateContaining:
		launch.markReleaseConsumed()
		return launch.containAndFinalize(ctx, ref, QuiescenceCauseRecovery)
	case HeldLaunchStateRunning, HeldLaunchStateReleaseAccepted:
		return VerifiedQuiescence{}, ErrHeldLaunchExecutionPossible
	case HeldLaunchStateFinalized:
		return VerifiedQuiescence{}, ErrHeldLaunchAlreadyConsumed
	default:
		return VerifiedQuiescence{}, fmt.Errorf("%w: invalid control-loss state %s", ErrInvalidHeldLaunch, state)
	}
}

func (launch *HeldLaunchCore) beginRelease(ctx context.Context) (context.Context, model.GroupRef, error) {
	releaseCtx, cancel := context.WithCancel(ctx)
	launch.mu.Lock()
	defer launch.mu.Unlock()
	if launch.releaseConsumed {
		cancel()
		return nil, model.GroupRef{}, ErrHeldLaunchAlreadyConsumed
	}
	if launch.state != HeldLaunchStatePrepared {
		cancel()
		return nil, model.GroupRef{}, launch.refusalForStateLocked(launch.state)
	}
	if err := launch.spec.ReleaseSecret.Validate(); err != nil {
		cancel()
		return nil, model.GroupRef{}, fmt.Errorf("%w: release_secret: %v", ErrInvalidHeldLaunch, err)
	}
	launch.releaseConsumed = true
	launch.releaseCancel = cancel
	launch.releaseCtrlLost = false
	launch.state = HeldLaunchStateReleasing
	return releaseCtx, launch.ref, nil
}

func (launch *HeldLaunchCore) abortPrepared(ctx context.Context) (VerifiedQuiescence, error) {
	launch.mu.Lock()
	if launch.abortConsumed {
		launch.mu.Unlock()
		return VerifiedQuiescence{}, ErrHeldLaunchAlreadyConsumed
	}
	state := launch.state
	ref := launch.ref
	if state != HeldLaunchStatePrepared {
		launch.mu.Unlock()
		return VerifiedQuiescence{}, launch.refusalForStateLocked(state)
	}
	launch.abortConsumed = true
	launch.state = HeldLaunchStateAborting
	launch.mu.Unlock()

	verified, err := launch.effects.AbortAndVerify(ctx, ref)
	if err != nil {
		return verified, err
	}
	launch.setState(HeldLaunchStateFinalized)
	return verified, nil
}

func (launch *HeldLaunchCore) containAfterUnknown(ctx context.Context, ref model.GroupRef, cause QuiescenceCause) error {
	_, err := launch.containAndFinalize(ctx, ref, cause)
	return err
}

// containAndFinalize is fail-closed: a failed containment leaves the launch in
// Containing with active custody so HandleControlLoss can retry. The process-wide
// fail-stop latch owns escalation while execution remains possible and unproven.
func (launch *HeldLaunchCore) containAndFinalize(ctx context.Context, ref model.GroupRef, cause QuiescenceCause) (VerifiedQuiescence, error) {
	launch.setState(HeldLaunchStateContaining)
	verified, err := launch.effects.ContainAndVerify(ctx, ref, cause)
	if err != nil {
		return verified, err
	}
	launch.setState(HeldLaunchStateFinalized)
	return verified, nil
}

func (launch *HeldLaunchCore) markReleaseConsumed() {
	launch.mu.Lock()
	launch.releaseConsumed = true
	launch.mu.Unlock()
}

func (launch *HeldLaunchCore) tripInFlightReleaseControlLoss() {
	var cancel context.CancelFunc
	launch.mu.Lock()
	if launch.state == HeldLaunchStateReleasing {
		launch.releaseCtrlLost = true
		cancel = launch.releaseCancel
	}
	launch.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (launch *HeldLaunchCore) releaseCompletionState() (HeldLaunchState, bool) {
	launch.mu.Lock()
	state := launch.state
	controlLost := launch.releaseCtrlLost
	cancel := launch.releaseCancel
	launch.releaseCancel = nil
	launch.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return state, controlLost
}

func normalizeReleaseTuple(running RunningProcess, outcome ReleaseOutcome, releaseErr error) (RunningProcess, ReleaseOutcome, error) {
	if !outcome.Valid() {
		outcome = ReleaseOutcomeUnknown
		releaseErr = errors.Join(releaseErr, ErrHeldLaunchOutcomeRequired)
	}
	switch outcome {
	case ReleaseDefinitelyNotSent:
		if running != nil {
			return nil, ReleaseOutcomeUnknown, errors.Join(releaseErr, ErrHeldLaunchReleaseContradiction)
		}
		return nil, outcome, releaseErr
	case ReleaseAccepted:
		if running == nil || releaseErr != nil {
			return nil, ReleaseOutcomeUnknown, errors.Join(releaseErr, ErrHeldLaunchReleaseContradiction)
		}
		return running, outcome, nil
	case ReleaseOutcomeUnknown:
		if running != nil {
			releaseErr = errors.Join(releaseErr, ErrHeldLaunchReleaseContradiction)
		}
		return nil, outcome, releaseErr
	default:
		panic("unreachable release outcome")
	}
}

func (launch *HeldLaunchCore) setState(state HeldLaunchState) {
	launch.mu.Lock()
	launch.state = state
	launch.mu.Unlock()
}

func (launch *HeldLaunchCore) refusalForStateLocked(state HeldLaunchState) error {
	switch state {
	case HeldLaunchStateRunning, HeldLaunchStateReleaseAccepted, HeldLaunchStateReleasing, HeldLaunchStateReleaseUnknown, HeldLaunchStateContaining:
		return ErrHeldLaunchExecutionPossible
	case HeldLaunchStateAborting, HeldLaunchStateFinalized:
		return ErrHeldLaunchAlreadyConsumed
	default:
		return fmt.Errorf("%w: state %s", ErrInvalidHeldLaunch, state)
	}
}
