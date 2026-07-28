package custodian

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

var (
	ErrInvalidAttestation    = errors.New("invalid quiescence attestation")
	ErrSupervisorUnavailable = errors.New("supervisor_unavailable")
	ErrInvalidSupport        = errors.New("invalid custodian support")
)

type SupportClass uint8

const (
	SupportAvailable SupportClass = iota
	SupportRetryable
	SupportUnsupported
	SupportUnsafe
)

func (class SupportClass) String() string {
	switch class {
	case SupportAvailable:
		return "available"
	case SupportRetryable:
		return "retryable"
	case SupportUnsupported:
		return "unsupported"
	case SupportUnsafe:
		return "unsafe"
	default:
		return "unknown"
	}
}

type SupportAssessment struct {
	Class       SupportClass
	Cause       error
	Attempts    int
	CleanupSafe bool
}

func (assessment SupportAssessment) Validate() error {
	switch assessment.Class {
	case SupportAvailable:
		if assessment.Cause != nil {
			return fmt.Errorf("%w: available support cannot carry a cause", ErrInvalidSupport)
		}
		if !assessment.CleanupSafe {
			return fmt.Errorf("%w: available support requires verified cleanup", ErrInvalidSupport)
		}
	case SupportRetryable:
		if assessment.Cause == nil {
			return fmt.Errorf("%w: %s support requires a cause", ErrInvalidSupport, assessment.Class)
		}
		if !assessment.CleanupSafe {
			return fmt.Errorf("%w: retryable support requires verified cleanup", ErrInvalidSupport)
		}
	case SupportUnsupported, SupportUnsafe:
		if assessment.Cause == nil {
			return fmt.Errorf("%w: %s support requires a cause", ErrInvalidSupport, assessment.Class)
		}
	default:
		return fmt.Errorf("%w: unknown support class %d", ErrInvalidSupport, assessment.Class)
	}
	if assessment.Attempts < 0 {
		return fmt.Errorf("%w: attempts cannot be negative", ErrInvalidSupport)
	}
	return nil
}

type QuiescenceCause string

const (
	QuiescenceCauseAbort    QuiescenceCause = "abort"
	QuiescenceCauseWait     QuiescenceCause = "wait"
	QuiescenceCauseContain  QuiescenceCause = "contain"
	QuiescenceCauseRecovery QuiescenceCause = "recovery"
	QuiescenceCauseHostBoot QuiescenceCause = "host_boot"
)

type ProcessCustodian interface {
	processCustodian()
	Prepare(context.Context, command.ExecSpec, model.LaunchKey) (PreparedProcess, error)
	ContainAndVerify(context.Context, model.GroupRef, QuiescenceCause) (VerifiedQuiescence, CleanupStatus, error)
	ActiveCustodyCount() int
}

type PreparedProcess interface {
	preparedProcess()
	Ref() model.GroupRef
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Release(context.Context) (RunningProcess, ReleaseOutcome, error)
	AbortAndVerify(context.Context) (VerifiedQuiescence, CleanupStatus, error)
}

type RunningProcess interface {
	runningProcess()
	WaitAndVerify(context.Context) (command.ExitObservation, VerifiedQuiescence, CleanupStatus, error)
	ContainAndVerify(context.Context, QuiescenceCause) (VerifiedQuiescence, CleanupStatus, error)
}

type UnavailableCustodian struct{}

func (UnavailableCustodian) processCustodian() {}

func (UnavailableCustodian) Prepare(context.Context, command.ExecSpec, model.LaunchKey) (PreparedProcess, error) {
	return nil, ErrSupervisorUnavailable
}

func (UnavailableCustodian) ContainAndVerify(context.Context, model.GroupRef, QuiescenceCause) (VerifiedQuiescence, CleanupStatus, error) {
	return VerifiedQuiescence{}, CleanupStatus{}, ErrSupervisorUnavailable
}

func (UnavailableCustodian) ActiveCustodyCount() int {
	return 0
}

// Support reports separate lifecycle facts: compiled-in implementation,
// runtime probe result, local configuration, and advertised availability are
// distinct states and must not be collapsed into one supported flag.
type Support struct {
	Assessment SupportAssessment

	ParkedExec             bool
	VerifiedContainment    bool
	ImplementationCompiled bool
	RuntimeProbePassed     bool
	FeatureConfigured      bool
	FeatureAdvertised      bool
	RuntimeProbeResult     error
	Platform               string
	Reason                 error
}

func NewSupport(support Support) (Support, error) {
	support = normalizeSupportAssessment(support)
	if err := support.Validate(); err != nil {
		return Support{}, err
	}
	return support, nil
}

func (support Support) Validate() error {
	if err := support.Assessment.Validate(); err != nil {
		return err
	}
	assessmentPassed := support.Assessment.Class == SupportAvailable
	if support.RuntimeProbePassed != assessmentPassed {
		return fmt.Errorf("%w: runtime probe flag contradicts support class %s", ErrInvalidSupport, support.Assessment.Class)
	}
	if support.RuntimeProbePassed {
		if support.RuntimeProbeResult != nil {
			return fmt.Errorf("%w: passing runtime probe cannot carry a failure result", ErrInvalidSupport)
		}
	} else if support.RuntimeProbeResult == nil {
		return fmt.Errorf("%w: failing runtime probe requires a result", ErrInvalidSupport)
	}
	if support.Assessment.Cause != support.RuntimeProbeResult {
		return fmt.Errorf("%w: assessment cause must match runtime probe result", ErrInvalidSupport)
	}
	if support.Reason != support.RuntimeProbeResult {
		return fmt.Errorf("%w: support reason must match runtime probe result", ErrInvalidSupport)
	}
	if support.FeatureAdvertised && !support.FeatureConfigured {
		return fmt.Errorf("%w: advertised feature requires configuration", ErrInvalidSupport)
	}
	if support.FeatureConfigured && !support.RuntimeProbePassed {
		return fmt.Errorf("%w: configured feature requires passing runtime probe", ErrInvalidSupport)
	}
	if support.RuntimeProbePassed && !support.ImplementationCompiled {
		return fmt.Errorf("%w: passing runtime probe requires compiled implementation", ErrInvalidSupport)
	}
	if support.FeatureAdvertised && !support.ParkedExec {
		return fmt.Errorf("%w: advertised feature requires parked exec support", ErrInvalidSupport)
	}
	if support.FeatureAdvertised && !support.VerifiedContainment {
		return fmt.Errorf("%w: advertised feature requires verified containment support", ErrInvalidSupport)
	}
	return nil
}

func (support Support) AdvertisedAvailable() bool {
	if support.Validate() != nil || support.Assessment.Class != SupportAvailable {
		return false
	}
	return support.FeatureAdvertised &&
		support.FeatureConfigured &&
		support.RuntimeProbePassed &&
		support.ImplementationCompiled &&
		support.ParkedExec &&
		support.VerifiedContainment
}

func normalizeSupportAssessment(support Support) Support {
	if support.Assessment != (SupportAssessment{}) {
		return support
	}
	if support.RuntimeProbePassed {
		support.Assessment = SupportAssessment{
			Class:       SupportAvailable,
			Attempts:    1,
			CleanupSafe: true,
		}
		return support
	}
	if support.RuntimeProbeResult != nil {
		support.Assessment = SupportAssessment{
			Class:       SupportUnsupported,
			Cause:       support.RuntimeProbeResult,
			CleanupSafe: true,
		}
		return support
	}
	return support
}

func newSupportFromAssessment(assessment SupportAssessment, platform string, configured, advertised bool) (Support, error) {
	probePassed := assessment.Class == SupportAvailable
	reason := assessment.Cause
	support := Support{
		Assessment:             assessment,
		ParkedExec:             probePassed,
		VerifiedContainment:    probePassed,
		ImplementationCompiled: true,
		RuntimeProbePassed:     probePassed,
		FeatureConfigured:      configured,
		FeatureAdvertised:      advertised,
		RuntimeProbeResult:     reason,
		Platform:               platform,
		Reason:                 reason,
	}
	if probePassed {
		support.RuntimeProbeResult = nil
		support.Reason = nil
	}
	return NewSupport(support)
}

type Runtime struct {
	process  ProcessCustodian
	verifier AttestationVerifier
	state    *runtimeState
}

type runtimeState struct {
	mu       sync.Mutex
	support  Support
	platform string
	selfTest func(context.Context, AttestationVerifier) SupportAssessment
	close    func() error

	reusableAfterClose bool
	closed             bool
	consumed           bool
	closeOnce          sync.Once
	closeErr           error
}

func NewUnavailableRuntime(reason error) Runtime {
	if reason == nil {
		reason = ErrSupervisorUnavailable
	}
	_, verifier := NewAttestationChannel()
	support, err := NewSupport(Support{
		Assessment: SupportAssessment{
			Class:       SupportUnsupported,
			Cause:       reason,
			CleanupSafe: true,
		},
		ParkedExec:             false,
		VerifiedContainment:    false,
		ImplementationCompiled: false,
		RuntimeProbePassed:     false,
		FeatureConfigured:      false,
		FeatureAdvertised:      false,
		RuntimeProbeResult:     reason,
		Platform:               runtime.GOOS,
		Reason:                 reason,
	})
	if err != nil {
		panic(err)
	}
	return Runtime{
		process:  UnavailableCustodian{},
		verifier: verifier,
		state: &runtimeState{
			support:            support,
			platform:           runtime.GOOS,
			reusableAfterClose: true,
		},
	}
}

func NewUnavailableRuntimeForTest(reason error, close func() error) Runtime {
	runtime := NewUnavailableRuntime(reason)
	runtime.state.close = close
	runtime.state.reusableAfterClose = false
	return runtime
}

func (runtime Runtime) Process() ProcessCustodian {
	return runtime.process
}

func (runtime Runtime) ActiveCustodyCount() int {
	if runtime.process == nil {
		return 0
	}
	return runtime.process.ActiveCustodyCount()
}

func (runtime Runtime) Verifier() AttestationVerifier {
	return runtime.verifier
}

func (runtime Runtime) Support() Support {
	if runtime.state == nil {
		return Support{}
	}
	runtime.state.mu.Lock()
	defer runtime.state.mu.Unlock()
	return runtime.state.support
}

func (runtime Runtime) SupportAssessment() SupportAssessment {
	return runtime.Support().Assessment
}

func (runtime Runtime) SelfTest(ctx context.Context) Support {
	if runtime.state == nil || runtime.state.selfTest == nil {
		return runtime.Support()
	}
	assessment := runtime.state.selfTest(ctx, runtime.verifier)
	support, err := newSupportFromAssessment(assessment, runtime.state.platform, false, false)
	if err != nil {
		support = NewUnavailableRuntime(err).Support()
	}
	runtime.state.mu.Lock()
	runtime.state.support = support
	runtime.state.mu.Unlock()
	return support
}

func (runtime Runtime) Closed() bool {
	if runtime.state == nil {
		return false
	}
	runtime.state.mu.Lock()
	defer runtime.state.mu.Unlock()
	return runtime.state.closed
}

func (runtime Runtime) ReusableAfterClose() bool {
	if runtime.state == nil {
		return true
	}
	runtime.state.mu.Lock()
	defer runtime.state.mu.Unlock()
	return runtime.state.reusableAfterClose
}

func (runtime Runtime) Consumed() bool {
	if runtime.state == nil {
		return false
	}
	runtime.state.mu.Lock()
	defer runtime.state.mu.Unlock()
	return runtime.state.consumed && !runtime.state.reusableAfterClose
}

// MarkConsumed records that ownership disposal has begun. It is deliberately
// earlier than Closed(): a close-capable runtime is spent once Serve commits to
// disposing it, even if repository close consumes the shutdown budget, runtime
// close blocks, or the handle is intentionally leaked; reusable no-op runtimes
// remain reusable.
func (runtime Runtime) MarkConsumed() {
	if runtime.state == nil {
		return
	}
	runtime.state.mu.Lock()
	if !runtime.state.reusableAfterClose {
		runtime.state.consumed = true
	}
	runtime.state.mu.Unlock()
}

func (runtime Runtime) Close() error {
	if runtime.state == nil {
		return nil
	}
	runtime.state.closeOnce.Do(func() {
		runtime.MarkConsumed()
		if runtime.state.close != nil {
			runtime.state.closeErr = runtime.state.close()
		}
		runtime.state.mu.Lock()
		if !runtime.state.reusableAfterClose {
			runtime.state.closed = true
		}
		runtime.state.mu.Unlock()
	})
	return runtime.state.closeErr
}

type AttestationIssuer struct {
	token *attestationToken
}

type AttestationVerifier struct {
	token *attestationToken
}

type VerifiedQuiescence struct {
	token   *attestationToken
	payload PhysicalQuiescence
}

// CleanupStatus carries post-proof cleanup failures separately from attestation
// errors. A non-nil proof error means the VerifiedQuiescence must not be used.
type CleanupStatus struct {
	Err error
}

type PhysicalQuiescence struct {
	Group  model.GroupRef
	Method model.QuiescenceMethod
}

func (quiescence PhysicalQuiescence) Validate() error {
	if err := quiescence.Group.Validate(); err != nil {
		return err
	}
	return quiescence.Method.Validate()
}

type attestationToken struct {
	_ byte
}

func NewAttestationChannel() (AttestationIssuer, AttestationVerifier) {
	token := &attestationToken{}
	return AttestationIssuer{token: token}, AttestationVerifier{token: token}
}

func (issuer AttestationIssuer) AttestQuiescence(quiescence PhysicalQuiescence) (VerifiedQuiescence, error) {
	if issuer.token == nil {
		return VerifiedQuiescence{}, ErrInvalidAttestation
	}
	if err := quiescence.Validate(); err != nil {
		return VerifiedQuiescence{}, err
	}
	return VerifiedQuiescence{token: issuer.token, payload: quiescence}, nil
}

func (verifier AttestationVerifier) VerifyQuiescence(attestation VerifiedQuiescence) (PhysicalQuiescence, error) {
	if verifier.token == nil || attestation.token == nil || verifier.token != attestation.token {
		return PhysicalQuiescence{}, ErrInvalidAttestation
	}
	if err := attestation.payload.Validate(); err != nil {
		return PhysicalQuiescence{}, err
	}
	return attestation.payload, nil
}
