package custodian

import (
	"context"
	"errors"
	"io"
	"runtime"

	"github.com/charlesnpx/agentbus/engine/command"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

var (
	ErrInvalidAttestation    = errors.New("invalid quiescence attestation")
	ErrSupervisorUnavailable = errors.New("supervisor_unavailable")
)

type GrantToken string

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
	ContainAndVerify(context.Context, model.GroupRef, QuiescenceCause) (VerifiedQuiescence, error)
}

type PreparedProcess interface {
	preparedProcess()
	Ref() model.GroupRef
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Release(context.Context, GrantToken) (RunningProcess, error)
	AbortAndVerify(context.Context) (VerifiedQuiescence, error)
}

type RunningProcess interface {
	runningProcess()
	WaitAndVerify(context.Context) (command.ExitObservation, VerifiedQuiescence, error)
	ContainAndVerify(context.Context, QuiescenceCause) (VerifiedQuiescence, error)
}

type UnavailableCustodian struct{}

func (UnavailableCustodian) processCustodian() {}

func (UnavailableCustodian) Prepare(context.Context, command.ExecSpec, model.LaunchKey) (PreparedProcess, error) {
	return nil, ErrSupervisorUnavailable
}

func (UnavailableCustodian) ContainAndVerify(context.Context, model.GroupRef, QuiescenceCause) (VerifiedQuiescence, error) {
	return VerifiedQuiescence{}, ErrSupervisorUnavailable
}

type Support struct {
	ParkedExec          bool
	VerifiedContainment bool
	Platform            string
	Reason              error
}

type Runtime struct {
	process  ProcessCustodian
	verifier AttestationVerifier
	support  Support
}

func NewUnavailableRuntime(reason error) Runtime {
	if reason == nil {
		reason = ErrSupervisorUnavailable
	}
	_, verifier := NewAttestationChannel()
	return Runtime{
		process:  UnavailableCustodian{},
		verifier: verifier,
		support: Support{
			ParkedExec:          false,
			VerifiedContainment: false,
			Platform:            runtime.GOOS,
			Reason:              reason,
		},
	}
}

func (runtime Runtime) Process() ProcessCustodian {
	return runtime.process
}

func (runtime Runtime) Verifier() AttestationVerifier {
	return runtime.verifier
}

func (runtime Runtime) Support() Support {
	return runtime.support
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
