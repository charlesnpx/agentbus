package custodian

import (
	"context"
	"errors"
	"io"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

var (
	ErrInvalidAttestation    = errors.New("invalid quiescence attestation")
	ErrSupervisorUnavailable = errors.New("supervisor_unavailable")
)

type ExecSpec struct {
	Argv []string
	Env  []string
	Dir  string
}

type ExitObservation struct {
	Exited   bool
	Code     int
	Signal   string
	Evidence model.Evidence
}

type GrantToken string

type QuiescenceCause string

const (
	QuiescenceCauseAbort    QuiescenceCause = "abort"
	QuiescenceCauseWait     QuiescenceCause = "wait"
	QuiescenceCauseContain  QuiescenceCause = "contain"
	QuiescenceCauseRecovery QuiescenceCause = "recovery"
	QuiescenceCauseHostBoot QuiescenceCause = "host_boot"
)

type CommandRunner interface {
	Start(context.Context, ExecSpec) (RunningCommand, error)
}

type RunningCommand interface {
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Wait(context.Context) (ExitObservation, error)
	Interrupt(context.Context) error
}

type ProcessCustodian interface {
	processCustodian()
	Prepare(context.Context, ExecSpec, model.LaunchKey) (PreparedProcess, error)
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
	WaitAndVerify(context.Context) (ExitObservation, VerifiedQuiescence, error)
	ContainAndVerify(context.Context, QuiescenceCause) (VerifiedQuiescence, error)
}

type UnavailableCustodian struct{}

func (UnavailableCustodian) processCustodian() {}

func (UnavailableCustodian) Prepare(context.Context, ExecSpec, model.LaunchKey) (PreparedProcess, error) {
	return nil, ErrSupervisorUnavailable
}

func (UnavailableCustodian) ContainAndVerify(context.Context, model.GroupRef, QuiescenceCause) (VerifiedQuiescence, error) {
	return VerifiedQuiescence{}, ErrSupervisorUnavailable
}

type AttestationIssuer struct {
	channel *attestationChannel
}

type AttestationVerifier struct {
	channel *attestationChannel
}

type VerifiedQuiescence struct {
	channel     *attestationChannel
	certificate model.QuiescenceCertificate
}

type attestationChannel struct {
	secret byte
}

func NewAttestationChannel() (AttestationIssuer, AttestationVerifier) {
	channel := &attestationChannel{secret: 1}
	return AttestationIssuer{channel: channel}, AttestationVerifier{channel: channel}
}

func (issuer AttestationIssuer) AttestQuiescence(certificate model.QuiescenceCertificate) (VerifiedQuiescence, error) {
	if issuer.channel == nil {
		return VerifiedQuiescence{}, ErrInvalidAttestation
	}
	if err := certificate.Validate(); err != nil {
		return VerifiedQuiescence{}, err
	}
	return VerifiedQuiescence{channel: issuer.channel, certificate: certificate}, nil
}

func (verifier AttestationVerifier) VerifyQuiescence(attestation VerifiedQuiescence) (model.QuiescenceCertificate, error) {
	if verifier.channel == nil || attestation.channel == nil || verifier.channel != attestation.channel {
		return model.QuiescenceCertificate{}, ErrInvalidAttestation
	}
	if err := attestation.certificate.Validate(); err != nil {
		return model.QuiescenceCertificate{}, err
	}
	return attestation.certificate, nil
}
