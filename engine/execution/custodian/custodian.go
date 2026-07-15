package custodian

import (
	"context"
	"errors"
	"io"

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

func (UnavailableCustodian) VerifiedContainmentSupported(context.Context) error {
	return ErrSupervisorUnavailable
}

type AttestationIssuer struct {
	token *attestationToken
}

type AttestationVerifier struct {
	token *attestationToken
}

type VerifiedQuiescence struct {
	token       *attestationToken
	certificate model.QuiescenceCertificate
}

type attestationToken struct {
	_ byte
}

func NewAttestationChannel() (AttestationIssuer, AttestationVerifier) {
	token := &attestationToken{}
	return AttestationIssuer{token: token}, AttestationVerifier{token: token}
}

func (issuer AttestationIssuer) AttestQuiescence(certificate model.QuiescenceCertificate) (VerifiedQuiescence, error) {
	if issuer.token == nil {
		return VerifiedQuiescence{}, ErrInvalidAttestation
	}
	if err := certificate.Validate(); err != nil {
		return VerifiedQuiescence{}, err
	}
	return VerifiedQuiescence{token: issuer.token, certificate: certificate}, nil
}

func (verifier AttestationVerifier) VerifyQuiescence(attestation VerifiedQuiescence) (model.QuiescenceCertificate, error) {
	if verifier.token == nil || attestation.token == nil || verifier.token != attestation.token {
		return model.QuiescenceCertificate{}, ErrInvalidAttestation
	}
	if err := attestation.certificate.Validate(); err != nil {
		return model.QuiescenceCertificate{}, err
	}
	return attestation.certificate, nil
}
