package procgroup

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

// ErrProcessMissing is returned only when the kernel read can prove that the
// requested PID has no process table entry.
var ErrProcessMissing = errors.New("process missing")

type groupExistenceProbeResult int

const (
	groupExistenceIndeterminate groupExistenceProbeResult = iota
	groupExistenceDefinitelyAbsent
	groupExistenceExists
)

// StartToken identifies one process incarnation within a host boot. Identical
// native values across an in-same-instant PID reuse remain theoretically
// possible on both Darwin and Linux; the S3B custodian binds stronger identity.
type StartToken string

func (token StartToken) String() string {
	return string(token)
}

func (token StartToken) validate(field string) error {
	const maxTokenBytes = 256
	value := string(token)
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(value) > maxTokenBytes {
		return fmt.Errorf("%s is too long", field)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must be valid UTF-8", field)
	}
	for _, r := range value {
		if r <= ' ' || r == 0x7f {
			return fmt.Errorf("%s must not contain whitespace or control characters", field)
		}
	}
	return nil
}

// ProcessClaim is the helper's self-reported process identity. Classification
// re-reads the kernel and trusts this claim only when the independent read
// confirms the same PID, PGID, start token, and kernel domain.
type ProcessClaim struct {
	PID            int
	PGID           int
	StartToken     StartToken
	KernelDomainID model.KernelDomainID
}

func NewProcessClaim(pid, pgid int, startToken StartToken, domain model.KernelDomainID) (ProcessClaim, error) {
	claim := ProcessClaim{PID: pid, PGID: pgid, StartToken: startToken, KernelDomainID: domain}
	if err := claim.validate(); err != nil {
		return ProcessClaim{}, err
	}
	return claim, nil
}

func (claim ProcessClaim) validate() error {
	if claim.PID <= 0 {
		return fmt.Errorf("process pid must be positive")
	}
	if claim.PGID <= 0 {
		return fmt.Errorf("process pgid must be positive")
	}
	if err := claim.StartToken.validate("process start token"); err != nil {
		return err
	}
	if err := claim.KernelDomainID.Validate(); err != nil {
		return err
	}
	return nil
}

// ProcessRunState is the coarse liveness state needed by native containment.
// Running means the process-table entry is not a zombie/defunct entry; it does
// not distinguish runnable, sleeping, stopped, or other non-zombie states.
type ProcessRunState string

const (
	ProcessRunStateUnknown ProcessRunState = "unknown"
	ProcessRunStateRunning ProcessRunState = "running"
	ProcessRunStateZombie  ProcessRunState = "zombie"
)

func (state ProcessRunState) known() ProcessRunState {
	if state == "" {
		return ProcessRunStateUnknown
	}
	return state
}

func (state ProcessRunState) validate() error {
	switch state.known() {
	case ProcessRunStateUnknown, ProcessRunStateRunning, ProcessRunStateZombie:
		return nil
	default:
		return fmt.Errorf("process run state is unknown")
	}
}

// ProcessObservation adds run-state to the legacy identity classification
// without changing the meaning of ProcessIdentityMatching for existing callers.
type ProcessObservation struct {
	Identity model.ProcessIdentityObservation
	RunState ProcessRunState
}

func (observation ProcessObservation) validate() error {
	if err := observation.Identity.Validate(); err != nil {
		return err
	}
	return observation.RunState.validate()
}

// GroupClaim is the expected process-group identity in a kernel domain.
type GroupClaim struct {
	PGID           int
	KernelDomainID model.KernelDomainID
}

func NewGroupClaim(pgid int, domain model.KernelDomainID) (GroupClaim, error) {
	claim := GroupClaim{PGID: pgid, KernelDomainID: domain}
	if err := claim.validate(); err != nil {
		return GroupClaim{}, err
	}
	return claim, nil
}

func (claim GroupClaim) validate() error {
	if claim.PGID <= 0 {
		return fmt.Errorf("group pgid must be positive")
	}
	if err := claim.KernelDomainID.Validate(); err != nil {
		return err
	}
	return nil
}

type processSnapshot struct {
	PID        int
	PGID       int
	StartToken StartToken
	RunState   ProcessRunState
}

func (snapshot processSnapshot) validate() error {
	if snapshot.PID <= 0 {
		return fmt.Errorf("snapshot pid must be positive")
	}
	if snapshot.PGID <= 0 {
		return fmt.Errorf("snapshot pgid must be positive")
	}
	if err := snapshot.StartToken.validate("snapshot start token"); err != nil {
		return err
	}
	return snapshot.RunState.validate()
}
