package model

import "errors"

var ErrIncompatibleExecutionCapabilities = errors.New("incompatible execution capabilities")

type IncompatibleExecutionCapabilitiesError struct {
	Capabilities ExecutionCapabilities
	Reason       string
}

func (err IncompatibleExecutionCapabilitiesError) Error() string {
	if err.Reason == "" {
		return ErrIncompatibleExecutionCapabilities.Error()
	}
	return ErrIncompatibleExecutionCapabilities.Error() + ": " + err.Reason
}

func (err IncompatibleExecutionCapabilitiesError) Is(target error) bool {
	return target == ErrIncompatibleExecutionCapabilities
}

// ExecutionCapabilities are the pre-accept facts used to choose a submission
// mode. External runners lack durable task identity in this contract; fenced
// external runners therefore use LegacyFenced, while unfenced external runners
// are explicitly LegacyUnfenced. A built-in backend without fenced launch is
// rejected before acceptance because it must not create an identified job that
// cannot be custodian-fenced.
type ExecutionCapabilities struct {
	ExternalRunner bool
	FencedLaunch   bool
}

func RouteSubmissionMode(caps ExecutionCapabilities) (Mode, error) {
	switch {
	case !caps.ExternalRunner && caps.FencedLaunch:
		return ModeIdentifiedFenced, nil
	case caps.ExternalRunner && caps.FencedLaunch:
		return ModeLegacyFenced, nil
	case caps.ExternalRunner && !caps.FencedLaunch:
		return ModeLegacyUnfenced, nil
	default:
		return 0, IncompatibleExecutionCapabilitiesError{
			Capabilities: caps,
			Reason:       "built-in identified admission requires fenced launch before acceptance",
		}
	}
}
