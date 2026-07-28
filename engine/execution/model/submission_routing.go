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

// ExecutionCapabilities are strict-admission pre-accept facts. They are derived
// during Serve bootstrap and consumed together with backend controlled-runner
// facts and runtime support; strict identified admission must reject incompatible
// capabilities before acceptance rather than routing to a legacy mode.
type ExecutionCapabilities struct {
	ExternalRunner bool
	FencedLaunch   bool
}
