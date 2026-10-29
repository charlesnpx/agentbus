package engine

import (
	"errors"
	"time"
)

var (
	// ErrTurnInterrupted marks a backend-confirmed turn interruption that was
	// not requested by agentbus. It is carried in an in-process Event.Err so
	// served can classify the condition without interpreting backend-controlled
	// text.
	ErrTurnInterrupted = errors.New("backend turn interrupted")
	// ErrProviderOverloaded marks an adapter-confirmed provider capacity or
	// overload refusal. It does not establish whether backend work occurred or
	// license an automatic retry. It is carried in an in-process Event.Err so
	// served can classify the condition without parsing backend-controlled text.
	ErrProviderOverloaded = errors.New("provider overloaded")
	// ErrTransportFrameTooLarge marks an adapter-observed backend transport
	// frame that exceeded the configured limit. It is carried in an in-process
	// Event.Err so served can classify the condition without parsing
	// backend-controlled text.
	ErrTransportFrameTooLarge = errors.New("backend transport frame exceeded configured limit")
)

// ProviderFailureEvidence is metadata an adapter obtained from a provider's
// structured error response. Message is separate from the job failure reason.
type ProviderFailureEvidence struct {
	Class      string
	Code       string
	Message    string
	RetryAfter *time.Time
}

// ProviderFailureError keeps structured provider evidence attached to the
// original adapter error without changing its text or error identity.
type ProviderFailureError struct {
	Evidence ProviderFailureEvidence
	Cause    error
}

func (err *ProviderFailureError) Error() string {
	if err == nil {
		return "provider failure"
	}
	if err.Cause != nil {
		return err.Cause.Error()
	}
	if err.Evidence.Message != "" {
		return err.Evidence.Message
	}
	return "provider failure"
}

func (err *ProviderFailureError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}
