package engine

import "errors"

var (
	// ErrTurnInterrupted marks a backend-confirmed turn interruption that was
	// not requested by agentbus. It is carried in an in-process Event.Err so
	// served can classify the condition without interpreting backend-controlled
	// text.
	ErrTurnInterrupted = errors.New("backend turn interrupted")
	// ErrProviderOverloaded marks an adapter-confirmed provider refusal due to
	// capacity or overload before backend work began. It is carried in an
	// in-process Event.Err so served can classify the condition without parsing
	// backend-controlled text.
	ErrProviderOverloaded = errors.New("provider overloaded")
)
