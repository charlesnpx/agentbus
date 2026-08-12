package engine

import "errors"

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
