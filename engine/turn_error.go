package engine

import "errors"

// ErrTurnInterrupted marks a backend-confirmed turn interruption that was not
// requested by agentbus. It is carried in an in-process Event.Err so served
// can classify the condition without interpreting backend-controlled text.
var ErrTurnInterrupted = errors.New("backend turn interrupted")
