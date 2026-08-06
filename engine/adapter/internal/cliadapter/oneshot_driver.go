package cliadapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/duplex"
	"github.com/charlesnpx/agentbus/engine/command"
)

type oneShotDriver struct {
	binary    string
	buildArgs func(resumeID string, opts engine.SessionOpts, input engine.TurnInput) ([]string, error)
	parse     func(map[string]any) ([]engine.Event, string, error)
}

func newOneShotDriver(backend *Backend) (duplex.Driver, error) {
	if backend.BuildArgs == nil {
		return nil, errors.New("cliadapter one-shot backend requires BuildArgs")
	}
	if backend.Parse == nil {
		return nil, errors.New("cliadapter one-shot backend requires Parse")
	}
	return oneShotDriver{
		binary:    backend.binary(),
		buildArgs: backend.BuildArgs,
		parse:     backend.Parse,
	}, nil
}

func (d oneShotDriver) ExecSpec(resumeID string, opts engine.SessionOpts, input engine.TurnInput) (command.ExecSpec, error) {
	args, err := d.buildArgs(resumeID, opts, input)
	if err != nil {
		return command.ExecSpec{}, err
	}
	return command.ExecSpec{
		Argv: append([]string{d.binary}, args...),
		Dir:  opts.CWD,
	}, nil
}

func (d oneShotDriver) RunTurn(ctx context.Context, conn *duplex.Conn, _ string, _ engine.SessionOpts, input engine.TurnInput, emit duplex.EmitFunc) (string, error) {
	// A stdin write failure is a SYMPTOM, not the turn's root cause. A one-shot
	// backend commonly emits its output and exits without draining the prompt,
	// so the prompt write races the child's exit and can fail with EPIPE
	// ("broken pipe"). That downstream error must not pre-empt the authoritative
	// terminal cause carried on the OUTPUT stream (a decode error for a malformed
	// stream, or a clean completion). Defer the write error and let the read side
	// decide precedence: a decode error wins, parsed frames mean success, and the
	// deferred write error is surfaced only if the backend produced nothing at
	// all. This yields deterministic, root-cause terminal classification instead
	// of racing the write error against the read side.
	writeErr := writeOneShotPrompt(conn, input.Prompt)

	var id string
	// producedOutput tracks whether the backend emitted AUTHORITATIVE output — a
	// parsed event or a session id — not merely any frame. A lone ignored/progress
	// frame must NOT suppress a genuine stdin-write failure: only real output means
	// the turn completed despite the write, so the deferred write error is dropped.
	producedOutput := false
	frames := conn.Frames()
	// Drain frames (the reader delivers every parsed frame before it surfaces a
	// decode error and closes the channel), then read the pending decode error.
	// Do NOT select on DecodeErrors() concurrently with frames: when a valid frame
	// and a decode error are both ready, a concurrent select can take the error
	// first and drop the buffered parsed event.
	for {
		select {
		case frame, ok := <-frames:
			if !ok {
				if err := pendingOneShotDecodeError(conn); err != nil {
					return id, malformedBackendStreamError(err)
				}
				if !producedOutput && writeErr != nil {
					return id, writeErr
				}
				return id, nil
			}
			events, nextID, err := d.parse(frame.Object)
			if err != nil {
				return id, err
			}
			if len(events) > 0 || nextID != "" {
				producedOutput = true
			}
			if id == "" && nextID != "" {
				id = nextID
			}
			for _, event := range events {
				emit(event)
			}
		case <-ctx.Done():
			return id, ctx.Err()
		}
	}
}

// writeOneShotPrompt writes the prompt to the backend's stdin and closes it,
// always attempting the close even when the write fails so the child still sees
// EOF. It returns the first error (write preferred over close). Callers treat
// the result as a DEFERRED, non-authoritative error — see RunTurn.
func writeOneShotPrompt(conn *duplex.Conn, prompt string) error {
	_, writeErr := conn.WriteStdin([]byte(prompt))
	closeErr := conn.CloseStdin()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func (d oneShotDriver) Interrupt(context.Context, *duplex.Conn) error {
	return nil
}

func pendingOneShotDecodeError(conn *duplex.Conn) error {
	select {
	case err, ok := <-conn.DecodeErrors():
		if ok {
			return err
		}
	default:
	}
	return nil
}

func malformedBackendStreamError(err error) error {
	return fmt.Errorf("malformed backend stream: %w", err)
}
