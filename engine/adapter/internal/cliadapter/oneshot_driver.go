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
	if _, err := conn.WriteStdin([]byte(input.Prompt)); err != nil {
		return "", err
	}
	if err := conn.CloseStdin(); err != nil {
		return "", err
	}

	var id string
	frames := conn.Frames()
	decodeErrs := conn.DecodeErrors()
	for frames != nil || decodeErrs != nil {
		select {
		case frame, ok := <-frames:
			if !ok {
				if err := pendingOneShotDecodeError(conn); err != nil {
					return id, malformedBackendStreamError(err)
				}
				return id, nil
			}
			events, nextID, err := d.parse(frame.Object)
			if err != nil {
				return id, err
			}
			if id == "" && nextID != "" {
				id = nextID
			}
			for _, event := range events {
				emit(event)
			}
		case err, ok := <-decodeErrs:
			if !ok {
				decodeErrs = nil
				continue
			}
			if ok && err != nil {
				return id, malformedBackendStreamError(err)
			}
		case <-ctx.Done():
			return id, ctx.Err()
		}
	}
	return id, nil
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
