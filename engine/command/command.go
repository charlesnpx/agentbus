package command

import (
	"context"
	"io"
)

type ExecSpec struct {
	Argv []string
	Env  []string
	Dir  string
}

type ExitObservation struct {
	Exited bool
	Code   int
	Signal string
}

type Runner interface {
	Start(context.Context, ExecSpec) (RunningCommand, error)
}

type CommandRunner = Runner

type RunningCommand interface {
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Wait(context.Context) (ExitObservation, error)
	Interrupt(context.Context) error
}
