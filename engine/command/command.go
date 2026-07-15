package command

import (
	"context"
	"io"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type ExecSpec struct {
	Argv []string
	Env  []string
	Dir  string
}

type ExitObservation struct {
	Exited   bool
	Code     int
	Signal   string
	Evidence model.Evidence
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
