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

type ProbeSpec struct {
	Argv []string
	Env  []string
	Dir  string
}

type ProbeResult struct {
	Stdout []byte
	Stderr []byte
}

type ExitObservation struct {
	Exited bool
	Code   int
	Signal string
}

type Runner interface {
	Start(context.Context, ExecSpec) (RunningCommand, error)
}

type ProbeRunner interface {
	LookPath(string) (string, error)
	Run(context.Context, ProbeSpec) (ProbeResult, error)
}

// RunningCommand exposes single-consumer output readers. Callers must read
// stdout and stderr concurrently while the process runs; waiting before reading
// large output can deadlock on normal OS-pipe backpressure. Close is idempotent.
type RunningCommand interface {
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Wait(context.Context) (ExitObservation, error)
	Interrupt(context.Context) error
}

type FinalObservation struct {
	Exit         ExitObservation
	ExecutionErr error
	CleanupErr   error
}

type FinalObserver interface {
	FinalObservation(context.Context) (FinalObservation, error)
}
