package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/charlesnpx/agentbus/internal/nativecustody"
)

const internalMonitorCommand = "internal-monitor"

type internalMonitorOptions struct {
	daemonFD int
	targetFD int
	readyFD  int
	leafFD   int
}

func (a *app) runInternalMonitor(args []string, errOut io.Writer) int {
	opts, err := parseInternalMonitorOptions(args, errOut)
	if err != nil {
		return commandError(errOut, err)
	}
	if err := nativecustody.RunMonitorFromFDs(context.Background(), opts.daemonFD, opts.targetFD, opts.readyFD, opts.leafFD); err != nil {
		return commandError(errOut, err)
	}
	return 0
}

func parseInternalMonitorOptions(args []string, errOut io.Writer) (internalMonitorOptions, error) {
	fs := flag.NewFlagSet("agentbus "+internalMonitorCommand, flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.Usage = func() {
		fmt.Fprintln(errOut, "agentbus: internal command")
	}
	daemonFD := fs.Int("daemon-fd", -1, "internal daemon control fd")
	targetFD := fs.Int("target-fd", -1, "internal monitor target fd")
	readyFD := fs.Int("ready-fd", -1, "internal monitor ready fd")
	leafFD := fs.Int("leaf-fd", -1, "internal retained leaf fd")
	if err := fs.Parse(args); err != nil {
		return internalMonitorOptions{}, err
	}
	if fs.NArg() != 0 {
		return internalMonitorOptions{}, fmt.Errorf("%s does not accept positional arguments", internalMonitorCommand)
	}
	if *daemonFD < 3 {
		return internalMonitorOptions{}, fmt.Errorf("daemon fd must be >= 3")
	}
	if *targetFD < 3 {
		return internalMonitorOptions{}, fmt.Errorf("target fd must be >= 3")
	}
	if *readyFD < 3 {
		return internalMonitorOptions{}, fmt.Errorf("ready fd must be >= 3")
	}
	if *leafFD < 3 {
		return internalMonitorOptions{}, fmt.Errorf("leaf fd must be >= 3")
	}
	if *daemonFD == *targetFD || *daemonFD == *readyFD || *daemonFD == *leafFD || *targetFD == *readyFD || *targetFD == *leafFD || *readyFD == *leafFD {
		return internalMonitorOptions{}, fmt.Errorf("monitor fds must be distinct")
	}
	return internalMonitorOptions{
		daemonFD: *daemonFD,
		targetFD: *targetFD,
		readyFD:  *readyFD,
		leafFD:   *leafFD,
	}, nil
}
