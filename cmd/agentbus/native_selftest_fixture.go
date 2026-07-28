package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

const internalNativeSelfTestFixtureCommand = "internal-native-self-test-fixture"

func (a *app) runInternalNativeSelfTestFixture(args []string, errOut io.Writer) int {
	fs := flag.NewFlagSet("agentbus "+internalNativeSelfTestFixtureCommand, flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.Usage = func() {
		fmt.Fprintln(errOut, "agentbus: internal command")
	}
	markerPath := fs.String("marker", "", "internal marker path")
	if err := fs.Parse(args); err != nil {
		return commandError(errOut, err)
	}
	if fs.NArg() != 0 {
		return commandError(errOut, fmt.Errorf("%s does not accept positional arguments", internalNativeSelfTestFixtureCommand))
	}
	if *markerPath == "" {
		return commandError(errOut, fmt.Errorf("marker path is required"))
	}
	signal.Ignore(syscall.SIGTERM)
	if err := os.WriteFile(*markerPath, []byte("executed\n"), 0o600); err != nil {
		return commandError(errOut, err)
	}
	select {}
}
