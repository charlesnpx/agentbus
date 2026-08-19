//go:build windows

package command

import (
	"os/exec"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

func processRefForCmd(cmd *exec.Cmd) engine.ProcessRef {
	ref := engine.ProcessRef{}
	if cmd == nil || cmd.Process == nil {
		return ref
	}
	ref.PID = cmd.Process.Pid
	if info, alive, err := (engine.NativeProcessTable{}).Lookup(ref.PID); err == nil && alive {
		ref.StartTime = info.StartTime
	}
	return ref
}

func setProcessGroup(_ *exec.Cmd) {}

var terminateProcessGroup = terminateProcessGroupImpl

func terminateProcessGroupImpl(cmd *exec.Cmd, _ engine.ProcessRef, _ time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	_ = cmd.Process.Kill()
	return nil
}
