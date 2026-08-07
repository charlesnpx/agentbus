//go:build !windows

package command

import (
	"os/exec"
	"runtime"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

func processRefForCmd(cmd *exec.Cmd) engine.ProcessRef {
	ref := engine.ProcessRef{}
	if cmd == nil || cmd.Process == nil {
		return ref
	}
	ref.PID = cmd.Process.Pid
	if runtime.GOOS != "windows" {
		if pgid, err := syscall.Getpgid(ref.PID); err == nil {
			ref.PGID = pgid
		}
	}
	if info, alive, err := (engine.NativeProcessTable{}).Lookup(ref.PID); err == nil && alive {
		ref.StartTime = info.StartTime
	}
	return ref
}

func setProcessGroup(cmd *exec.Cmd) {
	if runtime.GOOS == "windows" {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

var terminateProcessGroup = terminateProcessGroupImpl

func terminateProcessGroupImpl(cmd *exec.Cmd, grace time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pid := cmd.Process.Pid
	if runtime.GOOS == "windows" {
		_ = cmd.Process.Kill()
		return nil
	}
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		pgid = pid
	}
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	waitForProcessGroupExit(pgid, grace)
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	return nil
}

func waitForProcessGroupExit(pgid int, grace time.Duration) {
	if grace <= 0 {
		return
	}
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(-pgid, 0); err != nil {
			if err == syscall.ESRCH {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}
