//go:build !windows

package command

import (
	"errors"
	"fmt"
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

var (
	terminateProcessGroup = terminateProcessGroupImpl
	// processGroupSignal is the host syscall; tests replace it to observe the
	// exact escalation decisions without requiring host group-signal permission.
	processGroupSignal = syscall.Kill
)

func terminateProcessGroupImpl(cmd *exec.Cmd, grace time.Duration) error {
	return terminateProcessGroupForRef(cmd, capturedProcessRefForTermination(cmd), grace)
}

func capturedProcessRefForTermination(cmd *exec.Cmd) engine.ProcessRef {
	if cmd != nil {
		if ref, ok := capturedDirectProcessRefs.Load(cmd); ok {
			if ref, ok := ref.(engine.ProcessRef); ok {
				return ref
			}
		}
	}
	return processRefForCmd(cmd)
}

func terminateProcessGroupForRef(cmd *exec.Cmd, ref engine.ProcessRef, grace time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pid := cmd.Process.Pid
	if runtime.GOOS == "windows" {
		_ = cmd.Process.Kill()
		return nil
	}
	pgid := ref.PGID
	if pgid <= 0 {
		var err error
		pgid, err = syscall.Getpgid(pid)
		if err != nil {
			pgid = pid
		}
	}
	// A missing group is already stopped. Check it before reading the leader so
	// an already-exited group remains a prompt successful cancellation.
	if gone, err := processGroupSignalResult("inspect", pgid, processGroupSignal(-pgid, 0)); err != nil {
		return err
	} else if gone {
		return nil
	}
	if err := verifyProcessGroupLeader(ref, pid); err != nil {
		return err
	}
	if gone, err := processGroupSignalResult("send SIGTERM to", pgid, processGroupSignal(-pgid, syscall.SIGTERM)); err != nil {
		return err
	} else if gone {
		return nil
	}
	if gone, err := waitForProcessGroupExit(pgid, grace); err != nil {
		return err
	} else if gone {
		return nil
	}
	if err := verifyProcessGroupLeader(ref, pid); err != nil {
		return err
	}
	if _, err := processGroupSignalResult("send SIGKILL to", pgid, processGroupSignal(-pgid, syscall.SIGKILL)); err != nil {
		return err
	}
	return nil
}

func processGroupSignalResult(action string, pgid int, err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return true, nil
	}
	if errors.Is(err, syscall.EPERM) {
		return false, fmt.Errorf("%s process group %d: permission denied: %w", action, pgid, err)
	}
	return false, fmt.Errorf("%s process group %d: %w", action, pgid, err)
}

func waitForProcessGroupExit(pgid int, grace time.Duration) (bool, error) {
	if grace <= 0 {
		return false, nil
	}
	deadline := time.Now().Add(grace)
	for {
		if gone, err := processGroupSignalResult("inspect", pgid, processGroupSignal(-pgid, 0)); err != nil {
			return false, err
		} else if gone {
			return true, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false, nil
		}
		if remaining > 10*time.Millisecond {
			remaining = 10 * time.Millisecond
		}
		time.Sleep(remaining)
	}
}

func verifyProcessGroupLeader(ref engine.ProcessRef, pid int) error {
	if ref.PID <= 0 {
		return fmt.Errorf("%w: process group can no longer be identified: recorded leader pid is missing", engine.ErrProcessIdentityUnverifiable)
	}
	if ref.PID != pid {
		return fmt.Errorf("%w: process group can no longer be identified: recorded leader pid %d differs from command pid %d", engine.ErrProcessIdentityUnverifiable, ref.PID, pid)
	}
	if ref.StartTime == "" {
		return fmt.Errorf("%w: process group can no longer be identified: recorded leader start token is missing", engine.ErrProcessIdentityUnverifiable)
	}
	info, alive, err := (engine.NativeProcessTable{}).Lookup(ref.PID)
	if err != nil {
		return fmt.Errorf("%w: process group can no longer be identified: read leader %d start token: %v", engine.ErrProcessIdentityUnverifiable, ref.PID, err)
	}
	if !alive {
		return fmt.Errorf("%w: process group can no longer be identified: leader %d is missing", engine.ErrProcessIdentityUnverifiable, ref.PID)
	}
	if info.StartTime == "" {
		return fmt.Errorf("%w: process group can no longer be identified: observed leader start token is missing", engine.ErrProcessIdentityUnverifiable)
	}
	if info.StartTime != ref.StartTime {
		return fmt.Errorf("%w: process group can no longer be identified: leader %d start token changed", engine.ErrProcessIdentityUnverifiable, ref.PID)
	}
	return nil
}
