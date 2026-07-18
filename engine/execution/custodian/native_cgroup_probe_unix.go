//go:build darwin || linux

package custodian

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

type nativeCgroupProbeHelper struct {
	cmd  *exec.Cmd
	done chan error
}

func startNativeCgroupProbeHelper(ctx context.Context) (*nativeCgroupProbeHelper, error) {
	return startNativeCgroupProbeHelperCommand(ctx, "/bin/sh", "-c", "trap '' TERM; while :; do sleep 1; done")
}

func startNativeCgroupProbeHelperCommand(ctx context.Context, path string, args ...string) (*nativeCgroupProbeHelper, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	helper := &nativeCgroupProbeHelper{
		cmd:  cmd,
		done: make(chan error, 1),
	}
	go func() {
		helper.done <- cmd.Wait()
	}()
	return helper, nil
}

func (helper *nativeCgroupProbeHelper) pid() int {
	if helper == nil || helper.cmd == nil || helper.cmd.Process == nil {
		return 0
	}
	return helper.cmd.Process.Pid
}

func (helper *nativeCgroupProbeHelper) requireRunning(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if helper == nil {
		return fmt.Errorf("probe helper is nil")
	}
	if helper.done == nil {
		return fmt.Errorf("probe helper has already exited")
	}
	select {
	case err := <-helper.done:
		helper.done = nil
		return fmt.Errorf("probe helper exited before containment: %s", nativeProbeHelperExitDescription(err))
	default:
	}
	pid := helper.pid()
	if pid <= 0 {
		return fmt.Errorf("probe helper pid is invalid")
	}
	if err := syscall.Kill(pid, 0); err != nil {
		return fmt.Errorf("probe helper pid %d is not live: %w", pid, err)
	}
	return nil
}

func (helper *nativeCgroupProbeHelper) wait(ctx context.Context, timeout time.Duration) error {
	if helper == nil {
		return fmt.Errorf("probe helper is nil")
	}
	if helper.done == nil {
		return fmt.Errorf("probe helper has already exited")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-helper.done:
		helper.done = nil
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return requireNativeProbeHelperSignal(err, syscall.SIGKILL)
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("timeout after %s", timeout)
	}
}

func (helper *nativeCgroupProbeHelper) requireGone(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if helper == nil {
		return fmt.Errorf("probe helper is nil")
	}
	pid := helper.pid()
	if pid <= 0 {
		return fmt.Errorf("probe helper pid is invalid")
	}
	if err := syscall.Kill(pid, 0); err == nil {
		return fmt.Errorf("probe helper pid %d is still live", pid)
	} else if err != syscall.ESRCH {
		return fmt.Errorf("probe helper pid %d gone check: %w", pid, err)
	}
	return nil
}

func requireNativeProbeHelperSignal(err error, want syscall.Signal) error {
	if signal, ok := nativeProbeHelperExitSignal(err); ok && signal == want {
		return nil
	}
	return fmt.Errorf("probe helper exit = %s, want signal %s", nativeProbeHelperExitDescription(err), want)
}

func nativeProbeHelperExitSignal(err error) (syscall.Signal, bool) {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return 0, false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() {
		return 0, false
	}
	return status.Signal(), true
}

func nativeProbeHelperExitDescription(err error) string {
	if err == nil {
		return "exit status 0"
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			if status.Signaled() {
				return fmt.Sprintf("signal %s", status.Signal())
			}
			if status.Exited() {
				return fmt.Sprintf("exit status %d", status.ExitStatus())
			}
		}
	}
	return err.Error()
}

func (helper *nativeCgroupProbeHelper) cleanup() {
	if helper == nil || helper.cmd == nil || helper.cmd.Process == nil || helper.done == nil {
		return
	}
	select {
	case <-helper.done:
		helper.done = nil
		return
	default:
	}
	_ = helper.cmd.Process.Kill()
	<-helper.done
	helper.done = nil
}
