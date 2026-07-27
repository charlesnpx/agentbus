//go:build darwin

package custodian

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"golang.org/x/sys/unix"
)

func TestDarwinLeaderRetentionAcquiredDuringMonitorBind(t *testing.T) {
	group := testPhysicalQuiescence().Group
	acquireCalls := 0
	backend := &leaderNativeContainmentBackend{
		factory: func(g model.GroupRef) (*leaderRetention, error) {
			acquireCalls++
			if !g.Equal(group) {
				t.Fatalf("retention factory group = %+v, want %+v", g, group)
			}
			return &leaderRetention{group: g, heldSince: time.Now()}, nil
		},
	}
	launchContainment := &nativeLaunchContainment{}

	bound, err := backend.beforeMonitorBind(context.Background(), group)
	if err != nil {
		t.Fatalf("beforeMonitorBind() error = %v", err)
	}
	if !bound.Equal(group) {
		t.Fatalf("beforeMonitorBind() group = %+v, want %+v", bound, group)
	}
	if !backend.witnessAcquired() || backend.witness() == nil {
		t.Fatal("beforeMonitorBind() did not acquire leader-retention witness")
	}
	if witness := backend.witness(); witness != nil {
		launchContainment.setWitness(witness)
	}
	launchContainment.mu.RLock()
	syncedWitness := launchContainment.witness
	launchContainment.mu.RUnlock()
	if syncedWitness == nil || syncedWitness != backend.witness() {
		t.Fatalf("launch containment witness = %T, want acquired leader retention", syncedWitness)
	}
	if err := backend.beforeRelease(context.Background(), bound); err != nil {
		t.Fatalf("beforeRelease() error = %v", err)
	}
	if acquireCalls != 1 {
		t.Fatalf("retention acquisitions = %d, want exactly one during beforeMonitorBind", acquireCalls)
	}
}

func TestDarwinLeaderRetentionFailureStopsBeforeMonitorBind(t *testing.T) {
	acquireErr := errors.New("retention acquisition failed")
	backend := &leaderNativeContainmentBackend{
		factory: func(model.GroupRef) (*leaderRetention, error) {
			return nil, acquireErr
		},
	}

	if _, err := backend.beforeMonitorBind(context.Background(), testPhysicalQuiescence().Group); !errors.Is(err, acquireErr) {
		t.Fatalf("beforeMonitorBind() error = %v, want acquisition failure", err)
	}
	if backend.witnessAcquired() || backend.witness() != nil {
		t.Fatal("failed beforeMonitorBind() published a leader-retention witness")
	}
	if err := backend.beforeRelease(context.Background(), testPhysicalQuiescence().Group); !errors.Is(err, ErrNativeCustodianUnavailable) {
		t.Fatalf("beforeRelease() after failed bind error = %v, want unavailable assertion", err)
	}
}

func TestDarwinPlatformProbeGroupSignalFailureReapsChild(t *testing.T) {
	originalCommand := darwinPlatformProbeCommand
	originalKill := darwinPlatformProbeKill
	defer func() {
		darwinPlatformProbeCommand = originalCommand
		darwinPlatformProbeKill = originalKill
	}()

	var probeCmd *exec.Cmd
	darwinPlatformProbeCommand = func(name string, arg ...string) *exec.Cmd {
		probeCmd = originalCommand(name, arg...)
		return probeCmd
	}
	groupSignals := 0
	childSignals := 0
	darwinPlatformProbeKill = func(pid int, signal syscall.Signal) error {
		if pid < 0 {
			groupSignals++
			return unix.EPERM
		}
		childSignals++
		return originalKill(pid, signal)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := probeDarwinPlatformProcessGroup(ctx, "direct-pid-fallback-test", &syscall.SysProcAttr{Setpgid: true}, unix.SIGTERM)
	if !errors.Is(err, unix.EPERM) {
		t.Fatalf("probe error = %v, want group signaling EPERM", err)
	}
	if probeCmd == nil || probeCmd.Process == nil {
		t.Fatal("probe command was not started")
	}
	if groupSignals == 0 {
		t.Fatal("probe did not attempt process-group signaling")
	}
	if childSignals == 0 {
		t.Fatal("probe did not fall back to direct child PID signaling")
	}
	if probeCmd.ProcessState == nil {
		t.Fatal("probe returned before the child process was waited/reaped")
	}
}

func TestDarwinPlatformProbeCleanupPGIDMismatchSignalsChildAndWaits(t *testing.T) {
	originalKill := darwinPlatformProbeKill
	defer func() {
		darwinPlatformProbeKill = originalKill
	}()

	var signals []int
	darwinPlatformProbeKill = func(pid int, _ syscall.Signal) error {
		signals = append(signals, pid)
		return nil
	}
	waitDone := make(chan error, 1)
	cleanupDone := make(chan error, 1)
	go func() {
		cleanupDone <- cleanupDarwinPlatformProbeProcess(12345, 23456, waitDone)
		close(cleanupDone)
	}()
	select {
	case err := <-cleanupDone:
		t.Fatalf("cleanup returned before wait completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	waitDone <- nil
	close(waitDone)
	if err := <-cleanupDone; err != nil {
		t.Fatalf("cleanup error = %v", err)
	}
	if len(signals) != 2 || signals[0] != -23456 || signals[1] != 12345 {
		t.Fatalf("cleanup signals = %+v, want group kill then direct child kill", signals)
	}
}

func debugGroupMembers(group model.GroupRef) string {
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.pgrp", group.PGID)
	if err != nil {
		return err.Error()
	}
	var parts []string
	for _, process := range processes {
		parts = append(parts, fmt.Sprintf("pid=%d stat=%d", process.Proc.P_pid, process.Proc.P_stat))
	}
	return strings.Join(parts, ",")
}
