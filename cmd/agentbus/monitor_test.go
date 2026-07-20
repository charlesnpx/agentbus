//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/model"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
	"github.com/charlesnpx/agentbus/internal/procgroup"
	"golang.org/x/sys/unix"
)

func TestInternalMonitorHiddenFromHelp(t *testing.T) {
	t.Parallel()
	a := testApp(t)
	code, stdout, stderr := runTestCLI(t, a, []string{"--help"}, "")
	if code != 0 {
		t.Fatalf("help exit = %d stderr=%s", code, stderr)
	}
	if strings.Contains(stdout, internalMonitorCommand) {
		t.Fatalf("root help exposes hidden command:\n%s", stdout)
	}
}

func TestParseInternalMonitorOptionsRejectsMalformedFDs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing", args: nil, want: "daemon fd must be >= 3"},
		{name: "daemon low", args: []string{"--daemon-fd", "2", "--target-fd", "4", "--ready-fd", "5"}, want: "daemon fd must be >= 3"},
		{name: "target low", args: []string{"--daemon-fd", "3", "--target-fd", "2", "--ready-fd", "5"}, want: "target fd must be >= 3"},
		{name: "ready low", args: []string{"--daemon-fd", "3", "--target-fd", "4", "--ready-fd", "2"}, want: "ready fd must be >= 3"},
		{name: "duplicate", args: []string{"--daemon-fd", "3", "--target-fd", "3", "--ready-fd", "5"}, want: "monitor fds must be distinct"},
		{name: "positional", args: []string{"--daemon-fd", "3", "--target-fd", "4", "--ready-fd", "5", "extra"}, want: "internal-monitor does not accept positional arguments"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseInternalMonitorOptions(tt.args, io.Discard)
			if err == nil {
				t.Fatal("parseInternalMonitorOptions() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parseInternalMonitorOptions() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestInternalMonitorCommandContainsTargetOnDaemonEOF(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("real internal-monitor process-group EOF containment is covered on Linux")
	}
	if testing.Short() {
		t.Skip("real subprocess containment test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	agentbus := buildAgentbusForMonitorTest(t)
	target := exec.CommandContext(ctx, "/bin/sh", "-c", "exec sleep 60")
	target.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := target.Start(); err != nil {
		t.Fatalf("start target: %v", err)
	}
	targetDone := make(chan error, 1)
	go func() {
		targetDone <- target.Wait()
	}()
	t.Cleanup(func() {
		if target.Process != nil {
			_ = unix.Kill(-target.Process.Pid, unix.SIGKILL)
		}
		select {
		case <-targetDone:
		case <-time.After(5 * time.Second):
			t.Fatalf("target cleanup wait timed out")
		}
	})
	targetClaim := waitMonitorTestGroupLeaderClaim(t, ctx, target.Process.Pid)

	daemonRead, daemonWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	targetRead, targetWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	readyRead, readyWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer daemonWrite.Close()
	defer targetWrite.Close()
	defer readyRead.Close()

	monitor := exec.CommandContext(ctx, agentbus,
		internalMonitorCommand,
		"--daemon-fd", strconv.Itoa(parklaunch.MonitorDaemonControlFD),
		"--target-fd", strconv.Itoa(parklaunch.MonitorTargetFD),
		"--ready-fd", strconv.Itoa(parklaunch.MonitorReadyFD),
	)
	var stderr bytes.Buffer
	monitor.Stderr = &stderr
	monitor.ExtraFiles = []*os.File{daemonRead, targetRead, readyWrite}
	// Spawn the monitor as its own process-group leader, matching how
	// parklaunch.StartMonitorProcess spawns it. Without this the monitor inherits
	// the test process's group (PGID 1 inside a container PID namespace) and never
	// becomes a leader, so the group-leader claim below never resolves.
	monitor.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := monitor.Start(); err != nil {
		t.Fatalf("start internal-monitor: %v", err)
	}
	_ = daemonRead.Close()
	_ = targetRead.Close()
	_ = readyWrite.Close()
	monitorDone := make(chan error, 1)
	go func() {
		monitorDone <- monitor.Wait()
	}()
	monitorWaited := false
	t.Cleanup(func() {
		if monitorWaited {
			return
		}
		if monitor.Process != nil {
			_ = monitor.Process.Kill()
		}
		select {
		case <-monitorDone:
		case <-time.After(5 * time.Second):
			t.Fatalf("monitor cleanup wait timed out stderr=%s", stderr.String())
		}
	})

	monitorClaim := waitMonitorTestGroupLeaderClaim(t, ctx, monitor.Process.Pid)
	group := monitorTestGroupRef(t, targetClaim, monitorClaim)
	raw, err := json.Marshal(group)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := targetWrite.Write(raw); err != nil {
		t.Fatalf("write target group: %v", err)
	}
	if err := targetWrite.Close(); err != nil {
		t.Fatalf("close target writer: %v", err)
	}
	var ready [1]byte
	if _, err := io.ReadFull(readyRead, ready[:]); err != nil {
		t.Fatalf("read monitor ready: %v stderr=%s", err, stderr.String())
	}
	if ready[0] != '1' {
		t.Fatalf("ready byte = %q, want '1'", ready[0])
	}
	if err := daemonWrite.Close(); err != nil {
		t.Fatalf("close daemon control: %v", err)
	}
	select {
	case err := <-monitorDone:
		monitorWaited = true
		if err != nil {
			t.Fatalf("internal-monitor exited with error: %v stderr=%s", err, stderr.String())
		}
	case <-ctx.Done():
		t.Fatalf("internal-monitor did not exit: %v stderr=%s", ctx.Err(), stderr.String())
	}
	waitMonitorTestGroupAbsent(t, ctx, group)
}

func buildAgentbusForMonitorTest(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	exe := filepath.Join(dir, "agentbus")
	cmd := exec.Command("go", "build", "-o", exe, "./cmd/agentbus")
	cmd.Dir = monitorTestRepoRoot(t)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build ./cmd/agentbus: %v\n%s", err, output)
	}
	return exe
}

func monitorTestRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func waitMonitorTestGroupLeaderClaim(t *testing.T, ctx context.Context, pid int) procgroup.ProcessClaim {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var last procgroup.ProcessClaim
	var lastErr error
	for {
		claim, err := procgroup.ReadProcessClaim(pid)
		if err == nil && claim.PID == pid && claim.PGID == pid {
			return claim
		}
		if err == nil {
			last = claim
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			t.Fatalf("process %d did not become process-group leader: last=%+v err=%v", pid, last, lastErr)
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatalf("wait process-group leader: %v", ctx.Err())
		}
	}
}

func monitorTestGroupRef(t *testing.T, target, monitor procgroup.ProcessClaim) model.GroupRef {
	t.Helper()
	attempt := model.AttemptRef{JobID: "job-monitor-eof", AttemptID: "attempt-monitor-eof", Epoch: 1}
	group := model.GroupRef{
		Version:             1,
		CustodyID:           "custody-monitor-eof",
		Launch:              model.LaunchKey{Attempt: attempt, Ordinal: model.LaunchOrdinalOne},
		HostBootID:          target.KernelDomainID.HostBootID,
		PIDNamespaceID:      target.KernelDomainID.PIDNamespaceID,
		PIDNamespaceState:   target.KernelDomainID.PIDNamespaceState,
		RetainedDomainID:    target.KernelDomainID.RetainedDomainID,
		RetainedDomainState: target.KernelDomainID.RetainedDomainState,
		PGID:                target.PGID,
		Leader: model.ProcessIdentity{
			PID:               target.PID,
			HighResStartToken: target.StartToken.String(),
		},
		Monitor: model.ProcessIdentity{
			PID:               monitor.PID,
			HighResStartToken: monitor.StartToken.String(),
		},
	}
	if err := group.Validate(); err != nil {
		t.Fatalf("monitor test group Validate() error = %v", err)
	}
	return group
}

func waitMonitorTestGroupAbsent(t *testing.T, ctx context.Context, group model.GroupRef) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := unix.Kill(-group.PGID, 0)
		if errors.Is(err, unix.ESRCH) {
			return
		}
		if err != nil {
			t.Fatalf("kill(-%d, 0) error = %v", group.PGID, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("group %d still live after monitor containment", group.PGID)
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			t.Fatalf("wait group absent: %v", ctx.Err())
		}
	}
}
