//go:build darwin || linux

package nativecustody

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
)

func ProductionContainmentParams() containment.Params {
	return containment.Params{
		GracePeriod:                5 * time.Second,
		PollInterval:               100 * time.Millisecond,
		PollTimeout:                30 * time.Second,
		TrustedMonitorWait:         2 * time.Second,
		TrustedMonitorPollInterval: 100 * time.Millisecond,
	}
}

func MonitorCommand(exe string, env []string, dir string) parklaunch.CommandSpec {
	if env == nil {
		env = os.Environ()
	}
	if dir == "" {
		dir = filepath.Dir(exe)
	}
	return parklaunch.CommandSpec{
		Path: exe,
		Args: []string{
			exe,
			"internal-monitor",
			"--daemon-fd", strconv.Itoa(parklaunch.MonitorDaemonControlFD),
			"--target-fd", strconv.Itoa(parklaunch.MonitorTargetFD),
			"--ready-fd", strconv.Itoa(parklaunch.MonitorReadyFD),
		},
		Env: append([]string(nil), env...),
		Dir: dir,
	}
}

func NativeOptions(exe string, env []string, dir string) custodian.NativeOptions {
	if env == nil {
		env = os.Environ()
	}
	if dir == "" {
		dir = filepath.Dir(exe)
	}
	return custodian.NativeOptions{
		AgentbusPath:      exe,
		MonitorCommand:    MonitorCommand(exe, env, dir),
		ContainmentParams: ProductionContainmentParams(),
		WorkerEnv:         append([]string(nil), env...),
		WorkerDir:         dir,
	}
}

func MonitorContainment() parklaunch.Containment {
	return custodian.RealContainment{Params: ProductionContainmentParams()}
}

func RunMonitorFromFDs(ctx context.Context, daemonFD, targetFD, readyFD int) error {
	return parklaunch.RunMonitorFromFDs(ctx, daemonFD, targetFD, readyFD, MonitorContainment())
}
