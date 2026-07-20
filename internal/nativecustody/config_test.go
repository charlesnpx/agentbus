//go:build darwin || linux

package nativecustody

import (
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/internal/parklaunch"
)

func TestProductionContainmentDefaultsMatchRuntimeOptions(t *testing.T) {
	params := ProductionContainmentParams()
	if params.GracePeriod != 5*time.Second ||
		params.PollInterval != 100*time.Millisecond ||
		params.PollTimeout != 30*time.Second ||
		params.TrustedMonitorWait != 2*time.Second ||
		params.TrustedMonitorPollInterval != 100*time.Millisecond {
		t.Fatalf("production containment params = %+v, want 5s/100ms/30s/2s/100ms", params)
	}

	options := NativeOptions("/tmp/agentbus", []string{"A=B"}, "/tmp")
	if options.ContainmentParams != params {
		t.Fatalf("NativeOptions containment params = %+v, want shared %+v", options.ContainmentParams, params)
	}
	if options.MonitorCommand.Path != "/tmp/agentbus" {
		t.Fatalf("monitor path = %q, want /tmp/agentbus", options.MonitorCommand.Path)
	}
	if !slices.Equal(options.MonitorCommand.Env, []string{"A=B"}) || !slices.Equal(options.WorkerEnv, []string{"A=B"}) {
		t.Fatalf("env not copied into monitor/worker options: monitor=%v worker=%v", options.MonitorCommand.Env, options.WorkerEnv)
	}
	wantArgs := []string{
		"/tmp/agentbus",
		"internal-monitor",
		"--daemon-fd", strconv.Itoa(parklaunch.MonitorDaemonControlFD),
		"--target-fd", strconv.Itoa(parklaunch.MonitorTargetFD),
		"--ready-fd", strconv.Itoa(parklaunch.MonitorReadyFD),
	}
	if !slices.Equal(options.MonitorCommand.Args, wantArgs) {
		t.Fatalf("monitor args = %v, want %v", options.MonitorCommand.Args, wantArgs)
	}
}
