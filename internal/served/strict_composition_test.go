//go:build darwin || linux

package served

import (
	"slices"
	"strings"
	"testing"
)

func TestStrictAdmissionNativeOptionsStripsDaemonReadinessEnv(t *testing.T) {
	t.Setenv(daemonReadinessFDEnvName, "3")

	defaults, err := strictAdmissionNativeOptions(StrictAdmissionOptions{AgentbusPath: "/tmp/agentbus"})
	if err != nil {
		t.Fatal(err)
	}
	assertEnvAbsent(t, defaults.WorkerEnv, daemonReadinessFDEnvName)
	assertEnvAbsent(t, defaults.MonitorCommand.Env, daemonReadinessFDEnvName)

	explicit, err := strictAdmissionNativeOptions(StrictAdmissionOptions{
		AgentbusPath: "/tmp/agentbus",
		WorkerEnv:    []string{"A=B", daemonReadinessFDEnvName + "=9"},
		MonitorEnv:   []string{"C=D", daemonReadinessFDEnvName + "=10"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(explicit.WorkerEnv, []string{"A=B"}) {
		t.Fatalf("worker env = %v, want readiness env stripped", explicit.WorkerEnv)
	}
	if !slices.Equal(explicit.MonitorCommand.Env, []string{"C=D"}) {
		t.Fatalf("monitor env = %v, want readiness env stripped", explicit.MonitorCommand.Env)
	}
}

func assertEnvAbsent(t *testing.T, env []string, name string) {
	t.Helper()
	prefix := name + "="
	for _, item := range env {
		if item == name || strings.HasPrefix(item, prefix) {
			t.Fatalf("env unexpectedly contains %s: %v", name, env)
		}
	}
}
