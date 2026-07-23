//go:build darwin || linux

package served

import (
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	"github.com/charlesnpx/agentbus/internal/containment"
	"github.com/charlesnpx/agentbus/internal/parklaunch"
)

// StrictAdmissionOptions configures the production strict admission
// composition. R7B can call StrictAdmissionConfig from the serve flag wiring;
// tests may vary paths and timeouts without replacing the runtime or authority
// components.
type StrictAdmissionOptions struct {
	AgentbusPath      string
	WorkerEnv         []string
	WorkerDir         string
	MonitorEnv        []string
	MonitorDir        string
	ContainmentParams containment.Params
}

// StrictAdmissionConfig returns cfg configured for strict admission using the
// native custodian runtime. The returned runtime is owned by the eventual Serve
// call and is single-use when native support is available.
func StrictAdmissionConfig(cfg Config, opts StrictAdmissionOptions) (Config, error) {
	runtime, err := NewStrictAdmissionRuntime(opts)
	cfg.Runtime = runtime
	return cfg, err
}

func NewStrictAdmissionRuntime(opts StrictAdmissionOptions) (custodian.Runtime, error) {
	nativeOpts, err := strictAdmissionNativeOptions(opts)
	if err != nil {
		return custodian.NewUnavailableRuntime(err), err
	}
	return custodian.NewNativeRuntime(nativeOpts)
}

func strictAdmissionNativeOptions(opts StrictAdmissionOptions) (custodian.NativeOptions, error) {
	agentbusPath := opts.AgentbusPath
	if agentbusPath == "" {
		exe, err := os.Executable()
		if err != nil {
			return custodian.NativeOptions{}, err
		}
		agentbusPath = exe
	}
	agentbusPath, err := filepath.Abs(agentbusPath)
	if err != nil {
		return custodian.NativeOptions{}, err
	}
	workerEnv := opts.WorkerEnv
	if workerEnv == nil {
		workerEnv = os.Environ()
	}
	monitorEnv := opts.MonitorEnv
	if monitorEnv == nil {
		monitorEnv = workerEnv
	}
	workerDir := opts.WorkerDir
	if workerDir == "" {
		workerDir = filepath.Dir(agentbusPath)
	}
	monitorDir := opts.MonitorDir
	if monitorDir == "" {
		monitorDir = workerDir
	}
	params := opts.ContainmentParams
	if params == (containment.Params{}) {
		params = DefaultStrictAdmissionContainmentParams()
	}
	return custodian.NativeOptions{
		AgentbusPath: agentbusPath,
		MonitorCommand: parklaunch.CommandSpec{
			Path: agentbusPath,
			Args: []string{
				agentbusPath,
				"internal-monitor",
				"--daemon-fd", strconv.Itoa(parklaunch.MonitorDaemonControlFD),
				"--target-fd", strconv.Itoa(parklaunch.MonitorTargetFD),
				"--ready-fd", strconv.Itoa(parklaunch.MonitorReadyFD),
				"--leaf-fd", strconv.Itoa(parklaunch.MonitorLeafFD),
			},
			Env: append([]string(nil), monitorEnv...),
			Dir: monitorDir,
		},
		ContainmentParams: params,
		WorkerEnv:         append([]string(nil), workerEnv...),
		WorkerDir:         workerDir,
	}, nil
}

func DefaultStrictAdmissionContainmentParams() containment.Params {
	return containment.Params{
		GracePeriod:                500 * time.Millisecond,
		PollInterval:               50 * time.Millisecond,
		PollTimeout:                5 * time.Second,
		TrustedMonitorWait:         500 * time.Millisecond,
		TrustedMonitorPollInterval: 50 * time.Millisecond,
	}
}
