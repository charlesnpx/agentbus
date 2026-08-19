package served

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
)

func TestMain(m *testing.M) {
	if os.Getenv(servedLaunchHelperEnv) == "1" {
		os.Exit(runServedLaunchHelper())
	}
	os.Exit(m.Run())
}

func runServedLaunchHelper() int {
	root := os.Getenv("AGENTBUS_STATE_ROOT")
	cwd := os.Getenv(servedLaunchHelperCWDEnv)
	if cwd == "" {
		var err error
		cwd, err = os.Getwd()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
	}
	reporter, hasReporter, err := daemonlaunch.InheritedReporterFromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if !hasReporter {
		fmt.Fprintln(os.Stderr, "missing readiness reporter")
		return 2
	}
	defer reporter.Close()
	serveCtx, cancelStartup, err := daemonlaunch.InheritedStartupContext(context.Background())
	if err != nil {
		_ = reporter.Failed("error", err.Error())
		return 1
	}
	defer cancelStartup()
	if barrier := os.Getenv(servedLaunchHelperBarrierEnv); barrier != "" {
		if err := waitAtServedLaunchBarrier(barrier, 2, 2*time.Second); err != nil {
			_ = reporter.Failed("error", err.Error())
			return 1
		}
	}
	server, err := New(Config{
		StateRoot:    root,
		CWD:          cwd,
		Backends:     []engine.Backend{newFakeBackend("fake")},
		ProcessTable: mapProcessTable{entries: map[int]engine.ProcessInfo{os.Getpid(): {PID: os.Getpid(), StartTime: "daemon-start"}}},
		IdleTimeout:  -1,
		ReadyHook: func(info ServeReadyInfo) error {
			canonicalRoot, err := daemonlaunch.CanonicalStateRoot(info.StateRoot)
			if err != nil {
				return err
			}
			return reporter.Ready(canonicalRoot, info.SocketPath)
		},
	})
	if err != nil {
		_ = reporter.Failed(startupFailureCodeForServedLaunch(err), err.Error())
		return 1
	}
	configureServedLaunchAdmission(server)
	if barrier := os.Getenv(servedLaunchHelperCreateBarrierEnv); barrier != "" {
		previous := admissionRepositoryBeforeCreateForTest
		admissionRepositoryBeforeCreateForTest = func() error {
			return waitAtServedLaunchBarrier(barrier, 2, 2*time.Second)
		}
		defer func() { admissionRepositoryBeforeCreateForTest = previous }()
	}
	if os.Getenv(servedLaunchHelperModeEnv) == "bind-race" {
		marker := os.Getenv(servedLaunchHelperMarkerEnv)
		ready := os.Getenv(servedLaunchHelperReadyEnv)
		server.beforeListenBindHook = func() {
			_ = os.WriteFile(marker, []byte("pre-bind\n"), 0o600)
			_ = waitForServedLaunchPath(ready, 2*time.Second)
		}
	}
	if os.Getenv(servedLaunchHelperModeEnv) == "pre-bind-delay" {
		marker := os.Getenv(servedLaunchHelperMarkerEnv)
		server.admissionStartupHooks.BeforePolicyInstall = func() {
			_ = os.WriteFile(marker, []byte("pre-bind-delay\n"), 0o600)
			time.Sleep(1500 * time.Millisecond)
		}
	}
	if err := server.Serve(serveCtx); err != nil {
		_ = reporter.Failed(startupFailureCodeForServedLaunch(err), err.Error())
		return 1
	}
	return 0
}

func startupFailureCodeForServedLaunch(err error) string {
	switch {
	case errors.Is(err, ErrDaemonAlreadyListening):
		return daemonlaunch.CodeAlreadyListening
	case errors.Is(err, ErrAdmissionRootBusy):
		return daemonlaunch.CodeAdmissionRootBusy
	case errors.Is(err, ErrAdmissionStrictSupportUnavailable):
		return ErrAdmissionStrictSupportUnavailable.Error()
	default:
		return "error"
	}
}

func waitAtServedLaunchBarrier(dir string, want int, timeout time.Duration) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	marker := filepath.Join(dir, strconv.Itoa(os.Getpid()))
	if err := os.WriteFile(marker, []byte("ready\n"), 0o600); err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		if len(entries) >= want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("served launch barrier got %d participants, want %d", len(entries), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
