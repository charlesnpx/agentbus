package served

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/custodian"
	bboltrepo "github.com/charlesnpx/agentbus/engine/execution/storage/bbolt"
	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	servedLaunchHelperEnv        = "AGENTBUS_SERVED_LAUNCH_HELPER"
	servedLaunchHelperModeEnv    = "AGENTBUS_SERVED_LAUNCH_HELPER_MODE"
	servedLaunchHelperCWDEnv     = "AGENTBUS_SERVED_LAUNCH_HELPER_CWD"
	servedLaunchHelperBarrierEnv = "AGENTBUS_SERVED_LAUNCH_HELPER_BARRIER"
	servedLaunchHelperMarkerEnv  = "AGENTBUS_SERVED_LAUNCH_HELPER_MARKER"
	servedLaunchHelperReadyEnv   = "AGENTBUS_SERVED_LAUNCH_HELPER_READY"
)

func TestMain(m *testing.M) {
	if os.Getenv(servedLaunchHelperEnv) == "1" {
		os.Exit(runServedLaunchHelper())
	}
	os.Exit(m.Run())
}

func TestLaunchConcurrentFreshRootConvergesOnSingleToken(t *testing.T) {
	parent := shortTempDir(t)
	root := filepath.Join(parent, "state")
	cwd := shortTempDir(t)
	barrier := filepath.Join(parent, "barrier")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	results := make(chan daemonlaunch.Result, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := daemonlaunch.Launch(ctx, servedLaunchOptions(t, root, cwd, "serve", barrier))
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if launchRaceSocketDenied(err) {
			t.Skipf("Unix socket bind denied by sandbox: %v", err)
		}
		t.Fatalf("Launch error: %v", err)
	}
	close(results)

	var winner daemonlaunch.Result
	resultCount := 0
	existingCount := 0
	for result := range results {
		resultCount++
		if result.ExistingDaemon {
			existingCount++
			continue
		}
		if result.PID <= 0 {
			t.Fatalf("result = %+v, want winner pid", result)
		}
		if winner.PID != 0 {
			t.Fatalf("two launchers reported daemon pids: first=%+v second=%+v", winner, result)
		}
		winner = result
	}
	if resultCount != 2 || winner.PID == 0 || existingCount != 1 {
		t.Fatalf("results count=%d winner=%+v existing=%d, want one winner and one existing daemon", resultCount, winner, existingCount)
	}
	t.Cleanup(func() { _ = winner.KillAndWait() })

	token := readServedLaunchToken(t, filepath.Join(root, protocol.TokenFileName))
	helloServedLaunchDaemon(t, filepath.Join(root, protocol.SocketName), token)
}

func TestLaunchBindRaceAlreadyListeningVerifiesWinner(t *testing.T) {
	parent := shortTempDir(t)
	root := filepath.Join(parent, "state")
	cwd := shortTempDir(t)
	marker := filepath.Join(parent, "bind-marker")
	ready := filepath.Join(parent, "winner-ready")
	socketPath := filepath.Join(root, protocol.SocketName)
	helloSeen := make(chan struct{}, 1)
	winnerErr := make(chan error, 1)
	go func() {
		if err := waitForServedLaunchPath(marker, 2*time.Second); err != nil {
			winnerErr <- err
			return
		}
		listener, err := net.Listen("unix", socketPath)
		if err != nil {
			_ = os.WriteFile(ready, []byte("failed\n"), 0o600)
			winnerErr <- err
			return
		}
		defer listener.Close()
		if err := os.WriteFile(ready, []byte("ready\n"), 0o600); err != nil {
			winnerErr <- err
			return
		}
		token, err := waitForServedLaunchToken(filepath.Join(root, protocol.TokenFileName), 2*time.Second)
		if err != nil {
			winnerErr <- err
			return
		}
		serveServedLaunchHello(listener, token, helloSeen)
		winnerErr <- nil
	}()

	result, err := daemonlaunch.Launch(context.Background(), servedLaunchOptions(t, root, cwd, "bind-race", ""))
	if err != nil {
		if launchRaceSocketDenied(err) {
			t.Skipf("Unix socket bind denied by sandbox: %v", err)
		}
		t.Fatalf("Launch bind-race error: %v", err)
	}
	if result.PID > 0 {
		t.Cleanup(func() { _ = result.KillAndWait() })
	}
	if err := <-winnerErr; err != nil {
		if launchRaceSocketDenied(err) {
			t.Skipf("Unix socket bind denied by sandbox: %v", err)
		}
		t.Fatal(err)
	}
	if !result.ExistingDaemon || result.PID != 0 {
		t.Fatalf("result = %+v, want verified existing daemon", result)
	}
	select {
	case <-helloSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("winner did not receive authenticated hello")
	}
}

func TestLaunchAdmissionReadOnlyLockWithoutSocketReportsRootBusy(t *testing.T) {
	parent := shortTempDir(t)
	root := filepath.Join(parent, "state")
	cwd := shortTempDir(t)
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	repoPath := filepath.Join(root, admissionRepositoryFile)
	repo, err := bboltrepo.NewRepository(repoPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Close(); err != nil {
		t.Fatal(err)
	}
	holder, err := bboltrepo.OpenReadOnly(repoPath)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	socketPath := filepath.Join(root, protocol.SocketName)
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket stat before launch = %v, want not exist", err)
	}

	start := time.Now()
	_, err = daemonlaunch.Launch(context.Background(), servedLaunchOptionsWithTimeout(t, root, cwd, "serve", "", 1500*time.Millisecond))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Launch succeeded, want root busy startup failure")
	}
	if errors.Is(err, daemonlaunch.ErrExistingDaemonVerification) {
		t.Fatalf("Launch error = %v, want root busy without existing-daemon verification", err)
	}
	var startup *daemonlaunch.StartupError
	if !errors.As(err, &startup) || !errors.Is(startup, daemonlaunch.ErrStartupFailed) {
		t.Fatalf("Launch error = %T %v, want startup failure", err, err)
	}
	if startup.Code != daemonlaunch.CodeAdmissionRootBusy {
		t.Fatalf("startup code = %q, want %q", startup.Code, daemonlaunch.CodeAdmissionRootBusy)
	}
	if !strings.Contains(startup.Message, ErrAdmissionRootBusy.Error()) {
		t.Fatalf("startup message = %q, want root busy diagnostic", startup.Message)
	}
	if elapsed < 900*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("Launch elapsed = %s, want root busy at child startup deadline", elapsed)
	}
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket stat after launch = %v, want not exist", err)
	}
}

func TestLaunchAdmissionLockPreBindDelayVerifiesWinner(t *testing.T) {
	parent := shortTempDir(t)
	root := filepath.Join(parent, "state")
	cwd := shortTempDir(t)
	marker := filepath.Join(parent, "pre-bind-delay-marker")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	type launchOutcome struct {
		result daemonlaunch.Result
		err    error
	}
	winnerOptions := servedLaunchOptionsWithTimeout(t, root, cwd, "pre-bind-delay", "", 6*time.Second)
	winnerDone := make(chan launchOutcome, 1)
	go func() {
		result, err := daemonlaunch.Launch(ctx, winnerOptions)
		winnerDone <- launchOutcome{result: result, err: err}
	}()
	if err := waitForServedLaunchPath(marker, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	result, err := daemonlaunch.Launch(ctx, servedLaunchOptionsWithTimeout(t, root, cwd, "serve", "", 6*time.Second))
	if err != nil {
		if launchRaceSocketDenied(err) {
			t.Skipf("Unix socket bind denied by sandbox: %v", err)
		}
		t.Fatalf("Launch during pre-bind delay error: %v", err)
	}
	if !result.ExistingDaemon || result.PID != 0 {
		t.Fatalf("loser result = %+v, want verified existing daemon", result)
	}

	var winner daemonlaunch.Result
	select {
	case outcome := <-winnerDone:
		if outcome.err != nil {
			if launchRaceSocketDenied(outcome.err) {
				t.Skipf("Unix socket bind denied by sandbox: %v", outcome.err)
			}
			t.Fatalf("winner launch error: %v", outcome.err)
		}
		winner = outcome.result
	case <-time.After(2 * time.Second):
		t.Fatal("winner did not report ready after pre-bind delay")
	}
	if winner.PID <= 0 || winner.ExistingDaemon {
		t.Fatalf("winner result = %+v, want launched daemon pid", winner)
	}
	t.Cleanup(func() { _ = winner.KillAndWait() })
	token := readServedLaunchToken(t, filepath.Join(root, protocol.TokenFileName))
	helloServedLaunchDaemon(t, filepath.Join(root, protocol.SocketName), token)
}

func TestLaunchAdmissionLockWithDialableWinnerVerifiesExistingDaemon(t *testing.T) {
	parent := shortTempDir(t)
	root := filepath.Join(parent, "state")
	cwd := shortTempDir(t)
	ready := make(chan ServeReadyInfo, 1)
	server, err := New(Config{
		StateRoot:    root,
		CWD:          cwd,
		Backends:     []engine.Backend{newFakeBackend("fake")},
		ProcessTable: mapProcessTable{entries: map[int]engine.ProcessInfo{os.Getpid(): {PID: os.Getpid(), StartTime: "daemon-start"}}},
		IdleTimeout:  -1,
		ReadyHook: func(info ServeReadyInfo) error {
			ready <- info
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	configureServedLaunchAdmission(server)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ctx)
	}()

	var info ServeReadyInfo
	select {
	case info = <-ready:
	case err := <-done:
		cancel()
		if launchRaceSocketDenied(err) {
			t.Skipf("Unix socket bind denied by sandbox: %v", err)
		}
		t.Fatalf("winner server exited before ready: %v", err)
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("timed out waiting for winner server readiness")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("winner server exited with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("winner server did not stop")
		}
	})
	token := readServedLaunchToken(t, filepath.Join(root, protocol.TokenFileName))
	helloServedLaunchDaemon(t, info.SocketPath, token)

	result, err := daemonlaunch.Launch(context.Background(), servedLaunchOptions(t, root, cwd, "serve", ""))
	if err != nil {
		if launchRaceSocketDenied(err) {
			t.Skipf("Unix socket bind denied by sandbox: %v", err)
		}
		t.Fatalf("Launch with dialable winner error: %v", err)
	}
	if !result.ExistingDaemon || result.PID != 0 {
		t.Fatalf("result = %+v, want verified existing daemon", result)
	}
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

func configureServedLaunchAdmission(server *Server) {
	issuer, verifier := custodian.NewAttestationChannel()
	launcher := &admissionFakeLaunchCustodian{issuer: issuer, verifier: verifier}
	support, err := custodian.NewSupport(custodian.Support{
		ParkedExec:             true,
		VerifiedContainment:    true,
		ImplementationCompiled: true,
		RuntimeProbePassed:     true,
		FeatureConfigured:      true,
		FeatureAdvertised:      true,
		Platform:               "test",
	})
	if err != nil {
		panic(err)
	}
	server.admissionRuntime = &servedAdmissionRuntime{
		runtime:          custodian.NewUnavailableRuntime(custodian.ErrSupervisorUnavailable),
		launchCustodian:  launcher,
		supportOverride:  &support,
		verifierOverride: launcher.verifier,
	}
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

func servedLaunchOptions(t *testing.T, root, cwd, mode, barrier string) daemonlaunch.Options {
	t.Helper()
	return servedLaunchOptionsWithTimeout(t, root, cwd, mode, barrier, 5*time.Second)
}

func servedLaunchOptionsWithTimeout(t *testing.T, root, cwd, mode, barrier string, timeout time.Duration) daemonlaunch.Options {
	t.Helper()
	env := append(os.Environ(),
		servedLaunchHelperEnv+"=1",
		servedLaunchHelperModeEnv+"="+mode,
		servedLaunchHelperCWDEnv+"="+cwd,
	)
	if barrier != "" {
		env = append(env, servedLaunchHelperBarrierEnv+"="+barrier)
	}
	if mode == "bind-race" {
		parent := filepath.Dir(root)
		env = append(env,
			servedLaunchHelperMarkerEnv+"="+filepath.Join(parent, "bind-marker"),
			servedLaunchHelperReadyEnv+"="+filepath.Join(parent, "winner-ready"),
		)
	}
	if mode == "pre-bind-delay" {
		env = append(env,
			servedLaunchHelperMarkerEnv+"="+filepath.Join(filepath.Dir(root), "pre-bind-delay-marker"),
		)
	}
	return daemonlaunch.Options{
		CommandPath: os.Args[0],
		Args:        []string{"serve", "--foreground"},
		StateRoot:   root,
		Timeout:     timeout,
		Starter:     servedLaunchProcessStarter,
		Env:         env,
	}
}

type servedLaunchProcess struct {
	cmd *exec.Cmd
}

func servedLaunchProcessStarter(config daemonlaunch.ProcessConfig) (daemonlaunch.Process, error) {
	cmd := exec.Command(config.CommandPath, config.Args...)
	cmd.Env = config.Env
	cmd.ExtraFiles = config.ExtraFiles
	cmd.Stdin = config.Stdin
	cmd.Stdout = config.Stdout
	cmd.Stderr = config.Stderr
	if config.Setsid {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return servedLaunchProcess{cmd: cmd}, nil
}

func (process servedLaunchProcess) PID() int {
	return process.cmd.Process.Pid
}

func (process servedLaunchProcess) Kill() error {
	return process.cmd.Process.Kill()
}

func (process servedLaunchProcess) Wait() error {
	return process.cmd.Wait()
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

func waitForServedLaunchPath(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readServedLaunchToken(t *testing.T, path string) string {
	t.Helper()
	token, err := waitForServedLaunchToken(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func waitForServedLaunchToken(path string, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			if token := strings.TrimSpace(string(raw)); token != "" {
				return token, nil
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("timed out waiting for token at %s", path)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func helloServedLaunchDaemon(t *testing.T, socketPath, token string) {
	t.Helper()
	conn, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	if err := json.NewEncoder(conn).Encode(protocol.Request{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`"hello"`),
		Method:  protocol.MethodHello,
		Params:  mustMarshal(t, protocol.HelloParams{ClientProtocolVersion: protocol.Version, Token: token}),
	}); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var resp protocol.Response
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(line))), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != nil {
		t.Fatalf("hello error = %+v", resp.Error)
	}
}

func serveServedLaunchHello(listener net.Listener, token string, helloSeen chan<- struct{}) {
	conn, err := listener.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return
	}
	var req protocol.Request
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(line))), &req); err != nil {
		return
	}
	resp := protocol.Response{JSONRPC: "2.0", ID: req.ID}
	var params protocol.HelloParams
	if req.Method != protocol.MethodHello || json.Unmarshal(req.Params, &params) != nil || params.Token != token {
		resp.Error = protocol.NewError(protocol.ErrorUnauthorized, "unauthorized", protocol.ErrorData{})
		_ = json.NewEncoder(conn).Encode(resp)
		return
	}
	resp.Result = protocol.HelloResult{
		ProtocolVersion: protocol.Version,
		Backends:        []string{},
		Capabilities:    protocol.DefaultCapabilities(),
	}
	_ = json.NewEncoder(conn).Encode(resp)
	helloSeen <- struct{}{}
}

func launchRaceSocketDenied(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "operation not permitted") || strings.Contains(err.Error(), "bind: permission denied"))
}
