package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	agentclient "github.com/charlesnpx/agentbus/client"
	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/claudecli"
	"github.com/charlesnpx/agentbus/engine/adapter/codexcli"
	"github.com/charlesnpx/agentbus/engine/execution/authority"
	"github.com/charlesnpx/agentbus/internal/agentbusserve"
	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	cliJSONSchema = 1

	cliExitUnknownJob           = 10
	cliExitDaemonStartupFailure = 11
	cliExitAuthorityFailStop    = 12
)

var version = "dev"

type setupProber interface {
	SetupProbe(ctx context.Context) (engine.BackendSetupProbe, error)
}

type backendSpec struct {
	name    string
	backend engine.Backend
	probe   setupProber
}

type app struct {
	version        string
	stateRoot      string
	cwd            string
	setupCachePath string
	backends       []backendSpec
	registry       *engine.PolicyRegistry
	processes      engine.ProcessTable
	clock          engine.Clock
	daemonLauncher func(context.Context, daemonlaunch.Options) (daemonlaunch.Result, error)
	clientConnect  func(context.Context, agentclient.Options) (protocolClient, error)
}

type protocolClient interface {
	JobStatus(context.Context, agentclient.JobStatusParams) (agentclient.JobStatusResult, error)
	JobResult(context.Context, agentclient.JobResultParams) (agentclient.JobResult, error)
	JobCancel(context.Context, agentclient.JobCancelParams) (agentclient.JobCancelResult, error)
	Close() error
}

func main() {
	os.Exit(newDefaultApp().run(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func newDefaultApp() *app {
	a := &app{
		version:   version,
		stateRoot: os.Getenv("AGENTBUS_STATE_ROOT"),
		registry:  engine.NewPolicyRegistry(),
	}
	cachePath, err := a.cachePath()
	if err != nil {
		cachePath = ""
	}
	a.backends = defaultBackendSpecs(cachePath)
	return a
}

func defaultBackendSpecs(cachePath string) []backendSpec {
	return []backendSpec{
		newBackendSpec(codexcli.New(codexcli.Options{Binary: resolvedDefaultBackendBinary("codex"), CachePath: cachePath})),
		newBackendSpec(claudecli.New(claudecli.Options{Binary: resolvedDefaultBackendBinary("claude"), CachePath: cachePath})),
	}
}

func resolvedDefaultBackendBinary(name string) string {
	path, err := exec.LookPath(name)
	if err != nil {
		return ""
	}
	return path
}

func newBackendSpec(backend engine.Backend) backendSpec {
	probe, _ := backend.(setupProber)
	return backendSpec{name: backend.Name(), backend: backend, probe: probe}
}

func (a *app) run(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer) int {
	if len(args) == 0 {
		printRootHelp(errOut)
		return 2
	}
	switch args[0] {
	case "-h", "--help", "help":
		printRootHelp(out)
		return 0
	case "version":
		return a.runVersion(args[1:], out, errOut)
	case "setup":
		return a.runSetup(ctx, args[1:], out, errOut)
	case "serve":
		return a.runServe(ctx, args[1:], errOut)
	case "admission":
		return a.runAdmission(ctx, args[1:], out, errOut)
	case "status":
		return a.runStatus(ctx, args[1:], out, errOut)
	case "result":
		return a.runResult(ctx, args[1:], out, errOut)
	case "cancel":
		return a.runCancel(ctx, args[1:], out, errOut)
	case "validate":
		return a.runValidate(args[1:], in, out, errOut)
	case "internal-parked-worker":
		return a.runInternalParkedWorker(args[1:], errOut)
	case "internal-monitor":
		return a.runInternalMonitor(args[1:], errOut)
	case "internal-native-self-test-fixture":
		return a.runInternalNativeSelfTestFixture(args[1:], errOut)
	default:
		return usageError(errOut, "unknown command %q", args[0])
	}
}

func (a *app) runVersion(args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("version", errOut)
	jsonOut := fs.Bool("json", false, fmt.Sprintf("emit JSON: {\"schema\":%d,\"version\":\"...\",\"protocolVersion\":%d}", cliJSONSchema, protocol.Version))
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(errOut, "version does not accept positional arguments")
	}
	if *jsonOut {
		return writeOrError(out, errOut, versionOutput{Schema: cliJSONSchema, Version: a.version, ProtocolVersion: protocol.Version})
	}
	fmt.Fprintf(out, "agentbus %s\n", a.version)
	return 0
}

func (a *app) runSetup(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("setup", errOut)
	jsonOut := fs.Bool("json", false, "emit JSON: {\"schema\":1,\"backends\":[{\"backend\":\"codex\",\"binaryPath\":\"...\",\"version\":\"...\",\"configMode\":{\"write\":\"user\",\"readOnly\":\"hermetic\"},\"sandboxModes\":[\"workspace-write\",\"read-only\"],\"jsonEventsProbe\":{\"ran\":true,\"version\":\"...\",\"streamSchema\":\"...\"}}]}")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(errOut, "setup does not accept positional arguments")
	}
	result, ok := a.setup(ctx)
	if *jsonOut {
		code := 0
		if !ok {
			code = 1
		}
		if writeOrError(out, errOut, result) != 0 {
			return 1
		}
		return code
	}
	for _, backend := range result.Backends {
		if backend.Error != "" {
			fmt.Fprintf(errOut, "%s: %s\n", backend.Backend, backend.Error)
			continue
		}
		fmt.Fprintf(out, "%s: %s %s schema=%s probe=%t\n", backend.Backend, backend.BinaryPath, backend.Version, backend.JSONEventsProbe.StreamSchema, backend.JSONEventsProbe.Ran)
		for _, warning := range backend.Warnings {
			fmt.Fprintf(errOut, "%s: warning: %s\n", backend.Backend, warning)
		}
	}
	if result.Error != "" {
		fmt.Fprintln(errOut, result.Error)
	}
	if !ok {
		return 1
	}
	return 0
}

func (a *app) runServe(ctx context.Context, args []string, errOut io.Writer) int {
	fs := newCommandFlagSet("serve", errOut)
	foreground := fs.Bool("foreground", false, "run daemon in foreground")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(errOut, "serve does not accept positional arguments")
	}
	if !*foreground {
		if err := a.startBackgroundDaemon(ctx); err != nil {
			return commandError(errOut, err)
		}
		return 0
	}
	backends := make([]engine.Backend, 0, len(a.backends))
	for _, spec := range a.backends {
		if spec.backend != nil {
			backends = append(backends, spec.backend)
		}
	}
	err := agentbusserve.Serve(ctx, agentbusserve.Config{
		StateRoot:    a.stateRoot,
		CWD:          a.cwd,
		Backends:     backends,
		Registry:     a.registryOrDefault(),
		Clock:        a.clock,
		ProcessTable: a.processes,
	})
	if err != nil {
		return commandError(errOut, err)
	}
	return 0
}

func (a *app) runAdmission(ctx context.Context, args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		printAdmissionHelp(errOut)
		return 2
	}
	switch args[0] {
	case "-h", "--help", "help":
		printAdmissionHelp(out)
		return 0
	case "inspect":
		return a.runAdmissionInspect(ctx, args[1:], out, errOut)
	case "recover":
		return a.runAdmissionRecover(ctx, args[1:], out, errOut)
	case "reset-empty-root":
		return a.runAdmissionResetEmptyRoot(ctx, args[1:], out, errOut)
	case "seal":
		return a.runAdmissionSeal(ctx, args[1:], out, errOut)
	case "clear-fail-stop":
		return a.runAdmissionClearFailStop(ctx, args[1:], out, errOut)
	default:
		fmt.Fprintf(errOut, "agentbus: unknown admission command %q\n\n", args[0])
		printAdmissionHelp(errOut)
		return 2
	}
}

func (a *app) runAdmissionInspect(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := newAdmissionFlagSet("inspect", errOut)
	stateRoot := fs.String("state-root", "", "admission state root")
	jsonOut := fs.Bool("json", false, "emit JSON admission root inspection")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return admissionUsageError(errOut, "inspect does not accept positional arguments")
	}
	if *stateRoot == "" {
		return admissionUsageError(errOut, "inspect requires --state-root <path>")
	}
	inspection, err := authority.InspectAdmissionRoot(ctx, *stateRoot)
	if err != nil {
		return commandError(errOut, err)
	}
	if *jsonOut {
		return writeOrError(out, errOut, inspection)
	}
	printAdmissionInspection(out, inspection)
	return 0
}

func (a *app) runAdmissionRecover(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := newAdmissionFlagSet("recover", errOut)
	stateRoot := fs.String("state-root", "", "admission state root")
	jsonOut := fs.Bool("json", false, "emit JSON recovery report")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return admissionUsageError(errOut, "recover does not accept positional arguments")
	}
	if *stateRoot == "" {
		return admissionUsageError(errOut, "recover requires --state-root <path>")
	}
	report, err := agentbusserve.RecoverAdmissionRoot(ctx, agentbusserve.Config{
		StateRoot: *stateRoot,
		CWD:       a.cwd,
	})
	if err != nil {
		return commandError(errOut, err)
	}
	if *jsonOut {
		return writeOrError(out, errOut, report)
	}
	fmt.Fprintf(out, "mode=%s workItems=%d quiescedLaunches=%d finalizedJobs=%d recoveryPasses=%d\n", report.Mode, report.WorkItems, report.QuiescedLaunches, report.FinalizedJobs, report.RecoveryPasses)
	return 0
}

func (a *app) runAdmissionResetEmptyRoot(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := newAdmissionFlagSet("reset-empty-root", errOut)
	stateRoot := fs.String("state-root", "", "admission state root")
	jsonOut := fs.Bool("json", false, "emit JSON admission root inspection after reset")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return admissionUsageError(errOut, "reset-empty-root does not accept positional arguments")
	}
	if *stateRoot == "" {
		return admissionUsageError(errOut, "reset-empty-root requires --state-root <path>")
	}
	inspection, err := authority.ResetEmptyAdmissionRoot(ctx, *stateRoot)
	if err != nil {
		return commandError(errOut, err)
	}
	if *jsonOut {
		return writeOrError(out, errOut, inspection)
	}
	fmt.Fprintln(out, "reset-empty-root complete")
	printAdmissionInspection(out, inspection)
	return 0
}

func (a *app) runAdmissionSeal(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := newAdmissionFlagSet("seal", errOut)
	stateRoot := fs.String("state-root", "", "admission state root")
	newStateRoot := fs.String("new-state-root", "", "new admission state root; parent directory must already exist")
	startNew := fs.Bool("start-new-authority-domain", false, "acknowledge service must continue on a new state root/authority domain")
	ackReplayReset := fs.Bool("acknowledge-replay-history-reset", false, "acknowledge cross-root request replay history is reset")
	jsonOut := fs.Bool("json", false, "emit JSON admission seal report")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return admissionUsageError(errOut, "seal does not accept positional arguments")
	}
	if *stateRoot == "" {
		return admissionUsageError(errOut, "seal requires --state-root <path>")
	}
	report, err := authority.SealAdmissionRoot(ctx, *stateRoot, authority.SealOptions{
		StartNewAuthorityDomain:       *startNew,
		AcknowledgeReplayHistoryReset: *ackReplayReset,
		NewStateRoot:                  *newStateRoot,
	})
	if err != nil {
		return commandError(errOut, err)
	}
	if *jsonOut {
		return writeOrError(out, errOut, report)
	}
	fmt.Fprintf(out, "seal complete oldRootSealed=%t newStateRoot=%s newDomainUUID=%s\n", report.OldRootSealed, report.NewRoot, report.NewDomainUUID)
	printAdmissionInspection(out, report.OldInspection)
	return 0
}

func (a *app) runAdmissionClearFailStop(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := newAdmissionFlagSet("clear-fail-stop", errOut)
	stateRoot := fs.String("state-root", "", "admission state root")
	ack := fs.Bool("acknowledge-unsafe-diagnosis", false, "acknowledge operator diagnosis of the unsafe fail-stop reason")
	jsonOut := fs.Bool("json", false, "emit JSON clear fail-stop report")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return admissionUsageError(errOut, "clear-fail-stop does not accept positional arguments")
	}
	if *stateRoot == "" {
		return admissionUsageError(errOut, "clear-fail-stop requires --state-root <path>")
	}
	report, err := authority.ClearAdmissionFailStop(ctx, *stateRoot, authority.ClearFailStopOptions{AcknowledgeUnsafeDiagnosis: *ack})
	if err != nil {
		return commandError(errOut, err)
	}
	if *jsonOut {
		return writeOrError(out, errOut, report)
	}
	if report.Cleared {
		fmt.Fprintf(out, "clear-fail-stop complete reason=%q\n", report.ClearedReason)
	} else {
		fmt.Fprintln(out, "clear-fail-stop complete no fail-stop state present")
	}
	printAdmissionInspection(out, report.Inspection)
	return 0
}

func (a *app) startBackgroundDaemon(ctx context.Context) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	launcher := daemonlaunch.Launch
	if a.daemonLauncher != nil {
		launcher = a.daemonLauncher
	}
	result, err := launcher(ctx, daemonlaunch.Options{
		CommandPath: exe,
		Args:        []string{"serve", "--foreground"},
		StateRoot:   a.stateRoot,
		Timeout:     daemonlaunch.DefaultTimeout,
		Starter:     startDaemonProcess,
	})
	if err != nil {
		return err
	}
	if result.ExistingDaemon {
		return nil
	}
	if result.PID <= 0 {
		killErr := result.KillAndWait()
		return errors.Join(fmt.Errorf("daemon launcher returned invalid pid %d", result.PID), killErr)
	}
	root := result.CanonicalStateRoot
	if root == "" {
		root = a.stateRoot
	}
	if root == "" {
		root, err = engine.ResolveStateRoot()
		if err != nil {
			killErr := result.KillAndWait()
			return errors.Join(err, killErr)
		}
	}
	pidPath := filepath.Join(root, "agentbus.pid")
	if err := atomicWriteFile(pidPath, []byte(fmt.Sprintf("%d\n", result.PID)), 0o600); err != nil {
		removeErr := os.Remove(pidPath)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		killErr := result.KillAndWait()
		return errors.Join(err, removeErr, killErr)
	}
	return nil
}

type daemonProcess struct {
	cmd *exec.Cmd
}

func startDaemonProcess(config daemonlaunch.ProcessConfig) (daemonlaunch.Process, error) {
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
	return daemonProcess{cmd: cmd}, nil
}

func (process daemonProcess) PID() int {
	return process.cmd.Process.Pid
}

func (process daemonProcess) Kill() error {
	return process.cmd.Process.Kill()
}

func (process daemonProcess) Wait() error {
	return process.cmd.Wait()
}

func atomicWriteFile(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return os.Chmod(path, mode)
}

func (a *app) runStatus(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("status", errOut)
	jsonOut := fs.Bool("json", false, "emit protocol-v2 authority JSON: {\"jobs\":[{\"jobId\":\"...\",\"sessionId\":\"...\",\"state\":\"running\"}]}")
	jobID := fs.String("job", "", "job id")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(errOut, "status does not accept positional arguments")
	}
	client, err := a.connectProtocolClient(ctx)
	if err != nil {
		return protocolCommandError(errOut, "status", err)
	}
	defer client.Close()
	params := agentclient.JobStatusParams{All: true}
	if *jobID != "" {
		params = agentclient.JobStatusParams{JobID: *jobID}
	}
	statuses, err := client.JobStatus(ctx, params)
	if err != nil {
		return protocolCommandError(errOut, "status", err)
	}
	if *jobID != "" {
		if len(statuses.Jobs) == 0 {
			return unknownJobCommandError(errOut, "status", *jobID)
		}
		if len(statuses.Jobs) != 1 || statuses.Jobs[0].JobID != *jobID {
			return commandError(errOut, fmt.Errorf("status returned unexpected jobs for %s", *jobID))
		}
	}
	if *jsonOut {
		if code := writeOrError(out, errOut, statuses); code != 0 {
			return code
		}
	} else {
		for _, status := range statuses.Jobs {
			printJobStatus(out, status)
		}
	}
	if *jobID == "" {
		return 0
	}
	return cliExitCodeForState(statuses.Jobs[0].State)
}

func (a *app) runResult(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("result", errOut)
	jsonOut := fs.Bool("json", false, "emit protocol-v2 authority JSON: {\"jobId\":\"...\",\"state\":\"completed\",\"result\":{\"text\":\"...\",\"resultPath\":\"...\",\"sha256\":\"...\",\"bytes\":1}}")
	jobID := fs.String("job", "", "job id")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(errOut, "result does not accept positional arguments")
	}
	if *jobID == "" {
		return usageError(errOut, "result requires --job <id>")
	}
	client, err := a.connectProtocolClient(ctx)
	if err != nil {
		return protocolCommandError(errOut, "result", err)
	}
	defer client.Close()
	result, err := client.JobResult(ctx, agentclient.JobResultParams{JobID: *jobID})
	if err != nil {
		return protocolCommandError(errOut, "result", err)
	}
	if *jsonOut {
		if code := writeOrError(out, errOut, result); code != 0 {
			return code
		}
	} else {
		printJobResult(out, result)
	}
	return cliExitCodeForState(result.State)
}

func (a *app) runCancel(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("cancel", errOut)
	jsonOut := fs.Bool("json", false, "emit protocol-v2 authority JSON: {\"jobId\":\"...\",\"state\":\"canceled\"}")
	jobID := fs.String("job", "", "job id")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(errOut, "cancel does not accept positional arguments")
	}
	if *jobID == "" {
		return usageError(errOut, "cancel requires --job <id>")
	}
	client, err := a.connectProtocolClient(ctx)
	if err != nil {
		return protocolCommandError(errOut, "cancel", err)
	}
	defer client.Close()
	result, err := client.JobCancel(ctx, agentclient.JobCancelParams{JobID: *jobID})
	if err != nil {
		return protocolCommandError(errOut, "cancel", err)
	}
	if *jsonOut {
		if code := writeOrError(out, errOut, result); code != 0 {
			return code
		}
	} else {
		fmt.Fprintf(out, "%s\t%s\n", result.JobID, result.State)
	}
	return cliExitCodeForState(result.State)
}

func (a *app) runValidate(args []string, in io.Reader, out, errOut io.Writer) int {
	fs := newCommandFlagSet("validate", errOut)
	contractRef := fs.String("contract", "", "contract spec file or registered name")
	textFile := fs.String("text-file", "", "text file to validate; stdin is used when omitted")
	jsonOut := fs.Bool("json", false, "emit JSON: {\"valid\":true,\"missing\":[],\"contractName\":\"...\",\"contractSha256\":\"sha256:...\"}")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(errOut, "validate does not accept positional arguments")
	}
	if *contractRef == "" {
		return usageError(errOut, "validate requires --contract <file|name>")
	}
	text, err := readValidationText(*textFile, in)
	if err != nil {
		return commandError(errOut, err)
	}
	contract, contractName, err := a.resolveContract(*contractRef)
	if err != nil {
		return commandError(errOut, err)
	}
	result, err := engine.ValidateContract(text, contract)
	if err != nil {
		return commandError(errOut, err)
	}
	result.ContractName = contractName
	if *jsonOut {
		if code := writeOrError(out, errOut, result); code != 0 {
			return code
		}
	} else if result.Valid {
		fmt.Fprintf(out, "valid %s\n", result.ContractSHA256)
	} else {
		fmt.Fprintf(out, "invalid %s missing=%s\n", result.ContractSHA256, strings.Join(result.Missing, ","))
	}
	if !result.Valid {
		return 3
	}
	return 0
}

func (a *app) setup(ctx context.Context) (setupOutput, bool) {
	result := setupOutput{Schema: cliJSONSchema}
	if len(a.backends) == 0 {
		result.Error = "no backends configured"
		return result, false
	}
	cache := engine.SetupProbeCache{Version: engine.SetupProbeCacheVersion, Backends: make([]engine.BackendSetupProbe, 0, len(a.backends))}
	ok := true
	for _, spec := range a.backends {
		name := spec.name
		if name == "" && spec.backend != nil {
			name = spec.backend.Name()
		}
		report := setupBackendReport{Backend: name}
		if spec.probe == nil {
			report.Error = "setup probe is unavailable for backend"
			result.Backends = append(result.Backends, report)
			ok = false
			continue
		}
		probe, err := spec.probe.SetupProbe(ctx)
		if err != nil {
			report.Error = err.Error()
			result.Backends = append(result.Backends, report)
			ok = false
			continue
		}
		probe = normalizeProbe(probe, name)
		cache.Backends = append(cache.Backends, probe)
		result.Backends = append(result.Backends, setupReportFromProbe(probe))
	}
	if !ok {
		result.Error = "setup probe failed"
		return result, false
	}
	cachePath, err := a.cachePath()
	if err != nil {
		result.Error = err.Error()
		return result, false
	}
	if err := engine.WriteSetupProbeCache(cachePath, cache); err != nil {
		result.Error = err.Error()
		return result, false
	}
	for i, spec := range a.backends {
		if spec.backend == nil {
			result.Backends[i].Error = "backend is unavailable"
			ok = false
			continue
		}
		health, err := spec.backend.Preflight(ctx)
		if err != nil {
			result.Backends[i].Error = err.Error()
			ok = false
			continue
		}
		if health.BinaryPath != "" {
			result.Backends[i].BinaryPath = health.BinaryPath
		}
		if health.Version != "" {
			result.Backends[i].Version = health.Version
			result.Backends[i].JSONEventsProbe.Version = health.Version
		}
		if health.StreamSchema != "" {
			result.Backends[i].JSONEventsProbe.StreamSchema = health.StreamSchema
		}
		if health.Warning != "" {
			result.Backends[i].Warnings = append(result.Backends[i].Warnings, health.Warning)
		}
	}
	if !ok {
		result.Error = "setup preflight failed"
	}
	return result, ok
}

func normalizeProbe(probe engine.BackendSetupProbe, name string) engine.BackendSetupProbe {
	if probe.Backend == "" {
		probe.Backend = name
	}
	if probe.ConfigMode.Write == "" {
		probe.ConfigMode.Write = "user"
	}
	if probe.ConfigMode.ReadOnly == "" {
		probe.ConfigMode.ReadOnly = "hermetic"
	}
	if len(probe.SandboxModes) == 0 {
		probe.SandboxModes = []string{"workspace-write", "read-only"}
	}
	return probe
}

func setupReportFromProbe(probe engine.BackendSetupProbe) setupBackendReport {
	return setupBackendReport{
		Backend:                probe.Backend,
		BinaryPath:             probe.BinaryPath,
		Version:                probe.Version,
		ConfigMode:             probe.ConfigMode,
		SandboxModes:           append([]string(nil), probe.SandboxModes...),
		DiscoveredModels:       append([]string(nil), probe.DiscoveredModels...),
		DiscoveredEfforts:      append([]string(nil), probe.DiscoveredEfforts...),
		DiscoveryFetchedAt:     probe.DiscoveryFetchedAt,
		DiscoveryClientVersion: probe.DiscoveryClientVersion,
		Warnings:               append([]string(nil), probe.DiscoveryWarnings...),
		JSONEventsProbe: setupJSONEventsProbe{
			Ran:          probe.JSONEventsProbed,
			Version:      probe.Version,
			StreamSchema: probe.StreamSchema,
		},
	}
}

func (a *app) cachePath() (string, error) {
	if a.setupCachePath != "" {
		return a.setupCachePath, nil
	}
	return engine.SetupProbeCachePath(a.stateRoot)
}

func (a *app) registryOrDefault() *engine.PolicyRegistry {
	if a.registry != nil {
		return a.registry
	}
	return engine.NewPolicyRegistry()
}

func (a *app) resolveContract(ref string) (engine.ContractSpec, string, error) {
	if info, err := os.Stat(ref); err == nil {
		if info.IsDir() {
			return engine.ContractSpec{}, "", fmt.Errorf("contract path is a directory: %s", ref)
		}
		raw, err := os.ReadFile(ref)
		if err != nil {
			return engine.ContractSpec{}, "", err
		}
		spec, err := parseContractSpec(raw)
		if err != nil {
			return engine.ContractSpec{}, "", err
		}
		resolved, name, _, err := engine.ResolveContract(spec, a.registryOrDefault())
		return resolved, name, err
	} else if !os.IsNotExist(err) {
		return engine.ContractSpec{}, "", err
	}
	spec, _, err := a.registryOrDefault().Resolve(ref)
	if err != nil {
		return engine.ContractSpec{}, "", fmt.Errorf("contract name %q is not registered in the embedded registry: %w", ref, err)
	}
	return spec, ref, nil
}

func parseContractSpec(raw []byte) (engine.ContractSpec, error) {
	var spec engine.ContractSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return engine.ContractSpec{}, err
	}
	if hasContractVariant(spec) {
		return spec, nil
	}
	var wrapper struct {
		Contract engine.ContractSpec `json:"contract"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return engine.ContractSpec{}, err
	}
	if hasContractVariant(wrapper.Contract) {
		return wrapper.Contract, nil
	}
	return spec, nil
}

func hasContractVariant(spec engine.ContractSpec) bool {
	return spec.JSONSchema != nil || spec.Shape != nil || spec.Named != ""
}

func readValidationText(path string, in io.Reader) (string, error) {
	if path == "" {
		raw, err := io.ReadAll(in)
		if err != nil {
			return "", err
		}
		return string(raw), nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func tagsMatch(actual map[string]string, want map[string]string) bool {
	for key, value := range want {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func (a *app) connectProtocolClient(ctx context.Context) (protocolClient, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	opts := agentclient.Options{
		StateRoot:   a.stateRoot,
		CommandPath: exe,
	}
	if a.clientConnect != nil {
		return a.clientConnect(ctx, opts)
	}
	return agentclient.Connect(ctx, opts)
}

func printJobStatus(out io.Writer, status agentclient.JobStatus) {
	fmt.Fprintf(out, "%s\t%s", status.JobID, status.State)
	if status.Backend != "" {
		fmt.Fprintf(out, "\t%s", status.Backend)
	}
	if status.Lease != nil && status.Lease.Expired {
		fmt.Fprint(out, "\tlease=expired")
	}
	fmt.Fprintln(out)
}

func printJobResult(out io.Writer, result agentclient.JobResult) {
	if !engine.IsTerminal(result.State) {
		fmt.Fprintf(out, "%s\t%s\n", result.JobID, result.State)
		return
	}
	if result.Result == nil {
		fmt.Fprintf(out, "%s\t%s\tno result\n", result.JobID, result.State)
		return
	}
	if result.Result.Text != "" {
		fmt.Fprint(out, result.Result.Text)
		if !strings.HasSuffix(result.Result.Text, "\n") {
			fmt.Fprintln(out)
		}
		return
	}
	fmt.Fprintf(out, "resultPath=%s sha256=%s bytes=%d\n", result.Result.ResultPath, result.Result.SHA256, result.Result.Bytes)
}

func newCommandFlagSet(name string, errOut io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("agentbus "+name, flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.Usage = func() { printRootHelp(errOut) }
	return fs
}

func newAdmissionFlagSet(name string, errOut io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("agentbus admission "+name, flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.Usage = func() { printAdmissionHelp(errOut) }
	return fs
}

func parseFlags(fs *flag.FlagSet, args []string) (int, bool) {
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0, false
		}
		return 2, false
	}
	return 0, true
}

func usageError(errOut io.Writer, format string, args ...any) int {
	fmt.Fprintf(errOut, "agentbus: "+format+"\n\n", args...)
	printRootHelp(errOut)
	return 2
}

func admissionUsageError(errOut io.Writer, format string, args ...any) int {
	fmt.Fprintf(errOut, "agentbus admission: "+format+"\n\n", args...)
	printAdmissionHelp(errOut)
	return 2
}

func commandError(errOut io.Writer, err error) int {
	fmt.Fprintf(errOut, "agentbus: %v\n", err)
	return 1
}

func protocolCommandError(errOut io.Writer, operation string, err error) int {
	var rpcErr *protocol.RPCError
	if errors.As(err, &rpcErr) {
		data := rpcErr.Object.Data
		fields := []string{}
		if data.Code != "" {
			fields = append(fields, "code="+data.Code)
		}
		if data.JobID != "" {
			fields = append(fields, "jobId="+data.JobID)
		}
		if data.AdmissionCause != "" {
			fields = append(fields, "admissionCause="+data.AdmissionCause)
		}
		if len(fields) > 0 {
			fmt.Fprintf(errOut, "agentbus: %s failed (%s): %s\n", operation, strings.Join(fields, " "), rpcErr.Object.Message)
		} else {
			fmt.Fprintf(errOut, "agentbus: %s failed: %s\n", operation, rpcErr.Object.Message)
		}
		return cliExitCodeForProtocolError(rpcErr)
	}
	fmt.Fprintf(errOut, "agentbus: %s failed: %v\n", operation, err)
	if errors.Is(err, daemonlaunch.ErrStartupFailed) {
		return cliExitDaemonStartupFailure
	}
	return 1
}

func unknownJobCommandError(errOut io.Writer, operation, jobID string) int {
	fmt.Fprintf(errOut, "agentbus: %s failed (code=%s jobId=%s): job is not known\n", operation, protocol.ErrorInvalidTaskSpec, jobID)
	return cliExitUnknownJob
}

func cliExitCodeForProtocolError(err *protocol.RPCError) int {
	if err == nil {
		return 1
	}
	data := err.Object.Data
	switch data.AdmissionCause {
	case protocol.AdmissionRejectRootFailStopped,
		protocol.AdmissionRejectRootCorrupt,
		protocol.AdmissionRejectRootIdentityMismatch:
		return cliExitAuthorityFailStop
	case protocol.AdmissionRejectUnavailableNativeRuntime:
		return cliExitDaemonStartupFailure
	}
	if data.JobID != "" && data.Code == protocol.ErrorInvalidTaskSpec && strings.Contains(err.Object.Message, "job is not known") {
		return cliExitUnknownJob
	}
	return 1
}

func cliExitCodeForState(state engine.JobState) int {
	return engine.ExitCodeForState(state)
}

func writeOrError(out, errOut io.Writer, v any) int {
	if err := writeJSON(out, v); err != nil {
		return commandError(errOut, err)
	}
	return 0
}

func writeJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

func printRootHelp(out io.Writer) {
	fmt.Fprint(out, `Usage:
  agentbus version [--json]
  agentbus setup [--json]
  agentbus serve [--foreground]
  agentbus admission <inspect|recover|reset-empty-root|seal|clear-fail-stop> --state-root <path>
  agentbus status [--job <id>] [--json]
  agentbus result --job <id> [--json]
  agentbus cancel --job <id> [--json]
  agentbus validate --contract <file|name> [--text-file <f>] [--json]

JSON shapes:
  version:  {"schema":1,"version":"dev","protocolVersion":2}
  setup:    {"schema":1,"backends":[{"backend":"codex","binaryPath":"...","version":"...","configMode":{"write":"user","readOnly":"hermetic"},"sandboxModes":["workspace-write","read-only"],"jsonEventsProbe":{"ran":true,"version":"...","streamSchema":"codex-json-v1"}}]}
  status:   {"jobs":[{"jobId":"...","sessionId":"...","state":"running"}]}
  result:   {"jobId":"...","sessionId":"...","state":"completed","result":{"text":"...","resultPath":"...","sha256":"...","bytes":1}}
  cancel:   {"jobId":"...","state":"canceled"}
  validate: {"valid":true,"missing":[],"contractName":"...","contractSha256":"sha256:..."}

Exit codes for single-job status/result/cancel:
  completed=0, running/non-terminal=2, completed_noncompliant=3, failed=4, timed_out=5, interrupted=6, canceled=7, reaped=8, quarantined=9, unknown-job=10, daemon-startup-failure=11, fail-stop=12

Status/result/cancel are protocol-v2 daemon clients. Offline authority diagnosis stays under admission inspect/recover/admin commands.

Serve admission:
  serve always starts the strict identified admission runtime. Unsupported hosts fail closed at startup. Strict activation is one-way for a state root; use admission recover, seal, or reset-empty-root for the admin escape hatches. The first strict release supports one active state root.
`)
}

func printAdmissionHelp(out io.Writer) {
	fmt.Fprint(out, `Usage:
  agentbus admission inspect --state-root <path> [--json]
  agentbus admission recover --state-root <path> [--json]
  agentbus admission reset-empty-root --state-root <path> [--json]
  agentbus admission seal --state-root <path> --new-state-root <path> --start-new-authority-domain --acknowledge-replay-history-reset [--json]
  agentbus admission clear-fail-stop --state-root <path> --acknowledge-unsafe-diagnosis [--json]

Admission administration:
  inspect:          read activation metadata, contract version, counts, domain UUID, and sealed flag; never mutates
  recover:          requires strict support; reconciles durable nonterminal obligations without opening a listener
  reset-empty-root: reinitializes only when jobs, bindings, tombstones, launch records, and recovery obligations are all zero
  seal:             marks the old domain permanently closed for audit; --new-state-root must be a new leaf whose parent directory already exists
  clear-fail-stop:  clears a persisted unsafe fail-stop only after explicit operator diagnosis acknowledgement

Multi-root read/cancel/result routing is out of scope in this first release.
`)
}

func printAdmissionInspection(out io.Writer, inspection authority.RootInspection) {
	metadata := inspection.ActivationMetadata
	fmt.Fprintf(out, "domainUUID=%s sealed=%t generation=%d\n", inspection.DomainUUID, inspection.Sealed, inspection.Generation)
	fmt.Fprintf(out, "activated=%t contractVersion=%d activatedAtGen=%d\n", metadata.Activated, metadata.ContractVersion, metadata.ActivatedAtGen)
	if inspection.FailStopped {
		fmt.Fprintf(out, "failStopped=true reason=%q\n", inspection.FailStopReason)
	}
	fmt.Fprintf(out, "counts jobs=%d bindings=%d tombstones=%d launchRecords=%d recoveryObligations=%d\n",
		inspection.Counts.Jobs,
		inspection.Counts.Bindings,
		inspection.Counts.Tombstones,
		inspection.Counts.LaunchRecords,
		inspection.Counts.RecoveryObligations,
	)
}

type tagFilter map[string]string

func (t *tagFilter) String() string {
	if t == nil || len(*t) == 0 {
		return ""
	}
	keys := make([]string, 0, len(*t))
	for key := range *t {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		parts = append(parts, key+"="+(*t)[key])
	}
	return strings.Join(parts, ",")
}

func (t *tagFilter) Set(raw string) error {
	if *t == nil {
		*t = make(map[string]string)
	}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok || key == "" {
			return fmt.Errorf("tag filter must be k=v: %q", part)
		}
		(*t)[key] = value
	}
	return nil
}

type versionOutput struct {
	Schema          int    `json:"schema"`
	Version         string `json:"version"`
	ProtocolVersion int    `json:"protocolVersion"`
}

type setupOutput struct {
	Schema   int                  `json:"schema"`
	Backends []setupBackendReport `json:"backends"`
	Error    string               `json:"error,omitempty"`
}

type setupBackendReport struct {
	Backend                string               `json:"backend"`
	BinaryPath             string               `json:"binaryPath,omitempty"`
	Version                string               `json:"version,omitempty"`
	ConfigMode             engine.ModeInfo      `json:"configMode"`
	SandboxModes           []string             `json:"sandboxModes,omitempty"`
	JSONEventsProbe        setupJSONEventsProbe `json:"jsonEventsProbe"`
	DiscoveredModels       []string             `json:"discoveredModels"`
	DiscoveredEfforts      []string             `json:"discoveredEfforts"`
	DiscoveryFetchedAt     string               `json:"discoveryFetchedAt,omitempty"`
	DiscoveryClientVersion string               `json:"discoveryClientVersion,omitempty"`
	Warnings               []string             `json:"warnings,omitempty"`
	Error                  string               `json:"error,omitempty"`
}

type setupJSONEventsProbe struct {
	Ran          bool   `json:"ran"`
	Version      string `json:"version,omitempty"`
	StreamSchema string `json:"streamSchema,omitempty"`
}
