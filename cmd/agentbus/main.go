package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/claudecli"
	"github.com/charlesnpx/agentbus/engine/adapter/codexcli"
	"github.com/charlesnpx/agentbus/internal/served"
)

const (
	protocolMajor = 1
	cliJSONSchema = 1
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
		newBackendSpec(codexcli.New(codexcli.Options{CachePath: cachePath})),
		newBackendSpec(claudecli.New(claudecli.Options{CachePath: cachePath})),
	}
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
	case "sessions":
		return a.runSessions(args[1:], out, errOut)
	case "status":
		return a.runStatus(args[1:], out, errOut)
	case "result":
		return a.runResult(args[1:], out, errOut)
	case "cancel":
		return a.runCancel(args[1:], out, errOut)
	case "validate":
		return a.runValidate(args[1:], in, out, errOut)
	default:
		return usageError(errOut, "unknown command %q", args[0])
	}
}

func (a *app) runVersion(args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("version", errOut)
	jsonOut := fs.Bool("json", false, "emit JSON: {\"schema\":1,\"version\":\"...\",\"protocolVersion\":1}")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(errOut, "version does not accept positional arguments")
	}
	if *jsonOut {
		return writeOrError(out, errOut, versionOutput{Schema: cliJSONSchema, Version: a.version, ProtocolVersion: protocolMajor})
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
	server, err := served.New(served.Config{
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
	if err := server.Serve(ctx); err != nil {
		return commandError(errOut, err)
	}
	return 0
}

func (a *app) startBackgroundDaemon(ctx context.Context) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, exe, "serve", "--foreground")
	cmd.Env = os.Environ()
	if a.stateRoot != "" {
		cmd.Env = append(cmd.Env, "AGENTBUS_STATE_ROOT="+a.stateRoot)
	}
	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer devNull.Close()
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull
	if err := cmd.Start(); err != nil {
		return err
	}
	root := a.stateRoot
	if root == "" {
		root, err = engine.ResolveStateRoot()
		if err != nil {
			return err
		}
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	pidPath := filepath.Join(root, "agentbus.pid")
	return atomicWriteFile(pidPath, []byte(fmt.Sprintf("%d\n", cmd.Process.Pid)), 0o600)
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

func (a *app) runSessions(args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("sessions", errOut)
	jsonOut := fs.Bool("json", false, "emit JSON: {\"sessions\":[{\"sessionId\":\"...\",\"backend\":\"codex\",\"cwd\":\"...\",\"tags\":{},\"activeTurnId\":null}]}")
	tags := tagFilter{}
	fs.Var(&tags, "tags", "exact tag filter k=v; repeat for multiple tags")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(errOut, "sessions does not accept positional arguments")
	}
	store, err := a.newStore()
	if err != nil {
		return commandError(errOut, err)
	}
	records, err := store.List()
	if err != nil {
		return commandError(errOut, err)
	}
	sessions := sessionsFromRecords(records, store.Layout().Workspace, tags)
	result := sessionsOutput{Sessions: sessions}
	if *jsonOut {
		return writeOrError(out, errOut, result)
	}
	for _, session := range sessions {
		active := "-"
		if session.ActiveTurnID != nil {
			active = *session.ActiveTurnID
		}
		fmt.Fprintf(out, "%s\t%s\t%s\tactive=%s\n", session.SessionID, session.Backend, session.CWD, active)
	}
	return 0
}

func (a *app) runStatus(args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("status", errOut)
	jsonOut := fs.Bool("json", false, "emit JSON: {\"jobs\":[{\"jobId\":\"...\",\"state\":\"running\",\"lease\":{\"expiresAt\":\"...\",\"expired\":false}}]}")
	jobID := fs.String("job", "", "job id")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(errOut, "status does not accept positional arguments")
	}
	store, err := a.newStore()
	if err != nil {
		return commandError(errOut, err)
	}
	if *jobID != "" {
		record, err := store.Load(*jobID)
		if err != nil {
			return commandError(errOut, err)
		}
		status := statusFromRecord(*record)
		if *jsonOut {
			if code := writeOrError(out, errOut, statusOutput{Jobs: []jobStatus{status}}); code != 0 {
				return code
			}
		} else {
			printJobStatus(out, status)
		}
		return engine.ExitCodeForState(record.State)
	}
	records, err := store.List()
	if err != nil {
		return commandError(errOut, err)
	}
	statuses := make([]jobStatus, 0, len(records))
	for _, record := range records {
		statuses = append(statuses, statusFromRecord(record))
	}
	if *jsonOut {
		return writeOrError(out, errOut, statusOutput{Jobs: statuses})
	}
	for _, status := range statuses {
		printJobStatus(out, status)
	}
	return 0
}

func (a *app) runResult(args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("result", errOut)
	jsonOut := fs.Bool("json", false, "emit JSON: {\"jobId\":\"...\",\"state\":\"completed\",\"result\":{\"text\":\"...\",\"resultPath\":\"...\",\"sha256\":\"...\",\"bytes\":1},\"contract\":{...}}")
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
	store, err := a.newStore()
	if err != nil {
		return commandError(errOut, err)
	}
	record, err := store.Load(*jobID)
	if err != nil {
		return commandError(errOut, err)
	}
	result := resultFromRecord(*record)
	if *jsonOut {
		if code := writeOrError(out, errOut, result); code != 0 {
			return code
		}
	} else {
		printJobResult(out, result)
	}
	return engine.ExitCodeForState(record.State)
}

func (a *app) runCancel(args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("cancel", errOut)
	jsonOut := fs.Bool("json", false, "emit JSON: {\"jobId\":\"...\",\"state\":\"canceled\"}")
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
	store, err := a.newStore()
	if err != nil {
		return commandError(errOut, err)
	}
	record, err := store.Cancel(*jobID)
	if err != nil {
		return commandError(errOut, err)
	}
	result := cancelOutput{JobID: record.JobID, State: record.State}
	if *jsonOut {
		if code := writeOrError(out, errOut, result); code != 0 {
			return code
		}
	} else {
		fmt.Fprintf(out, "%s\t%s\n", result.JobID, result.State)
	}
	return engine.ExitCodeForState(record.State)
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

func (a *app) newStore() (*engine.Store, error) {
	return engine.NewStore(engine.StoreConfig{
		Root:      a.stateRoot,
		CWD:       a.cwd,
		Clock:     a.clock,
		Processes: a.processes,
	})
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

func sessionsFromRecords(records []engine.JobRecord, cwd string, tags map[string]string) []sessionInfo {
	byID := make(map[string]*sessionInfo)
	for _, record := range records {
		if record.SessionID == "" || !tagsMatch(record.Tags, tags) {
			continue
		}
		session := byID[record.SessionID]
		if session == nil {
			session = &sessionInfo{
				SessionID: record.SessionID,
				Backend:   record.Backend,
				CWD:       cwd,
				Tags:      cloneTags(record.Tags),
			}
			byID[record.SessionID] = session
		}
		if session.Backend == "" {
			session.Backend = record.Backend
		}
		if len(session.Tags) == 0 {
			session.Tags = cloneTags(record.Tags)
		}
		if !engine.IsTerminal(record.State) {
			active := record.JobID
			session.ActiveTurnID = &active
		}
	}
	sessions := make([]sessionInfo, 0, len(byID))
	for _, session := range byID {
		sessions = append(sessions, *session)
	}
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].SessionID < sessions[j].SessionID })
	return sessions
}

func tagsMatch(actual map[string]string, want map[string]string) bool {
	for key, value := range want {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func cloneTags(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func statusFromRecord(record engine.JobRecord) jobStatus {
	return jobStatus{
		JobID:                 record.JobID,
		SessionID:             record.SessionID,
		Backend:               record.Backend,
		State:                 record.State,
		LateFinalization:      record.LateFinalization,
		Tags:                  cloneTags(record.Tags),
		CreatedAt:             timePtr(record.CreatedAt),
		StartedAt:             timePtr(record.StartedAt),
		UpdatedAt:             timePtr(record.UpdatedAt),
		HeartbeatAt:           timePtr(record.HeartbeatAt),
		Lease:                 leasePtr(record.Lease),
		WorkerPID:             record.Worker.PID,
		WorkerStartTime:       record.Worker.StartTime,
		BackendChildPID:       record.BackendChildPID,
		BackendChildStartTime: record.BackendChildStartTime,
		StatePath:             record.StatePath,
		LogPaths:              record.LogPaths,
	}
}

func resultFromRecord(record engine.JobRecord) jobResult {
	return jobResult{
		JobID:            record.JobID,
		SessionID:        record.SessionID,
		State:            record.State,
		LateFinalization: record.LateFinalization,
		Result:           record.Result,
		Contract:         record.Contract,
	}
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func leasePtr(lease engine.Lease) *engine.Lease {
	if lease.ExpiresAt.IsZero() {
		return nil
	}
	return &lease
}

func printJobStatus(out io.Writer, status jobStatus) {
	fmt.Fprintf(out, "%s\t%s", status.JobID, status.State)
	if status.Backend != "" {
		fmt.Fprintf(out, "\t%s", status.Backend)
	}
	if status.Lease != nil && status.Lease.Expired {
		fmt.Fprint(out, "\tlease=expired")
	}
	fmt.Fprintln(out)
}

func printJobResult(out io.Writer, result jobResult) {
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

func commandError(errOut io.Writer, err error) int {
	fmt.Fprintf(errOut, "agentbus: %v\n", err)
	return 1
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
  agentbus sessions [--tags k=v] [--json]
  agentbus status [--job <id>] [--json]
  agentbus result --job <id> [--json]
  agentbus cancel --job <id> [--json]
  agentbus validate --contract <file|name> [--text-file <f>] [--json]

JSON shapes:
  version:  {"schema":1,"version":"dev","protocolVersion":1}
  setup:    {"schema":1,"backends":[{"backend":"codex","binaryPath":"...","version":"...","configMode":{"write":"user","readOnly":"hermetic"},"sandboxModes":["workspace-write","read-only"],"jsonEventsProbe":{"ran":true,"version":"...","streamSchema":"codex-json-v1"}}]}
  sessions: {"sessions":[{"sessionId":"...","backend":"codex","cwd":"...","tags":{},"activeTurnId":null}]}
  status:   {"jobs":[{"jobId":"...","state":"running","lease":{"expiresAt":"...","expired":false}}]}
  result:   {"jobId":"...","state":"completed","result":{"text":"...","resultPath":"...","sha256":"...","bytes":1},"contract":{...}}
  cancel:   {"jobId":"...","state":"canceled"}
  validate: {"valid":true,"missing":[],"contractName":"...","contractSha256":"sha256:..."}

Exit codes for single-job status/result/cancel:
  completed=0, non-terminal=2, completed_noncompliant=3, failed=4, timed_out=5, interrupted=6, canceled=7, reaped=8, quarantined=9
`)
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

type sessionsOutput struct {
	Sessions []sessionInfo `json:"sessions"`
}

type sessionInfo struct {
	SessionID    string            `json:"sessionId"`
	Backend      string            `json:"backend,omitempty"`
	CWD          string            `json:"cwd,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
	ActiveTurnID *string           `json:"activeTurnId"`
}

type statusOutput struct {
	Jobs []jobStatus `json:"jobs"`
}

type jobStatus struct {
	JobID                 string            `json:"jobId"`
	SessionID             string            `json:"sessionId,omitempty"`
	Backend               string            `json:"backend,omitempty"`
	State                 engine.JobState   `json:"state"`
	LateFinalization      bool              `json:"lateFinalization,omitempty"`
	Tags                  map[string]string `json:"tags,omitempty"`
	CreatedAt             *time.Time        `json:"createdAt,omitempty"`
	StartedAt             *time.Time        `json:"startedAt,omitempty"`
	UpdatedAt             *time.Time        `json:"updatedAt,omitempty"`
	HeartbeatAt           *time.Time        `json:"heartbeatAt,omitempty"`
	Lease                 *engine.Lease     `json:"lease,omitempty"`
	WorkerPID             int               `json:"workerPid,omitempty"`
	WorkerStartTime       string            `json:"workerStartTime,omitempty"`
	BackendChildPID       int               `json:"backendChildPid,omitempty"`
	BackendChildStartTime string            `json:"backendChildStartTime,omitempty"`
	StatePath             string            `json:"statePath,omitempty"`
	LogPaths              engine.LogPaths   `json:"logPaths,omitempty"`
}

type jobResult struct {
	JobID            string                `json:"jobId"`
	SessionID        string                `json:"sessionId,omitempty"`
	State            engine.JobState       `json:"state"`
	LateFinalization bool                  `json:"lateFinalization,omitempty"`
	Result           *engine.ResultInfo    `json:"result,omitempty"`
	Contract         *engine.ContractStamp `json:"contract,omitempty"`
}

type cancelOutput struct {
	JobID string          `json:"jobId"`
	State engine.JobState `json:"state"`
}
