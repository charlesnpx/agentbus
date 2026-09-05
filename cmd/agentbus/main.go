package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	agentclient "github.com/charlesnpx/agentbus/client"
	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/claudecli"
	"github.com/charlesnpx/agentbus/engine/adapter/codexcli"
	"github.com/charlesnpx/agentbus/engine/adapter/cursorcli"
	"github.com/charlesnpx/agentbus/internal/agentbusserve"
	"github.com/charlesnpx/agentbus/internal/daemonlaunch"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

const (
	cliJSONSchema = 1

	cliExitUnknownJob           = 10
	cliExitDaemonStartupFailure = 11
	cliExitShutdownForced       = 13
	cliExitResultUnavailable    = 15
	shortSHA256HexLength        = 12
)

var version = "dev"

type app struct {
	version        string
	stateRoot      string
	backends       []engine.Backend
	daemonLauncher func(context.Context, daemonlaunch.Options) (daemonlaunch.Result, error)
	clientConnect  func(context.Context, agentclient.Options) (protocolClient, error)
}

type protocolClient interface {
	JobGet(context.Context, agentclient.JobGetParams) (agentclient.JobGetResult, error)
	JobList(context.Context, agentclient.JobListParams) (agentclient.JobListResult, error)
	JobTranscript(context.Context, agentclient.JobTranscriptParams) (agentclient.JobTranscriptResult, error)
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
	}
	a.backends = defaultBackends()
	return a
}

func defaultBackends() []engine.Backend {
	return []engine.Backend{
		codexcli.New(codexcli.Options{Binary: resolvedDefaultBackendBinary("codex")}),
		claudecli.New(claudecli.Options{Binary: resolvedDefaultBackendBinary("claude")}),
		cursorcli.New(cursorcli.Options{Binary: resolvedCursorBinary()}),
	}
}

func resolvedDefaultBackendBinary(name string) string {
	path, err := exec.LookPath(name)
	if err != nil {
		return ""
	}
	return path
}

func resolvedCursorBinary() string {
	for _, name := range []string{"cursor-agent", "agent"} {
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
	}
	return ""
}

func (a *app) run(ctx context.Context, args []string, _ io.Reader, out, errOut io.Writer) int {
	if len(args) == 0 {
		printRootHelp(errOut)
		return 2
	}
	// These are spellings of version, not additional commands.
	command := args[0]
	if command == "--version" || command == "-version" || command == "-V" {
		command = "version"
	}
	switch command {
	case "version":
		return a.runVersion(args[1:], out, errOut)
	case "serve":
		return a.runServe(ctx, args[1:], errOut)
	case "status":
		return a.runStatus(ctx, args[1:], out, errOut)
	case "transcript":
		return a.runTranscript(ctx, args[1:], out, errOut)
	case "result":
		return a.runResult(ctx, args[1:], out, errOut)
	case "cancel":
		return a.runCancel(ctx, args[1:], out, errOut)
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
	fmt.Fprintf(out, "agentbus %s protocol %d\n", a.version, protocol.Version)
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
			return protocolCommandError(errOut, "serve", err)
		}
		return 0
	}
	codexHomeOverride, codexHomeInherit, codexAuthHome := codexHomeSettings()
	serveCtx, stopSignals := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	err := agentbusserve.Serve(serveCtx, agentbusserve.Config{
		StateRoot:         a.stateRoot,
		Backends:          append([]engine.Backend(nil), a.backends...),
		CodexHomeOverride: codexHomeOverride,
		CodexHomeInherit:  codexHomeInherit,
		CodexAuthHome:     codexAuthHome,
	})
	if err != nil {
		return serveCommandError(errOut, err)
	}
	return 0
}

// codexHomeSettings reads the daemon-owned home controls once at startup.
func codexHomeSettings() (override string, inherit bool, authHome string) {
	return strings.TrimSpace(os.Getenv("AGENTBUS_CODEX_HOME")),
		strings.TrimSpace(os.Getenv("AGENTBUS_CODEX_HOME_INHERIT")) == "1",
		strings.TrimSpace(os.Getenv("CODEX_HOME"))
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
	jsonOut := fs.Bool("json", false, "emit the protocol response")
	jobID := fs.String("job", "", "job id")
	workspaceKey := fs.String("workspace-key", "", "filter list by submitter workspace key")
	var tagValues []string
	fs.Func("tag", "filter list by key=value (repeatable)", func(value string) error {
		tagValues = append(tagValues, value)
		return nil
	})
	var stateValues []string
	fs.Func("state", "filter list by public state (repeatable)", func(value string) error {
		stateValues = append(stateValues, value)
		return nil
	})
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(errOut, "status does not accept positional arguments")
	}
	if *jobID != "" && (*workspaceKey != "" || len(tagValues) > 0 || len(stateValues) > 0) {
		return usageError(errOut, "--workspace-key, --tag, and --state apply only without --job")
	}
	var listParams agentclient.JobListParams
	if *jobID == "" {
		var err error
		listParams, err = a.statusListParams(*workspaceKey, tagValues, stateValues)
		if err != nil {
			return commandError(errOut, fmt.Errorf("status list parameters: %w", err))
		}
	}
	client, err := a.connectProtocolClient(ctx)
	if err != nil {
		return protocolCommandError(errOut, "status", err)
	}
	defer client.Close()
	if *jobID == "" {
		list, err := client.JobList(ctx, listParams)
		if err != nil {
			return protocolCommandError(errOut, "status", err)
		}
		if *jsonOut {
			return writeOrError(out, errOut, list)
		}
		for _, summary := range list.Jobs {
			printJobSummary(out,
				summary.JobID,
				summary.State,
				summary.Backend,
				summary.Cleanup,
				summary.CreatedAt,
				summary.FailureClass,
				summary.Contract,
				summary.Tags,
				summary.ItemCount,
				summary.LastItemAt,
				summary.LastActivityAt,
				summary.Liveness,
			)
		}
		return 0
	}

	record, err := client.JobGet(ctx, agentclient.JobGetParams{JobID: *jobID})
	if err != nil {
		return protocolCommandError(errOut, "status", err)
	}
	if *jsonOut {
		if code := writeOrError(out, errOut, record); code != 0 {
			return code
		}
	} else {
		printJobRecordStatus(out, record)
	}
	return cliExitCodeForRecord(record)
}

func (a *app) statusListParams(workspaceKey string, tagValues, stateValues []string) (agentclient.JobListParams, error) {
	tags, err := parseTagFilters(tagValues)
	if err != nil {
		return agentclient.JobListParams{}, err
	}
	states, err := parseStateFilters(stateValues)
	if err != nil {
		return agentclient.JobListParams{}, err
	}
	return agentclient.JobListParams{WorkspaceKey: workspaceKey, Tags: tags, States: states}, nil
}

func (a *app) runTranscript(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("transcript", errOut)
	jsonOut := fs.Bool("json", false, "emit the job.transcript response")
	jobID := fs.String("job", "", "job id")
	var kinds []string
	fs.Func("kind", "include transcript kind (repeatable)", func(value string) error {
		kinds = append(kinds, value)
		return nil
	})
	since := fs.String("since", "", "include items after this RFC3339 timestamp")
	sinceOrdinal := fs.Int("since-ordinal", 0, "include items after this ordinal")
	last := fs.Int("last", 0, "include the last N matching items")
	limit := fs.Int("limit", 0, "limit matching items")
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(errOut, "transcript does not accept positional arguments")
	}
	if *jobID == "" {
		return usageError(errOut, "transcript requires --job <id>")
	}

	params := agentclient.JobTranscriptParams{JobID: *jobID, Kinds: append([]string(nil), kinds...)}
	if transcriptFlagSet(fs, "since") {
		parsed, err := time.Parse(time.RFC3339Nano, *since)
		if err != nil {
			return usageError(errOut, "--since requires an RFC3339 timestamp: %v", err)
		}
		params.Since = &parsed
	}
	if transcriptFlagSet(fs, "since-ordinal") {
		params.SinceOrdinal = sinceOrdinal
	}
	if transcriptFlagSet(fs, "last") {
		params.Last = last
	}
	if transcriptFlagSet(fs, "limit") {
		params.Limit = limit
	}

	client, err := a.connectProtocolClient(ctx)
	if err != nil {
		return protocolCommandError(errOut, "transcript", err)
	}
	defer client.Close()
	transcript, err := client.JobTranscript(ctx, params)
	if err != nil {
		return protocolCommandError(errOut, "transcript", err)
	}
	if *jsonOut {
		return writeOrError(out, errOut, transcript)
	}
	printJobTranscript(out, transcript, transcriptIsDigestRequest(params))
	return 0
}

func transcriptFlagSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(current *flag.Flag) {
		if current.Name == name {
			set = true
		}
	})
	return set
}

func transcriptIsDigestRequest(params agentclient.JobTranscriptParams) bool {
	return len(params.Kinds) == 0 && params.Since == nil && params.SinceOrdinal == nil && params.Last == nil && params.Limit == nil
}

func printJobTranscript(out io.Writer, transcript agentclient.JobTranscriptResult, digest bool) {
	fmt.Fprintf(out, "state=%s itemCount=%d", transcript.State, transcript.ItemCount)
	if transcript.Liveness != "" {
		fmt.Fprintf(out, " liveness=%s", transcript.Liveness)
	}
	fmt.Fprintf(out, " gap=%t\n", transcript.Gap)
	if digest {
		fmt.Fprintf(out, "counts message=%d reasoning=%d tool=%d toolResult=%d fileChange=%d warning=%d error=%d\n",
			transcript.Counts["message"],
			transcript.Counts["reasoning"],
			transcript.Counts["tool"],
			transcript.Counts["toolResult"],
			transcript.Counts["fileChange"],
			transcript.Counts["warning"],
			transcript.Counts["error"],
		)
	}
	for _, item := range transcript.Items {
		fmt.Fprintf(out, "%d kind=%s at=%s", item.Ordinal, item.Kind, humanTime(item.At))
		if item.Name != "" {
			fmt.Fprintf(out, " name=%q", item.Name)
		}
		if item.Text != "" {
			fmt.Fprintf(out, " text=%q", item.Text)
		}
		if item.Truncated {
			fmt.Fprint(out, " truncated=true")
		}
		fmt.Fprintln(out)
	}
}

func parseTagFilters(values []string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	tags := make(map[string]string, len(values))
	for _, value := range values {
		key, tagValue, ok := strings.Cut(value, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("--tag requires key=value, got %q", value)
		}
		if _, exists := tags[key]; exists {
			return nil, fmt.Errorf("--tag repeats key %q", key)
		}
		tags[key] = tagValue
	}
	return tags, nil
}

func parseStateFilters(values []string) ([]protocol.PublicState, error) {
	if len(values) == 0 {
		return nil, nil
	}
	states := make([]protocol.PublicState, 0, len(values))
	for _, value := range values {
		state := protocol.PublicState(value)
		if !state.Valid() {
			return nil, fmt.Errorf("--state requires a public state, got %q", value)
		}
		states = append(states, state)
	}
	return states, nil
}

func (a *app) runResult(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("result", errOut)
	jsonOut := fs.Bool("json", false, "emit the v3 job.get response")
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
	record, err := client.JobGet(ctx, agentclient.JobGetParams{JobID: *jobID})
	if err != nil {
		return protocolCommandError(errOut, "result", err)
	}
	code := cliExitCodeForRecord(record)
	if *jsonOut {
		if writeCode := writeOrError(out, errOut, record); writeCode != 0 {
			return writeCode
		}
		if resultArtifactUnavailable(record) {
			return cliExitResultUnavailable
		}
		return code
	}
	if resultCode := printJobRecordResult(out, errOut, record); resultCode != 0 {
		return resultCode
	}
	return code
}

func (a *app) runCancel(ctx context.Context, args []string, out, errOut io.Writer) int {
	fs := newCommandFlagSet("cancel", errOut)
	jsonOut := fs.Bool("json", false, "emit the v3 job.cancel response")
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
	// job.cancel has only jobId and state. Fetch the full record before choosing
	// an exit code so a completed noncompliant job retains exit 3.
	record, err := client.JobGet(ctx, agentclient.JobGetParams{JobID: *jobID})
	if err != nil {
		return protocolCommandError(errOut, "cancel", err)
	}
	if *jsonOut {
		if code := writeOrError(out, errOut, result); code != 0 {
			return code
		}
	} else {
		fmt.Fprintf(out, "jobId=%s state=%s\n", result.JobID, result.State)
	}
	return cliExitCodeForRecord(record)
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

func printJobSummary(out io.Writer, jobID string, state protocol.PublicState, backend string, cleanup protocol.Cleanup, createdAt time.Time, failureClass protocol.FailureClass, contract *protocol.ContractVerdict, tags map[string]string, itemCount *int, lastItemAt, lastActivityAt *time.Time, liveness protocol.Liveness) {
	fmt.Fprintf(out, "jobId=%s state=%s backend=%s cleanup=%s age=%s", jobID, state, backend, cleanup, jobAge(createdAt))
	if failureClass != "" {
		fmt.Fprintf(out, " failure.class=%s", failureClass)
	}
	if contract != nil {
		fmt.Fprintf(out, " contract.evaluated=%t contract.compliant=%t", contract.Evaluated, contract.Compliant)
	}
	if len(tags) > 0 {
		fmt.Fprintf(out, " tags=%s", formatTags(tags))
	}
	if itemCount != nil {
		fmt.Fprintf(out, " itemCount=%d", *itemCount)
	}
	if lastItemAt != nil {
		fmt.Fprintf(out, " lastItemAt=%s", humanTime(*lastItemAt))
	}
	if lastActivityAt != nil {
		fmt.Fprintf(out, " lastActivityAt=%s", humanTime(*lastActivityAt))
	}
	if liveness != "" {
		fmt.Fprintf(out, " liveness=%s", liveness)
	}
	fmt.Fprintln(out)
}

func printJobRecordStatus(out io.Writer, record agentclient.JobGetResult) {
	printJobSummary(out, record.JobID, record.State, record.Backend, record.Cleanup, record.CreatedAt, "", nil, nil, nil, nil, nil, "")
	fmt.Fprintf(out, "createdAt=%s", humanTime(record.CreatedAt))
	if record.StartedAt != nil {
		fmt.Fprintf(out, " startedAt=%s", humanTime(*record.StartedAt))
	}
	if record.FinishedAt != nil {
		fmt.Fprintf(out, " finishedAt=%s", humanTime(*record.FinishedAt))
	}
	if record.Timeout != nil {
		fmt.Fprintf(out, " timeout.effective=%d timeout.source=%s", record.Timeout.Effective, record.Timeout.Source)
	}
	fmt.Fprintf(out, " model=%s tags=%s", record.ModelReported, formatTags(record.Tags))
	if record.Result != nil {
		fmt.Fprintf(out, " result.bytes=%d result.sha256=%s", record.Result.Bytes, shortSHA256(record.Result.SHA256))
	}
	if record.Failure != nil {
		fmt.Fprintf(out, " failure.class=%s failure.reason=%s", record.Failure.Class, record.Failure.Reason)
	}
	if record.Contract != nil {
		fmt.Fprintf(out, " contract.evaluated=%t contract.compliant=%t", record.Contract.Evaluated, record.Contract.Compliant)
	}
	if record.LogPaths != nil {
		fmt.Fprintf(out, " logPaths.stdout=%s logPaths.stderr=%s", record.LogPaths.Stdout, record.LogPaths.Stderr)
		printLogPathTruncationStatus(out, "stdout", record.LogPaths.Stdout, record.LogPaths.StdoutTruncated)
		printLogPathTruncationStatus(out, "stderr", record.LogPaths.Stderr, record.LogPaths.StderrTruncated)
	}
	fmt.Fprintln(out)
}

func printLogPathTruncationStatus(out io.Writer, stream, path string, truncated *bool) {
	if path == "" || (truncated != nil && !*truncated) {
		return
	}
	if truncated == nil {
		fmt.Fprintf(out, " logPaths.%s.truncation=unknown", stream)
		return
	}
	fmt.Fprintf(out, " logPaths.%s.truncation=truncated", stream)
}

func printJobRecordResult(out, errOut io.Writer, record agentclient.JobGetResult) int {
	switch record.State {
	case protocol.PublicStateFailed:
		class, reason := "", ""
		if record.Failure != nil {
			class, reason = string(record.Failure.Class), record.Failure.Reason
		}
		fmt.Fprintf(errOut, "jobId=%s state=%s failure.class=%s failure.reason=%s\n", record.JobID, record.State, class, reason)
		return 0
	case protocol.PublicStateQueued, protocol.PublicStateRunning:
		fmt.Fprintf(errOut, "jobId=%s state=%s\n", record.JobID, record.State)
		return 0
	case protocol.PublicStateCanceled, protocol.PublicStateUnknown:
		fmt.Fprintf(errOut, "jobId=%s state=%s: no authoritative result\n", record.JobID, record.State)
		return 0
	case protocol.PublicStateCompleted:
		if record.Result == nil {
			fmt.Fprintf(errOut, "jobId=%s state=%s: no authoritative result\n", record.JobID, record.State)
			return cliExitResultUnavailable
		}
		if record.Result.Text != "" {
			fmt.Fprint(out, record.Result.Text)
			if !strings.HasSuffix(record.Result.Text, "\n") {
				fmt.Fprintln(out)
			}
			return 0
		}
		if record.Result.ResultPath == "" {
			fmt.Fprintf(errOut, "jobId=%s state=%s: no authoritative result\n", record.JobID, record.State)
			return 0
		}
		artifact, err := openVerifiedResultArtifact(record.Result)
		if err != nil {
			fmt.Fprintf(errOut, "jobId=%s state=%s: %v\n", record.JobID, record.State, err)
			return cliExitResultUnavailable
		}
		defer artifact.Close()
		if _, err := io.Copy(out, artifact); err != nil {
			fmt.Fprintf(errOut, "jobId=%s state=%s: write result artifact %q: %v\n", record.JobID, record.State, record.Result.ResultPath, err)
			return 1
		}
		return 0
	default:
		fmt.Fprintf(errOut, "jobId=%s state=%s: no authoritative result\n", record.JobID, record.State)
		return 0
	}
}

func openVerifiedResultArtifact(result *protocol.ResultInfoWire) (*os.File, error) {
	artifact, err := os.Open(result.ResultPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("result artifact %q is missing: %w", result.ResultPath, err)
		}
		return nil, fmt.Errorf("result artifact %q is unreadable: %w", result.ResultPath, err)
	}

	digest := sha256.New()
	bytes, err := io.Copy(digest, artifact)
	if err != nil {
		_ = artifact.Close()
		return nil, fmt.Errorf("result artifact %q is unreadable during verification: %w", result.ResultPath, err)
	}
	if bytes != result.Bytes {
		_ = artifact.Close()
		return nil, fmt.Errorf("result artifact %q byte-count check failed: got %d, want %d", result.ResultPath, bytes, result.Bytes)
	}
	actualSHA256 := hex.EncodeToString(digest.Sum(nil))
	if actualSHA256 != result.SHA256 {
		_ = artifact.Close()
		return nil, fmt.Errorf("result artifact %q SHA-256 check failed: got %s, want %s", result.ResultPath, actualSHA256, result.SHA256)
	}
	if _, err := artifact.Seek(0, io.SeekStart); err != nil {
		_ = artifact.Close()
		return nil, fmt.Errorf("result artifact %q is unreadable after verification: %w", result.ResultPath, err)
	}
	return artifact, nil
}

// resultArtifactUnavailable checks the artifact only when the record needs it
// to deliver a completed result. JSON still emits the authoritative record;
// its exit status must nevertheless report an unusable large result.
func resultArtifactUnavailable(record agentclient.JobGetResult) bool {
	if record.State != protocol.PublicStateCompleted || record.Result == nil || record.Result.Text != "" || record.Result.ResultPath == "" {
		return false
	}
	artifact, err := openVerifiedResultArtifact(record.Result)
	if err != nil {
		return true
	}
	_ = artifact.Close()
	return false
}

func jobAge(createdAt time.Time) string {
	if createdAt.IsZero() {
		return "unknown"
	}
	age := time.Since(createdAt)
	if age < 0 {
		age = 0
	}
	return age.Round(time.Second).String()
}

func humanTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func formatTags(tags map[string]string) string {
	if len(tags) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+tags[key])
	}
	return strings.Join(parts, ",")
}

func shortSHA256(value string) string {
	digest := value
	if len(digest) > shortSHA256HexLength {
		digest = digest[:shortSHA256HexLength]
	}
	if digest == "" {
		return ""
	}
	return "sha256:" + digest
}

func cliExitCodeForRecord(record agentclient.JobGetResult) int {
	switch record.State {
	case protocol.PublicStateCompleted:
		if record.Contract != nil && record.Contract.Evaluated && !record.Contract.Compliant {
			return 3
		}
		return 0
	case protocol.PublicStateQueued, protocol.PublicStateRunning:
		return 2
	case protocol.PublicStateFailed:
		if record.Failure != nil {
			switch record.Failure.Class {
			case protocol.FailureClassTimeout:
				return 5
			case protocol.FailureClassInterrupted:
				return 6
			}
		}
		return 4
	case protocol.PublicStateCanceled:
		return 7
	case protocol.PublicStateUnknown:
		return 14
	default:
		return 1
	}
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
	if err != nil {
		fmt.Fprintf(errOut, "agentbus: %v\n", err)
	}
	return 1
}

func serveCommandError(errOut io.Writer, err error) int {
	if errors.Is(err, agentbusserve.ErrShutdownDeadlineExceeded) {
		fmt.Fprintf(errOut, "agentbus: %v\n", err)
		return cliExitShutdownForced
	}
	if err != nil {
		fmt.Fprintf(errOut, "agentbus: %v\n", err)
	}
	return cliExitDaemonStartupFailure
}

func protocolCommandError(errOut io.Writer, operation string, err error) int {
	var startupErr *daemonlaunch.StartupError
	if errors.As(err, &startupErr) {
		fmt.Fprintf(errOut, "agentbus: %s: %v\n", operation, startupErr)
		return cliExitDaemonStartupFailure
	}
	var rpcErr *protocol.RPCError
	if errors.As(err, &rpcErr) && rpcErr.Object.Data.Code == protocol.ErrorUnknownJob {
		jobID := rpcErr.Object.Data.JobID
		if jobID == "" {
			jobID = "unknown"
		}
		fmt.Fprintf(errOut, "agentbus: %s: code=%s jobId=%s: %s\n", operation, rpcErr.Object.Data.Code, jobID, rpcErr.Object.Message)
		return cliExitUnknownJob
	}
	if err != nil {
		fmt.Fprintf(errOut, "agentbus: %s: %v\n", operation, err)
	}
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
  agentbus version [--json] (aliases: --version, -version, -V)
  agentbus serve [--foreground]
  agentbus status [--job <id>] [--workspace-key <key>] [--tag <key=value>] [--state <state>] [--json]
  agentbus transcript --job <id> [--kind <kind>]... [--last <n>] [--since <rfc3339>] [--since-ordinal <n>] [--limit <n>] [--json]
  agentbus result --job <id> [--json]
  agentbus cancel --job <id> [--json]

status and result use the same job.get record for a selected job. Their JSON
output is identical; status is the operator projection and result is pipeable.

Exit codes for a selected job:
  completed=0, queued/running=2, completed-noncompliant=3, failed=4,
  timeout=5, interrupted=6, canceled=7, unknown-job=10,
  daemon-startup-failure=11, shutdown-deadline=13, unknown=14,
  result-artifact-unavailable=15: completed, but the authoritative result artifact is missing, unreadable, or does not match its recorded digest
`)
}

type versionOutput struct {
	Schema          int    `json:"schema"`
	Version         string `json:"version"`
	ProtocolVersion int    `json:"protocolVersion"`
}
