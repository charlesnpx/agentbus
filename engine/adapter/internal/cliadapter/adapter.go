package cliadapter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

const DriftError = "backend version changed since setup; re-run agentbus setup"

type Backend struct {
	NameValue        string
	Binary           string
	MinimumVersion   string
	CachePath        string
	StreamSchema     string
	AllowedModels    map[string]struct{}
	AllowedEfforts   map[string]struct{}
	BuildArgs        func(resumeID string, opts engine.SessionOpts, input engine.TurnInput) ([]string, error)
	Parse            func(map[string]any) ([]engine.Event, string, error)
	VersionTransform func(string) string
	Discover         func(context.Context, string) (*engine.ModelDiscovery, error)
}

func (b *Backend) Name() string { return b.NameValue }

func (b *Backend) Preflight(ctx context.Context) (engine.Health, error) {
	binary, err := exec.LookPath(b.binary())
	if err != nil {
		return engine.Health{}, fmt.Errorf("backend_unavailable: %s binary not found: %w", b.NameValue, err)
	}
	version, err := commandOutput(ctx, binary, "--version")
	if err != nil {
		return engine.Health{}, fmt.Errorf("backend_unavailable: %s version check failed: %w", b.NameValue, err)
	}
	version = b.normalizeVersion(version)
	if compareVersion(version, b.MinimumVersion) < 0 {
		return engine.Health{}, fmt.Errorf("backend_unavailable: %s version %s is below minimum known-good %s", b.NameValue, version, b.MinimumVersion)
	}
	probe, err := b.cachedProbe()
	if err != nil {
		return engine.Health{}, err
	}
	if probe.Version != version || probe.BinaryPath != binary {
		return engine.Health{}, errors.New(DriftError)
	}
	if probe.StreamSchema == "" || probe.StreamSchema != b.StreamSchema {
		return engine.Health{}, fmt.Errorf("backend_unavailable: setup cache for %s lacks stream schema %q", b.NameValue, b.StreamSchema)
	}
	return engine.Health{
		Backend:      b.NameValue,
		BinaryPath:   binary,
		Version:      version,
		StreamSchema: probe.StreamSchema,
		Minimum:      b.MinimumVersion,
		Warning:      b.discoveryWarning(probe, version),
	}, nil
}

func (b *Backend) DiscoverModels(ctx context.Context) (*engine.ModelDiscovery, error) {
	binary, err := exec.LookPath(b.binary())
	if err != nil || b.Discover == nil {
		return nil, err
	}
	return b.Discover(ctx, binary)
}

func (b *Backend) BackendMetadata(context.Context) engine.BackendMetadata {
	meta := engine.BackendMetadata{Name: b.NameValue}
	probe, err := b.cachedProbe()
	if err == nil && probe.Version != "" {
		meta.Models = append([]string(nil), probe.DiscoveredModels...)
		meta.Efforts = append([]string(nil), probe.DiscoveredEfforts...)
	}
	return meta
}

// SetupProbe runs the live setup-time stream probe and returns the cache entry
// later consumed by Preflight.
func (b *Backend) SetupProbe(ctx context.Context) (engine.BackendSetupProbe, error) {
	binary, err := exec.LookPath(b.binary())
	if err != nil {
		return engine.BackendSetupProbe{}, fmt.Errorf("backend_unavailable: %s binary not found: %w", b.NameValue, err)
	}
	version, err := commandOutput(ctx, binary, "--version")
	if err != nil {
		return engine.BackendSetupProbe{}, fmt.Errorf("backend_unavailable: %s version check failed: %w", b.NameValue, err)
	}
	version = b.normalizeVersion(version)
	if compareVersion(version, b.MinimumVersion) < 0 {
		return engine.BackendSetupProbe{}, fmt.Errorf("backend_unavailable: %s version %s is below minimum known-good %s", b.NameValue, version, b.MinimumVersion)
	}
	cwd, err := os.Getwd()
	if err != nil {
		return engine.BackendSetupProbe{}, err
	}
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	session := &Session{backend: b, opts: engine.SessionOpts{
		CWD:     cwd,
		Write:   false,
		Timeout: 2 * time.Minute,
	}, suppressValidationWarning: true}
	events, err := session.Turn(probeCtx, engine.TurnInput{
		Prompt:  "Reply with exactly: OK\n",
		Write:   false,
		Timeout: 2 * time.Minute,
	})
	if err != nil {
		return engine.BackendSetupProbe{}, err
	}
	var sawEvent bool
	var warnings []string
	for event := range events {
		if event.Type == engine.EventWarning || event.Type == engine.EventTerminalError {
			warnings = append(warnings, event.Text)
			continue
		}
		sawEvent = true
	}
	if probeCtx.Err() != nil {
		return engine.BackendSetupProbe{}, fmt.Errorf("backend_unavailable: %s setup stream probe failed: %w", b.NameValue, probeCtx.Err())
	}
	if len(warnings) > 0 {
		return engine.BackendSetupProbe{}, fmt.Errorf("backend_unavailable: %s setup stream probe warning: %s", b.NameValue, strings.Join(warnings, "; "))
	}
	if !sawEvent {
		return engine.BackendSetupProbe{}, fmt.Errorf("backend_unavailable: %s setup stream probe produced no JSON events", b.NameValue)
	}
	probe := engine.BackendSetupProbe{
		Backend:      b.NameValue,
		BinaryPath:   binary,
		Version:      version,
		StreamSchema: b.StreamSchema,
		ConfigMode: engine.ModeInfo{
			Write:    "user",
			ReadOnly: "hermetic",
		},
		SandboxModes:     []string{"workspace-write", "read-only"},
		JSONEventsProbed: true,
	}
	if discovered, discoverErr := b.DiscoverModels(ctx); discoverErr == nil && discovered != nil {
		probe.DiscoveredModels = discovered.Models
		probe.DiscoveredEfforts = discovered.Efforts
		probe.DiscoverySource = discovered.Source
		probe.DiscoveryFetchedAt = discovered.FetchedAt
		probe.DiscoveryClientVersion = discovered.ClientVersion
		probe.DiscoveryWarnings = append(probe.DiscoveryWarnings, discovered.Warnings...)
		if discovered.ClientVersion != "" && b.normalizeVersion(discovered.ClientVersion) != version {
			probe.DiscoveryWarnings = append(probe.DiscoveryWarnings, fmt.Sprintf("%s model discovery cache client_version %q does not match probed version %q", b.NameValue, discovered.ClientVersion, version))
		}
	} else if discoverErr != nil {
		probe.DiscoveryWarnings = append(probe.DiscoveryWarnings, fmt.Sprintf("%s model discovery failed: %v", b.NameValue, discoverErr))
	}
	return probe, nil
}

func (b *Backend) Start(ctx context.Context, opts engine.SessionOpts) (engine.Session, error) {
	warning, err := b.validateOptions(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Session{backend: b, opts: opts, validationWarning: warning}, nil
}

func (b *Backend) Resume(ctx context.Context, id string, opts engine.SessionOpts) (engine.Session, error) {
	if strings.TrimSpace(id) == "" {
		return nil, errors.New("resume session id is required")
	}
	warning, err := b.validateOptions(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Session{backend: b, id: id, opts: opts, validationWarning: warning}, nil
}

func (b *Backend) binary() string {
	if b.Binary != "" {
		return b.Binary
	}
	return b.NameValue
}

func (b *Backend) normalizeVersion(s string) string {
	s = strings.TrimSpace(s)
	if b.VersionTransform != nil {
		s = b.VersionTransform(s)
	}
	fields := strings.Fields(s)
	for _, f := range fields {
		if isVersionToken(f) {
			return strings.TrimPrefix(f, "v")
		}
	}
	return strings.TrimPrefix(s, "v")
}

func (b *Backend) validateOptions(ctx context.Context, opts engine.SessionOpts) (string, error) {
	models, efforts, modelsDiscovered, effortsDiscovered, warning := b.validationSets(ctx)
	if opts.Model != "" {
		if _, ok := models[opts.Model]; !ok {
			if modelsDiscovered {
				warning = appendWarning(warning, fmt.Sprintf("model %q is not in the discovered %s catalog; passing through to backend", opts.Model, b.NameValue))
			} else if len(models) > 0 {
				return warning, fmt.Errorf("unsupported model %q for %s", opts.Model, b.NameValue)
			}
		}
	}
	if opts.Effort != "" {
		if _, ok := efforts[opts.Effort]; !ok {
			if effortsDiscovered {
				warning = appendWarning(warning, fmt.Sprintf("effort %q is not in the discovered %s catalog; passing through to backend", opts.Effort, b.NameValue))
			} else if len(efforts) > 0 {
				return warning, fmt.Errorf("unsupported effort %q for %s", opts.Effort, b.NameValue)
			}
		}
	}
	return warning, nil
}

func (b *Backend) validationSets(ctx context.Context) (map[string]struct{}, map[string]struct{}, bool, bool, string) {
	cache, cacheErr := b.readCache()
	probe, err := b.cachedProbe()
	if cacheErr == nil && cache.Version == engine.SetupProbeCacheVersion && err == nil {
		binary, binaryErr := exec.LookPath(b.binary())
		version := ""
		var versionErr error
		if binaryErr == nil {
			version, versionErr = commandOutput(ctx, binary, "--version")
		}
		if binaryErr != nil || versionErr != nil || probe.BinaryPath != binary || probe.Version != b.normalizeVersion(version) {
			return b.AllowedModels, b.AllowedEfforts, false, false, "model discovery cache is stale; using static known-good validation lists"
		}
		models := b.AllowedModels
		efforts := b.AllowedEfforts
		modelsDiscovered := probe.DiscoverySource != "" && len(probe.DiscoveredModels) > 0
		effortsDiscovered := probe.DiscoverySource != "" && len(probe.DiscoveredEfforts) > 0
		if modelsDiscovered {
			models = StringSet(probe.DiscoveredModels...)
		}
		if effortsDiscovered {
			efforts = StringSet(probe.DiscoveredEfforts...)
		}
		return models, efforts, modelsDiscovered, effortsDiscovered, ""
	}
	if cacheErr == nil {
		return b.AllowedModels, b.AllowedEfforts, false, false, "model discovery cache is stale; using static known-good validation lists"
	}
	return b.AllowedModels, b.AllowedEfforts, false, false, ""
}

func appendWarning(existing, addition string) string {
	if existing == "" {
		return addition
	}
	return existing + "; " + addition
}

func (b *Backend) discoveryWarning(probe engine.BackendSetupProbe, version string) string {
	if cache, err := b.readCache(); err == nil && cache.Version != engine.SetupProbeCacheVersion {
		return "model discovery cache is stale; using static known-good validation lists"
	}
	if len(probe.DiscoveryWarnings) > 0 {
		return strings.Join(probe.DiscoveryWarnings, "; ")
	}
	if probe.DiscoverySource == "" && len(probe.DiscoveredModels) == 0 && len(probe.DiscoveredEfforts) == 0 {
		return "model discovery unavailable; using static known-good validation lists"
	}
	if probe.Version != version {
		return "model discovery cache is stale; using static known-good validation lists"
	}
	return ""
}

func (b *Backend) readCache() (engine.SetupProbeCache, error) {
	path := b.CachePath
	if path == "" {
		var err error
		path, err = engine.SetupProbeCachePath("")
		if err != nil {
			return engine.SetupProbeCache{}, err
		}
	}
	return engine.ReadSetupProbeCache(path)
}

func (b *Backend) cachedProbe() (engine.BackendSetupProbe, error) {
	cache, err := b.readCache()
	if err != nil {
		return engine.BackendSetupProbe{}, fmt.Errorf("backend_unavailable: setup cache missing for %s; re-run agentbus setup: %w", b.NameValue, err)
	}
	if cache.Version != engine.SetupProbeCacheVersion {
		return engine.BackendSetupProbe{}, fmt.Errorf("backend_unavailable: setup cache version %d is stale; re-run agentbus setup", cache.Version)
	}
	for _, p := range cache.Backends {
		if p.Backend == b.NameValue {
			return p, nil
		}
	}
	return engine.BackendSetupProbe{}, fmt.Errorf("backend_unavailable: setup cache missing backend %s; re-run agentbus setup", b.NameValue)
}

type Session struct {
	backend                   *Backend
	id                        string
	opts                      engine.SessionOpts
	validationWarning         string
	suppressValidationWarning bool
	mu                        sync.Mutex
	active                    *exec.Cmd
	lastAgentMessage          string
}

func (s *Session) ID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

func (s *Session) Turn(ctx context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
	warningText, err := s.backend.validateOptions(ctx, s.opts)
	if err != nil {
		return nil, err
	}
	if warningText == "" {
		warningText = s.validationWarning
	}
	if s.suppressValidationWarning {
		warningText = ""
	}
	s.mu.Lock()
	if s.active != nil {
		s.mu.Unlock()
		return nil, errors.New("session_busy")
	}
	s.lastAgentMessage = ""
	timeout := input.Timeout
	if timeout == 0 {
		timeout = s.opts.Timeout
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer func() {
			if ctx.Err() != nil {
				cancel()
			}
		}()
	}
	resumeID := s.id
	args, err := s.backend.BuildArgs(resumeID, s.opts, input)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	cmd := exec.CommandContext(ctx, s.backend.binary(), args...)
	cmd.Cancel = func() error { return terminateProcessGroup(cmd, engine.DefaultCancelGrace) }
	cmd.WaitDelay = 200 * time.Millisecond
	if s.opts.CWD != "" {
		cmd.Dir = s.opts.CWD
	}
	setProcessGroup(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	var stderr bytes.Buffer
	var stderrLog *engine.CappedLogWriter
	stderrWriter := io.Writer(&stderr)
	if input.LogPaths.Stderr != "" {
		stderrLog, err = engine.NewCappedLogWriter(input.LogPaths.Stderr, 0)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		stderrWriter = io.MultiWriter(&stderr, stderrLog)
	}
	cmd.Stderr = stderrWriter
	var stdoutLog *engine.CappedLogWriter
	if input.LogPaths.Stdout != "" {
		stdoutLog, err = engine.NewCappedLogWriter(input.LogPaths.Stdout, 0)
		if err != nil {
			if stderrLog != nil {
				_ = stderrLog.Close()
			}
			s.mu.Unlock()
			return nil, err
		}
	}
	if err := cmd.Start(); err != nil {
		if stdoutLog != nil {
			_ = stdoutLog.Close()
		}
		if stderrLog != nil {
			_ = stderrLog.Close()
		}
		s.mu.Unlock()
		return nil, err
	}
	if input.OnProcessStart != nil {
		input.OnProcessStart(processRefForCmd(cmd), cmd.Process.Pid)
	}
	s.active = cmd
	s.mu.Unlock()

	events := make(chan engine.Event, 16)
	go func() {
		defer close(events)
		if warningText != "" {
			events <- warning(warningText)
		}
		defer func() {
			if stdoutLog != nil {
				_ = stdoutLog.Close()
			}
			if stderrLog != nil {
				_ = stderrLog.Close()
			}
		}()
		go func() {
			_, _ = io.WriteString(stdin, input.Prompt)
			_ = stdin.Close()
		}()
		var stdoutReader io.Reader = stdout
		if stdoutLog != nil {
			stdoutReader = io.TeeReader(stdout, stdoutLog)
		}
		parseErr := s.scan(stdoutReader, events)
		waitErr := cmd.Wait()
		s.mu.Lock()
		if s.active == cmd {
			s.active = nil
		}
		s.mu.Unlock()
		if ctx.Err() == context.DeadlineExceeded {
			events <- warning("backend turn timed out")
			return
		}
		if parseErr != nil {
			events <- terminalError(parseErr.Error())
		}
		if waitErr != nil {
			msg := strings.TrimSpace(stderr.String())
			if msg == "" {
				msg = waitErr.Error()
			}
			events <- terminalError(msg)
		}
	}()
	return events, nil
}

func (s *Session) Interrupt(ctx context.Context) error {
	s.mu.Lock()
	cmd := s.active
	s.mu.Unlock()
	if cmd == nil {
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- terminateProcessGroup(cmd, engine.DefaultCancelGrace) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (s *Session) scan(r io.Reader, out chan<- engine.Event) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal(line, &obj); err != nil {
			return fmt.Errorf("malformed backend stream: %w", err)
		}
		events, id, err := s.backend.Parse(obj)
		if err != nil {
			return err
		}
		if id != "" {
			s.mu.Lock()
			if s.id == "" {
				s.id = id
			}
			s.mu.Unlock()
		}
		for _, ev := range events {
			if ev.Type == engine.EventAgentText && ev.Text != "" {
				s.mu.Lock()
				s.lastAgentMessage = ev.Text
				s.mu.Unlock()
			}
			if ev.Type == engine.EventResultMessage && ev.Text == "" {
				s.mu.Lock()
				ev.Text = s.lastAgentMessage
				s.mu.Unlock()
			}
			out <- capEvent(ev)
		}
	}
	return scanner.Err()
}

func processRefForCmd(cmd *exec.Cmd) engine.ProcessRef {
	ref := engine.ProcessRef{}
	if cmd == nil || cmd.Process == nil {
		return ref
	}
	ref.PID = cmd.Process.Pid
	if runtime.GOOS != "windows" {
		if pgid, err := syscall.Getpgid(ref.PID); err == nil {
			ref.PGID = pgid
		}
	}
	if info, alive, err := (engine.NativeProcessTable{}).Lookup(ref.PID); err == nil && alive {
		ref.StartTime = info.StartTime
	}
	return ref
}

func capEvent(ev engine.Event) engine.Event {
	ev.RawText = ev.Text
	text := engine.TruncateEventText([]byte(ev.Text), engine.DefaultEventTextCap)
	ev.Text = text.Text
	ev.Truncated = ev.Truncated || text.Truncated
	ev.Metadata = engine.SanitizeEventMetadata(ev.Metadata)
	return ev
}

func warning(text string) engine.Event {
	return engine.Event{Type: engine.EventWarning, Text: text}
}

func terminalError(text string) engine.Event {
	return engine.Event{Type: engine.EventTerminalError, Text: text}
}

func setProcessGroup(cmd *exec.Cmd) {
	if runtime.GOOS == "windows" {
		return
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

var terminateProcessGroup = terminateProcessGroupImpl

func terminateProcessGroupImpl(cmd *exec.Cmd, grace time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pid := cmd.Process.Pid
	if runtime.GOOS == "windows" {
		_ = cmd.Process.Kill()
		return nil
	}
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		pgid = pid
	}
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	waitForProcessGroupExit(pgid, grace)
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	return nil
}

func waitForProcessGroupExit(pgid int, grace time.Duration) {
	if grace <= 0 {
		return
	}
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(-pgid, 0); err != nil {
			if err == syscall.ESRCH {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func commandOutput(ctx context.Context, binary string, arg ...string) (string, error) {
	cmd := exec.CommandContext(ctx, binary, arg...)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func compareVersion(a, b string) int {
	ap := versionParts(a)
	bp := versionParts(b)
	for i := 0; i < len(ap) || i < len(bp); i++ {
		av, bv := 0, 0
		if i < len(ap) {
			av = ap[i]
		}
		if i < len(bp) {
			bv = bp[i]
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
	}
	return 0
}

func versionParts(s string) []int {
	var out []int
	for _, p := range strings.Split(strings.TrimPrefix(s, "v"), ".") {
		n, _ := strconv.Atoi(leadingDigits(p))
		out = append(out, n)
	}
	return out
}

func leadingDigits(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r < '0' || r > '9' {
			break
		}
		b.WriteRune(r)
	}
	if b.Len() == 0 {
		return "0"
	}
	return b.String()
}

func isVersionToken(s string) bool {
	s = strings.TrimPrefix(s, "v")
	parts := strings.Split(s, ".")
	if len(parts) < 2 {
		return false
	}
	for _, p := range parts {
		if leadingDigits(p) == "0" && !strings.HasPrefix(p, "0") {
			return false
		}
	}
	return true
}

func StringSet(values ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, v := range values {
		out[v] = struct{}{}
	}
	return out
}
