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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/duplex"
	"github.com/charlesnpx/agentbus/engine/command"
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
	Driver           duplex.Driver
	VersionTransform func(string) string
	Discover         func(context.Context, command.ProbeRunner, string) (*engine.ModelDiscovery, error)
	SetupQualify     func(context.Context, command.Runner, engine.SessionOpts) (engine.ModelDiscovery, error)
	probed           *ProbedBackendDescriptor
}

type StaticBackendDescriptor struct {
	NameValue              string
	Binary                 string
	MinimumVersion         string
	StreamSchema           string
	AllowedModels          map[string]struct{}
	AllowedEfforts         map[string]struct{}
	DiscoveredModels       []string
	DiscoveredEfforts      []string
	DiscoverySource        string
	DiscoveryFetchedAt     string
	DiscoveryClientVersion string
	DiscoveryWarning       string
	VersionTransform       func(string) string
	Discover               func(context.Context, command.ProbeRunner, string) (*engine.ModelDiscovery, error)
}

type ProbedBackendDescriptor struct {
	StaticBackendDescriptor
	BinaryPath string
	Version    string
}

func (b *Backend) Name() string { return b.NameValue }

func (b *Backend) AdmissionParkable() bool { return true }

func (b *Backend) AdmissionControlledRunner() bool { return true }

func (b *Backend) Preflight(ctx context.Context) (engine.Health, error) {
	probed, err := ProbeBackend(ctx, command.DirectProbeRunner{}, b.staticDescriptor())
	if err != nil {
		return engine.Health{}, err
	}
	probe, err := b.cachedProbe()
	if err != nil {
		return engine.Health{}, err
	}
	if probe.Version != probed.Version || probe.BinaryPath != probed.BinaryPath {
		return engine.Health{}, errors.New(DriftError)
	}
	if probe.StreamSchema == "" || probe.StreamSchema != b.StreamSchema {
		return engine.Health{}, fmt.Errorf("backend_unavailable: setup cache for %s lacks stream schema %q", b.NameValue, b.StreamSchema)
	}
	return engine.Health{
		Backend:      b.NameValue,
		BinaryPath:   probed.BinaryPath,
		Version:      probed.Version,
		StreamSchema: probe.StreamSchema,
		Minimum:      b.MinimumVersion,
		Warning:      b.discoveryWarning(probe, probed.Version),
	}, nil
}

func (b *Backend) DiscoverModels(ctx context.Context, runner command.ProbeRunner) (*engine.ModelDiscovery, error) {
	if runner == nil {
		return nil, errors.New("probe runner is required")
	}
	if b.Discover == nil {
		return nil, nil
	}
	binary := b.binary()
	if b.probed != nil && b.probed.BinaryPath != "" {
		binary = b.probed.BinaryPath
	} else {
		resolved, err := runner.LookPath(binary)
		if err != nil {
			return nil, err
		}
		binary = resolved
	}
	return b.Discover(ctx, runner, binary)
}

func (b *Backend) BackendMetadata(context.Context) engine.BackendMetadata {
	meta := engine.BackendMetadata{Name: b.NameValue}
	if b.probed != nil {
		meta.Models = append([]string(nil), b.probed.DiscoveredModels...)
		meta.Efforts = append([]string(nil), b.probed.DiscoveredEfforts...)
		return meta
	}
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
	probed, err := ProbeBackend(ctx, command.DirectProbeRunner{}, b.staticDescriptor())
	if err != nil {
		return engine.BackendSetupProbe{}, err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return engine.BackendSetupProbe{}, err
	}
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	probeOpts := engine.SessionOpts{
		CWD:     cwd,
		Write:   false,
		Timeout: 2 * time.Minute,
	}
	if b.SetupQualify != nil {
		discovery, err := b.SetupQualify(probeCtx, command.DirectCommandRunner{}, probeOpts)
		if err != nil {
			return engine.BackendSetupProbe{}, fmt.Errorf("backend_unavailable: %s setup qualification failed: %w", b.NameValue, err)
		}
		probe := b.setupProbe(probed)
		probe.DiscoveredModels = append([]string(nil), discovery.Models...)
		probe.DiscoveredEfforts = append([]string(nil), discovery.Efforts...)
		probe.DiscoverySource = discovery.Source
		probe.DiscoveryFetchedAt = discovery.FetchedAt
		probe.DiscoveryClientVersion = probed.Version
		probe.DiscoveryWarnings = append([]string(nil), discovery.Warnings...)
		return probe, nil
	}
	session, err := b.newSession("", probeOpts, "", true)
	if err != nil {
		return engine.BackendSetupProbe{}, err
	}
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
	probe := b.setupProbe(probed)
	probe.DiscoveredModels = probed.DiscoveredModels
	probe.DiscoveredEfforts = probed.DiscoveredEfforts
	probe.DiscoverySource = probed.DiscoverySource
	probe.DiscoveryFetchedAt = probed.DiscoveryFetchedAt
	probe.DiscoveryClientVersion = probed.DiscoveryClientVersion
	probe.DiscoveryWarnings = discoveryWarnings(probed.DiscoveryWarning)
	return probe, nil
}

func (b *Backend) setupProbe(probed ProbedBackendDescriptor) engine.BackendSetupProbe {
	return engine.BackendSetupProbe{
		Backend:      b.NameValue,
		BinaryPath:   probed.BinaryPath,
		Version:      probed.Version,
		StreamSchema: b.StreamSchema,
		ConfigMode: engine.ModeInfo{
			Write:    "user",
			ReadOnly: "hermetic",
		},
		SandboxModes:     []string{"workspace-write", "read-only"},
		JSONEventsProbed: true,
	}
}

func (b *Backend) Start(ctx context.Context, opts engine.SessionOpts) (engine.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	warning, err := b.validateOptions(opts)
	if err != nil {
		return nil, err
	}
	return b.newSession("", opts, warning, false)
}

func (b *Backend) Resume(ctx context.Context, id string, opts engine.SessionOpts) (engine.Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(id) == "" {
		return nil, errors.New("resume session id is required")
	}
	warning, err := b.validateOptions(opts)
	if err != nil {
		return nil, err
	}
	return b.newSession(id, opts, warning, false)
}

func (b *Backend) newSession(id string, opts engine.SessionOpts, validationWarning string, suppressValidationWarning bool) (*Session, error) {
	session := &Session{
		backend:                   b,
		id:                        id,
		opts:                      opts,
		validationWarning:         validationWarning,
		suppressValidationWarning: suppressValidationWarning,
	}
	if b.Driver == nil {
		return session, nil
	}
	duplexSession, err := duplex.NewSession(duplex.SessionConfig{
		Driver:   b.Driver,
		Options:  opts,
		ResumeID: id,
	})
	if err != nil {
		return nil, err
	}
	session.duplexSession = duplexSession
	return session, nil
}

func (b *Backend) binary() string {
	if b.Binary != "" {
		return b.Binary
	}
	return b.NameValue
}

func (b *Backend) staticDescriptor() StaticBackendDescriptor {
	return StaticBackendDescriptor{
		NameValue:        b.NameValue,
		Binary:           b.binary(),
		MinimumVersion:   b.MinimumVersion,
		StreamSchema:     b.StreamSchema,
		AllowedModels:    cloneStringSet(b.AllowedModels),
		AllowedEfforts:   cloneStringSet(b.AllowedEfforts),
		VersionTransform: b.VersionTransform,
		Discover:         b.Discover,
	}
}

func (b *Backend) validationDescriptor() StaticBackendDescriptor {
	if b.probed != nil {
		return b.probed.StaticBackendDescriptor
	}
	return b.staticDescriptor()
}

func (b *Backend) ProbeBackend(ctx context.Context, runner command.ProbeRunner) (engine.Backend, error) {
	probed, err := ProbeBackend(ctx, runner, b.staticDescriptor())
	if err != nil {
		return nil, err
	}
	clone := *b
	clone.probed = &probed
	return &clone, nil
}

func (b *Backend) normalizeVersion(s string) string {
	return normalizeVersionWith(s, b.VersionTransform)
}

func ProbeBackend(ctx context.Context, runner command.ProbeRunner, descriptor StaticBackendDescriptor) (ProbedBackendDescriptor, error) {
	if err := ctx.Err(); err != nil {
		return ProbedBackendDescriptor{}, err
	}
	if runner == nil {
		return ProbedBackendDescriptor{}, errors.New("probe runner is required")
	}
	binary, err := runner.LookPath(descriptor.Binary)
	if err != nil {
		return ProbedBackendDescriptor{}, fmt.Errorf("backend_unavailable: %s binary not found: %w", descriptor.NameValue, err)
	}
	versionResult, err := runner.Run(ctx, command.ProbeSpec{Argv: []string{binary, "--version"}})
	if err != nil {
		return ProbedBackendDescriptor{}, fmt.Errorf("backend_unavailable: %s version check failed: %w", descriptor.NameValue, err)
	}
	version := normalizeVersionWith(string(versionResult.Stdout), descriptor.VersionTransform)
	if compareVersion(version, descriptor.MinimumVersion) < 0 {
		return ProbedBackendDescriptor{}, fmt.Errorf("backend_unavailable: %s version %s is below minimum known-good %s", descriptor.NameValue, version, descriptor.MinimumVersion)
	}
	probed := ProbedBackendDescriptor{
		StaticBackendDescriptor: descriptor,
		BinaryPath:              binary,
		Version:                 version,
	}
	if descriptor.Discover == nil {
		return probed, nil
	}
	discovered, discoverErr := descriptor.Discover(ctx, runner, binary)
	if discoverErr != nil {
		probed.DiscoveryWarning = appendWarning(probed.DiscoveryWarning, fmt.Sprintf("%s model discovery failed: %v", descriptor.NameValue, discoverErr))
		return probed, nil
	}
	if discovered == nil {
		return probed, nil
	}
	probed.DiscoveredModels = append([]string(nil), discovered.Models...)
	probed.DiscoveredEfforts = append([]string(nil), discovered.Efforts...)
	probed.DiscoverySource = discovered.Source
	probed.DiscoveryFetchedAt = discovered.FetchedAt
	probed.DiscoveryClientVersion = discovered.ClientVersion
	for _, warning := range discovered.Warnings {
		probed.DiscoveryWarning = appendWarning(probed.DiscoveryWarning, warning)
	}
	if discovered.ClientVersion != "" && normalizeVersionWith(discovered.ClientVersion, descriptor.VersionTransform) != version {
		probed.DiscoveryWarning = appendWarning(probed.DiscoveryWarning, fmt.Sprintf("%s model discovery cache client_version %q does not match probed version %q", descriptor.NameValue, discovered.ClientVersion, version))
	}
	return probed, nil
}

func normalizeVersionWith(s string, transform func(string) string) string {
	s = strings.TrimSpace(s)
	if transform != nil {
		s = transform(s)
	}
	fields := strings.Fields(s)
	for _, f := range fields {
		if isVersionToken(f) {
			return strings.TrimPrefix(f, "v")
		}
	}
	return strings.TrimPrefix(s, "v")
}

func (b *Backend) validateOptions(opts engine.SessionOpts) (string, error) {
	return ValidateStaticOptions(b.validationDescriptor(), opts)
}

func ValidateStaticOptions(descriptor StaticBackendDescriptor, opts engine.SessionOpts) (string, error) {
	models, efforts, modelsDiscovered, effortsDiscovered, warning := validationSets(descriptor)
	if opts.Model != "" {
		if _, ok := models[opts.Model]; !ok {
			if modelsDiscovered {
				warning = appendWarning(warning, fmt.Sprintf("model %q is not in the discovered %s catalog; passing through to backend", opts.Model, descriptor.NameValue))
			} else if len(models) > 0 {
				return warning, fmt.Errorf("unsupported model %q for %s", opts.Model, descriptor.NameValue)
			}
		}
	}
	if opts.Effort != "" {
		if _, ok := efforts[opts.Effort]; !ok {
			if effortsDiscovered {
				warning = appendWarning(warning, fmt.Sprintf("effort %q is not in the discovered %s catalog; passing through to backend", opts.Effort, descriptor.NameValue))
			} else if len(efforts) > 0 {
				return warning, fmt.Errorf("unsupported effort %q for %s", opts.Effort, descriptor.NameValue)
			}
		}
	}
	return warning, nil
}

func validationSets(descriptor StaticBackendDescriptor) (map[string]struct{}, map[string]struct{}, bool, bool, string) {
	models := descriptor.AllowedModels
	efforts := descriptor.AllowedEfforts
	modelsDiscovered := descriptor.DiscoverySource != "" && len(descriptor.DiscoveredModels) > 0
	effortsDiscovered := descriptor.DiscoverySource != "" && len(descriptor.DiscoveredEfforts) > 0
	if modelsDiscovered {
		models = StringSet(descriptor.DiscoveredModels...)
	}
	if effortsDiscovered {
		efforts = StringSet(descriptor.DiscoveredEfforts...)
	}
	return models, efforts, modelsDiscovered, effortsDiscovered, descriptor.DiscoveryWarning
}

func cloneStringSet(in map[string]struct{}) map[string]struct{} {
	if in == nil {
		return nil
	}
	out := make(map[string]struct{}, len(in))
	for value := range in {
		out[value] = struct{}{}
	}
	return out
}

func discoveryWarnings(warning string) []string {
	if warning == "" {
		return nil
	}
	return strings.Split(warning, "; ")
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
	duplexSession             *duplex.Session
	mu                        sync.Mutex
	active                    command.RunningCommand
	lastAgentMessage          string
}

func (s *Session) ID() string {
	if s.duplexSession != nil {
		return s.duplexSession.ID()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

func (s *Session) Turn(ctx context.Context, input engine.TurnInput) (<-chan engine.Event, error) {
	return s.TurnWithRunner(ctx, input, command.DirectCommandRunner{})
}

func (s *Session) TurnWithRunner(ctx context.Context, input engine.TurnInput, runner command.Runner) (<-chan engine.Event, error) {
	if runner == nil {
		return nil, errors.New("command runner is required")
	}
	warningText, err := s.backend.validateOptions(s.opts)
	if err != nil {
		return nil, err
	}
	if warningText == "" {
		warningText = s.validationWarning
	}
	if s.suppressValidationWarning {
		warningText = ""
	}
	if s.duplexSession != nil {
		return s.turnWithDuplexRunner(ctx, input, runner, warningText)
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
	spec := command.ExecSpec{Argv: append([]string{s.backend.binary()}, args...)}
	if s.opts.CWD != "" {
		spec.Dir = s.opts.CWD
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
	running, err := runner.Start(ctx, spec)
	if err != nil {
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
		if reporter, ok := running.(interface {
			ProcessRef() (engine.ProcessRef, int)
		}); ok {
			ref, backendChildPID := reporter.ProcessRef()
			input.OnProcessStart(ref, backendChildPID)
		}
	}
	stdin := running.Stdin()
	stdout := running.Stdout()
	stderrPipe := running.Stderr()
	s.active = running
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
		stderrDone := make(chan error, 1)
		go func() {
			stderrDone <- copyAndClose(stderrWriter, stderrPipe)
		}()
		go func() {
			_, _ = io.WriteString(stdin, input.Prompt)
			_ = stdin.Close()
		}()
		var stdoutReader io.Reader = stdout
		if stdoutLog != nil {
			stdoutReader = io.TeeReader(stdout, stdoutLog)
		}
		waitDone := make(chan command.FinalObservation, 1)
		go func() {
			waitDone <- finalObservation(ctx, running)
		}()
		parseErr := s.scan(stdoutReader, events)
		observation := <-waitDone
		stderrCopyErr := <-stderrDone
		s.mu.Lock()
		if s.active == running {
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
		if observation.CleanupErr != nil {
			events <- warning(observation.CleanupErr.Error())
		}
		if observation.ExecutionErr != nil {
			msg := strings.TrimSpace(stderr.String())
			if msg == "" && stderrCopyErr != nil {
				msg = stderrCopyErr.Error()
			}
			if msg == "" {
				msg = observation.ExecutionErr.Error()
			}
			events <- terminalError(msg)
			return
		}
		if stderrCopyErr != nil {
			events <- terminalError(stderrCopyErr.Error())
		}
	}()
	return events, nil
}

func (s *Session) turnWithDuplexRunner(ctx context.Context, input engine.TurnInput, runner command.Runner, warningText string) (<-chan engine.Event, error) {
	events, err := s.duplexSession.TurnWithRunner(ctx, input, runner)
	if err != nil {
		return nil, err
	}
	if warningText == "" {
		return events, nil
	}
	out := make(chan engine.Event, 16)
	go func() {
		defer close(out)
		out <- warning(warningText)
		for ev := range events {
			out <- ev
		}
	}()
	return out, nil
}

func finalObservation(ctx context.Context, running command.RunningCommand) command.FinalObservation {
	exit, waitErr := running.Wait(ctx)
	if observer, ok := running.(command.FinalObserver); ok {
		observation, err := observer.FinalObservation(context.WithoutCancel(ctx))
		if observation.Exit == (command.ExitObservation{}) {
			observation.Exit = exit
		}
		if observation.ExecutionErr == nil && observation.CleanupErr == nil {
			observation.ExecutionErr = errors.Join(waitErr, err)
		}
		return observation
	}
	return command.FinalObservation{Exit: exit, ExecutionErr: waitErr}
}

func (s *Session) Interrupt(ctx context.Context) error {
	if s.duplexSession != nil {
		return s.duplexSession.Interrupt(ctx)
	}
	s.mu.Lock()
	command := s.active
	s.mu.Unlock()
	if command == nil {
		return nil
	}
	return command.Interrupt(ctx)
}

func (s *Session) NativeInterrupt(ctx context.Context) error {
	if s.duplexSession != nil {
		return s.duplexSession.NativeInterrupt(ctx)
	}
	return nil
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

func copyAndClose(dst io.Writer, src io.ReadCloser) error {
	defer func() { _ = src.Close() }()
	_, err := io.Copy(dst, src)
	return err
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
