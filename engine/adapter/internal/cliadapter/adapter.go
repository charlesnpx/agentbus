package cliadapter

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/duplex"
	"github.com/charlesnpx/agentbus/engine/command"
)

type Backend struct {
	NameValue        string
	Binary           string
	MinimumVersion   string
	StreamSchema     string
	AllowedModels    map[string]struct{}
	AllowedEfforts   map[string]struct{}
	BuildArgs        func(resumeID string, opts engine.SessionOpts, input engine.TurnInput) ([]string, error)
	Parse            func(map[string]any) ([]engine.Event, string, error)
	Driver           duplex.Driver
	VersionTransform func(string) string
	Discover         func(context.Context, command.ProbeRunner, string) (*engine.ModelDiscovery, error)
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

func (b *Backend) Preflight(ctx context.Context) (engine.Health, error) {
	probed, err := ProbeBackend(ctx, command.DirectProbeRunner{}, b.staticDescriptor())
	if err != nil {
		return engine.Health{}, err
	}
	return engine.Health{
		Backend:      b.NameValue,
		BinaryPath:   probed.BinaryPath,
		Version:      probed.Version,
		StreamSchema: b.StreamSchema,
		Minimum:      b.MinimumVersion,
		Warning:      b.discoveryWarning(probed),
	}, nil
}

func (b *Backend) DiscoverModels(ctx context.Context, runner command.ProbeRunner) (*engine.ModelDiscovery, error) {
	if runner == nil {
		return nil, errors.New("probe runner is required")
	}
	if b.Discover == nil {
		return nil, nil
	}
	binary, err := runner.LookPath(b.binary())
	if err != nil {
		return nil, err
	}
	return b.Discover(ctx, runner, binary)
}

func (b *Backend) BackendMetadata(context.Context) engine.BackendMetadata {
	return engine.BackendMetadata{
		Name:    b.NameValue,
		Models:  sortedStringSet(b.AllowedModels),
		Efforts: sortedStringSet(b.AllowedEfforts),
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
		opts:                      opts,
		validationWarning:         validationWarning,
		suppressValidationWarning: suppressValidationWarning,
	}
	driver := b.Driver
	if driver == nil {
		var err error
		driver, err = newOneShotDriver(b)
		if err != nil {
			return nil, err
		}
	}
	duplexSession, err := duplex.NewSession(duplex.SessionConfig{
		Driver:   driver,
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
	return ValidateStaticOptions(b.staticDescriptor(), opts)
}

func ValidateStaticOptions(descriptor StaticBackendDescriptor, opts engine.SessionOpts) (string, error) {
	if opts.Model != "" {
		if _, ok := descriptor.AllowedModels[opts.Model]; !ok && len(descriptor.AllowedModels) > 0 {
			return "", fmt.Errorf("unsupported model %q for %s", opts.Model, descriptor.NameValue)
		}
	}
	if opts.Effort != "" {
		if _, ok := descriptor.AllowedEfforts[opts.Effort]; !ok && len(descriptor.AllowedEfforts) > 0 {
			return "", fmt.Errorf("unsupported effort %q for %s", opts.Effort, descriptor.NameValue)
		}
	}
	return "", nil
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

func sortedStringSet(values map[string]struct{}) []string {
	ordered := make([]string, 0, len(values))
	for value := range values {
		ordered = append(ordered, value)
	}
	sort.Strings(ordered)
	return ordered
}

func appendWarning(existing, addition string) string {
	if existing == "" {
		return addition
	}
	return existing + "; " + addition
}

func (b *Backend) discoveryWarning(probe ProbedBackendDescriptor) string {
	if probe.DiscoveryWarning != "" {
		return probe.DiscoveryWarning
	}
	if probe.DiscoverySource == "" && len(probe.DiscoveredModels) == 0 && len(probe.DiscoveredEfforts) == 0 {
		return "model discovery unavailable; using static known-good validation lists"
	}
	return ""
}

type Session struct {
	backend                   *Backend
	opts                      engine.SessionOpts
	validationWarning         string
	suppressValidationWarning bool
	duplexSession             *duplex.Session
}

func (s *Session) ID() string {
	return s.duplexSession.ID()
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
	return s.turnWithDuplexRunner(ctx, input, runner, warningText)
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

func (s *Session) Interrupt(ctx context.Context) error {
	return s.duplexSession.Interrupt(ctx)
}

func (s *Session) NativeInterrupt(ctx context.Context) (bool, error) {
	return s.duplexSession.NativeInterrupt(ctx)
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
