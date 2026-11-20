package cliadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/duplex"
	"github.com/charlesnpx/agentbus/engine/command"
)

func TestTurnFinalPayloadDoesNotChangeEventJSON(t *testing.T) {
	event := engine.Event{
		Type: engine.EventTurnFinal,
		TurnFinal: &engine.TurnFinalObservation{
			BackendSessionID: "resume-1",
		},
	}
	wire, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), "turnFinal") || strings.Contains(string(wire), "resume-1") {
		t.Fatalf("TurnFinal payload leaked into event JSON: %s", wire)
	}
}

func TestSessionTurnSurfacesMalformedStreamAsTerminalError(t *testing.T) {
	fake := fakeTerminalErrorCLI(t)
	backend := &Backend{
		NameValue: "fake",
		Driver:    duplex.FixtureDriver{Spec: command.ExecSpec{Argv: []string{fake, "malformed"}}},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := session.Turn(context.Background(), engine.TurnInput{Intent: engine.IntentInspect, Prompt: "go"})
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(events)
	got = withoutTurnFinal(got)
	if len(got) == 0 || got[len(got)-1].Type != engine.EventTerminalError || !strings.Contains(got[len(got)-1].Text, "malformed duplex stdout") {
		t.Fatalf("events = %#v, want terminal malformed-stream error", got)
	}
}

func TestSessionTurnSurfacesNonzeroExitAsTerminalError(t *testing.T) {
	fake := fakeTerminalErrorCLI(t)
	backend := &Backend{
		NameValue: "fake",
		Driver:    duplex.FixtureDriver{Spec: command.ExecSpec{Argv: []string{fake, "fail"}}},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	// Exceed the pipe capacity so the child must close stdin before the write completes.
	events, err := session.Turn(context.Background(), engine.TurnInput{Intent: engine.IntentInspect, Prompt: strings.Repeat("go", 1<<19)})
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(events)
	if len(got) == 0 || got[len(got)-1].Type != engine.EventTurnFinal || got[len(got)-1].TurnFinal == nil || !got[len(got)-1].TurnFinal.ReturnCodeKnown || got[len(got)-1].TurnFinal.ReturnCode != 7 {
		t.Fatalf("events = %#v, want final observation with exit status 7", got)
	}
	got = withoutTurnFinal(got)
	if len(got) == 0 || got[len(got)-1].Type != engine.EventTerminalError || !strings.Contains(got[len(got)-1].Text, "backend exploded") {
		t.Fatalf("events = %#v, want terminal nonzero-exit error", got)
	}
}

func TestSessionTurnPreservesLargeStderrTail(t *testing.T) {
	fake := fakeTerminalErrorCLI(t)
	backend := &Backend{
		NameValue: "fake",
		Driver:    duplex.FixtureDriver{Spec: command.ExecSpec{Argv: []string{fake, "large-stderr"}}},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	events, err := session.Turn(ctx, engine.TurnInput{Intent: engine.IntentInspect, Prompt: "go"})
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 7*time.Second)
	got = withoutTurnFinal(got)
	if ctx.Err() != nil {
		t.Fatalf("turn context ended unexpectedly: %v", ctx.Err())
	}
	if len(got) == 0 || got[len(got)-1].Type != engine.EventTerminalError || !strings.Contains(got[len(got)-1].Text, "TAIL_MARKER") {
		t.Fatalf("events = %#v, want terminal stderr error with tail marker", got)
	}
}

func TestSessionTurnInheritedStderrDoesNotHang(t *testing.T) {
	fake := fakeTerminalErrorCLI(t)
	backend := &Backend{
		NameValue: "fake",
		Driver:    duplex.FixtureDriver{Spec: command.ExecSpec{Argv: []string{fake, "inherited-stderr"}}},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	events, err := session.Turn(ctx, engine.TurnInput{Intent: engine.IntentInspect, Prompt: "go"})
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 7*time.Second)
	if ctx.Err() != nil {
		t.Fatalf("turn context ended unexpectedly: %v", ctx.Err())
	}
	for _, ev := range got {
		if ev.Type == engine.EventAgentText && ev.Text == "ok" {
			return
		}
	}
	t.Fatalf("events = %#v, want parsed event before inherited stderr closes", got)
}

func TestBackendStartNeverExecsBinary(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "version-probe-marker")
	backend := markerBackendForLifecycleTest(t, marker)
	if _, err := backend.Start(context.Background(), engine.SessionOpts{}); err != nil {
		t.Fatal(err)
	}
	assertFileMissing(t, marker)
}

func TestBackendResumeNeverExecsBinary(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "version-probe-marker")
	backend := markerBackendForLifecycleTest(t, marker)
	if _, err := backend.Resume(context.Background(), "resume-id", engine.SessionOpts{}); err != nil {
		t.Fatal(err)
	}
	assertFileMissing(t, marker)
}

func TestBackendMetadataListsHelloAcceptedValuesInStableOrder(t *testing.T) {
	backend := &Backend{
		NameValue:      "fake",
		AllowedModels:  StringSet("zeta", "alpha", "middle"),
		AllowedEfforts: StringSet("ultra", "high", "minimal"),
	}
	wantModels := []string{"alpha", "middle", "zeta"}
	wantEfforts := []string{"high", "minimal", "ultra"}

	for range 2 {
		metadata := backend.BackendMetadata(context.Background())
		if metadata.Name != "fake" || !slices.Equal(metadata.Models, wantModels) || !slices.Equal(metadata.Efforts, wantEfforts) {
			t.Fatalf("hello backend metadata = %#v, want models=%#v efforts=%#v", metadata, wantModels, wantEfforts)
		}
	}
}

func TestBackendRequiresDriver(t *testing.T) {
	backend := &Backend{NameValue: "fake"}
	if _, err := backend.Start(context.Background(), engine.SessionOpts{}); err == nil || err.Error() != "duplex driver is required" {
		t.Fatalf("Start error = %v, want missing driver error", err)
	}
	if _, err := backend.Resume(context.Background(), "resume-id", engine.SessionOpts{}); err == nil || err.Error() != "duplex driver is required" {
		t.Fatalf("Resume error = %v, want missing driver error", err)
	}
}

func TestSessionTurnTimeoutCompletesBoundedlyWithTimeoutEvents(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process group signal assertions are unix-only")
	}
	const script = `IFS= read -r prompt; sleep 30 & printf '%s\n' '{"type":"message","text":"READY"}'; exit 0`
	backend := &Backend{
		NameValue: "sh",
		Driver:    duplex.FixtureDriver{Spec: command.ExecSpec{Argv: []string{"/bin/sh", "-c", script}}},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	var processGroup int
	started := time.Now()
	events, err := session.Turn(ctx, engine.TurnInput{Intent: engine.IntentInspect,
		Prompt: "go",
		OnProcessStart: func(ref engine.ProcessRef, _ int) {
			processGroup = ref.PGID
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if processGroup == 0 {
			return
		}
		if ownGroup, err := syscall.Getpgid(os.Getpid()); err == nil && processGroup == ownGroup {
			return
		}
		_ = syscall.Kill(-processGroup, syscall.SIGKILL)
	})
	got := collectEventsWithTimeout(t, events, 5*time.Second)
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("turn completed in %s, want under 5s", elapsed)
	}
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("turn context error = %v, want %v", ctx.Err(), context.DeadlineExceeded)
	}
	var sawReady, sawTimeout bool
	for _, ev := range got {
		if ev.Type == engine.EventAgentText && ev.Text == "READY" {
			sawReady = true
		}
		if ev.Type == engine.EventWarning && strings.Contains(ev.Text, "backend turn timed out") {
			sawTimeout = true
		}
	}
	if !sawReady {
		t.Fatalf("events = %#v, want READY event before timeout completion", got)
	}
	if !sawTimeout {
		t.Fatalf("events = %#v, want backend timeout warning", got)
	}
}

func TestSessionTurnUsesCommandRunnerExecSpec(t *testing.T) {
	runner := &fakeCommandRunner{}
	marker := filepath.Join(t.TempDir(), "direct-exec-marker")
	binary := markerCLI(t, marker)
	cwd := t.TempDir()
	backend := &Backend{
		NameValue: "fake",
		Driver:    duplex.FixtureDriver{Spec: command.ExecSpec{Argv: []string{binary, "run", "--json"}, Dir: cwd}},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{CWD: cwd})
	if err != nil {
		t.Fatal(err)
	}
	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Intent: engine.IntentInspect, Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(events)
	got = withoutTurnFinal(got)
	if len(got) != 2 || got[0].Type != engine.EventAgentText || got[0].Text != "ok" || got[1].Type != engine.EventResultMessage {
		t.Fatalf("events = %#v, want agent and result events", got)
	}
	if session.ID() != "stream-session" {
		t.Fatalf("session id = %q, want stream-session", session.ID())
	}
	if strings.Join(runner.spec.Argv, "\x00") != strings.Join([]string{binary, "run", "--json"}, "\x00") {
		t.Fatalf("argv = %#v", runner.spec.Argv)
	}
	if runner.spec.Dir != cwd {
		t.Fatalf("dir = %q, want %q", runner.spec.Dir, cwd)
	}
	assertFileMissing(t, marker)
}

func TestBackendWithDriverRunsDuplexTurn(t *testing.T) {
	driver := newCliDuplexTestDriver("resume-1")
	runner := &duplexCommandRunner{finishOnStart: true}
	backend := &Backend{
		NameValue:      "fake",
		Binary:         "fake-bin",
		MinimumVersion: "0.1.0",
		Driver:         driver,
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{CWD: "/tmp/work"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := session.(interface {
		TurnWithRunner(context.Context, engine.TurnInput, command.Runner) (<-chan engine.Event, error)
	}); !ok {
		t.Fatal("session does not implement TurnWithRunner")
	}

	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Intent: engine.IntentInspect, Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	got = withoutTurnFinal(got)
	if len(got) != 2 || got[0].Type != engine.EventAgentText || !strings.HasSuffix(got[0].Text, "\n\nhello") || got[1].Type != engine.EventResultMessage {
		t.Fatalf("events = %#v, want duplex agent text and result", got)
	}
	if session.ID() != "resume-1" {
		t.Fatalf("session id = %q, want resume-1", session.ID())
	}
	spec := runner.lastSpec()
	if len(spec.Argv) != 2 || spec.Argv[0] != "duplex-fake" || !strings.HasSuffix(spec.Argv[1], "\n\nhello") || strings.Count(spec.Argv[1], "Inspect the task and report your findings.") != 1 {
		t.Fatalf("argv = %#v", spec.Argv)
	}
	if spec.Dir != "/tmp/work" {
		t.Fatalf("dir = %q, want /tmp/work", spec.Dir)
	}
}

func TestBackendWithDriverThreadsResumeIDAcrossTurns(t *testing.T) {
	driver := newCliDuplexTestDriver("resume-1", "resume-2")
	runner := &duplexCommandRunner{finishOnStart: true}
	backend := &Backend{NameValue: "fake", Driver: driver}
	session, err := backend.Resume(context.Background(), "resume-0", engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}

	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Intent: engine.IntentInspect, Prompt: "first"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	_ = collectEventsWithTimeout(t, events, 2*time.Second)
	events, err = session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Intent: engine.IntentInspect, Prompt: "second"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	_ = collectEventsWithTimeout(t, events, 2*time.Second)

	got := driver.runResumeIDs()
	want := []string{"resume-0", "resume-1"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("driver resume ids = %#v, want %#v", got, want)
	}
	if session.ID() != "resume-2" {
		t.Fatalf("session id = %q, want resume-2", session.ID())
	}
}

func TestBackendWithDriverInterruptForwardsToActiveDuplexTurn(t *testing.T) {
	driver := newCliDuplexTestDriver("resume-interrupted")
	driver.waitForInterrupt = true
	runner := &duplexCommandRunner{}
	driver.finish = runner.finishLast
	backend := &Backend{NameValue: "fake", Driver: driver}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}

	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Intent: engine.IntentInspect, Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	first := receiveEventWithTimeout(t, events, 2*time.Second)
	if first.Type != engine.EventAgentText || !strings.HasSuffix(first.Text, "\n\nhello") {
		t.Fatalf("first event = %#v, want duplex start event", first)
	}
	if err := session.Interrupt(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	got = withoutTurnFinal(got)
	if driver.nativeInterrupts.Load() != 1 {
		t.Fatalf("native interrupt count = %d, want 1", driver.nativeInterrupts.Load())
	}
	if len(got) != 1 || got[0].Type != engine.EventResultMessage || session.ID() != "resume-interrupted" {
		t.Fatalf("events = %#v id = %q, want interrupted result", got, session.ID())
	}
}

func TestBackendWithDriverNativeInterruptReportsSettlement(t *testing.T) {
	driver := newCliDuplexTestDriver("resume-native-settled")
	driver.waitForInterrupt = true
	runner := &duplexCommandRunner{}
	driver.finish = runner.finishLast
	backend := &Backend{NameValue: "fake", Driver: driver}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}

	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Intent: engine.IntentInspect, Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	first := receiveEventWithTimeout(t, events, 2*time.Second)
	if first.Type != engine.EventAgentText || !strings.HasSuffix(first.Text, "\n\nhello") {
		t.Fatalf("first event = %#v, want duplex start event", first)
	}
	settled, err := session.(*Session).NativeInterrupt(context.Background())
	if !settled || err != nil {
		t.Fatalf("NativeInterrupt = (%t, %v), want (true, nil)", settled, err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	got = withoutTurnFinal(got)
	if driver.nativeInterrupts.Load() != 1 {
		t.Fatalf("native interrupt count = %d, want 1", driver.nativeInterrupts.Load())
	}
	if len(got) != 1 || got[0].Type != engine.EventResultMessage || session.ID() != "resume-native-settled" {
		t.Fatalf("events = %#v id = %q, want native interrupted result", got, session.ID())
	}
}

func TestBackendWithDriverRejectsConcurrentTurn(t *testing.T) {
	block := make(chan struct{})
	driver := newCliDuplexTestDriver("resume-1")
	driver.blockCh = block
	runner := &duplexCommandRunner{finishOnStart: true}
	backend := &Backend{NameValue: "fake", Driver: driver}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}

	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Intent: engine.IntentInspect, Prompt: "first"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	first := receiveEventWithTimeout(t, events, 2*time.Second)
	if first.Type != engine.EventAgentText || !strings.HasSuffix(first.Text, "\n\nfirst") {
		t.Fatalf("first event = %#v, want duplex start event", first)
	}
	_, err = session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Intent: engine.IntentInspect, Prompt: "second"}, runner)
	if err == nil || err.Error() != "session_busy" {
		t.Fatalf("concurrent turn error = %v, want session_busy", err)
	}
	close(block)
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	got = withoutTurnFinal(got)
	if len(got) != 1 || got[0].Type != engine.EventResultMessage {
		t.Fatalf("events = %#v, want first turn result", got)
	}
}

var errSyntheticReadFailure = errors.New("synthetic read failure")

func TestSessionTurnSurfacesReadFailureAsTerminalError(t *testing.T) {
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	runner := &fakeCommandRunner{
		stdout: &errorAfterReader{
			reader:  strings.NewReader(`{"type":"message","text":"ok"}` + "\n"),
			err:     errSyntheticReadFailure,
			release: release,
		},
	}
	backend := &Backend{
		NameValue: "fake",
		Driver:    duplex.FixtureDriver{Spec: command.ExecSpec{Argv: []string{"/bin/fake", "run"}}},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Intent: engine.IntentInspect, Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	first := receiveEventWithTimeout(t, events, 2*time.Second)
	close(release)
	got := append([]engine.Event{first}, collectEvents(events)...)
	got = withoutTurnFinal(got)
	if len(got) != 2 || got[0].Type != engine.EventAgentText || got[1].Type != engine.EventTerminalError || !strings.Contains(got[1].Text, "incomplete backend transport") || !errors.Is(got[1].Err, errSyntheticReadFailure) {
		t.Fatalf("events = %#v, want parsed event followed by read failure terminal error", got)
	}
}

func fakeTerminalErrorCLI(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fakecli")
	script := `#!/bin/sh
case "$1" in
  malformed) IFS= read -r prompt; printf 'not-json\n'; exit 0 ;;
  fail) printf 'backend exploded\n' >&2; exit 7 ;;
  large-stderr)
    IFS= read -r prompt
    i=0
    while [ "$i" -lt 4096 ]; do
      printf '0123456789abcdef0123456789abcdef\n' >&2
      i=$((i + 1))
    done
    printf 'TAIL_MARKER\n' >&2
    exit 9
    ;;
  inherited-stderr)
    IFS= read -r prompt
    (sleep 5 >/dev/null &)
    printf '{"type":"message","text":"ok"}\n'
    printf '{"type":"complete"}\n'
    exit 0
    ;;
esac
exit 0
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func markerBackendForLifecycleTest(t *testing.T, marker string) *Backend {
	t.Helper()
	binary := markerCLI(t, marker)
	return &Backend{
		NameValue: "fake",
		Binary:    binary,
		Driver:    duplex.FixtureDriver{Spec: command.ExecSpec{Argv: []string{binary, "run"}}},
	}
}

func markerCLI(t *testing.T, marker string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "marker-cli")
	script := fmt.Sprintf(`#!/bin/sh
printf marker > %q
if [ "$1" = "--version" ]; then
  printf 'fake 1.0.0\n'
  exit 0
fi
printf '{"event":"ok"}\n'
`, marker)
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertFileMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s stat err = %v, want not exist", path, err)
	}
}

func collectEvents(ch <-chan engine.Event) []engine.Event {
	var out []engine.Event
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func withoutTurnFinal(events []engine.Event) []engine.Event {
	withoutFinal := make([]engine.Event, 0, len(events))
	for _, event := range events {
		if event.Type != engine.EventTurnFinal {
			withoutFinal = append(withoutFinal, event)
		}
	}
	return withoutFinal
}

func collectEventsWithTimeout(t *testing.T, ch <-chan engine.Event, timeout time.Duration) []engine.Event {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var out []engine.Event
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-timer.C:
			t.Fatalf("timed out collecting events after %s; collected %#v", timeout, out)
		}
	}
}

func receiveEventWithTimeout(t *testing.T, ch <-chan engine.Event, timeout time.Duration) engine.Event {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("event channel closed before event")
		}
		return ev
	case <-timer.C:
		t.Fatalf("timed out waiting for event after %s", timeout)
	}
	return engine.Event{}
}

type fakeCommandRunner struct {
	spec   command.ExecSpec
	stdout io.ReadCloser
}

func (r *fakeCommandRunner) Start(_ context.Context, spec command.ExecSpec) (command.RunningCommand, error) {
	r.spec = spec
	stdout := r.stdout
	if stdout == nil {
		stdout = io.NopCloser(strings.NewReader(`{"type":"message","text":"ok"}` + "\n" + `{"type":"complete","resumeId":"stream-session"}` + "\n"))
	}
	return &fakeRunningCommand{
		stdin:  discardWriteCloser{},
		stdout: stdout,
		stderr: io.NopCloser(strings.NewReader("")),
	}, nil
}

type fakeRunningCommand struct {
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
}

func (c fakeRunningCommand) Stdin() io.WriteCloser {
	return c.stdin
}

func (c fakeRunningCommand) Stdout() io.ReadCloser {
	return c.stdout
}

func (c fakeRunningCommand) Stderr() io.ReadCloser {
	return c.stderr
}

func (c fakeRunningCommand) Wait(context.Context) (command.ExitObservation, error) {
	return command.ExitObservation{Exited: true, Code: 0}, nil
}

func (c fakeRunningCommand) Interrupt(context.Context) error {
	return nil
}

type discardWriteCloser struct{}

func (discardWriteCloser) Write(p []byte) (int, error) {
	return len(p), nil
}

func (discardWriteCloser) Close() error {
	return nil
}

type errorAfterReader struct {
	reader  io.Reader
	err     error
	release <-chan struct{}
}

func (r *errorAfterReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err == io.EOF {
		<-r.release
		return 0, r.err
	}
	return n, err
}

func (r *errorAfterReader) Close() error {
	return nil
}

type cliDuplexTestDriver struct {
	mu               sync.Mutex
	nextIDs          []string
	resumeIDs        []string
	blockCh          <-chan struct{}
	waitForInterrupt bool
	interruptCh      chan struct{}
	interruptOnce    sync.Once
	nativeInterrupts atomic.Int32
	finish           func()
}

func newCliDuplexTestDriver(nextIDs ...string) *cliDuplexTestDriver {
	return &cliDuplexTestDriver{
		nextIDs:     append([]string(nil), nextIDs...),
		interruptCh: make(chan struct{}),
	}
}

func (d *cliDuplexTestDriver) ExecSpec(_ string, opts engine.SessionOpts, input engine.TurnInput) (command.ExecSpec, error) {
	return command.ExecSpec{
		Argv: []string{"duplex-fake", input.Prompt},
		Dir:  opts.CWD,
	}, nil
}

func (d *cliDuplexTestDriver) RunTurn(ctx context.Context, _ *duplex.Conn, resumeID string, _ engine.SessionOpts, input engine.TurnInput, emit duplex.EmitFunc) (string, error) {
	d.mu.Lock()
	d.resumeIDs = append(d.resumeIDs, resumeID)
	d.mu.Unlock()
	emit(engine.Event{Type: engine.EventAgentText, Text: "duplex:" + input.Prompt})
	if d.waitForInterrupt {
		select {
		case <-d.interruptCh:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if d.blockCh != nil {
		select {
		case <-d.blockCh:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	nextID := d.takeNextID(resumeID)
	emit(engine.Event{Type: engine.EventResultMessage, Text: "done"})
	return nextID, nil
}

func (d *cliDuplexTestDriver) Interrupt(context.Context, *duplex.Conn) error {
	d.nativeInterrupts.Add(1)
	d.interruptOnce.Do(func() {
		close(d.interruptCh)
		if d.finish != nil {
			d.finish()
		}
	})
	return nil
}

func (d *cliDuplexTestDriver) takeNextID(fallback string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.nextIDs) == 0 {
		return fallback
	}
	nextID := d.nextIDs[0]
	d.nextIDs = d.nextIDs[1:]
	return nextID
}

func (d *cliDuplexTestDriver) runResumeIDs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.resumeIDs...)
}

type duplexCommandRunner struct {
	mu            sync.Mutex
	specs         []command.ExecSpec
	last          *duplexRunningCommand
	finishOnStart bool
}

func (r *duplexCommandRunner) Start(_ context.Context, spec command.ExecSpec) (command.RunningCommand, error) {
	cmd := newDuplexRunningCommand()
	r.mu.Lock()
	r.specs = append(r.specs, spec)
	r.last = cmd
	r.mu.Unlock()
	if r.finishOnStart {
		cmd.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
	}
	return cmd, nil
}

func (r *duplexCommandRunner) lastSpec() command.ExecSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.specs) == 0 {
		return command.ExecSpec{}
	}
	return r.specs[len(r.specs)-1]
}

func (r *duplexCommandRunner) finishLast() {
	r.mu.Lock()
	cmd := r.last
	r.mu.Unlock()
	if cmd != nil {
		cmd.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
	}
}

type duplexRunningCommand struct {
	done   chan struct{}
	once   sync.Once
	exit   command.ExitObservation
	err    error
	cancel atomic.Int32
}

func newDuplexRunningCommand() *duplexRunningCommand {
	return &duplexRunningCommand{done: make(chan struct{})}
}

func (c *duplexRunningCommand) Stdin() io.WriteCloser {
	return discardWriteCloser{}
}

func (c *duplexRunningCommand) Stdout() io.ReadCloser {
	return io.NopCloser(strings.NewReader(""))
}

func (c *duplexRunningCommand) Stderr() io.ReadCloser {
	return io.NopCloser(strings.NewReader(""))
}

func (c *duplexRunningCommand) Wait(ctx context.Context) (command.ExitObservation, error) {
	select {
	case <-c.done:
		return c.exit, c.err
	case <-ctx.Done():
		return command.ExitObservation{}, ctx.Err()
	}
}

func (c *duplexRunningCommand) Interrupt(context.Context) error {
	c.cancel.Add(1)
	c.finish(command.ExitObservation{Exited: true, Code: 0}, nil)
	return nil
}

func (c *duplexRunningCommand) finish(exit command.ExitObservation, err error) {
	c.once.Do(func() {
		c.exit = exit
		c.err = err
		close(c.done)
	})
}
