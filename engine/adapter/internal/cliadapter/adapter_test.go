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
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
)

func TestCapEventKeepsRawTextOutOfJSONMetadata(t *testing.T) {
	raw := strings.Repeat("a", engine.DefaultEventTextCap) + "SECRET_RAW_TAIL"
	ev := capEvent(engine.Event{
		Type: engine.EventAgentText,
		Text: raw,
		Metadata: map[string]any{
			"agentbusRawText": raw,
			"text":            raw,
			"nested": map[string]any{
				"content": raw,
			},
		},
	})
	if ev.RawText != raw {
		t.Fatal("raw text was not preserved in the non-JSON field")
	}
	if !ev.Truncated || strings.Contains(ev.Text, "SECRET_RAW_TAIL") {
		t.Fatalf("event text was not capped: truncated=%v len=%d", ev.Truncated, len(ev.Text))
	}
	wire, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), "agentbusRawText") || strings.Contains(string(wire), "SECRET_RAW_TAIL") {
		t.Fatalf("wire event leaked raw text metadata: %s", wire)
	}
}

func TestSessionTurnSurfacesMalformedStreamAsTerminalError(t *testing.T) {
	fake := fakeTerminalErrorCLI(t)
	backend := &Backend{
		NameValue: "fake",
		Binary:    fake,
		BuildArgs: func(string, engine.SessionOpts, engine.TurnInput) ([]string, error) {
			return []string{"malformed"}, nil
		},
		Parse: func(map[string]any) ([]engine.Event, string, error) {
			return nil, "", nil
		},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "go"})
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(events)
	if len(got) == 0 || got[len(got)-1].Type != engine.EventTerminalError || !strings.Contains(got[len(got)-1].Text, "malformed backend stream") {
		t.Fatalf("events = %#v, want terminal malformed-stream error", got)
	}
}

func TestSessionTurnSurfacesNonzeroExitAsTerminalError(t *testing.T) {
	fake := fakeTerminalErrorCLI(t)
	backend := &Backend{
		NameValue: "fake",
		Binary:    fake,
		BuildArgs: func(string, engine.SessionOpts, engine.TurnInput) ([]string, error) {
			return []string{"fail"}, nil
		},
		Parse: func(map[string]any) ([]engine.Event, string, error) {
			return nil, "", nil
		},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "go"})
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(events)
	if len(got) == 0 || got[len(got)-1].Type != engine.EventTerminalError || !strings.Contains(got[len(got)-1].Text, "backend exploded") {
		t.Fatalf("events = %#v, want terminal nonzero-exit error", got)
	}
}

func TestSessionTurnPreservesLargeStderrTail(t *testing.T) {
	fake := fakeTerminalErrorCLI(t)
	backend := &Backend{
		NameValue: "fake",
		Binary:    fake,
		BuildArgs: func(string, engine.SessionOpts, engine.TurnInput) ([]string, error) {
			return []string{"large-stderr"}, nil
		},
		Parse: func(map[string]any) ([]engine.Event, string, error) {
			return nil, "", nil
		},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	events, err := session.Turn(ctx, engine.TurnInput{Prompt: "go"})
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 3*time.Second)
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
		Binary:    fake,
		BuildArgs: func(string, engine.SessionOpts, engine.TurnInput) ([]string, error) {
			return []string{"inherited-stderr"}, nil
		},
		Parse: func(map[string]any) ([]engine.Event, string, error) {
			return []engine.Event{{Type: engine.EventAgentText, Text: "ok"}}, "", nil
		},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	events, err := session.Turn(ctx, engine.TurnInput{Prompt: "go"})
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 3*time.Second)
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

func TestSessionTurnTimeoutKillsDescendantHoldingStdout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("process group signal assertions are unix-only")
	}
	const script = `sleep 30 & printf '%s\n' '{"event":"ready","text":"READY"}'; exit 0`
	backend := &Backend{
		NameValue: "sh",
		Binary:    "/bin/sh",
		CachePath: filepath.Join(t.TempDir(), "missing-cache.json"),
		BuildArgs: func(string, engine.SessionOpts, engine.TurnInput) ([]string, error) {
			return []string{"-c", script}, nil
		},
		Parse: func(obj map[string]any) ([]engine.Event, string, error) {
			if obj["event"] != "ready" {
				return nil, "", nil
			}
			text, _ := obj["text"].(string)
			return []engine.Event{{Type: engine.EventAgentText, Text: text}}, "", nil
		},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	var processGroup int
	started := time.Now()
	events, err := session.Turn(ctx, engine.TurnInput{
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

func TestCopyAndCloseClosesSourceOnWriterFailure(t *testing.T) {
	wantErr := errors.New("writer failed")
	src := &recordingReadCloser{Reader: strings.NewReader("stderr")}
	err := copyAndClose(errorWriter{err: wantErr}, src)
	if !errors.Is(err, wantErr) {
		t.Fatalf("copyAndClose error = %v, want %v", err, wantErr)
	}
	if !src.closed {
		t.Fatal("copyAndClose did not close the source after writer failure")
	}
}

func TestSessionTurnUsesCommandRunnerExecSpec(t *testing.T) {
	runner := &fakeCommandRunner{}
	marker := filepath.Join(t.TempDir(), "direct-exec-marker")
	backend := &Backend{
		NameValue: "fake",
		Binary:    markerCLI(t, marker),
		CachePath: filepath.Join(t.TempDir(), "missing-cache.json"),
		BuildArgs: func(string, engine.SessionOpts, engine.TurnInput) ([]string, error) {
			return []string{"run", "--json"}, nil
		},
		Parse: func(map[string]any) ([]engine.Event, string, error) {
			return []engine.Event{{Type: engine.EventAgentText, Text: "ok"}}, "stream-session", nil
		},
	}
	cwd := t.TempDir()
	session, err := backend.Start(context.Background(), engine.SessionOpts{CWD: cwd})
	if err != nil {
		t.Fatal(err)
	}
	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(events)
	if len(got) != 1 || got[0].Type != engine.EventAgentText || got[0].Text != "ok" {
		t.Fatalf("events = %#v, want parsed adapter event", got)
	}
	if session.ID() != "stream-session" {
		t.Fatalf("session id = %q, want stream-session", session.ID())
	}
	if strings.Join(runner.spec.Argv, "\x00") != strings.Join([]string{backend.Binary, "run", "--json"}, "\x00") {
		t.Fatalf("argv = %#v", runner.spec.Argv)
	}
	if runner.spec.Dir != cwd {
		t.Fatalf("dir = %q, want %q", runner.spec.Dir, cwd)
	}
	assertFileMissing(t, marker)
}

func TestSessionTurnSurfacesOutputTruncationAsTerminalError(t *testing.T) {
	runner := &fakeCommandRunner{
		stdout: &errorAfterReader{
			reader: strings.NewReader(`{"event":"ok"}` + "\n"),
			err:    command.ErrOutputTruncated,
		},
	}
	backend := &Backend{
		NameValue: "fake",
		Binary:    "/bin/fake",
		BuildArgs: func(string, engine.SessionOpts, engine.TurnInput) ([]string, error) {
			return []string{"run"}, nil
		},
		Parse: func(map[string]any) ([]engine.Event, string, error) {
			return []engine.Event{{Type: engine.EventAgentText, Text: "ok"}}, "", nil
		},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEvents(events)
	if len(got) != 2 || got[0].Type != engine.EventAgentText || got[1].Type != engine.EventTerminalError || !strings.Contains(got[1].Text, command.ErrOutputTruncated.Error()) {
		t.Fatalf("events = %#v, want parsed event followed by truncation terminal error", got)
	}
}

func fakeTerminalErrorCLI(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fakecli")
	script := `#!/bin/sh
case "$1" in
  malformed) printf 'not-json\n'; exit 0 ;;
  fail) printf 'backend exploded\n' >&2; exit 7 ;;
  large-stderr)
    i=0
    while [ "$i" -lt 4096 ]; do
      printf '0123456789abcdef0123456789abcdef\n' >&2
      i=$((i + 1))
    done
    printf 'TAIL_MARKER\n' >&2
    exit 9
    ;;
  inherited-stderr)
    (sleep 5 >/dev/null &)
    printf '{"event":"ok"}\n'
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
	return &Backend{
		NameValue: "fake",
		Binary:    markerCLI(t, marker),
		BuildArgs: func(string, engine.SessionOpts, engine.TurnInput) ([]string, error) {
			return []string{"run"}, nil
		},
		Parse: func(map[string]any) ([]engine.Event, string, error) {
			return []engine.Event{{Type: engine.EventAgentText, Text: "ok"}}, "", nil
		},
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

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) {
	return 0, w.err
}

type recordingReadCloser struct {
	io.Reader
	closed bool
}

func (r *recordingReadCloser) Close() error {
	r.closed = true
	return nil
}

type fakeCommandRunner struct {
	spec   command.ExecSpec
	stdout io.ReadCloser
}

func (r *fakeCommandRunner) Start(_ context.Context, spec command.ExecSpec) (command.RunningCommand, error) {
	r.spec = spec
	stdout := r.stdout
	if stdout == nil {
		stdout = io.NopCloser(strings.NewReader(`{"event":"ok"}` + "\n"))
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
	reader io.Reader
	err    error
}

func (r *errorAfterReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err == io.EOF {
		return 0, r.err
	}
	return n, err
}

func (r *errorAfterReader) Close() error {
	return nil
}
