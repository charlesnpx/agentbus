package cliadapter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
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

func TestSessionInterruptUsesProtocolDefaultGrace(t *testing.T) {
	original := terminateProcessGroup
	defer func() { terminateProcessGroup = original }()
	seen := make(chan time.Duration, 1)
	terminateProcessGroup = func(_ *exec.Cmd, grace time.Duration) error {
		seen <- grace
		return nil
	}
	session := &Session{active: &directRunningCommand{
		cmd:         &exec.Cmd{Process: &os.Process{Pid: os.Getpid()}},
		cancelGrace: engine.DefaultCancelGrace,
	}}
	if err := session.Interrupt(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-seen:
		if got != engine.DefaultCancelGrace {
			t.Fatalf("grace = %s, want %s", got, engine.DefaultCancelGrace)
		}
	default:
		t.Fatal("interrupt did not terminate the active process group")
	}
}

func TestDirectCommandRunnerTimeoutTerminatesOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	assertDirectCommandRunnerContextDoneTerminatesOnce(t, ctx, func() {})
}

func TestDirectCommandRunnerContextCancelTerminatesOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	assertDirectCommandRunnerContextDoneTerminatesOnce(t, ctx, cancel)
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
	backend := &Backend{
		NameValue: "fake",
		Binary:    "fake-binary",
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
	if strings.Join(runner.spec.Argv, "\x00") != strings.Join([]string{"fake-binary", "run", "--json"}, "\x00") {
		t.Fatalf("argv = %#v", runner.spec.Argv)
	}
	if runner.spec.Dir != cwd {
		t.Fatalf("dir = %q, want %q", runner.spec.Dir, cwd)
	}
}

func assertDirectCommandRunnerContextDoneTerminatesOnce(t *testing.T, ctx context.Context, triggerCancel func()) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("process group signal assertions are unix-only")
	}
	original := terminateProcessGroup
	var calls atomic.Int32
	terminateProcessGroup = func(cmd *exec.Cmd, grace time.Duration) error {
		calls.Add(1)
		return original(cmd, grace)
	}
	t.Cleanup(func() { terminateProcessGroup = original })

	running, err := (DirectCommandRunner{CancelGrace: 10 * time.Millisecond}).Start(ctx, command.ExecSpec{
		Argv: []string{"/bin/sh", "-c", "trap '' TERM; while :; do sleep 1; done"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = running.Stdin().Close() }()
	defer func() { _ = running.Stdout().Close() }()
	defer func() { _ = running.Stderr().Close() }()

	triggerCancel()
	_, err = running.Wait(context.Background())
	if err == nil {
		t.Fatal("Wait succeeded, want cancellation error")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("terminateProcessGroup calls = %d, want 1", got)
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
	spec command.ExecSpec
}

func (r *fakeCommandRunner) Start(_ context.Context, spec command.ExecSpec) (command.RunningCommand, error) {
	r.spec = spec
	return &fakeRunningCommand{
		stdin:  discardWriteCloser{},
		stdout: io.NopCloser(strings.NewReader(`{"event":"ok"}` + "\n")),
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
