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
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/adapter/internal/duplex"
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
	got = withoutTurnFinal(got)
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
	got = withoutTurnFinal(got)
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

func TestSessionTurnUsesCommandRunnerExecSpec(t *testing.T) {
	runner := &fakeCommandRunner{}
	marker := filepath.Join(t.TempDir(), "direct-exec-marker")
	backend := &Backend{
		NameValue: "fake",
		Binary:    markerCLI(t, marker),
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
	got = withoutTurnFinal(got)
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

func TestOneShotBackendRunsThroughDuplexRuntime(t *testing.T) {
	stdin := &recordingWriteCloser{}
	runner := &fakeCommandRunner{
		stdin:  stdin,
		stdout: io.NopCloser(strings.NewReader(`{"event":"agent","text":"from stdout"}` + "\n" + `{"event":"result"}` + "\n")),
	}
	backend := &Backend{
		NameValue: "fake",
		Binary:    "fake-bin",
		BuildArgs: func(string, engine.SessionOpts, engine.TurnInput) ([]string, error) {
			return []string{"run", "--json"}, nil
		},
		Parse: func(obj map[string]any) ([]engine.Event, string, error) {
			switch obj["event"] {
			case "agent":
				text, _ := obj["text"].(string)
				return []engine.Event{{Type: engine.EventAgentText, Text: text}}, "stream-session", nil
			case "result":
				return []engine.Event{{Type: engine.EventResultMessage}}, "", nil
			default:
				return nil, "", nil
			}
		},
		probed: &ProbedBackendDescriptor{StaticBackendDescriptor: StaticBackendDescriptor{
			NameValue:        "fake",
			DiscoveredModels: []string{"known-model"},
			DiscoverySource:  "test",
		}},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{Model: "new-model"})
	if err != nil {
		t.Fatal(err)
	}

	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "hello prompt"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	got = withoutTurnFinal(got)
	if len(got) != 3 {
		t.Fatalf("events = %#v, want warning, agent text, and result", got)
	}
	if got[0].Type != engine.EventWarning || !strings.Contains(got[0].Text, `model "new-model"`) {
		t.Fatalf("first event = %#v, want validation warning", got[0])
	}
	if got[1].Type != engine.EventAgentText || got[1].Text != "from stdout" {
		t.Fatalf("second event = %#v, want parsed agent text", got[1])
	}
	if got[2].Type != engine.EventResultMessage || got[2].Text != "from stdout" {
		t.Fatalf("third event = %#v, want backfilled result message", got[2])
	}
	if session.ID() != "stream-session" {
		t.Fatalf("session id = %q, want stream-session", session.ID())
	}
	written, closed := stdin.snapshot()
	if written != "hello prompt" || !closed {
		t.Fatalf("stdin = %q closed=%v, want prompt write and close", written, closed)
	}
	if strings.Join(runner.spec.Argv, "\x00") != strings.Join([]string{"fake-bin", "run", "--json"}, "\x00") {
		t.Fatalf("argv = %#v", runner.spec.Argv)
	}
}

func TestBackendWithDriverPreservesLiveProbeAndRunsDuplexTurn(t *testing.T) {
	const (
		binaryPath = "/tmp/fake-bin"
		schema     = "duplex-json-v1"
	)
	driver := newCliDuplexTestDriver("resume-1")
	runner := &duplexCommandRunner{finishOnStart: true}
	backend := &Backend{
		NameValue:      "fake",
		Binary:         "fake-bin",
		MinimumVersion: "0.1.0",
		StreamSchema:   schema,
		Driver:         driver,
	}
	probed, err := backend.ProbeBackend(context.Background(), fakeProbeRunner{path: binaryPath, version: "fake 1.0.0\n"})
	if err != nil {
		t.Fatal(err)
	}
	probedBackend, ok := probed.(*Backend)
	if !ok || probedBackend.Driver == nil {
		t.Fatalf("probed backend = %T, want *Backend with driver", probed)
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

	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	got = withoutTurnFinal(got)
	if len(got) != 2 || got[0].Type != engine.EventAgentText || got[0].Text != "duplex:hello" || got[1].Type != engine.EventResultMessage {
		t.Fatalf("events = %#v, want duplex agent text and result", got)
	}
	if session.ID() != "resume-1" {
		t.Fatalf("session id = %q, want resume-1", session.ID())
	}
	spec := runner.lastSpec()
	if strings.Join(spec.Argv, "\x00") != strings.Join([]string{"duplex-fake", "hello"}, "\x00") {
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

	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "first"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	_ = collectEventsWithTimeout(t, events, 2*time.Second)
	events, err = session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "second"}, runner)
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

func TestBackendWithDriverPrependsValidationWarning(t *testing.T) {
	driver := newCliDuplexTestDriver("resume-1")
	runner := &duplexCommandRunner{finishOnStart: true}
	backend := &Backend{
		NameValue: "fake",
		Driver:    driver,
		probed: &ProbedBackendDescriptor{StaticBackendDescriptor: StaticBackendDescriptor{
			NameValue:        "fake",
			DiscoveredModels: []string{"known-model"},
			DiscoverySource:  "test",
		}},
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{Model: "new-model"})
	if err != nil {
		t.Fatal(err)
	}

	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEventsWithTimeout(t, events, 2*time.Second)
	if len(got) < 2 || got[0].Type != engine.EventWarning || !strings.Contains(got[0].Text, `model "new-model"`) {
		t.Fatalf("events = %#v, want leading validation warning", got)
	}
	if got[1].Type != engine.EventAgentText || got[1].Text != "duplex:hello" {
		t.Fatalf("second event = %#v, want duplex agent text", got[1])
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

	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	first := receiveEventWithTimeout(t, events, 2*time.Second)
	if first.Type != engine.EventAgentText || first.Text != "duplex:hello" {
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

	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "hello"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	first := receiveEventWithTimeout(t, events, 2*time.Second)
	if first.Type != engine.EventAgentText || first.Text != "duplex:hello" {
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

	events, err := session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "first"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	first := receiveEventWithTimeout(t, events, 2*time.Second)
	if first.Type != engine.EventAgentText || first.Text != "duplex:first" {
		t.Fatalf("first event = %#v, want duplex start event", first)
	}
	_, err = session.(*Session).TurnWithRunner(context.Background(), engine.TurnInput{Prompt: "second"}, runner)
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
	got = withoutTurnFinal(got)
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
	stdin  io.WriteCloser
	stdout io.ReadCloser
}

func (r *fakeCommandRunner) Start(_ context.Context, spec command.ExecSpec) (command.RunningCommand, error) {
	r.spec = spec
	stdout := r.stdout
	if stdout == nil {
		stdout = io.NopCloser(strings.NewReader(`{"event":"ok"}` + "\n"))
	}
	stdin := r.stdin
	if stdin == nil {
		stdin = discardWriteCloser{}
	}
	return &fakeRunningCommand{
		stdin:  stdin,
		stdout: stdout,
		stderr: io.NopCloser(strings.NewReader("")),
	}, nil
}

type recordingWriteCloser struct {
	mu     sync.Mutex
	buf    strings.Builder
	closed bool
}

func (w *recordingWriteCloser) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *recordingWriteCloser) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func (w *recordingWriteCloser) snapshot() (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String(), w.closed
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

type fakeProbeRunner struct {
	path    string
	version string
}

func (r fakeProbeRunner) LookPath(binary string) (string, error) {
	if r.path != "" {
		return r.path, nil
	}
	return binary, nil
}

func (r fakeProbeRunner) Run(context.Context, command.ProbeSpec) (command.ProbeResult, error) {
	version := r.version
	if version == "" {
		version = "fake 1.0.0\n"
	}
	return command.ProbeResult{Stdout: []byte(version)}, nil
}
