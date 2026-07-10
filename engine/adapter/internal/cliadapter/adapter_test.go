package cliadapter

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
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
	session := &Session{active: &exec.Cmd{Process: &os.Process{Pid: os.Getpid()}}}
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
	if len(got) != 1 || got[0].Type != engine.EventTerminalError || !strings.Contains(got[0].Text, "malformed backend stream") {
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
	if len(got) != 1 || got[0].Type != engine.EventTerminalError || !strings.Contains(got[0].Text, "backend exploded") {
		t.Fatalf("events = %#v, want terminal nonzero-exit error", got)
	}
}

func fakeTerminalErrorCLI(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fakecli")
	script := `#!/bin/sh
case "$1" in
  malformed) printf 'not-json\n'; exit 0 ;;
  fail) printf 'backend exploded\n' >&2; exit 7 ;;
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
