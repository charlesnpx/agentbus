package claudecli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

func TestClaudeProfilesAndParsing(t *testing.T) {
	fake := fakeClaude(t)
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache})
	session, err := backend.Start(context.Background(), engine.SessionOpts{Write: true, Model: "sonnet", Effort: "max"})
	if err != nil {
		t.Fatal(err)
	}
	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello", Write: true})
	if err != nil {
		t.Fatal(err)
	}
	got := collect(events)
	if session.ID() != "claude-session" {
		t.Fatalf("session id = %q", session.ID())
	}
	if len(got) != 2 || got[0].Type != engine.EventAgentText || got[1].Type != engine.EventToolUse {
		t.Fatalf("events = %#v", got)
	}
	assertLog(t, fake.argv, "--print\n--output-format\nstream-json\n--model\nsonnet\n--effort\nmax\n--dangerously-skip-permissions\n")

	session, err = backend.Resume(context.Background(), "claude-session", engine.SessionOpts{Write: true})
	if err != nil {
		t.Fatal(err)
	}
	events, err = session.Turn(context.Background(), engine.TurnInput{Prompt: "repair", Write: false})
	if err != nil {
		t.Fatal(err)
	}
	_ = collect(events)
	args := readLog(t, fake.argv)
	wantPrefix := "--print\n--output-format\nstream-json\n--bare\n--strict-mcp-config\n--mcp-config\n{}\n--permission-mode\ndontAsk\n--allowedTools\n"
	if !strings.HasPrefix(args, wantPrefix) {
		t.Fatalf("read-only argv prefix = %q", args)
	}
	for _, want := range []string{"Read", "Grep", "Glob", "Bash(git diff*)", "--disallowedTools", "Edit", "Write", "NotebookEdit", "mcp__*", "Bash(git push*)", "--resume\nclaude-session\n"} {
		if !strings.Contains(args, want) {
			t.Fatalf("read-only argv missing %q in %q", want, args)
		}
	}
	if strings.Contains(args, "--dangerously-skip-permissions") {
		t.Fatalf("read-only argv included bypass flag: %q", args)
	}
	assertLog(t, fake.stdin, "repair")
}

func TestClaudePreflightAndFailures(t *testing.T) {
	fake := fakeClaude(t)
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache})
	health, err := backend.Preflight(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.Version != MinimumKnownGoodVersion || health.StreamSchema != StreamSchema {
		t.Fatalf("health = %#v", health)
	}
	writeCache(t, fake.cache, "claude", fake.bin, "2.1.204", StreamSchema)
	if _, err := backend.Preflight(context.Background()); err == nil || !strings.Contains(err.Error(), "backend version changed since setup") {
		t.Fatalf("drift err = %v", err)
	}
	if _, err := backend.Start(context.Background(), engine.SessionOpts{Effort: "extreme"}); err == nil || !strings.Contains(err.Error(), "unsupported effort") {
		t.Fatalf("unsupported effort err = %v", err)
	}
	restricted := New(Options{Binary: fake.bin, CachePath: fake.cache, SupportedModels: []string{"sonnet"}})
	if _, err := restricted.Start(context.Background(), engine.SessionOpts{Model: "not-a-model"}); err == nil || !strings.Contains(err.Error(), "unsupported model") {
		t.Fatalf("unsupported model err = %v", err)
	}
}

func TestClaudeTimeoutInterruptAndTruncation(t *testing.T) {
	fake := fakeClaude(t)
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache})
	session, err := backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "huge", Write: false})
	if err != nil {
		t.Fatal(err)
	}
	got := collect(events)
	if len(got) == 0 || !got[0].Truncated {
		t.Fatalf("expected truncated event, got %#v", got)
	}

	events, err = session.Turn(context.Background(), engine.TurnInput{Prompt: "sleep", Write: false, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	got = collect(events)
	if !containsWarning(got, "timed out") {
		t.Fatalf("expected timeout warning, got %#v", got)
	}

	events, err = session.Turn(context.Background(), engine.TurnInput{Prompt: "sleep", Write: false})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := session.Interrupt(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = collect(events)
	if b, _ := os.ReadFile(fake.term); !strings.Contains(string(b), "TERM") {
		t.Fatalf("expected fake backend to receive SIGTERM, got %q", string(b))
	}
}

type fakeCLI struct {
	bin   string
	cache string
	argv  string
	stdin string
	term  string
}

func fakeClaude(t *testing.T) fakeCLI {
	t.Helper()
	dir := t.TempDir()
	f := fakeCLI{
		bin:   filepath.Join(dir, "fakeclaude"),
		cache: filepath.Join(dir, "setup-probes.json"),
		argv:  filepath.Join(dir, "argv.log"),
		stdin: filepath.Join(dir, "stdin.log"),
		term:  filepath.Join(dir, "term.log"),
	}
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then echo "2.1.205 (Claude Code)"; exit 0; fi
trap 'echo TERM >> "$AGENTBUS_TERM_LOG"; sleep 5' TERM
: > "$AGENTBUS_ARGV_LOG"
for arg in "$@"; do printf '%s\n' "$arg" >> "$AGENTBUS_ARGV_LOG"; done
input=$(cat)
printf '%s' "$input" > "$AGENTBUS_STDIN_LOG"
case "$input" in
  *sleep*) sleep 5 ;;
  *huge*) printf '{"type":"assistant","session_id":"claude-session","message":{"content":[{"type":"text","text":"'; i=0; while [ $i -lt 70000 ]; do printf a; i=$((i+1)); done; printf '"}]}}\n' ;;
  *) printf '{"type":"system","session_id":"claude-session"}\n{"type":"assistant","message":{"content":[{"type":"text","text":"hello text"},{"type":"tool_use","name":"Bash","input":{"command":"git status"}}]}}\n' ;;
esac
`
	if err := os.WriteFile(f.bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTBUS_ARGV_LOG", f.argv)
	t.Setenv("AGENTBUS_STDIN_LOG", f.stdin)
	t.Setenv("AGENTBUS_TERM_LOG", f.term)
	writeCache(t, f.cache, "claude", f.bin, MinimumKnownGoodVersion, StreamSchema)
	return f
}

func writeCache(t *testing.T, path, backend, bin, version, schema string) {
	t.Helper()
	raw, err := json.Marshal(engine.SetupProbeCache{Backends: []engine.BackendSetupProbe{{
		Backend:          backend,
		BinaryPath:       bin,
		Version:          version,
		StreamSchema:     schema,
		ConfigMode:       engine.ModeInfo{Write: "user", ReadOnly: "hermetic"},
		SandboxModes:     []string{"workspace-write", "read-only"},
		JSONEventsProbed: true,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func collect(ch <-chan engine.Event) []engine.Event {
	var out []engine.Event
	for ev := range ch {
		out = append(out, ev)
	}
	return out
}

func assertLog(t *testing.T, path, want string) {
	t.Helper()
	if got := readLog(t, path); got != want {
		t.Fatalf("%s = %q, want %q", path, got, want)
	}
}

func readLog(t *testing.T, path string) string {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(got)
}

func containsWarning(events []engine.Event, sub string) bool {
	for _, ev := range events {
		if ev.Type == engine.EventWarning && strings.Contains(ev.Text, sub) {
			return true
		}
	}
	return false
}
