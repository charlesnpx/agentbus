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
	"github.com/charlesnpx/agentbus/engine/command"
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
	assertLog(t, fake.argv, "--print\n--output-format\nstream-json\n--verbose\n--model\nsonnet\n--effort\nmax\n--dangerously-skip-permissions\n")

	session, err = backend.Start(context.Background(), engine.SessionOpts{})
	if err != nil {
		t.Fatal(err)
	}
	events, err = session.Turn(context.Background(), engine.TurnInput{Prompt: "inspect", Write: false})
	if err != nil {
		t.Fatal(err)
	}
	_ = collect(events)
	assertLog(t, fake.argv, expectedClaudeReadOnlyArgv(""))

	session, err = backend.Resume(context.Background(), "claude-session", engine.SessionOpts{Write: true})
	if err != nil {
		t.Fatal(err)
	}
	events, err = session.Turn(context.Background(), engine.TurnInput{Prompt: "repair", Write: false})
	if err != nil {
		t.Fatal(err)
	}
	_ = collect(events)
	assertLog(t, fake.argv, expectedClaudeReadOnlyArgv("claude-session"))
	assertLog(t, fake.stdin, "repair")
}

func TestClaudeDiscoveryReportsHelpFailures(t *testing.T) {
	t.Run("command error", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "claude")
		if err := os.WriteFile(path, []byte("#!/bin/sh\necho help exploded >&2\nexit 7\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := discoverModels(context.Background(), command.DirectProbeRunner{}, path); err == nil || !strings.Contains(err.Error(), "exit status 7") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("parser miss", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "claude")
		if err := os.WriteFile(path, []byte("#!/bin/sh\necho generic help\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := discoverModels(context.Background(), command.DirectProbeRunner{}, path); err == nil || !strings.Contains(err.Error(), "parser found no model or effort") {
			t.Fatalf("err=%v", err)
		}
	})
}

func expectedClaudeReadOnlyArgv(resumeID string) string {
	args := []string{
		"--print",
		"--output-format",
		"stream-json",
		"--verbose",
		"--strict-mcp-config",
		"--mcp-config",
		`{"mcpServers":{}}`,
		"--permission-mode",
		"dontAsk",
		"--allowedTools",
		"Read,Grep,Glob,Bash(git diff*),Bash(git log*),Bash(git show*),Bash(git status*),Bash(cat*),Bash(rg*),Bash(grep*),Bash(ls*),Bash(head*),Bash(tail*),Bash(wc*)",
		"--disallowedTools",
		"Edit,Write,NotebookEdit,mcp__*,Bash(*&&*),Bash(*&*),Bash(*;*),Bash(*|*),Bash(*$(*),Bash(*`*),Bash(*<(*),Bash(*>*),Bash(*>>*),Bash(sed -i*),Bash(tee*),Bash(find*),Bash(rm*),Bash(mv*),Bash(cp*),Bash(git -c*),Bash(git --config-env*),Bash(git --paginate*),Bash(git -p*),Bash(git *--help*),Bash(*--output*),Bash(*--ext-diff*),Bash(*--textconv*),Bash(*--pre*),Bash(*--hostname-bin*),Bash(*--search-zip*),Bash(* -z*),Bash(git commit*),Bash(git push*),Bash(git checkout*),Bash(chmod*),Bash(curl*),Bash(wget*)",
	}
	if resumeID != "" {
		args = append(args, "--resume", resumeID)
	}
	return strings.Join(args, "\n") + "\n"
}

func TestClaudeSetupProbeUsesHermeticArgv(t *testing.T) {
	fake := fakeClaude(t)
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache})
	probeBackend, ok := backend.(interface {
		SetupProbe(context.Context) (engine.BackendSetupProbe, error)
	})
	if !ok {
		t.Fatal("backend does not implement SetupProbe")
	}
	probe, err := probeBackend.SetupProbe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !probe.JSONEventsProbed {
		t.Fatalf("probe = %#v", probe)
	}
	assertLog(t, fake.argv, expectedClaudeReadOnlyArgv(""))
	assertLog(t, fake.stdin, "Reply with exactly: OK")
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
	if _, err := backend.Start(context.Background(), engine.SessionOpts{Effort: "xhigh"}); err == nil || !strings.Contains(err.Error(), "unsupported effort") {
		t.Fatalf("xhigh effort err = %v", err)
	}
	restricted := New(Options{Binary: fake.bin, CachePath: fake.cache, SupportedModels: []string{"sonnet"}})
	if _, err := restricted.Start(context.Background(), engine.SessionOpts{Model: "not-a-model"}); err == nil || !strings.Contains(err.Error(), "unsupported model") {
		t.Fatalf("unsupported model err = %v", err)
	}
}

func TestClaudeDiscoveryParsesFakeHelp(t *testing.T) {
	fake := fakeClaude(t)
	script := "#!/bin/sh\nif [ \"$1\" = --help ]; then printf '%s\\n' '  --effort <level> Effort level' '    (low, medium, high, xhigh, max)' '  --model <model> Model' \"    (e.g. 'fable', 'opus', or 'sonnet')\"; exit 0; fi\nexec /bin/false\n"
	if err := os.WriteFile(fake.bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	discovery, err := New(Options{Binary: fake.bin, CachePath: fake.cache}).(interface {
		DiscoverModels(context.Context, command.ProbeRunner) (*engine.ModelDiscovery, error)
	}).DiscoverModels(context.Background(), command.DirectProbeRunner{})
	if err != nil || discovery == nil || strings.Join(discovery.Models, ",") != "fable,opus,sonnet" || strings.Join(discovery.Efforts, ",") != "high,low,max,medium,xhigh" {
		t.Fatalf("discovery=%+v err=%v", discovery, err)
	}
}

func TestClaudeDiscoveredCatalogMismatchWarnsAndProceeds(t *testing.T) {
	fake := fakeClaude(t)
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache, SupportedModels: []string{"static"}, SupportedEfforts: []string{"low"}})
	probed := probeBackendWithRunnerForTest(t, backend, fakeProbeRunner{
		version: MinimumKnownGoodVersion + "\n",
		help: strings.Join([]string{
			"  --effort <level> Effort level",
			"    (medium)",
			"  --model <model> Model",
			"    (e.g. sonnet)",
		}, "\n"),
	})
	session, err := probed.Start(context.Background(), engine.SessionOpts{Model: "not-discovered", Effort: "high"})
	if err != nil {
		t.Fatalf("discovered mismatch should pass through: %v", err)
	}
	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if got := collect(events); !containsWarning(got, "model \"not-discovered\"") || !containsWarning(got, "effort \"high\"") {
		t.Fatalf("events=%#v, want model and effort discovery warnings", got)
	}
}

func TestClaudeResultEventIsAuthoritativeResultMessage(t *testing.T) {
	events, id, err := parseEvent(map[string]any{
		"type":       "result",
		"session_id": "claude-session",
		"result":     "final answer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if id != "claude-session" || len(events) != 1 || events[0].Type != engine.EventResultMessage || events[0].Text != "final answer" {
		t.Fatalf("parse result = events:%#v id:%q", events, id)
	}
}

func TestClaudeParseReportedModel(t *testing.T) {
	events, id, err := parseEvent(map[string]any{"type": "system", "session_id": "claude-session", "model": "claude-opus-4"})
	if err != nil || id != "claude-session" || len(events) != 1 || events[0].Type != engine.EventModelReported || events[0].ModelReported != "claude-opus-4" {
		t.Fatalf("system events=%#v id=%q err=%v", events, id, err)
	}
	events, _, err = parseEvent(map[string]any{"type": "system", "model": ""})
	if err != nil || len(events) != 0 {
		t.Fatalf("empty model events=%#v err=%v", events, err)
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

	events, err = session.Turn(context.Background(), engine.TurnInput{Prompt: "sleep interrupt-turn", Write: false})
	if err != nil {
		t.Fatal(err)
	}
	// The fixture writes this turn's prompt to the stdin log strictly AFTER
	// installing its TERM trap, so observing the prompt proves the trap is
	// armed. A blind sleep raced trap installation under full-sweep load and
	// let SIGTERM kill the untrapped shell before it could record the signal.
	waitForFileContains(t, fake.stdin, "interrupt-turn")
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

type fakeProbeRunner struct {
	version string
	help    string
}

func (r fakeProbeRunner) LookPath(file string) (string, error) {
	return file, nil
}

func (r fakeProbeRunner) Run(_ context.Context, spec command.ProbeSpec) (command.ProbeResult, error) {
	if len(spec.Argv) > 1 {
		switch spec.Argv[1] {
		case "--version":
			return command.ProbeResult{Stdout: []byte(r.version)}, nil
		case "--help":
			return command.ProbeResult{Stdout: []byte(r.help)}, nil
		}
	}
	return command.ProbeResult{}, nil
}

func probeBackendWithRunnerForTest(t *testing.T, backend engine.Backend, runner command.ProbeRunner) engine.Backend {
	t.Helper()
	probeable, ok := backend.(interface {
		ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error)
	})
	if !ok {
		t.Fatal("backend does not implement ProbeBackend")
	}
	probed, err := probeable.ProbeBackend(context.Background(), runner)
	if err != nil {
		t.Fatal(err)
	}
	return probed
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
if [ "$1" = "--help" ]; then echo "claude help"; exit 0; fi
trap 'echo TERM >> "$AGENTBUS_TERM_LOG"; exit 0' TERM
: > "$AGENTBUS_ARGV_LOG"
for arg in "$@"; do printf '%s\n' "$arg" >> "$AGENTBUS_ARGV_LOG"; done
input=$(cat)
printf '%s' "$input" > "$AGENTBUS_STDIN_LOG"
case "$input" in
  *sleep*) while :; do :; done ;;
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
	raw, err := json.Marshal(engine.SetupProbeCache{Version: engine.SetupProbeCacheVersion, Backends: []engine.BackendSetupProbe{{
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

func waitForFileContains(t *testing.T, path, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil && strings.Contains(string(b), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("fixture never wrote %q to %s", want, path)
}

func containsWarning(events []engine.Event, sub string) bool {
	for _, ev := range events {
		if ev.Type == engine.EventWarning && strings.Contains(ev.Text, sub) {
			return true
		}
	}
	return false
}
