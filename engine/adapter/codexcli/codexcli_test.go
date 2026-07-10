package codexcli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/charlesnpx/agentbus/engine"
)

func TestCodexProfilesAndParsing(t *testing.T) {
	fake := fakeCodex(t)
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache})
	session, err := backend.Start(context.Background(), engine.SessionOpts{Write: true, Model: "gpt-5", Effort: "high"})
	if err != nil {
		t.Fatal(err)
	}
	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello", Write: true})
	if err != nil {
		t.Fatal(err)
	}
	got := collect(events)
	if session.ID() != "codex-session" {
		t.Fatalf("session id = %q", session.ID())
	}
	if len(got) != 3 || got[0].Type != engine.EventAgentText || got[1].Type != engine.EventToolUse || got[2].Type != engine.EventResultMessage {
		t.Fatalf("events = %#v", got)
	}
	if got[2].Text != "final answer" {
		t.Fatalf("terminal result = %q", got[2].Text)
	}
	assertLog(t, fake.argv, "exec\n--json\n--model\ngpt-5\n--sandbox\nworkspace-write\n--config\nmodel_reasoning_effort=\"high\"\n")
	assertLog(t, fake.stdin, "hello")

	session, err = backend.Resume(context.Background(), "codex-session", engine.SessionOpts{Write: true})
	if err != nil {
		t.Fatal(err)
	}
	events, err = session.Turn(context.Background(), engine.TurnInput{Prompt: "repair", Write: false})
	if err != nil {
		t.Fatal(err)
	}
	_ = collect(events)
	assertLog(t, fake.argv, "exec\n--json\n--sandbox\nread-only\n--ignore-user-config\nresume\ncodex-session\n-\n")
	assertLog(t, fake.stdin, "repair")
}

func TestCodexPreflightAndFailures(t *testing.T) {
	fake := fakeCodex(t)
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache})
	health, err := backend.Preflight(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if health.Version != MinimumKnownGoodVersion || health.StreamSchema != StreamSchema {
		t.Fatalf("health = %#v", health)
	}
	writeCache(t, fake.cache, "codex", fake.bin, "0.142.0", StreamSchema)
	if _, err := backend.Preflight(context.Background()); err == nil || !strings.Contains(err.Error(), "backend version changed since setup") {
		t.Fatalf("drift err = %v", err)
	}
	if _, err := backend.Start(context.Background(), engine.SessionOpts{Effort: "xhigh"}); err != nil {
		t.Fatalf("xhigh effort err = %v", err)
	}
	if _, err := backend.Start(context.Background(), engine.SessionOpts{Effort: "extreme"}); err == nil || !strings.Contains(err.Error(), "unsupported effort") {
		t.Fatalf("unsupported effort err = %v", err)
	}
	restricted := New(Options{Binary: fake.bin, CachePath: fake.cache, SupportedModels: []string{"gpt-5"}})
	if _, err := restricted.Start(context.Background(), engine.SessionOpts{Model: "not-a-model"}); err == nil || !strings.Contains(err.Error(), "unsupported model") {
		t.Fatalf("unsupported model err = %v", err)
	}
}

func TestCodexDiscoveryParsesFakeHelp(t *testing.T) {
	fake := fakeCodex(t)
	script := "#!/bin/sh\nif [ \"$1\" = --help ]; then echo 'Models available: [gpt-5.4, gpt-5.5]'; echo 'Reasoning effort possible values: [low, high, xhigh]'; exit 0; fi\nexec /bin/false\n"
	if err := os.WriteFile(fake.bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	discovery, err := New(Options{Binary: fake.bin, CachePath: fake.cache}).(engine.ModelDiscoverer).DiscoverModels(context.Background())
	if err != nil || discovery == nil || strings.Join(discovery.Models, ",") != "gpt-5.4,gpt-5.5" || strings.Join(discovery.Efforts, ",") != "high,low,xhigh" {
		t.Fatalf("discovery=%+v err=%v", discovery, err)
	}
}

func TestCodexDiscoveryReportsParserMiss(t *testing.T) {
	path := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(path, []byte("#!/bin/sh\necho generic help\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := discoverModels(context.Background(), path); err == nil || !strings.Contains(err.Error(), "parser found no model or effort") {
		t.Fatalf("err=%v", err)
	}
}

func TestCodexDiscoveredCacheWinsAndLegacyFallsBack(t *testing.T) {
	fake := fakeCodex(t)
	cache := engine.SetupProbeCache{Version: engine.SetupProbeCacheVersion, Backends: []engine.BackendSetupProbe{{Backend: "codex", BinaryPath: fake.bin, Version: MinimumKnownGoodVersion, StreamSchema: StreamSchema, DiscoveredModels: []string{"discovered"}, DiscoveredEfforts: []string{"turbo"}}}}
	if err := engine.WriteSetupProbeCache(fake.cache, cache); err != nil {
		t.Fatal(err)
	}
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache, SupportedModels: []string{"static"}, SupportedEfforts: []string{"low"}})
	if _, err := backend.Start(context.Background(), engine.SessionOpts{Model: "discovered", Effort: "turbo"}); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Start(context.Background(), engine.SessionOpts{Model: "static"}); err == nil {
		t.Fatal("static model unexpectedly beat discovered cache")
	}
	v1 := fmt.Sprintf(`{"version":1,"backends":[{"backend":"codex","binaryPath":%q,"version":%q,"streamSchema":%q,"jsonEventsProbed":true}]}`, fake.bin, MinimumKnownGoodVersion, StreamSchema)
	if err := os.WriteFile(fake.cache, []byte(v1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Start(context.Background(), engine.SessionOpts{Model: "static", Effort: "low"}); err != nil {
		t.Fatalf("legacy fallback: %v", err)
	}
	health, err := backend.Preflight(context.Background())
	if err != nil || !strings.Contains(health.Warning, "stale") {
		t.Fatalf("health=%+v err=%v", health, err)
	}
	legacy, err := engine.ReadSetupProbeCache(fake.cache)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.WriteSetupProbeCache(fake.cache, legacy); err != nil {
		t.Fatal(err)
	}
	migrated, err := engine.ReadSetupProbeCache(fake.cache)
	if err != nil || migrated.Version != engine.SetupProbeCacheVersion {
		t.Fatalf("migrated=%+v err=%v", migrated, err)
	}
}

func TestCodexUpgradedVersionFallsBackAndWarnsOnTurn(t *testing.T) {
	fake := fakeCodex(t)
	cache := engine.SetupProbeCache{Version: engine.SetupProbeCacheVersion, Backends: []engine.BackendSetupProbe{{Backend: "codex", BinaryPath: fake.bin, Version: "0.142.0", StreamSchema: StreamSchema, DiscoveredModels: []string{"old-discovered"}}}}
	if err := engine.WriteSetupProbeCache(fake.cache, cache); err != nil {
		t.Fatal(err)
	}
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache, SupportedModels: []string{"static"}})
	if _, err := backend.Start(context.Background(), engine.SessionOpts{Model: "old-discovered"}); err == nil {
		t.Fatal("stale discovered model unexpectedly accepted")
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{Model: "static"})
	if err != nil {
		t.Fatal(err)
	}
	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if got := collect(events); !containsWarning(got, "stale") {
		t.Fatalf("events=%#v, want stale discovery warning", got)
	}
}

func TestCodexTaskCompleteIsAuthoritativeResultMessage(t *testing.T) {
	events, id, err := parseEvent(map[string]any{
		"type":               "task_complete",
		"session_id":         "codex-session",
		"last_agent_message": "final answer",
	})
	if err != nil {
		t.Fatal(err)
	}
	if id != "codex-session" || len(events) != 1 || events[0].Type != engine.EventResultMessage || events[0].Text != "final answer" {
		t.Fatalf("parse task_complete = events:%#v id:%q", events, id)
	}
}

func TestCodexCurrentEventNames(t *testing.T) {
	events, id, err := parseEvent(map[string]any{
		"type":       "agent_message",
		"session_id": "codex-session",
		"message":    "stream text",
	})
	if err != nil {
		t.Fatal(err)
	}
	if id != "codex-session" || len(events) != 1 || events[0].Type != engine.EventAgentText || events[0].Text != "stream text" {
		t.Fatalf("parse agent_message = events:%#v id:%q", events, id)
	}

	events, id, err = parseEvent(map[string]any{
		"type":       "item_completed",
		"session_id": "codex-session",
		"item": map[string]any{
			"type":    "local_shell_call",
			"command": "git status",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if id != "codex-session" || len(events) != 1 || events[0].Type != engine.EventToolUse || events[0].Text != "git status" {
		t.Fatalf("parse item_completed = events:%#v id:%q", events, id)
	}
}

func TestCodexTimeoutInterruptAndTruncation(t *testing.T) {
	fake := fakeCodex(t)
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

func fakeCodex(t *testing.T) fakeCLI {
	t.Helper()
	dir := t.TempDir()
	f := fakeCLI{
		bin:   filepath.Join(dir, "fakecodex"),
		cache: filepath.Join(dir, "setup-probes.json"),
		argv:  filepath.Join(dir, "argv.log"),
		stdin: filepath.Join(dir, "stdin.log"),
		term:  filepath.Join(dir, "term.log"),
	}
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then echo "codex-cli 0.143.0"; exit 0; fi
trap 'echo TERM >> "$AGENTBUS_TERM_LOG"; exit 0' TERM
: > "$AGENTBUS_ARGV_LOG"
for arg in "$@"; do printf '%s\n' "$arg" >> "$AGENTBUS_ARGV_LOG"; done
input=$(cat)
printf '%s' "$input" > "$AGENTBUS_STDIN_LOG"
case "$input" in
  *sleep*) while :; do :; done ;;
  *huge*) printf '{"type":"agent_message","session_id":"codex-session","message":"'; i=0; while [ $i -lt 70000 ]; do printf a; i=$((i+1)); done; printf '"}\n' ;;
  *) printf '{"type":"task_started","session_id":"codex-session"}\n{"type":"agent_message","session_id":"codex-session","message":"hello text"}\n{"type":"item_completed","session_id":"codex-session","item":{"type":"local_shell_call","command":"git status"}}\n{"type":"task_complete","session_id":"codex-session","last_agent_message":"final answer"}\n' ;;
esac
`
	if err := os.WriteFile(f.bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTBUS_ARGV_LOG", f.argv)
	t.Setenv("AGENTBUS_STDIN_LOG", f.stdin)
	t.Setenv("AGENTBUS_TERM_LOG", f.term)
	writeCache(t, f.cache, "codex", f.bin, MinimumKnownGoodVersion, StreamSchema)
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
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("%s = %q, want %q", path, string(got), want)
	}
}

func containsWarning(events []engine.Event, sub string) bool {
	for _, ev := range events {
		if ev.Type == engine.EventWarning && strings.Contains(ev.Text, sub) {
			return true
		}
	}
	return false
}
