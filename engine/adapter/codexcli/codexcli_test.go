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
	"github.com/charlesnpx/agentbus/engine/command"
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
	if len(got) != 4 || got[0].Type != engine.EventAgentText || got[1].Type != engine.EventToolUse || got[2].Type != engine.EventAgentText || got[3].Type != engine.EventResultMessage {
		t.Fatalf("events = %#v", got)
	}
	if got[3].Text != "final answer" {
		t.Fatalf("terminal result = %q", got[3].Text)
	}
	assertLog(t, fake.argv, "exec\n--json\n--model\ngpt-5\n--sandbox\nworkspace-write\n--config\nmodel_reasoning_effort=\"high\"\n-\n")
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

func TestCodexTrustCheckFlagMatchesCWD(t *testing.T) {
	gitRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(gitRoot, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	nestedGitDir := filepath.Join(gitRoot, "nested", "workspace")
	if err := os.MkdirAll(nestedGitDir, 0o700); err != nil {
		t.Fatal(err)
	}

	worktreeRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(worktreeRoot, ".git"), []byte("gitdir: /tmp/worktree.git\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		cwd      string
		resume   bool
		write    bool
		wantSkip bool
	}{
		{name: "git repository write", cwd: gitRoot, write: true},
		{name: "nested git repository read-only", cwd: nestedGitDir},
		{name: "git file worktree", cwd: worktreeRoot},
		{name: "non-git write", cwd: t.TempDir(), write: true, wantSkip: true},
		{name: "non-git read-only", cwd: t.TempDir(), wantSkip: true},
		{name: "non-git resume", cwd: t.TempDir(), resume: true, wantSkip: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := fakeCodex(t)
			backend := New(Options{Binary: fake.bin, CachePath: fake.cache})
			opts := engine.SessionOpts{CWD: test.cwd, Write: test.write}
			var session engine.Session
			var err error
			if test.resume {
				session, err = backend.Resume(context.Background(), "codex-session", opts)
			} else {
				session, err = backend.Start(context.Background(), opts)
			}
			if err != nil {
				t.Fatal(err)
			}
			events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello", Write: test.write})
			if err != nil {
				t.Fatal(err)
			}
			_ = collect(events)
			argv, err := os.ReadFile(fake.argv)
			if err != nil {
				t.Fatal(err)
			}
			gotSkip := strings.Contains(string(argv), "--skip-git-repo-check\n")
			if gotSkip != test.wantSkip {
				t.Fatalf("argv = %q, skip flag = %t, want %t", string(argv), gotSkip, test.wantSkip)
			}
			if test.resume && !strings.Contains(string(argv), "resume\ncodex-session\n-\n") {
				t.Fatalf("resume argv = %q", string(argv))
			}
		})
	}
}

func TestCodexTrustCheckFlagUsesProcessCWDWhenUnset(t *testing.T) {
	original, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(original); err != nil {
			t.Fatal(err)
		}
	})
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	args, err := buildArgs("", engine.SessionOpts{}, engine.TurnInput{})
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(args, "--skip-git-repo-check") {
		t.Fatalf("args = %#v, want non-git process CWD trust-check bypass", args)
	}
}

func TestCodexSetupProbeUsesStdinPromptArg(t *testing.T) {
	fake := fakeCodex(t)
	writeModelsCache(t, filepath.Dir(fake.bin), `{
  "fetched_at": "2026-07-10T12:00:00Z",
  "client_version": "0.142.0",
  "models": [{"slug":"gpt-5.4","visibility":"list","supported_reasoning_levels":[{"effort":"high"}]}]
}`)
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
	if probe.DiscoverySource != "models_cache" || probe.DiscoveryFetchedAt != "2026-07-10T12:00:00Z" || probe.DiscoveryClientVersion != "0.142.0" || !containsString(probe.DiscoveryWarnings, "client_version") {
		t.Fatalf("discovery provenance = %#v", probe)
	}
	assertLog(t, fake.argv, "exec\n--json\n--sandbox\nread-only\n--ignore-user-config\n-\n")
	assertLog(t, fake.stdin, "Reply with exactly: OK")
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

func TestCodexDiscoveryUsesModelsCacheRatherThanHelpText(t *testing.T) {
	fake := fakeCodex(t)
	script := "#!/bin/sh\nif [ \"$1\" = --help ]; then echo \"-m, --model <MODEL>  Model the agent should use\"; echo \"possible values: c, disk-full-read-access, o3\"; exit 0; fi\nexec /bin/false\n"
	if err := os.WriteFile(fake.bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	discovery, err := New(Options{Binary: fake.bin, CachePath: fake.cache}).(interface {
		DiscoverModels(context.Context, command.ProbeRunner) (*engine.ModelDiscovery, error)
	}).DiscoverModels(context.Background(), command.DirectProbeRunner{})
	if discovery != nil || err == nil || !strings.Contains(err.Error(), "models cache") {
		t.Fatalf("discovery=%+v err=%v", discovery, err)
	}
}

func TestCodexDiscoveryReadsModelsCache(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	writeModelsCache(t, home, `{
  "fetched_at": "2026-07-11T12:00:00Z",
  "client_version": "0.143.0",
  "unexpected": "ignored",
  "models": [
    {"slug":"gpt-5.4","display_name":"GPT-5.4","visibility":"list","supported_reasoning_levels":[{"effort":"high"},{"effort":"low"},{"effort":"turbo"}]},
    {"slug":"codex-auto-review","visibility":"hidden","supported_reasoning_levels":[{"effort":"ultra"}]},
    {"slug":"","visibility":"hidden"},
    {"slug":"  ","visibility":"list"},
    {"slug":"gpt-5.3","visibility":"list","supported_reasoning_levels":[{"effort":"none"},{"effort":"max"},{"effort":"turbo"},{"effort":"custom"}]}
  ]
}`)
	discovery, err := discoverModels(context.Background(), command.DirectProbeRunner{}, "unused")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(discovery.Models, ","), "gpt-5.4,gpt-5.3"; got != want {
		t.Fatalf("models = %q, want %q", got, want)
	}
	if got, want := strings.Join(discovery.Efforts, ","), "none,low,high,max,turbo,custom"; got != want {
		t.Fatalf("efforts = %q, want %q", got, want)
	}
	if discovery.Source != "models_cache" || discovery.FetchedAt != "2026-07-11T12:00:00Z" || discovery.ClientVersion != "0.143.0" {
		t.Fatalf("discovery provenance = %#v", discovery)
	}
	if !containsString(discovery.Warnings, "empty slug") {
		t.Fatalf("warnings = %#v, want empty-slug skip warning", discovery.Warnings)
	}
}

func TestCodexDiscoveryReportsMissingMalformedAndStaleCache(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", home)
	if discovery, err := discoverModels(context.Background(), command.DirectProbeRunner{}, "unused"); discovery != nil || err == nil || !strings.Contains(err.Error(), "models cache") {
		t.Fatalf("missing discovery=%+v err=%v", discovery, err)
	}
	writeModelsCache(t, home, `{not-json`)
	if discovery, err := discoverModels(context.Background(), command.DirectProbeRunner{}, "unused"); discovery != nil || err == nil || !strings.Contains(err.Error(), "parse models cache") {
		t.Fatalf("malformed discovery=%+v err=%v", discovery, err)
	}
	writeModelsCache(t, home, `{"fetched_at":"2020-01-01T00:00:00Z","models":[{"slug":"gpt-5.4","visibility":"list"}]}`)
	discovery, err := discoverModels(context.Background(), command.DirectProbeRunner{}, "unused")
	if err != nil || !containsString(discovery.Warnings, "older than 7 days") {
		t.Fatalf("stale discovery=%+v err=%v", discovery, err)
	}
}

func TestCodexSetupProbeReportsModelsCacheFailuresWithoutFailing(t *testing.T) {
	fake := fakeCodex(t)
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache})
	probeBackend := backend.(interface {
		SetupProbe(context.Context) (engine.BackendSetupProbe, error)
	})
	probe, err := probeBackend.SetupProbe(context.Background())
	if err != nil || len(probe.DiscoveredModels) != 0 || len(probe.DiscoveredEfforts) != 0 || !containsString(probe.DiscoveryWarnings, "read models cache") {
		t.Fatalf("missing cache probe=%#v err=%v", probe, err)
	}
	writeModelsCache(t, filepath.Dir(fake.bin), `{not-json`)
	probe, err = probeBackend.SetupProbe(context.Background())
	if err != nil || len(probe.DiscoveredModels) != 0 || len(probe.DiscoveredEfforts) != 0 || !containsString(probe.DiscoveryWarnings, "parse models cache") {
		t.Fatalf("malformed cache probe=%#v err=%v", probe, err)
	}
}

func TestCodexDiscoveredCacheWinsAndLegacyFallsBack(t *testing.T) {
	fake := fakeCodex(t)
	writeModelsCache(t, filepath.Dir(fake.bin), `{
  "fetched_at": "2026-07-11T12:00:00Z",
  "client_version": "0.143.0",
  "models": [{"slug":"discovered","visibility":"list","supported_reasoning_levels":[{"effort":"turbo"}]}]
}`)
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache, SupportedModels: []string{"static"}, SupportedEfforts: []string{"low"}})
	probed := probeBackendForTest(t, backend)
	if _, err := probed.Start(context.Background(), engine.SessionOpts{Model: "discovered", Effort: "turbo"}); err != nil {
		t.Fatal(err)
	}
	session, err := probed.Start(context.Background(), engine.SessionOpts{Model: "static", Effort: "low"})
	if err != nil {
		t.Fatalf("discovered mismatch should pass through: %v", err)
	}
	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if got := collect(events); !containsWarning(got, "not in the discovered") {
		t.Fatalf("events=%#v, want discovered-catalog warning", got)
	}
	if _, err := backend.Start(context.Background(), engine.SessionOpts{Model: "discovered"}); err == nil || !strings.Contains(err.Error(), "unsupported model") {
		t.Fatalf("unprobed backend should use static validation, err=%v", err)
	}
	v1 := fmt.Sprintf(`{"version":1,"backends":[{"backend":"codex","binaryPath":%q,"version":%q,"streamSchema":%q,"jsonEventsProbed":true}]}`, fake.bin, MinimumKnownGoodVersion, StreamSchema)
	if err := os.WriteFile(fake.cache, []byte(v1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Start(context.Background(), engine.SessionOpts{Model: "static", Effort: "low"}); err != nil {
		t.Fatalf("legacy fallback: %v", err)
	}
	health, err := backend.Preflight(context.Background())
	if err == nil || !strings.Contains(err.Error(), "re-run agentbus setup") {
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

func TestCodexEmptyDiscoveredCatalogFallsBackToStaticEnforcement(t *testing.T) {
	fake := fakeCodex(t)
	writeModelsCache(t, filepath.Dir(fake.bin), `{"fetched_at":"2026-07-11T12:00:00Z","client_version":"0.143.0","models":[]}`)
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache})
	probed := probeBackendForTest(t, backend)
	if _, err := probed.Start(context.Background(), engine.SessionOpts{Effort: "turbo"}); err == nil || !strings.Contains(err.Error(), "unsupported effort") {
		t.Fatalf("empty discovered catalog must fall back to static effort enforcement, err=%v", err)
	}
	if _, err := probed.Start(context.Background(), engine.SessionOpts{Effort: "xhigh"}); err != nil {
		t.Fatalf("static effort should pass: %v", err)
	}
}

func TestCodexSetupProbeCacheProvenanceRoundTripAndLegacyRequiresSetup(t *testing.T) {
	fake := fakeCodex(t)
	cache := engine.SetupProbeCache{Backends: []engine.BackendSetupProbe{{
		Backend:                "codex",
		BinaryPath:             fake.bin,
		Version:                MinimumKnownGoodVersion,
		StreamSchema:           StreamSchema,
		DiscoverySource:        "models_cache",
		DiscoveryFetchedAt:     "2026-07-11T12:00:00Z",
		DiscoveryClientVersion: MinimumKnownGoodVersion,
		DiscoveredModels:       []string{"gpt-5.4"},
		DiscoveredEfforts:      []string{"medium", "high"},
		DiscoveryWarnings:      []string{"cache is stale"},
	}}}
	if err := engine.WriteSetupProbeCache(fake.cache, cache); err != nil {
		t.Fatal(err)
	}
	roundTrip, err := engine.ReadSetupProbeCache(fake.cache)
	if err != nil {
		t.Fatal(err)
	}
	if roundTrip.Version != engine.SetupProbeCacheVersion || len(roundTrip.Backends) != 1 || roundTrip.Backends[0].DiscoveryFetchedAt != "2026-07-11T12:00:00Z" || roundTrip.Backends[0].DiscoveryClientVersion != MinimumKnownGoodVersion {
		t.Fatalf("cache round trip = %#v", roundTrip)
	}

	legacy := fmt.Sprintf(`{"version":2,"backends":[{"backend":"codex","binaryPath":%q,"version":%q,"streamSchema":%q}]}`, fake.bin, MinimumKnownGoodVersion, StreamSchema)
	if err := os.WriteFile(fake.cache, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache})
	if _, err := backend.Preflight(context.Background()); err == nil || !strings.Contains(err.Error(), "re-run agentbus setup") {
		t.Fatalf("legacy cache preflight err = %v", err)
	}
}

func TestCodexUpgradedVersionFallsBackAndWarnsOnTurn(t *testing.T) {
	fake := fakeCodex(t)
	backend := New(Options{Binary: fake.bin, CachePath: fake.cache, SupportedModels: []string{"static"}})
	if _, err := backend.Start(context.Background(), engine.SessionOpts{Model: "old-discovered"}); err == nil {
		t.Fatal("unprobed discovered model unexpectedly accepted")
	}
	session, err := backend.Start(context.Background(), engine.SessionOpts{Model: "static"})
	if err != nil {
		t.Fatal(err)
	}
	events, err := session.Turn(context.Background(), engine.TurnInput{Prompt: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if got := collect(events); containsWarning(got, "stale") {
		t.Fatalf("events=%#v, want no stale discovery warning from unprobed validation", got)
	}
}

func TestCodexLegacyAliasResultMessage(t *testing.T) {
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

func TestCodexDottedEventSchema(t *testing.T) {
	events, id, err := parseEvent(map[string]any{
		"type":      "thread.started",
		"thread_id": "codex-session",
	})
	if err != nil {
		t.Fatal(err)
	}
	if id != "codex-session" || len(events) != 0 {
		t.Fatalf("parse thread.started = events:%#v id:%q", events, id)
	}

	events, id, err = parseEvent(map[string]any{
		"type": "item.completed",
		"item": map[string]any{
			"type": "agent_message",
			"text": "stream text",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != engine.EventAgentText || events[0].Text != "stream text" {
		t.Fatalf("parse agent item.completed = events:%#v id:%q", events, id)
	}

	events, _, err = parseEvent(map[string]any{
		"type": "item.completed",
		"item": map[string]any{
			"type":    "command_execution",
			"command": "git status",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Type != engine.EventToolUse || events[0].Text != "git status" {
		t.Fatalf("parse tool item.completed = %#v", events)
	}
}

func TestCodexParseReportedModel(t *testing.T) {
	events, id, err := parseEvent(map[string]any{"type": "session_configured", "thread_id": "codex-session", "model": "gpt-5.4"})
	if err != nil || id != "codex-session" || len(events) != 1 || events[0].Type != engine.EventModelReported || events[0].ModelReported != "gpt-5.4" {
		t.Fatalf("session_configured events=%#v id=%q err=%v", events, id, err)
	}
	events, _, err = parseEvent(map[string]any{"type": "thread.started", "model": ""})
	if err != nil || len(events) != 0 {
		t.Fatalf("empty model events=%#v err=%v", events, err)
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

func probeBackendForTest(t *testing.T, backend engine.Backend) engine.Backend {
	t.Helper()
	probeable, ok := backend.(interface {
		ProbeBackend(context.Context, command.ProbeRunner) (engine.Backend, error)
	})
	if !ok {
		t.Fatal("backend does not implement ProbeBackend")
	}
	probed, err := probeable.ProbeBackend(context.Background(), command.DirectProbeRunner{})
	if err != nil {
		t.Fatal(err)
	}
	return probed
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
if [ "$1" = "--help" ]; then echo "codex help"; exit 0; fi
trap 'echo TERM >> "$AGENTBUS_TERM_LOG"; exit 0' TERM
: > "$AGENTBUS_ARGV_LOG"
for arg in "$@"; do printf '%s\n' "$arg" >> "$AGENTBUS_ARGV_LOG"; done
input=$(cat)
printf '%s' "$input" > "$AGENTBUS_STDIN_LOG"
last=
for arg in "$@"; do last=$arg; done
if [ "$1" = "exec" ] && [ "$last" != "-" ]; then exit 0; fi
case "$input" in
  *sleep*) while :; do :; done ;;
  *huge*) printf '{"type":"thread.started","thread_id":"codex-session"}\n{"type":"item.completed","item":{"type":"agent_message","text":"'; i=0; while [ $i -lt 70000 ]; do printf a; i=$((i+1)); done; printf '"}}\n{"type":"turn.completed"}\n' ;;
  *) printf '{"type":"thread.started","thread_id":"codex-session"}\n{"type":"turn.started"}\n{"type":"item.completed","item":{"type":"agent_message","text":"hello text"}}\n{"type":"item.completed","item":{"type":"command_execution","command":"git status"}}\n{"type":"item.updated","item":{"type":"todo_list"}}\n{"type":"item.completed","item":{"type":"agent_message","text":"final answer"}}\n{"type":"turn.completed","usage":{"input_tokens":15110}}\n' ;;
esac
`
	if err := os.WriteFile(f.bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTBUS_ARGV_LOG", f.argv)
	t.Setenv("AGENTBUS_STDIN_LOG", f.stdin)
	t.Setenv("AGENTBUS_TERM_LOG", f.term)
	t.Setenv("CODEX_HOME", dir)
	writeCache(t, f.cache, "codex", f.bin, MinimumKnownGoodVersion, StreamSchema)
	return f
}

func writeModelsCache(t *testing.T, home, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(home, "models_cache.json"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
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

func containsString(values []string, sub string) bool {
	for _, value := range values {
		if strings.Contains(value, sub) {
			return true
		}
	}
	return false
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
