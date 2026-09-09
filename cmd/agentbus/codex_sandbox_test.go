package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestConfigureCodexSandboxFixtures(t *testing.T) {
	for _, tc := range []struct {
		name              string
		initial           *string
		wantAction        string
		wantRawFragments  []string
		wantByteIdentical bool
		wantCRLF          bool
	}{
		{
			name:             "missing file",
			wantAction:       codexSandboxConfigured,
			wantRawFragments: []string{"[sandbox_workspace_write]", "writable_roots = ["},
		},
		{
			name:             "empty file",
			initial:          stringPtr(""),
			wantAction:       codexSandboxConfigured,
			wantRawFragments: []string{"[sandbox_workspace_write]", "writable_roots = ["},
		},
		{
			name:       "unrelated content and comments",
			initial:    stringPtr("# Preserve this user comment\nmodel = \"gpt-5\"\n\n[profiles.default]\nsandbox_mode = \"workspace-write\" # And this one\n"),
			wantAction: codexSandboxConfigured,
			wantRawFragments: []string{
				"# Preserve this user comment",
				"sandbox_mode = \"workspace-write\" # And this one",
			},
		},
		{
			name:             "existing section with one root",
			initial:          stringPtr("[sandbox_workspace_write]\n# Keep this comment.\nwritable_roots = [\n  \"STATE/agentbus\" # Keep this array comment.\n]\n"),
			wantAction:       codexSandboxConfigured,
			wantRawFragments: []string{"# Keep this comment.", "# Keep this array comment."},
		},
		{
			name:              "all roots are a byte-identical no-op",
			initial:           stringPtr("# This file must remain byte-for-byte unchanged.\n[sandbox_workspace_write]\nwritable_roots = [\n  \"STATE/agentbus\",\n  \"CACHE/agentbus\",\n  \"STATE/delegate\",\n]\n"),
			wantAction:        codexSandboxAlreadyConfigured,
			wantByteIdentical: true,
		},
		{
			name:       "CRLF existing section",
			initial:    stringPtr("[sandbox_workspace_write]\r\nwritable_roots = [\r\n  \"STATE/agentbus\",\r\n]\r\n[profiles.default]\r\nsandbox_mode = \"workspace-write\"\r\n"),
			wantAction: codexSandboxConfigured,
			wantCRLF:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			paths := codexSandboxPaths{
				ConfigPath: filepath.Join(tmp, ".codex", "config.toml"),
				WritableRoots: []string{
					filepath.Join(tmp, "state", "agentbus"),
					filepath.Join(tmp, "cache", "agentbus"),
					filepath.Join(tmp, "state", "delegate"),
				},
			}
			initial := ""
			if tc.initial != nil {
				initial = strings.ReplaceAll(*tc.initial, "STATE", filepath.Join(tmp, "state"))
				initial = strings.ReplaceAll(initial, "CACHE", filepath.Join(tmp, "cache"))
				if err := os.MkdirAll(filepath.Dir(paths.ConfigPath), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(paths.ConfigPath, []byte(initial), 0o640); err != nil {
					t.Fatal(err)
				}
			}

			result := configureCodexSandboxAt(paths)
			if result.Action != tc.wantAction {
				t.Fatalf("action = %q, want %q; warning=%q", result.Action, tc.wantAction, result.Warning)
			}
			raw, err := os.ReadFile(paths.ConfigPath)
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantByteIdentical && !bytes.Equal(raw, []byte(initial)) {
				t.Fatalf("already-configured config changed:\n got %q\nwant %q", raw, initial)
			}
			if tc.wantCRLF && bytes.Contains(bytes.ReplaceAll(raw, []byte("\r\n"), nil), []byte("\n")) {
				t.Fatalf("CRLF config gained a bare LF:\n%s", raw)
			}
			if tc.initial != nil {
				info, err := os.Stat(paths.ConfigPath)
				if err != nil {
					t.Fatal(err)
				}
				if got, want := info.Mode().Perm(), os.FileMode(0o640); got != want {
					t.Fatalf("config mode = %o, want preserved %o", got, want)
				}
			}
			for _, fragment := range tc.wantRawFragments {
				if !strings.Contains(string(raw), fragment) {
					t.Fatalf("config missing preserved fragment %q:\n%s", fragment, raw)
				}
			}
			parsed, err := decodeCodexSandboxConfig(raw)
			if err != nil {
				t.Fatalf("result does not parse: %v\n%s", err, raw)
			}
			if !allWritableRootsPresent(parsed.SandboxWorkspaceWrite.WritableRoots, paths.WritableRoots) {
				t.Fatalf("writable_roots = %#v, want all %#v", parsed.SandboxWorkspaceWrite.WritableRoots, paths.WritableRoots)
			}
		})
	}
}

func TestConfigureCodexSandboxUpdatesSymlinkTargetWithoutReplacingLink(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target", "config.toml")
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("model = \"gpt-5\"\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	paths := codexSandboxPaths{
		ConfigPath: filepath.Join(tmp, ".codex", "config.toml"),
		WritableRoots: []string{
			filepath.Join(tmp, "state", "agentbus"),
			filepath.Join(tmp, "cache", "agentbus"),
			filepath.Join(tmp, "state", "delegate"),
		},
	}
	if err := os.MkdirAll(filepath.Dir(paths.ConfigPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, paths.ConfigPath); err != nil {
		t.Skipf("symlinks are unavailable: %v", err)
	}

	result := configureCodexSandboxAt(paths)
	if result.Action != codexSandboxConfigured {
		t.Fatalf("action = %q, want configured; warning=%q", result.Action, result.Warning)
	}
	info, err := os.Lstat(paths.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("config path mode = %v, want symlink", info.Mode())
	}
	linkTarget, err := os.Readlink(paths.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if linkTarget != target {
		t.Fatalf("symlink target = %q, want %q", linkTarget, target)
	}
	raw, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := decodeCodexSandboxConfig(raw)
	if err != nil {
		t.Fatalf("target does not parse: %v\n%s", err, raw)
	}
	if !allWritableRootsPresent(parsed.SandboxWorkspaceWrite.WritableRoots, paths.WritableRoots) {
		t.Fatalf("target writable_roots = %#v, want %#v", parsed.SandboxWorkspaceWrite.WritableRoots, paths.WritableRoots)
	}
	targetInfo, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := targetInfo.Mode().Perm(), os.FileMode(0o640); got != want {
		t.Fatalf("target mode = %o, want %o", got, want)
	}
}

func TestConfigureCodexSandboxDetectsSymlinkSwappedInBeforeRename(t *testing.T) {
	paths := testCodexSandboxPaths(t)
	initial := []byte("model = \"before\"\n")
	if err := os.MkdirAll(filepath.Dir(paths.ConfigPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.ConfigPath, initial, 0o640); err != nil {
		t.Fatal(err)
	}
	// Swap the resolved regular file for a symlink to an identical-bytes target
	// between validation and rename: the byte compare alone cannot see this, so
	// the topology check must trigger a retry, and the retry must resolve the
	// NEW symlink and update its target while preserving the link.
	sideTarget := filepath.Join(filepath.Dir(paths.ConfigPath), "swapped-target.toml")
	calls := 0
	result := configureCodexSandboxAtWithBeforeRename(paths, func(path string) {
		calls++
		if calls == 1 {
			if err := os.WriteFile(sideTarget, initial, 0o640); err != nil {
				t.Errorf("write side target: %v", err)
			}
			if err := os.Remove(path); err != nil {
				t.Errorf("remove config for swap: %v", err)
			}
			if err := os.Symlink(sideTarget, path); err != nil {
				t.Errorf("swap in symlink: %v", err)
			}
		}
	})
	if result.Action != codexSandboxConfigured {
		t.Fatalf("action = %q, want configured; warning=%q", result.Action, result.Warning)
	}
	info, err := os.Lstat(paths.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("swapped-in symlink was replaced by a regular file")
	}
	raw, err := os.ReadFile(sideTarget)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := decodeCodexSandboxConfig(raw)
	if err != nil {
		t.Fatalf("updated target does not parse: %v\n%s", err, raw)
	}
	if !allWritableRootsPresent(parsed.SandboxWorkspaceWrite.WritableRoots, paths.WritableRoots) {
		t.Fatalf("symlink target missing writable roots:\n%s", raw)
	}
}

func TestConfigureCodexSandboxRetriesConcurrentConfigChangeOnce(t *testing.T) {
	paths := testCodexSandboxPaths(t)
	initial := []byte("model = \"before\"\n")
	if err := os.MkdirAll(filepath.Dir(paths.ConfigPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.ConfigPath, initial, 0o640); err != nil {
		t.Fatal(err)
	}
	calls := 0
	result := configureCodexSandboxAtWithBeforeRename(paths, func(path string) {
		calls++
		if calls == 1 {
			if err := os.WriteFile(path, []byte("model = \"concurrent\"\n"), 0o640); err != nil {
				t.Errorf("mutate config before rename: %v", err)
			}
		}
	})
	if result.Action != codexSandboxConfigured {
		t.Fatalf("action = %q, want configured; warning=%q", result.Action, result.Warning)
	}
	if calls != 2 {
		t.Fatalf("before-rename calls = %d, want retry with 2 calls", calls)
	}
	raw, err := os.ReadFile(paths.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte("model = \"concurrent\"")) {
		t.Fatalf("concurrent edit was lost:\n%s", raw)
	}
	parsed, err := decodeCodexSandboxConfig(raw)
	if err != nil {
		t.Fatalf("updated config does not parse: %v\n%s", err, raw)
	}
	if !allWritableRootsPresent(parsed.SandboxWorkspaceWrite.WritableRoots, paths.WritableRoots) {
		t.Fatalf("writable_roots = %#v, want %#v", parsed.SandboxWorkspaceWrite.WritableRoots, paths.WritableRoots)
	}
}

func TestConfigureCodexSandboxSkipsAfterSecondConcurrentChange(t *testing.T) {
	paths := testCodexSandboxPaths(t)
	if err := os.MkdirAll(filepath.Dir(paths.ConfigPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.ConfigPath, []byte("model = \"before\"\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	calls := 0
	result := configureCodexSandboxAtWithBeforeRename(paths, func(path string) {
		calls++
		content := "model = \"first change\"\n"
		if calls == 2 {
			content = "model = \"second change\"\n"
		}
		if err := os.WriteFile(path, []byte(content), 0o640); err != nil {
			t.Errorf("mutate config before rename: %v", err)
		}
	})
	if result.Action != codexSandboxSkipped {
		t.Fatalf("action = %q, want skipped; warning=%q", result.Action, result.Warning)
	}
	if !strings.Contains(result.Warning, "config changed concurrently") || !strings.Contains(result.Warning, "Add this snippet manually:") {
		t.Fatalf("warning = %q, want concurrent-change warning and snippet", result.Warning)
	}
	if calls != 2 {
		t.Fatalf("before-rename calls = %d, want 2", calls)
	}
	raw, err := os.ReadFile(paths.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "model = \"second change\"\n" {
		t.Fatalf("config after second concurrent change = %q", raw)
	}
}

func TestConfigureCodexSandboxSkippedSnippetRespectsExistingTable(t *testing.T) {
	for _, tc := range []struct {
		name          string
		existingTable bool
	}{
		{name: "inline table", existingTable: true},
		{name: "absent table"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paths := testCodexSandboxPaths(t)
			if tc.existingTable {
				if err := os.MkdirAll(filepath.Dir(paths.ConfigPath), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(paths.ConfigPath, []byte("sandbox_workspace_write = { writable_roots = [] }\n"), 0o640); err != nil {
					t.Fatal(err)
				}
			} else {
				blocker := filepath.Join(filepath.Dir(filepath.Dir(paths.ConfigPath)), "not-a-directory")
				if err := os.WriteFile(blocker, []byte("blocker"), 0o600); err != nil {
					t.Fatal(err)
				}
				paths.ConfigPath = filepath.Join(blocker, "config.toml")
			}

			result := configureCodexSandboxAt(paths)
			if result.Action != codexSandboxSkipped {
				t.Fatalf("action = %q, want skipped; warning=%q", result.Action, result.Warning)
			}
			for _, root := range paths.WritableRoots {
				if !strings.Contains(result.Warning, strconv.Quote(root)) {
					t.Fatalf("warning = %q, missing root %q", result.Warning, root)
				}
			}
			if tc.existingTable {
				if !strings.Contains(result.Warning, "Merge these paths into the existing sandbox_workspace_write.writable_roots array:") || strings.Contains(result.Warning, "[sandbox_workspace_write]") {
					t.Fatalf("existing-table warning = %q, want merge-only guidance", result.Warning)
				}
				return
			}
			if !strings.Contains(result.Warning, "Add this snippet manually:\n[sandbox_workspace_write]") {
				t.Fatalf("warning = %q, want full-table snippet", result.Warning)
			}
		})
	}
}

func TestDecodeCodexSandboxConfigDetectsExistingTableForms(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{name: "standard table", raw: "[sandbox_workspace_write]\nwritable_roots = []\n"},
		{name: "inline table", raw: "sandbox_workspace_write = { writable_roots = [] }\n"},
		{name: "dotted key", raw: "sandbox_workspace_write.writable_roots = []\n"},
		{name: "CRLF table", raw: "[sandbox_workspace_write]\r\nwritable_roots = []\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, hasSandboxWorkspaceWrite, err := decodeCodexSandboxConfigWithContext([]byte(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if !hasSandboxWorkspaceWrite {
				t.Fatalf("sandbox_workspace_write was not detected in %q", tc.raw)
			}
		})
	}
}

func TestConfigureCodexSandboxMalformedConfigIsUnchanged(t *testing.T) {
	tmp := t.TempDir()
	paths := codexSandboxPaths{
		ConfigPath: filepath.Join(tmp, ".codex", "config.toml"),
		WritableRoots: []string{
			filepath.Join(tmp, "state", "agentbus"),
			filepath.Join(tmp, "cache", "agentbus"),
			filepath.Join(tmp, "state", "delegate"),
		},
	}
	initial := "[sandbox_workspace_write\nwritable_roots = [\"not valid\"]\n"
	if err := os.MkdirAll(filepath.Dir(paths.ConfigPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.ConfigPath, []byte(initial), 0o600); err != nil {
		t.Fatal(err)
	}

	result := configureCodexSandboxAt(paths)
	if result.Action != codexSandboxSkipped {
		t.Fatalf("action = %q, want skipped", result.Action)
	}
	if !strings.Contains(result.Warning, "Add this snippet manually:") || !strings.Contains(result.Warning, paths.WritableRoots[0]) || !strings.Contains(result.Warning, paths.WritableRoots[1]) || !strings.Contains(result.Warning, paths.WritableRoots[2]) {
		t.Fatalf("warning = %q, want manual snippet with all roots", result.Warning)
	}
	raw, err := os.ReadFile(paths.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != initial {
		t.Fatalf("malformed config changed:\n got %q\nwant %q", raw, initial)
	}
}

func TestResolveCodexSandboxPathsUsesXDGStateHome(t *testing.T) {
	paths, err := resolveCodexSandboxPathsFrom(func(key string) string {
		switch key {
		case "CODEX_HOME":
			return "/opt/codex"
		case "XDG_STATE_HOME":
			return "/var/state"
		default:
			return ""
		}
	}, func() (string, error) { return "/home/delegate", nil }, func() (string, error) { return "/var/cache", nil })
	if err != nil {
		t.Fatal(err)
	}
	if got, want := paths.ConfigPath, "/opt/codex/config.toml"; got != want {
		t.Fatalf("config path = %q, want %q", got, want)
	}
	agentbusRoot, err := canonicalizeAgentbusStateRoot("test agentbus root", "/var/state/agentbus")
	if err != nil {
		t.Fatal(err)
	}
	cacheRoot, err := canonicalizeAgentbusStateRoot("test agentbus cache root", "/var/cache/agentbus")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(paths.WritableRoots, ","), strings.Join([]string{agentbusRoot, cacheRoot}, ","); got != want {
		t.Fatalf("writable roots = %q, want %q", got, want)
	}
}

func TestResolveCodexSandboxPathsUsesConfiguredAgentbusStateRoot(t *testing.T) {
	tmp := t.TempDir()
	configuredRoot := filepath.Join(tmp, "configured-agentbus")
	if err := os.MkdirAll(configuredRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	paths, err := resolveCodexSandboxPathsFrom(func(key string) string {
		switch key {
		case "AGENTBUS_STATE_ROOT":
			return configuredRoot
		case "XDG_STATE_HOME":
			return filepath.Join(tmp, "state")
		default:
			return ""
		}
	}, func() (string, error) {
		return filepath.Join(tmp, "home"), nil
	}, func() (string, error) {
		return filepath.Join(tmp, "cache"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	wantRoot, err := canonicalizeAgentbusStateRoot("test configured agentbus root", configuredRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths.WritableRoots) != 2 {
		t.Fatalf("writable roots = %#v, want Agentbus state and cache roots", paths.WritableRoots)
	}
	if paths.WritableRoots[0] != wantRoot {
		t.Fatalf("agentbus writable root = %q, want configured root %q", paths.WritableRoots[0], wantRoot)
	}
	wantCacheRoot, err := canonicalizeAgentbusStateRoot("test cache root", filepath.Join(tmp, "cache", "agentbus"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := paths.WritableRoots[1], wantCacheRoot; got != want {
		t.Fatalf("agentbus cache writable root = %q, want %q", got, want)
	}
}

func TestConfigureCodexSandboxCommandAddsWritableRoot(t *testing.T) {
	home := t.TempDir()
	codexHome := filepath.Join(home, "codex")
	stateRoot := filepath.Join(home, "agentbus-state")
	cacheHome := filepath.Join(home, "cache")
	extraRoot := filepath.Join(home, "review-workspace")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(codexHome, "config.toml"), []byte("# Keep this comment.\nmodel = \"gpt-5\"\n[sandbox_workspace_write]\nwritable_roots = [\n  \"/unrelated\",\n]\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", codexHome)
	t.Setenv("AGENTBUS_STATE_ROOT", stateRoot)
	t.Setenv("XDG_CACHE_HOME", cacheHome)

	code, stdout, stderr := runTestCLI(t, testApp(t), []string{
		"configure-codex-sandbox",
		"--writable-root", extraRoot,
		"--json",
	})
	if code != 0 || stderr != "" {
		t.Fatalf("configure exit=%d stderr=%q stdout=%q", code, stderr, stdout)
	}
	var result codexSandboxResult
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("result JSON = %q: %v", stdout, err)
	}
	if result.Action != codexSandboxConfigured || result.ConfigPath != filepath.Join(codexHome, "config.toml") {
		t.Fatalf("result = %+v", result)
	}
	paths, err := resolveCodexSandboxPaths(os.Getenv, os.UserHomeDir)
	if err != nil {
		t.Fatal(err)
	}
	wantRoots := appendUniqueCodexSandboxRoots(paths.WritableRoots, []string{extraRoot})
	if strings.Join(result.WritableRoots, "\x00") != strings.Join(wantRoots, "\x00") {
		t.Fatalf("result writable roots = %#v, want %#v", result.WritableRoots, wantRoots)
	}
	raw, err := os.ReadFile(filepath.Join(codexHome, "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := decodeCodexSandboxConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !allWritableRootsPresent(parsed.SandboxWorkspaceWrite.WritableRoots, wantRoots) {
		t.Fatalf("configured writable roots = %#v, want %#v", parsed.SandboxWorkspaceWrite.WritableRoots, wantRoots)
	}
	if !strings.Contains(string(raw), "# Keep this comment.") || !strings.Contains(string(raw), "\"/unrelated\"") {
		t.Fatalf("existing config content was not preserved:\n%s", raw)
	}
}

func TestConfigureCodexSandboxCommandRejectsInvalidWritableRoots(t *testing.T) {
	for _, tc := range []struct {
		name string
		root string
		want string
	}{
		{name: "relative", root: "relative/workspace", want: "must be absolute"},
		{name: "parent traversal", root: "/tmp/agentbus/../workspace", want: "must not contain '..'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runTestCLI(t, testApp(t), []string{
				"configure-codex-sandbox",
				"--writable-root", tc.root,
			})
			if code != 2 || stdout != "" || !strings.Contains(stderr, tc.want) {
				t.Fatalf("exit=%d stdout=%q stderr=%q, want validation error containing %q", code, stdout, stderr, tc.want)
			}
		})
	}
}

func testCodexSandboxPaths(t *testing.T) codexSandboxPaths {
	t.Helper()
	tmp := t.TempDir()
	return codexSandboxPaths{
		ConfigPath: filepath.Join(tmp, ".codex", "config.toml"),
		WritableRoots: []string{
			filepath.Join(tmp, "state", "agentbus"),
			filepath.Join(tmp, "cache", "agentbus"),
			filepath.Join(tmp, "state", "delegate"),
		},
	}
}

func stringPtr(value string) *string { return &value }
