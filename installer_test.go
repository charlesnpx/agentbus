package agentbus_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/charlesnpx/agentbus/internal/protocol"
)

type installerResult struct {
	Schema    int                              `json:"schema"`
	Name      string                           `json:"name"`
	Version   string                           `json:"version"`
	Operation string                           `json:"operation"`
	Kind      string                           `json:"kind"`
	Setup     []installerSetup                 `json:"setup"`
	Targets   map[string]installerTargetResult `json:"targets"`
	Warnings  []string                         `json:"warnings"`
}

type installerSetup struct {
	Kind        string `json:"kind"`
	Executable  string `json:"executable"`
	Remediation string `json:"remediation"`
}

type installerTargetResult struct {
	Files []installerFileResult `json:"files"`
}

type installerFileResult struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type cliVersionResult struct {
	Schema          int    `json:"schema"`
	Version         string `json:"version"`
	ProtocolVersion int    `json:"protocolVersion"`
}

var (
	offlineModCacheOnce sync.Once
	offlineModCachePath string
	offlineModCacheErr  error
)

func TestInstallerPlanJSONWithoutGoOnPath(t *testing.T) {
	root := repoRoot(t)
	version := readVersion(t, root)
	cmd := installerCommand(t, root, "--plan", "--target", "all", "--json")
	cmd.Env = commandEnv(t, map[string]string{
		"PATH": "/usr/bin:/bin",
	})
	stdout, stderr, err := runCommand(cmd)
	if err != nil {
		t.Fatalf("plan failed: %v stderr=%s stdout=%s", err, stderr, stdout)
	}

	result := decodeInstallerResult(t, stdout)
	if result.Schema != 1 || result.Name != "agentbus" || result.Version != version || result.Operation != "plan" || result.Kind != "delegated" {
		t.Fatalf("plan result = %+v", result)
	}
	if len(result.Setup) != 1 || result.Setup[0].Kind != "executable" || result.Setup[0].Executable != "go" {
		t.Fatalf("setup = %+v", result.Setup)
	}
	if len(result.Targets) != 1 {
		t.Fatalf("targets = %+v, want only tools", result.Targets)
	}
	if _, ok := result.Targets["claude"]; ok {
		t.Fatalf("claude target reported for --target all: %+v", result.Targets)
	}
	if _, ok := result.Targets["codex"]; ok {
		t.Fatalf("codex target reported for --target all: %+v", result.Targets)
	}
	if _, ok := result.Targets["tools"]; !ok {
		t.Fatalf("tools target missing from %+v", result.Targets)
	}
	toolFiles := result.Targets["tools"].Files
	if len(toolFiles) != 1 || !filepath.IsAbs(toolFiles[0].Path) || toolFiles[0].SHA256 != "" {
		t.Fatalf("plan tools files = %+v", toolFiles)
	}
}

func TestInstallerInstallAndUninstallStagedTools(t *testing.T) {
	root := repoRoot(t)
	version := readVersion(t, root)
	stage := t.TempDir()

	cmd := installerCommand(t, root, "--install", "--target", "tools", "--json", "--install-root", stage)
	cmd.Env = offlineGoEnv(t)
	stdout, stderr, err := runCommand(cmd)
	if err != nil {
		t.Fatalf("install failed: %v stderr=%s stdout=%s", err, stderr, stdout)
	}
	installResult := decodeInstallerResult(t, stdout)
	if installResult.Operation != "install" || installResult.Version != version {
		t.Fatalf("install result = %+v", installResult)
	}
	tools := installResult.Targets["tools"].Files
	if len(tools) != 1 {
		t.Fatalf("tools files = %+v", tools)
	}
	tool := tools[0]
	canonicalStage, err := filepath.EvalSymlinks(stage)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(tool.Path, canonicalStage+string(os.PathSeparator)) {
		t.Fatalf("tool path %q is not inside stage %q", tool.Path, stage)
	}
	if tool.SHA256 == "" {
		t.Fatalf("installed tool missing sha256: %+v", tool)
	}
	if got := fileSHA256(t, tool.Path); got != tool.SHA256 {
		t.Fatalf("sha256 = %s, want %s", got, tool.SHA256)
	}

	versionCmd := exec.Command(tool.Path, "version", "--json")
	versionOut, versionErr, err := runCommand(versionCmd)
	if err != nil {
		t.Fatalf("staged binary version failed: %v stderr=%s stdout=%s", err, versionErr, versionOut)
	}
	var cli cliVersionResult
	if err := json.Unmarshal([]byte(versionOut), &cli); err != nil {
		t.Fatalf("decode version JSON: %v: %s", err, versionOut)
	}
	if cli.Version != version || cli.Schema != 1 || cli.ProtocolVersion != protocol.Version {
		t.Fatalf("CLI version = %+v", cli)
	}

	uninstallCmd := installerCommand(t, root, "--uninstall", "--target", "tools", "--json", "--install-root", stage)
	uninstallCmd.Env = commandEnv(t, nil)
	uninstallOut, uninstallErr, err := runCommand(uninstallCmd)
	if err != nil {
		t.Fatalf("uninstall failed: %v stderr=%s stdout=%s", err, uninstallErr, uninstallOut)
	}
	uninstallResult := decodeInstallerResult(t, uninstallOut)
	if uninstallResult.Operation != "uninstall" {
		t.Fatalf("uninstall result = %+v", uninstallResult)
	}
	if _, err := os.Stat(tool.Path); !os.IsNotExist(err) {
		t.Fatalf("staged tool still exists after uninstall: stat err=%v", err)
	}
}

func TestInstallerStagedInstallDoesNotRestartDaemon(t *testing.T) {
	root := repoRoot(t)
	stage := t.TempDir()
	liveHome := t.TempDir()
	stubBin := t.TempDir()
	pgrepCalled := filepath.Join(t.TempDir(), "pgrep-called")
	writeExecutable(t, filepath.Join(stubBin, "pgrep"), "#!/bin/sh\ntouch \"$AGENTBUS_PGREP_CALLED\"\nexit 1\n")

	cmd := installerCommand(t, root, "--install", "--target", "tools", "--json", "--install-root", stage)
	cmd.Env = offlineInstallerEnv(t, map[string]string{
		"HOME":                  liveHome,
		"PATH":                  stubBin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"AGENTBUS_PGREP_CALLED": pgrepCalled,
	})
	stdout, stderr, err := runCommand(cmd)
	if err != nil {
		t.Fatalf("staged install failed: %v stderr=%s stdout=%s", err, stderr, stdout)
	}
	if _, err := os.Stat(pgrepCalled); !os.IsNotExist(err) {
		t.Fatalf("staged install invoked pgrep: stat err=%v", err)
	}
	if warnings := decodeInstallerResult(t, stdout).Warnings; containsWarningText(warnings, "daemon") {
		t.Fatalf("staged install warnings = %#v, want no daemon restart warning", warnings)
	}
}

func TestInstallerLiveInstallRestartsMatchingDaemon(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	stubBin := t.TempDir()
	daemon := exec.Command("sleep", "30")
	if err := daemon.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if daemon.Process != nil {
			_ = daemon.Process.Kill()
		}
		_ = daemon.Wait()
	})
	canonicalHome, err := filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatal(err)
	}
	toolPath := filepath.Join(canonicalHome, ".local", "bin", "agentbus")
	writeExecutable(t, filepath.Join(stubBin, "pgrep"), "#!/bin/sh\nprintf '%s\\n' \"$AGENTBUS_TEST_DAEMON_PID\"\n")
	writeExecutable(t, filepath.Join(stubBin, "ps"), "#!/bin/sh\nprintf '%s\\n' \"$AGENTBUS_TEST_DAEMON_COMMAND\"\n")

	cmd := installerCommand(t, root, "--install", "--target", "tools", "--json")
	cmd.Env = offlineInstallerEnv(t, map[string]string{
		"HOME":                         home,
		"PATH":                         stubBin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"AGENTBUS_TEST_DAEMON_PID":     fmt.Sprintf("%d", daemon.Process.Pid),
		"AGENTBUS_TEST_DAEMON_COMMAND": toolPath + " serve",
	})
	stdout, stderr, err := runCommand(cmd)
	if err != nil {
		t.Fatalf("live install failed: %v stderr=%s stdout=%s", err, stderr, stdout)
	}
	if warnings := decodeInstallerResult(t, stdout).Warnings; !containsWarningText(warnings, fmt.Sprintf("restarted running agentbus daemon (pid %d)", daemon.Process.Pid)) {
		t.Fatalf("live install warnings = %#v", warnings)
	}
	if err := daemon.Wait(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ProcessState.ExitCode() == 0 {
			t.Fatalf("daemon was not terminated by installer: %v", err)
		}
	}
	daemon.Process = nil
}

func TestInstallerRejectsMalformedFlags(t *testing.T) {
	root := repoRoot(t)
	tests := []struct {
		name           string
		args           []string
		stderrContains string
	}{
		{
			name:           "multiple operations",
			args:           []string{"--plan", "--install", "--target", "tools", "--json"},
			stderrContains: "agentbus installer:",
		},
		{
			name:           "unknown target",
			args:           []string{"--plan", "--target", "bogus", "--json"},
			stderrContains: "agentbus installer:",
		},
		{
			name:           "relative install root",
			args:           []string{"--install", "--target", "tools", "--json", "--install-root", "relative"},
			stderrContains: "agentbus installer:",
		},
		{
			name:           "claude target unavailable",
			args:           []string{"--plan", "--target", "claude", "--json"},
			stderrContains: "no skills in v1",
		},
		{
			name:           "codex target unavailable",
			args:           []string{"--plan", "--target", "codex", "--json"},
			stderrContains: "no skills in v1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := installerCommand(t, root, test.args...)
			cmd.Env = commandEnv(t, nil)
			stdout, stderr, err := runCommand(cmd)
			if err == nil {
				t.Fatalf("command succeeded unexpectedly: stdout=%s stderr=%s", stdout, stderr)
			}
			if strings.TrimSpace(stdout) != "" {
				t.Fatalf("malformed command wrote stdout: %q", stdout)
			}
			if !strings.Contains(stderr, test.stderrContains) {
				t.Fatalf("stderr does not contain %q: %q", test.stderrContains, stderr)
			}
		})
	}
}

func TestReleaseCheckValidatesInstallerJSONAndExactTag(t *testing.T) {
	root := repoRoot(t)
	version := readVersion(t, root)
	tag := "v" + version
	gitBin := fakeGitBin(t)

	cmd := releaseCheckCommand(t, root, tag)
	cmd.Env = releaseCheckEnv(t, gitBin, tag)
	stdout, stderr, err := runCommand(cmd)
	if err != nil {
		t.Fatalf("release-check failed: %v stderr=%s stdout=%s", err, stderr, stdout)
	}
	want := fmt.Sprintf("release-check: ok tag=%s version=%s", tag, version)
	if strings.TrimSpace(stdout) != want {
		t.Fatalf("release-check stdout = %q, want %q", strings.TrimSpace(stdout), want)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("release-check wrote stderr: %q", stderr)
	}

	wrongTag := "v0.0.0"
	if wrongTag == tag {
		wrongTag = "v9.9.9"
	}
	mismatch := releaseCheckCommand(t, root, tag)
	mismatch.Env = releaseCheckEnv(t, gitBin, wrongTag)
	mismatchOut, mismatchErr, err := runCommand(mismatch)
	if err == nil {
		t.Fatalf("release-check succeeded with mismatched HEAD tag: stdout=%s stderr=%s", mismatchOut, mismatchErr)
	}
	if strings.TrimSpace(mismatchOut) != "" {
		t.Fatalf("tag mismatch wrote stdout: %q", mismatchOut)
	}
	if !strings.Contains(mismatchErr, "not requested tag") {
		t.Fatalf("tag mismatch stderr = %q", mismatchErr)
	}
}

func installerCommand(t *testing.T, root string, args ...string) *exec.Cmd {
	t.Helper()
	script := filepath.Join(root, "install-skill.sh")
	cmd := exec.Command("/bin/bash", append([]string{script}, args...)...)
	cmd.Dir = root
	return cmd
}

func releaseCheckCommand(t *testing.T, root string, tag string) *exec.Cmd {
	t.Helper()
	script := filepath.Join(root, "scripts", "release-check.sh")
	cmd := exec.Command("/bin/bash", script, tag)
	cmd.Dir = root
	return cmd
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func readVersion(t *testing.T, root string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "VERSION"))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(raw))
}

func decodeInstallerResult(t *testing.T, raw string) installerResult {
	t.Helper()
	if !json.Valid([]byte(raw)) {
		t.Fatalf("output is not JSON: %q", raw)
	}
	var result installerResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		t.Fatalf("decode installer JSON: %v: %s", err, raw)
	}
	return result
}

func runCommand(cmd *exec.Cmd) (string, string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func offlineGoEnv(t *testing.T) []string {
	t.Helper()
	return commandEnv(t, map[string]string{
		"GOCACHE":    privateTmpDir(t, "agentbus-gocache-*"),
		"GOMODCACHE": offlineSeededModCache(t),
		"GOPROXY":    "off",
		"GOSUMDB":    "off",
	})
}

func offlineInstallerEnv(t *testing.T, overrides map[string]string) []string {
	t.Helper()
	base := map[string]string{
		"GOCACHE":    privateTmpDir(t, "agentbus-gocache-*"),
		"GOMODCACHE": offlineSeededModCache(t),
		"GOPROXY":    "off",
		"GOSUMDB":    "off",
	}
	for key, value := range overrides {
		base[key] = value
	}
	return commandEnv(t, base)
}

func releaseCheckEnv(t *testing.T, gitBin string, headTag string) []string {
	t.Helper()
	env := map[string]string{
		"FAKE_GIT_TAG": headTag,
		"GOCACHE":      privateTmpDir(t, "agentbus-gocache-*"),
		"GOMODCACHE":   offlineSeededModCache(t),
		"GOFLAGS":      "-buildvcs=false",
		"GOPROXY":      "off",
		"GOSUMDB":      "off",
		"PATH":         gitBin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	return commandEnv(t, env)
}

func offlineSeededModCache(t *testing.T) string {
	t.Helper()
	if cache := os.Getenv("AGENTBUS_OFFLINE_MODCACHE"); cache != "" {
		return cache
	}

	offlineModCacheOnce.Do(func() {
		root := repoRoot(t)
		dir, err := os.MkdirTemp(os.TempDir(), "agentbus-offline-gomodcache-*")
		if err != nil {
			offlineModCacheErr = fmt.Errorf("create offline module cache: %w", err)
			return
		}

		cmd := exec.Command("go", "mod", "download", "all")
		cmd.Dir = root
		cmd.Env = commandEnv(t, map[string]string{
			"GOMODCACHE": dir,
			"GOFLAGS":    "-mod=mod",
		})
		stdout, stderr, err := runCommand(cmd)
		if err != nil {
			_ = os.RemoveAll(dir)
			offlineModCacheErr = fmt.Errorf("go mod download all failed: %w stderr=%s stdout=%s", err, stderr, stdout)
			return
		}
		offlineModCachePath = dir
	})
	if offlineModCacheErr != nil {
		t.Skipf("offline installer build test needs a network-seeded or AGENTBUS_OFFLINE_MODCACHE module cache; skipping: %v", offlineModCacheErr)
	}
	if offlineModCachePath == "" {
		t.Skip("offline installer build test needs a network-seeded or AGENTBUS_OFFLINE_MODCACHE module cache; skipping")
	}
	return offlineModCachePath
}

func commandEnv(t *testing.T, overrides map[string]string) []string {
	t.Helper()
	env := os.Environ()
	if len(overrides) == 0 {
		return env
	}
	filtered := make([]string, 0, len(env)+len(overrides))
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if _, ok := overrides[key]; ok {
			continue
		}
		filtered = append(filtered, entry)
	}
	for key, value := range overrides {
		filtered = append(filtered, fmt.Sprintf("%s=%s", key, value))
	}
	return filtered
}

func privateTmpDir(t *testing.T, pattern string) string {
	t.Helper()
	dir, err := os.MkdirTemp(os.TempDir(), pattern)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return dir
}

func fakeGitBin(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	script := filepath.Join(dir, "git")
	body := `#!/bin/sh
if [ "$1" = "-C" ]; then
  shift 2
fi
if [ "$1" = "describe" ] && [ "$2" = "--tags" ] && [ "$3" = "--exact-match" ]; then
  if [ -n "${FAKE_GIT_TAG:-}" ]; then
    printf '%s\n' "$FAKE_GIT_TAG"
    exit 0
  fi
  exit 1
fi
printf 'unexpected git invocation: %s\n' "$*" >&2
exit 99
`
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

func fileSHA256(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func writeExecutable(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
}

func containsWarningText(warnings []string, want string) bool {
	for _, warning := range warnings {
		if strings.Contains(warning, want) {
			return true
		}
	}
	return false
}
