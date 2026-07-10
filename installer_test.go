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
	"testing"
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
	for _, target := range []string{"claude", "codex", "tools"} {
		if _, ok := result.Targets[target]; !ok {
			t.Fatalf("target %q missing from %+v", target, result.Targets)
		}
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
	if cli.Version != version || cli.Schema != 1 || cli.ProtocolVersion != 1 {
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

func TestInstallerRejectsMalformedFlags(t *testing.T) {
	root := repoRoot(t)
	tests := [][]string{
		{"--plan", "--install", "--target", "tools", "--json"},
		{"--plan", "--target", "bogus", "--json"},
		{"--install", "--target", "tools", "--json", "--install-root", "relative"},
	}
	for _, args := range tests {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			cmd := installerCommand(t, root, args...)
			cmd.Env = commandEnv(t, nil)
			stdout, stderr, err := runCommand(cmd)
			if err == nil {
				t.Fatalf("command succeeded unexpectedly: stdout=%s stderr=%s", stdout, stderr)
			}
			if strings.TrimSpace(stdout) != "" {
				t.Fatalf("malformed command wrote stdout: %q", stdout)
			}
			if !strings.Contains(stderr, "agentbus installer:") {
				t.Fatalf("stderr does not contain installer prefix: %q", stderr)
			}
		})
	}
}

func installerCommand(t *testing.T, root string, args ...string) *exec.Cmd {
	t.Helper()
	script := filepath.Join(root, "install-skill.sh")
	cmd := exec.Command("/bin/bash", append([]string{script}, args...)...)
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
		"GOMODCACHE": privateTmpDir(t, "agentbus-gomodcache-*"),
		"GOPROXY":    "off",
		"GOSUMDB":    "off",
	})
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
	dir, err := os.MkdirTemp("/private/tmp", pattern)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
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
