package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

const (
	codexSandboxConfigured        = "configured"
	codexSandboxAlreadyConfigured = "already-configured"
	codexSandboxSkipped           = "skipped"
)

type codexSandboxConfig struct {
	SandboxWorkspaceWrite codexSandboxWorkspaceWrite `toml:"sandbox_workspace_write"`
}

type codexSandboxWorkspaceWrite struct {
	WritableRoots []string `toml:"writable_roots"`
}

type codexSandboxPaths struct {
	ConfigPath    string
	WritableRoots []string
}

type codexSandboxResult struct {
	Action        string   `json:"action"`
	ConfigPath    string   `json:"configPath,omitempty"`
	WritableRoots []string `json:"writableRoots,omitempty"`
	Warning       string   `json:"warning,omitempty"`
}

var (
	sandboxTableHeaderRE = regexp.MustCompile(`(?m)^[\t ]*\[[\t ]*sandbox_workspace_write[\t ]*\][\t ]*(?:#[^\r\n]*)?\r?$`)
	tomlTableHeaderRE    = regexp.MustCompile(`(?m)^[\t ]*\[[^\r\n]*\][\t ]*(?:#[^\r\n]*)?\r?$`)
	writableRootsRE      = regexp.MustCompile(`(?m)^[\t ]*writable_roots[\t ]*=[\t ]*\[(?:[^\r\n]*)?\r?$`)

	errCodexConfigChangedConcurrently = errors.New("config changed concurrently")
)

func runConfigureCodexSandbox(args []string, stdout, stderr io.Writer) int {
	fs := newCommandFlagSet("configure-codex-sandbox", stderr)
	jsonOut := fs.Bool("json", false, "emit JSON")
	var extraWritableRoots []string
	fs.Func("writable-root", "add an absolute writable root (repeatable)", func(value string) error {
		if err := validateCodexSandboxWritableRoot(value); err != nil {
			return err
		}
		if !containsCodexSandboxRoot(extraWritableRoots, value) {
			extraWritableRoots = append(extraWritableRoots, value)
		}
		return nil
	})
	if code, ok := parseFlags(fs, args); !ok {
		return code
	}
	if fs.NArg() != 0 {
		return usageError(stderr, "configure-codex-sandbox does not accept positional arguments")
	}

	result := configureCodexSandboxWithWritableRoots(extraWritableRoots)
	if *jsonOut {
		if code := writeOrError(stdout, stderr, result); code != 0 {
			return code
		}
		return codexSandboxExitCode(result)
	}

	detail := result.ConfigPath
	if result.Warning != "" {
		detail = result.Warning
	}
	if _, err := fmt.Fprintf(stdout, "codex sandbox writable_roots %s: %s\n", result.Action, detail); err != nil {
		return commandError(stderr, err)
	}
	return codexSandboxExitCode(result)
}

func codexSandboxExitCode(result codexSandboxResult) int {
	if result.Action == codexSandboxConfigured || result.Action == codexSandboxAlreadyConfigured {
		return 0
	}
	return 1
}

func validateCodexSandboxWritableRoot(root string) error {
	if !filepath.IsAbs(root) {
		return fmt.Errorf("--writable-root %q must be absolute", root)
	}
	if strings.Contains(root, "..") {
		return fmt.Errorf("--writable-root %q must not contain '..'", root)
	}
	return nil
}

func configureCodexSandbox() codexSandboxResult {
	return configureCodexSandboxWithWritableRoots(nil)
}

func configureCodexSandboxWithWritableRoots(extraWritableRoots []string) codexSandboxResult {
	paths, err := resolveCodexSandboxPaths(os.Getenv, os.UserHomeDir)
	if err != nil {
		return codexSandboxResult{
			Action:        codexSandboxSkipped,
			WritableRoots: append([]string(nil), extraWritableRoots...),
			Warning:       err.Error(),
		}
	}
	paths.WritableRoots = appendUniqueCodexSandboxRoots(paths.WritableRoots, extraWritableRoots)
	return configureCodexSandboxAt(paths)
}

func resolveCodexSandboxPaths(env func(string) string, homeDir func() (string, error)) (codexSandboxPaths, error) {
	return resolveCodexSandboxPathsFrom(env, homeDir, os.UserCacheDir)
}

func resolveCodexSandboxPathsFrom(env func(string) string, homeDir func() (string, error), userCacheDir func() (string, error)) (codexSandboxPaths, error) {
	home, err := homeDir()
	if err != nil {
		return codexSandboxPaths{}, fmt.Errorf("resolve home for Codex sandbox: %w", err)
	}
	home, err = absoluteCleanPath("home directory", home)
	if err != nil {
		return codexSandboxPaths{}, err
	}

	codexHome := env("CODEX_HOME")
	if codexHome == "" {
		codexHome = filepath.Join(home, ".codex")
	}
	codexHome, err = absoluteCleanPath("CODEX_HOME", codexHome)
	if err != nil {
		return codexSandboxPaths{}, err
	}

	agentbusRoot, err := resolveAgentbusStateRootFrom(env, homeDir)
	if err != nil {
		return codexSandboxPaths{}, err
	}
	agentbusCacheRoot, err := resolveAgentbusUserCacheRootFrom(userCacheDir)
	if err != nil {
		return codexSandboxPaths{}, err
	}
	return codexSandboxPaths{
		ConfigPath: filepath.Join(codexHome, "config.toml"),
		WritableRoots: []string{
			agentbusRoot,
			agentbusCacheRoot,
		},
	}, nil
}

type agentbusStateRootUsageError struct {
	Name  string
	Value string
}

func (err agentbusStateRootUsageError) Error() string {
	return fmt.Sprintf("%s %q must be absolute", err.Name, err.Value)
}

func resolveAgentbusUserCacheRootFrom(userCacheDir func() (string, error)) (string, error) {
	cacheDir, err := userCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolve user cache dir for Agentbus Codex sandbox: %w", err)
	}
	if cacheDir == "" {
		return "", errors.New("user cache directory is empty")
	}
	return canonicalizeAgentbusStateRoot("user cache directory", filepath.Join(cacheDir, "agentbus"))
}

func resolveAgentbusStateRootFrom(env func(string) string, userHomeDir func() (string, error)) (string, error) {
	if root := env("AGENTBUS_STATE_ROOT"); root != "" {
		return canonicalizeAgentbusStateRoot("AGENTBUS_STATE_ROOT", root)
	}
	if stateHome := env("XDG_STATE_HOME"); stateHome != "" {
		if !filepath.IsAbs(stateHome) {
			return "", agentbusStateRootUsageError{Name: "XDG_STATE_HOME", Value: stateHome}
		}
		return canonicalizeAgentbusStateRoot("agentbus state root", filepath.Join(stateHome, "agentbus"))
	}
	home, err := userHomeDir()
	if err != nil {
		return "", err
	}
	if home == "" {
		return "", errors.New("home directory is empty")
	}
	return canonicalizeAgentbusStateRoot("agentbus state root", filepath.Join(home, ".local", "state", "agentbus"))
}

func canonicalizeAgentbusStateRoot(label, root string) (string, error) {
	if root == "" {
		return "", fmt.Errorf("%s is empty", label)
	}
	if !filepath.IsAbs(root) {
		return "", agentbusStateRootUsageError{Name: label, Value: root}
	}
	clean := filepath.Clean(root)
	if evaluated, err := filepath.EvalSymlinks(clean); err == nil {
		return filepath.Clean(evaluated), nil
	}
	return evalSymlinksAsFeasible(clean), nil
}

func evalSymlinksAsFeasible(path string) string {
	missing := []string{}
	for current := path; ; current = filepath.Dir(current) {
		if _, err := os.Lstat(current); err == nil {
			if evaluated, evalErr := filepath.EvalSymlinks(current); evalErr == nil {
				parts := append([]string{evaluated}, missing...)
				return filepath.Clean(filepath.Join(parts...))
			}
			break
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		missing = append([]string{filepath.Base(current)}, missing...)
	}
	return path
}

func absoluteCleanPath(label, path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("%s is empty", label)
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("%s %q must be absolute", label, path)
	}
	return filepath.Clean(path), nil
}

func appendUniqueCodexSandboxRoots(roots, extra []string) []string {
	result := append([]string(nil), roots...)
	for _, root := range extra {
		if !containsCodexSandboxRoot(result, root) {
			result = append(result, root)
		}
	}
	return result
}

func containsCodexSandboxRoot(roots []string, wanted string) bool {
	for _, root := range roots {
		if root == wanted {
			return true
		}
	}
	return false
}

func configureCodexSandboxAt(paths codexSandboxPaths) codexSandboxResult {
	return configureCodexSandboxAtWithBeforeRename(paths, nil)
}

// configureCodexSandboxAtWithBeforeRename provides a narrow test seam for a
// configuration change that occurs after candidate generation but before the
// destination is replaced.
func configureCodexSandboxAtWithBeforeRename(paths codexSandboxPaths, beforeRename func(string)) codexSandboxResult {
	result := codexSandboxResult{
		ConfigPath:    paths.ConfigPath,
		WritableRoots: append([]string(nil), paths.WritableRoots...),
	}
	for attempt := 0; attempt < 2; attempt++ {
		destination, exists, err := resolveCodexSandboxDestination(paths.ConfigPath)
		if err != nil {
			return codexSandboxSkippedResult(result, err.Error())
		}
		raw, exists, err := readCodexSandboxConfig(destination, exists)
		if err != nil {
			return codexSandboxSkippedResult(result, fmt.Sprintf("read Codex config %q: %v", paths.ConfigPath, err))
		}

		parsed, hasSandboxWorkspaceWrite, err := decodeCodexSandboxConfigWithContext(raw)
		if err != nil {
			return codexSandboxSkippedResult(result, fmt.Sprintf("TOML parse failed for Codex config %q: %v", paths.ConfigPath, err))
		}
		if allWritableRootsPresent(parsed.SandboxWorkspaceWrite.WritableRoots, paths.WritableRoots) {
			result.Action = codexSandboxAlreadyConfigured
			return result
		}

		missing := missingWritableRoots(parsed.SandboxWorkspaceWrite.WritableRoots, paths.WritableRoots)
		candidate, err := spliceCodexSandboxRoots(raw, missing)
		if err != nil {
			return codexSandboxSkippedResultWithContext(result, fmt.Sprintf("could not minimally update Codex config %q: %v", paths.ConfigPath, err), hasSandboxWorkspaceWrite)
		}
		validated, err := decodeCodexSandboxConfig(candidate)
		if err != nil {
			return codexSandboxSkippedResultWithContext(result, fmt.Sprintf("generated TOML did not parse for Codex config %q: %v", paths.ConfigPath, err), hasSandboxWorkspaceWrite)
		}
		if !allWritableRootsPresent(validated.SandboxWorkspaceWrite.WritableRoots, paths.WritableRoots) {
			return codexSandboxSkippedResultWithContext(result, fmt.Sprintf("generated TOML for Codex config %q did not contain all required writable roots", paths.ConfigPath), hasSandboxWorkspaceWrite)
		}

		mode := os.FileMode(0o600)
		if exists {
			info, statErr := os.Stat(destination)
			if statErr != nil {
				return codexSandboxSkippedResultWithContext(result, fmt.Sprintf("stat Codex config %q: %v", paths.ConfigPath, statErr), hasSandboxWorkspaceWrite)
			}
			mode = info.Mode().Perm()
		}
		err = atomicWriteCodexConfig(destination, raw, candidate, mode, beforeRename)
		if errors.Is(err, errCodexConfigChangedConcurrently) {
			if attempt == 0 {
				continue
			}
			return codexSandboxSkippedResultWithContext(result, errCodexConfigChangedConcurrently.Error(), hasSandboxWorkspaceWrite)
		}
		if err != nil {
			return codexSandboxSkippedResultWithContext(result, fmt.Sprintf("write Codex config %q: %v", paths.ConfigPath, err), hasSandboxWorkspaceWrite)
		}
		result.Action = codexSandboxConfigured
		return result
	}
	return codexSandboxSkippedResult(result, errCodexConfigChangedConcurrently.Error())
}

// resolveCodexSandboxDestination keeps a config.toml symlink in place by
// replacing its resolved target rather than the symlink itself.
func resolveCodexSandboxDestination(path string) (string, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return path, false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("lstat Codex config %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return path, true, nil
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", false, fmt.Errorf("resolve symlinked Codex config %q: %w", path, err)
	}
	return resolved, true, nil
}

func readCodexSandboxConfig(path string, exists bool) ([]byte, bool, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, exists, err
	}
	return raw, true, nil
}

func codexSandboxSkippedResult(result codexSandboxResult, reason string) codexSandboxResult {
	return codexSandboxSkippedResultWithContext(result, reason, false)
}

func codexSandboxSkippedResultWithContext(result codexSandboxResult, reason string, hasSandboxWorkspaceWrite bool) codexSandboxResult {
	result.Action = codexSandboxSkipped
	if hasSandboxWorkspaceWrite {
		result.Warning = reason + ". Merge these paths into the existing sandbox_workspace_write.writable_roots array:\n" + codexSandboxRootEntries(result.WritableRoots)
		return result
	}
	result.Warning = reason + ". Add this snippet manually:\n" + codexSandboxSnippet(result.WritableRoots)
	return result
}

func codexSandboxSnippet(roots []string) string {
	var b strings.Builder
	b.WriteString("[sandbox_workspace_write]\n")
	b.WriteString("writable_roots = [\n")
	b.WriteString(codexSandboxRootEntries(roots))
	b.WriteString("]")
	return b.String()
}

func codexSandboxRootEntries(roots []string) string {
	var b strings.Builder
	for _, root := range roots {
		fmt.Fprintf(&b, "  %s,\n", strconv.Quote(root))
	}
	return b.String()
}

func decodeCodexSandboxConfig(raw []byte) (codexSandboxConfig, error) {
	config, _, err := decodeCodexSandboxConfigWithContext(raw)
	return config, err
}

func decodeCodexSandboxConfigWithContext(raw []byte) (codexSandboxConfig, bool, error) {
	var config codexSandboxConfig
	metadata, err := toml.Decode(string(raw), &config)
	if err != nil {
		return codexSandboxConfig{}, false, err
	}
	hasSandboxWorkspaceWrite := metadata.IsDefined("sandbox_workspace_write") || metadata.IsDefined("sandbox_workspace_write", "writable_roots") || len(config.SandboxWorkspaceWrite.WritableRoots) > 0
	return config, hasSandboxWorkspaceWrite, nil
}

func allWritableRootsPresent(current, required []string) bool {
	return len(missingWritableRoots(current, required)) == 0
}

func missingWritableRoots(current, required []string) []string {
	missing := make([]string, 0, len(required))
	for _, root := range required {
		found := false
		for _, present := range current {
			if present == root {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, root)
		}
	}
	return missing
}

func spliceCodexSandboxRoots(raw []byte, missing []string) ([]byte, error) {
	if len(missing) == 0 {
		return append([]byte(nil), raw...), nil
	}
	start, end, found := sandboxWorkspaceWriteTable(raw)
	if !found {
		return appendSandboxWorkspaceWriteTable(raw, missing), nil
	}
	assignment := writableRootsRE.FindIndex(raw[start:end])
	if assignment == nil {
		return insertWritableRootsInSandboxTable(raw, end, missing), nil
	}
	assignmentRaw := raw[start+assignment[0] : start+assignment[1]]
	arrayOffset := bytes.IndexByte(assignmentRaw, '[')
	if arrayOffset < 0 {
		return nil, errors.New("writable_roots opening bracket was not found")
	}
	arrayStart := start + assignment[0] + arrayOffset
	arrayEnd, err := tomlArrayEnd(raw, arrayStart)
	if err != nil || arrayEnd >= end {
		if err == nil {
			err = errors.New("writable_roots array extends beyond its table")
		}
		return nil, err
	}
	return appendRootsToTOMLArray(raw, arrayStart, arrayEnd, missing), nil
}

// sandboxWorkspaceWriteTable returns the content bounds of the explicit table.
// We avoid converting TOML through a serializer because that loses user comments
// and formatting; all additions below are byte splices into the original text.
func sandboxWorkspaceWriteTable(raw []byte) (int, int, bool) {
	header := sandboxTableHeaderRE.FindIndex(raw)
	if header == nil {
		return 0, 0, false
	}
	start := header[1]
	end := len(raw)
	if next := tomlTableHeaderRE.FindIndex(raw[start:]); next != nil {
		end = start + next[0]
	}
	return start, end, true
}

func appendSandboxWorkspaceWriteTable(raw []byte, roots []string) []byte {
	out := append([]byte(nil), raw...)
	lineEnding := codexSandboxLineEnding(raw)
	if len(out) > 0 && !bytes.HasSuffix(out, []byte("\n")) {
		out = appendCodexSandboxLineEnding(out, lineEnding)
	}
	if len(out) > 0 && !bytes.HasSuffix(out, []byte(lineEnding+lineEnding)) {
		out = append(out, lineEnding...)
	}
	out = append(out, []byte(codexSandboxWithLineEnding(codexSandboxSnippet(roots), lineEnding)+lineEnding)...)
	return out
}

func insertWritableRootsInSandboxTable(raw []byte, at int, roots []string) []byte {
	lineEnding := codexSandboxLineEnding(raw)
	addition := codexSandboxSnippet(roots)
	addition = strings.TrimPrefix(addition, "[sandbox_workspace_write]\n")
	addition = codexSandboxWithLineEnding(addition, lineEnding)
	if at > 0 && raw[at-1] != '\n' {
		if raw[at-1] == '\r' && lineEnding == "\r\n" {
			addition = "\n" + addition
		} else {
			addition = lineEnding + addition
		}
	}
	addition += lineEnding
	out := make([]byte, 0, len(raw)+len(addition))
	out = append(out, raw[:at]...)
	out = append(out, addition...)
	out = append(out, raw[at:]...)
	return out
}

func tomlArrayEnd(raw []byte, start int) (int, error) {
	if start < 0 || start >= len(raw) || raw[start] != '[' {
		return 0, errors.New("writable_roots opening bracket was not found")
	}
	depth := 0
	var quote byte
	escaped := false
	for i := start; i < len(raw); i++ {
		ch := raw[i]
		if quote != 0 {
			if quote == '"' && escaped {
				escaped = false
				continue
			}
			if quote == '"' && ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		switch ch {
		case '"', '\'':
			quote = ch
		case '#':
			for i < len(raw) && raw[i] != '\n' {
				i++
			}
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return i, nil
			}
		}
	}
	return 0, errors.New("writable_roots array has no closing bracket")
}

func appendRootsToTOMLArray(raw []byte, start, end int, roots []string) []byte {
	var addition strings.Builder
	lineEnding := codexSandboxLineEnding(raw)
	if tomlArrayHasValuesWithoutTrailingComma(raw, start, end) {
		addition.WriteByte(',')
	}
	for _, root := range roots {
		addition.WriteString(lineEnding)
		addition.WriteString("  ")
		addition.WriteString(strconv.Quote(root))
		addition.WriteByte(',')
	}
	addition.WriteString(lineEnding)
	out := make([]byte, 0, len(raw)+addition.Len())
	out = append(out, raw[:end]...)
	out = append(out, addition.String()...)
	out = append(out, raw[end:]...)
	return out
}

func tomlArrayHasValuesWithoutTrailingComma(raw []byte, start, end int) bool {
	var significant bytes.Buffer
	var quote byte
	escaped := false
	for i := start + 1; i < end; i++ {
		ch := raw[i]
		if quote != 0 {
			significant.WriteByte(ch)
			if quote == '"' && escaped {
				escaped = false
				continue
			}
			if quote == '"' && ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '"' || ch == '\'' {
			quote = ch
			significant.WriteByte(ch)
			continue
		}
		if ch == '#' {
			for i < end && raw[i] != '\n' {
				i++
			}
			continue
		}
		significant.WriteByte(ch)
	}
	trimmed := bytes.TrimSpace(significant.Bytes())
	return len(trimmed) > 0 && trimmed[len(trimmed)-1] != ','
}

func codexSandboxLineEnding(raw []byte) string {
	if bytes.Contains(raw, []byte("\r\n")) {
		return "\r\n"
	}
	return "\n"
}

func codexSandboxWithLineEnding(value, lineEnding string) string {
	if lineEnding == "\n" {
		return value
	}
	return strings.ReplaceAll(value, "\n", lineEnding)
}

func appendCodexSandboxLineEnding(raw []byte, lineEnding string) []byte {
	if len(raw) > 0 && raw[len(raw)-1] == '\r' && lineEnding == "\r\n" {
		return append(raw, '\n')
	}
	return append(raw, lineEnding...)
}

func atomicWriteCodexConfig(path string, original, replacement []byte, mode os.FileMode, beforeRename func(string)) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create Codex config directory %q: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".config.toml-*")
	if err != nil {
		return fmt.Errorf("create temporary Codex config in %q: %w", dir, err)
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(mode); err != nil {
		return fmt.Errorf("preserve Codex config mode: %w", err)
	}
	if _, err := tmp.Write(replacement); err != nil {
		return fmt.Errorf("write temporary Codex config: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary Codex config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary Codex config: %w", err)
	}
	if beforeRename != nil {
		beforeRename(path)
	}
	current, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		current = nil
	} else if err != nil {
		return fmt.Errorf("re-read Codex config before replacement: %w", err)
	}
	if !bytes.Equal(current, original) {
		return errCodexConfigChangedConcurrently
	}
	// Topology must also be unchanged: a symlink swapped in after resolution
	// (with identical bytes) would otherwise be replaced by the rename instead
	// of its target being updated. path here is already the resolved canonical
	// destination, so it must still be a regular file (or absent) at rename time.
	if info, lerr := os.Lstat(path); lerr == nil && info.Mode()&os.ModeSymlink != 0 {
		return errCodexConfigChangedConcurrently
	} else if lerr != nil && !errors.Is(lerr, os.ErrNotExist) {
		return fmt.Errorf("inspect Codex config before replacement: %w", lerr)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace Codex config: %w", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open Codex config directory for sync: %w", err)
	}
	if err := dirFile.Sync(); err != nil {
		_ = dirFile.Close()
		return fmt.Errorf("sync Codex config directory: %w", err)
	}
	if err := dirFile.Close(); err != nil {
		return fmt.Errorf("close Codex config directory: %w", err)
	}
	cleanup = false
	return nil
}
