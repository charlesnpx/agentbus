package execution_test

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/charlesnpx/agentbus"

func TestArchitectureImportGuards(t *testing.T) {
	root := repoRoot(t)
	rules := []importGuardRule{
		{
			name: "model stays pure",
			dir:  filepath.Join(root, "engine", "execution", "model"),
			forbidden: []forbiddenImport{
				exactImport("os/exec"),
				exactImport("syscall"),
				prefixImport("golang.org/x/sys/"),
				exactImport(modulePath + "/internal/cgroup"),
				exactImport(modulePath + "/internal/parklaunch"),
				exactImport(modulePath + "/internal/procgroup"),
				exactImport(modulePath + "/engine/execution/custodian"),
			},
		},
		{
			name: "cliadapter uses command boundary",
			dir:  filepath.Join(root, "engine", "adapter", "internal", "cliadapter"),
			forbidden: []forbiddenImport{
				prefixImport(modulePath + "/engine/execution/"),
				exactImport(modulePath + "/engine/execution"),
			},
		},
		{
			name: "containment does not import cgroup backend",
			dir:  filepath.Join(root, "internal", "containment"),
			forbidden: []forbiddenImport{
				exactImport(modulePath + "/internal/cgroup"),
			},
		},
	}

	for _, rule := range rules {
		t.Run(rule.name, func(t *testing.T) {
			assertNoForbiddenImports(t, rule)
		})
	}
}

func TestServedStartupRecoveryDoesNotImportRepository(t *testing.T) {
	root := repoRoot(t)
	assertFileNoForbiddenImports(t,
		filepath.Join(root, "internal", "served", "admission_recovery.go"),
		[]forbiddenImport{exactImport(modulePath + "/engine/execution/repository")},
	)
}

func TestStartupRecoveryDoesNotDecodeOrReplayParkProtocolFrames(t *testing.T) {
	root := repoRoot(t)
	files := []string{
		filepath.Join(root, "engine", "execution", "authority", "bootstrap.go"),
		filepath.Join(root, "engine", "execution", "authority", "recovery_tokens.go"),
		filepath.Join(root, "engine", "execution", "authority", "startup.go"),
		filepath.Join(root, "internal", "served", "admission_recovery.go"),
	}
	for _, path := range files {
		assertFileNoForbiddenImports(t, path, []forbiddenImport{
			exactImport(modulePath + "/internal/parkproto"),
		})
	}
	assertFilesDoNotContain(t, files, []string{
		"parkproto.",
		"ReleaseBinding",
		"ReleaseExpectation",
		"WriteRelease(",
		"WriteFrame(",
	})
}

func TestOnlyLaunchImportsAuthorityAndCustodianTogether(t *testing.T) {
	root := filepath.Join(repoRoot(t), "engine", "execution")
	importsByPackage := map[string]map[string]bool{}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return fmt.Errorf("parse %s imports: %w", path, err)
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		if rel == "." {
			rel = ""
		}
		seen := importsByPackage[rel]
		if seen == nil {
			seen = map[string]bool{}
			importsByPackage[rel] = seen
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return fmt.Errorf("unquote import in %s: %w", path, err)
			}
			seen[importPath] = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	bridgePackages := 0
	for pkg, imports := range importsByPackage {
		if !imports[modulePath+"/engine/execution/authority"] || !imports[modulePath+"/engine/execution/custodian"] {
			continue
		}
		bridgePackages++
		if pkg != "launch" {
			t.Fatalf("engine/execution/%s imports both authority and custodian; only engine/execution/launch may bridge them", pkg)
		}
	}
	if bridgePackages == 0 {
		t.Fatal("no engine/execution package imports both authority and custodian; expected launch to be the only bridge")
	}
}

// TODO(S5): coordinator still references custodian types through the LaunchContainment
// seam and authority quiescence recording. Add a coordinator !-> custodian import
// guard after those boundary types move out of custodian.

type importGuardRule struct {
	name      string
	dir       string
	forbidden []forbiddenImport
}

type forbiddenImport struct {
	label string
	match func(string) bool
}

func exactImport(path string) forbiddenImport {
	return forbiddenImport{
		label: path,
		match: func(importPath string) bool {
			return importPath == path
		},
	}
}

func prefixImport(prefix string) forbiddenImport {
	return forbiddenImport{
		label: prefix + "*",
		match: func(importPath string) bool {
			return strings.HasPrefix(importPath, prefix)
		},
	}
}

func assertNoForbiddenImports(t *testing.T, rule importGuardRule) {
	t.Helper()
	entries, err := os.ReadDir(rule.dir)
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		checked++
		path := filepath.Join(rule.dir, entry.Name())
		assertFileNoForbiddenImports(t, path, rule.forbidden)
	}
	if checked == 0 {
		t.Fatalf("no Go files found in %s", rule.dir)
	}
}

func assertFileNoForbiddenImports(t *testing.T, path string, forbiddenImports []forbiddenImport) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s imports: %v", path, err)
	}
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatalf("unquote import in %s: %v", path, err)
		}
		for _, forbidden := range forbiddenImports {
			if forbidden.match(importPath) {
				t.Fatalf("%s imports forbidden package %q (matched %s)", path, importPath, forbidden.label)
			}
		}
	}
}

func assertFilesDoNotContain(t *testing.T, paths []string, forbidden []string) {
	t.Helper()
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, text := range forbidden {
			if strings.Contains(string(raw), text) {
				t.Fatalf("%s contains forbidden recovery frame token %q", path, text)
			}
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
