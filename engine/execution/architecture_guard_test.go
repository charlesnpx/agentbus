package execution_test

import (
	"fmt"
	"go/ast"
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
	files := collectNonTestGoFiles(t, []string{
		filepath.Join(root, "engine", "execution", "authority"),
		filepath.Join(root, "internal", "served"),
	})
	forbiddenImports := []forbiddenImport{
		exactImport(modulePath + "/internal/parkproto"),
	}
	for _, path := range files {
		assertFileNoForbiddenImports(t, path, forbiddenImports)
		assertFileNoParkProtocolFrameCalls(t, path)
	}
}

func TestLogicalLayerDoesNotReferenceReleaseCredentials(t *testing.T) {
	root := repoRoot(t)
	files := collectNonTestGoFiles(t, []string{
		filepath.Join(root, "engine", "execution", "model"),
		filepath.Join(root, "engine", "execution", "launch"),
		filepath.Join(root, "internal", "served"),
	})
	forbidden := map[string]bool{
		"ReleaseSecret": true,
		"GrantToken":    true,
	}
	for _, path := range files {
		file := parseGoFile(t, path, 0)
		ast.Inspect(file, func(node ast.Node) bool {
			ident, ok := node.(*ast.Ident)
			if !ok || !forbidden[ident.Name] {
				return true
			}
			t.Fatalf("%s references logical-layer release credential identifier %s", path, ident.Name)
			return false
		})
	}
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

func assertFileNoParkProtocolFrameCalls(t *testing.T, path string) {
	t.Helper()
	file := parseGoFile(t, path, 0)
	parkprotoAliases := map[string]bool{}
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatalf("unquote import in %s: %v", path, err)
		}
		if importPath != modulePath+"/internal/parkproto" {
			continue
		}
		switch {
		case spec.Name == nil:
			parkprotoAliases["parkproto"] = true
		case spec.Name.Name == ".":
			parkprotoAliases["."] = true
		default:
			parkprotoAliases[spec.Name.Name] = true
		}
	}
	forbiddenSelectors := map[string]bool{
		"NewReader":      true,
		"Read":           true,
		"ReadRawFrame":   true,
		"WriteFrame":     true,
		"WriteRelease":   true,
		"Release":        true,
		"ReleaseBinding": true,
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.SelectorExpr:
			ident, ok := n.X.(*ast.Ident)
			if ok && parkprotoAliases[ident.Name] && forbiddenSelectors[n.Sel.Name] {
				t.Fatalf("%s calls or references forbidden parkproto recovery frame member %s.%s", path, ident.Name, n.Sel.Name)
			}
		case *ast.Ident:
			if parkprotoAliases["."] && forbiddenSelectors[n.Name] {
				t.Fatalf("%s calls or references forbidden parkproto recovery frame member %s", path, n.Name)
			}
		}
		return true
	})
}

func collectNonTestGoFiles(t *testing.T, dirs []string) []string {
	t.Helper()
	var files []string
	for _, dir := range dirs {
		if err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			if strings.HasSuffix(entry.Name(), ".go") && !strings.HasSuffix(entry.Name(), "_test.go") {
				files = append(files, path)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(files) == 0 {
		t.Fatalf("no non-test Go files found in %v", dirs)
	}
	return files
}

func parseGoFile(t *testing.T, path string, mode parser.Mode) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, mode)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return file
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
