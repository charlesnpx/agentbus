package execution_test

import (
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

func TestExecutionModelDoesNotImportProcessPackages(t *testing.T) {
	root := repoRoot(t)
	assertNoForbiddenImports(t, importGuardRule{
		name: "model stays pure",
		dir:  filepath.Join(root, "engine", "execution", "model"),
		forbidden: []forbiddenImport{
			exactImport("os"),
			prefixImport("os/"),
			exactImport("syscall"),
			prefixImport("golang.org/x/sys/"),
			exactImport(modulePath + "/internal/cgroup"),
			exactImport(modulePath + "/internal/parklaunch"),
			exactImport(modulePath + "/internal/procgroup"),
		},
	})
}

func TestCliAdapterImportsCommandBoundary(t *testing.T) {
	root := repoRoot(t)
	files := collectNonTestGoFiles(t, []string{filepath.Join(root, "engine", "adapter", "internal", "cliadapter")})
	importsCommand := false
	for _, path := range files {
		file := parseGoFile(t, path, parser.ImportsOnly)
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("unquote import in %s: %v", path, err)
			}
			switch {
			case importPath == "os/exec":
				t.Fatalf("%s imports os/exec; cliadapter must use engine/command runners", path)
			case importPath == modulePath+"/engine/command":
				importsCommand = true
			case strings.HasPrefix(importPath, modulePath+"/engine/execution/") || importPath == modulePath+"/engine/execution":
				t.Fatalf("%s imports %s; cliadapter must not depend on execution internals", path, importPath)
			}
		}
	}
	if !importsCommand {
		t.Fatal("cliadapter does not import engine/command")
	}
}

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
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
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
