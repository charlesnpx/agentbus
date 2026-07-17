package execution_test

import (
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
	fileSet := token.NewFileSet()
	checked := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		checked++
		path := filepath.Join(rule.dir, entry.Name())
		file, err := parser.ParseFile(fileSet, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s imports: %v", path, err)
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("unquote import in %s: %v", path, err)
			}
			for _, forbidden := range rule.forbidden {
				if forbidden.match(importPath) {
					t.Fatalf("%s imports forbidden package %q (matched %s)", path, importPath, forbidden.label)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatalf("no Go files found in %s", rule.dir)
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
