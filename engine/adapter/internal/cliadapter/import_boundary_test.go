package cliadapter

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCliAdapterAndCommandDoNotImportExecutionPackages(t *testing.T) {
	packages := []struct {
		name string
		dir  string
	}{
		{name: "cliadapter", dir: "."},
		{name: "engine/command", dir: "../../../command"},
	}
	for _, pkg := range packages {
		t.Run(pkg.name, func(t *testing.T) {
			assertPackageDoesNotImportExecution(t, pkg.dir)
		})
	}
}

func assertPackageDoesNotImportExecution(t *testing.T, dir string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("no Go files found in %s", dir)
	}
	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.IsDir() {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s imports: %v", path, err)
		}
		for _, importSpec := range file.Imports {
			importPath := strings.Trim(importSpec.Path.Value, `"`)
			if importsExecutionPackage(importPath) {
				t.Fatalf("%s imports forbidden execution package %q", path, importPath)
			}
		}
	}
}

func importsExecutionPackage(importPath string) bool {
	return strings.Contains(importPath, "/engine/execution/") || strings.HasSuffix(importPath, "/engine/execution")
}
