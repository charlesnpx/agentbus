package engine_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestEngineDoesNotImportModuleInternals(t *testing.T) {
	_, guardFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	const moduleInternalImportRoot = "github.com/charlesnpx/agentbus/internal"
	err := filepath.WalkDir(filepath.Dir(guardFile), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if importPath == moduleInternalImportRoot || strings.HasPrefix(importPath, moduleInternalImportRoot+"/") {
				t.Fatalf("%s imports module-internal package %q", path, importPath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCLIAdapterUsesCommandRunners(t *testing.T) {
	_, guardFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	adapterDir := filepath.Join(filepath.Dir(guardFile), "adapter", "internal", "cliadapter")

	const commandImport = "github.com/charlesnpx/agentbus/engine/command"
	var execImportFiles []string
	usesCommandRunner := false
	err := filepath.WalkDir(adapterDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relativePath, err := filepath.Rel(adapterDir, path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			switch importPath {
			case "os/exec":
				execImportFiles = append(execImportFiles, filepath.ToSlash(relativePath))
			case commandImport:
				usesCommandRunner = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(execImportFiles) != 0 {
		t.Fatalf("cliadapter imports forbidden package os/exec in %s", strings.Join(execImportFiles, ", "))
	}
	if !usesCommandRunner {
		t.Fatalf("cliadapter does not import required command runner package %q", commandImport)
	}
}
