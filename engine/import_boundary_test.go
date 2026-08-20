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

func TestEngineDoesNotImportDaemonInternals(t *testing.T) {
	_, guardFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	forbidden := []string{
		"github.com/charlesnpx/agentbus/internal/service",
		"github.com/charlesnpx/agentbus/internal/jobstore",
		"github.com/charlesnpx/agentbus/internal/schema",
		"github.com/charlesnpx/agentbus/internal/jcs",
	}
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
			for _, blocked := range forbidden {
				if importPath == blocked || strings.HasPrefix(importPath, blocked+"/") {
					t.Fatalf("%s imports daemon-internal package %q", path, importPath)
				}
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
	files, err := filepath.Glob(filepath.Join(adapterDir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}

	const commandImport = "github.com/charlesnpx/agentbus/engine/command"
	var execImportFiles []string
	usesCommandRunner := false
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			switch importPath {
			case "os/exec":
				execImportFiles = append(execImportFiles, filepath.Base(path))
			case commandImport:
				usesCommandRunner = true
			}
		}
	}
	if len(execImportFiles) != 0 {
		t.Fatalf("cliadapter imports forbidden package os/exec in %s", strings.Join(execImportFiles, ", "))
	}
	if !usesCommandRunner {
		t.Fatalf("cliadapter does not import required command runner package %q", commandImport)
	}
}
