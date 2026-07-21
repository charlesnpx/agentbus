package cliadapter

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestCliAdapterHasNoExecCallSites(t *testing.T) {
	sites, err := findExecCallSites(".")
	if err != nil {
		t.Fatal(err)
	}

	var got []string
	for _, site := range sites {
		got = append(got, site.String())
	}
	sort.Strings(got)
	if len(got) != 0 {
		t.Fatalf("cliadapter production os/exec call sites:\n%s", strings.Join(got, "\n"))
	}
}

type execCallSite struct {
	File     string
	Func     string
	Selector string
}

func (site execCallSite) String() string {
	return fmt.Sprintf("%s:%s:%s", site.File, site.Func, site.Selector)
}

func findExecCallSites(dir string) ([]execCallSite, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}

	var sites []execCallSite
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fileSites, err := findExecCallSitesInFile(path)
		if err != nil {
			return nil, err
		}
		sites = append(sites, fileSites...)
	}
	sort.Slice(sites, func(i, j int) bool { return sites[i].String() < sites[j].String() })
	return sites, nil
}

func findExecCallSitesInFile(path string) ([]execCallSite, error) {
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	execImports := map[string]struct{}{}
	for _, importSpec := range file.Imports {
		if strings.Trim(importSpec.Path.Value, `"`) != "os/exec" {
			continue
		}
		name := "exec"
		if importSpec.Name != nil {
			name = importSpec.Name.Name
		}
		if name != "_" && name != "." {
			execImports[name] = struct{}{}
		}
	}
	if len(execImports) == 0 {
		return nil, nil
	}

	var sites []execCallSite
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		funcName := qualifiedFuncName(fn)
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			ident, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			if _, ok := execImports[ident.Name]; !ok {
				return true
			}
			sites = append(sites, execCallSite{
				File:     filepath.Base(path),
				Func:     funcName,
				Selector: ident.Name + "." + selector.Sel.Name,
			})
			return true
		})
	}
	return sites, nil
}

func qualifiedFuncName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	return receiverTypeName(fn.Recv.List[0].Type) + "." + fn.Name.Name
}

func receiverTypeName(expr ast.Expr) string {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.StarExpr:
		return receiverTypeName(typed.X)
	default:
		return fmt.Sprintf("%T", expr)
	}
}
