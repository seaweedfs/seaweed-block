package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The node registry (server id -> registered host) is static after start:
// the identity checks (the registered-host refusal beside the certificate
// identity) assume no one rewrites it while blockmaster serves. Its only
// writer is the cluster-spec import at start-up. This guard fails when a
// new non-test caller of RegisterNode or RemoveNode appears anywhere.
func TestNodeRegistryIsWrittenOnlyAtStartup(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	var sites []string
	fset := token.NewFileSet()
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "RegisterNode" || sel.Sel.Name == "RemoveNode") {
					sites = append(sites, filepath.ToSlash(rel)+":"+fn.Name.Name+"."+sel.Sel.Name)
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(sites)
	want := []string{"cmd/blockmaster/main.go:importLifecycleNodeInventory.RegisterNode"}
	if strings.Join(sites, ",") != strings.Join(want, ",") {
		t.Fatalf("node registry writers = %v, want only %v: a writer after start breaks the static-registry assumption of the identity checks", sites, want)
	}
}
