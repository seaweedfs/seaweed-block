package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The runtime uses the master-bound reader; the legacy constructor is not a
// fallback. Every constructor/resolver caller is a classified production site.
func TestWorkspaceLayoutHEADReaderWiring(t *testing.T) {
	want := map[string]int{"cmd/blockmaster/main.go run -> NewLayoutWorkspaceHEADReader": 1, "cmd/blockmaster/main.go run -> WorkspaceLayoutVolume": 1}
	found := map[string]int{}
	root := filepath.Join("..", "..")
	for _, top := range []string{"core", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return err
			}
			f, err := parser.ParseFile(token.NewFileSet(), p, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, p)
			for _, d := range f.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					c, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					name := ""
					switch x := c.Fun.(type) {
					case *ast.Ident:
						name = x.Name
					case *ast.SelectorExpr:
						name = x.Sel.Name
					}
					switch name {
					case "NewLayoutWorkspaceHEADReader", "NewWorkspaceHEADReader", "WorkspaceLayoutVolume":
						found[filepath.ToSlash(rel)+" "+fn.Name.Name+" -> "+name]++
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(want, found) {
		t.Fatalf("HEAD reader wiring changed; classify and probe the new site: want %v, got %v", want, found)
	}
}
