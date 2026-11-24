package chunkstore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// TestHeadCreate_SourceGuard holds every HEAD creator to head_create.go: a
// function of this package that builds a HEAD entry (headEntry) and creates
// an entry (CreateEntry, createResolved) must go through createHead or
// headCreateCheck; and no code but headEntry builds an entry named HEAD.
func TestHeadCreate_SourceGuard(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var creators []string
	for _, fi := range files {
		name := fi.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			calls := map[string]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					switch f := x.Fun.(type) {
					case *ast.Ident:
						calls[f.Name] = true
					case *ast.SelectorExpr:
						calls[f.Sel.Name] = true
					}
				case *ast.KeyValueExpr:
					if k, ok := x.Key.(*ast.Ident); ok && k.Name == "Name" {
						if v, ok := x.Value.(*ast.Ident); ok && v.Name == "headName" && fn.Name.Name != "headEntry" {
							t.Errorf("%s: %s builds an entry named HEAD; only headEntry may", fset.Position(x.Pos()), fn.Name.Name)
						}
					}
				}
				return true
			})
			if calls["headEntry"] && (calls["CreateEntry"] || calls["createResolved"] || calls["createHead"]) {
				creators = append(creators, fn.Name.Name)
				if !calls["createHead"] && !calls["headCreateCheck"] {
					t.Errorf("%s: %s creates a HEAD without createHead or headCreateCheck", fset.Position(fn.Pos()), fn.Name.Name)
				}
			}
		}
	}
	sort.Strings(creators)
	// The scan must see the known creators, or it proves nothing.
	for _, want := range []string{"CASHead", "ensureInitialHead", "fork", "writeHead"} {
		if i := sort.SearchStrings(creators, want); i == len(creators) || creators[i] != want {
			t.Errorf("guard did not find HEAD creator %s (found %v)", want, creators)
		}
	}
}
