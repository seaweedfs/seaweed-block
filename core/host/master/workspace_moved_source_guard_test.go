package master

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// This proof is only a startup stream refusal, never lease admission or a
// projection update. New readers must add their own causal probe here.
func TestMovedSourceProofCallers(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	var sites []string
	for _, top := range []string{"core", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return err
			}
			f, err := parser.ParseFile(token.NewFileSet(), p, nil, 0)
			if err != nil {
				return err
			}
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				owner := "<package>"
				var scope ast.Node = decl
				if ok {
					if fn.Body == nil {
						continue
					}
					owner, scope = fn.Name.Name, fn.Body
				}
				ast.Inspect(scope, func(n ast.Node) bool {
					if s, ok := n.(*ast.SelectorExpr); ok && s.Sel.Name == "WorkspaceSourceMoved" {
						rel, _ := filepath.Rel(root, p)
						sites = append(sites, filepath.ToSlash(rel)+" "+owner)
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
	want := []string{"core/host/master/services.go SubscribeAssignments"}
	if !slices.Equal(sites, want) {
		t.Fatalf("moved-source readers changed: %v; each needs its own refusal/control probe", sites)
	}
}
