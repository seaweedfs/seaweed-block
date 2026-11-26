package authority

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

// The guard census of promotion bases: every place in the product code that
// sets a PrimaryEvidence basis, with the basis it sets. The publisher mints a
// promotion only with a basis, so this list is every way a primary can be
// promoted; a new one fails this test until it is reviewed and added.
//
//   - fenced: the gate's decision on a fencing round (promotionEvidenceFor
//     with the round's fenced epoch);
//   - heal: the line's own primary continued after a fencing round that
//     published nothing;
//   - planned: a planned move after its guard (failback, rebalance), and the
//     single-replica workspace clean move.
func TestPromotionBasisCensus(t *testing.T) {
	want := []string{
		"core/authority/controller.go plannedEvidence BasisPlanned",
		"core/authority/workspace.go MoveWorkspace BasisPlanned",
		"core/host/master/planned_move.go plannedMoveEvidenceAt BasisPlanned",
		"core/host/master/product_loop.go healAfterFailedFence BasisHeal",
		"core/host/master/product_loop.go promotionEvidenceFor BasisFenced",
	}
	root := filepath.Join("..", "..")
	var got []string
	for _, dir := range []string{"core", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.KeyValueExpr: // Basis: adapter.BasisX
						if k, ok := n.Key.(*ast.Ident); ok && k.Name == "Basis" && isPromotionBasis(n.Value) {
							got = append(got, rel+" "+fn.Name.Name+" "+basisName(n.Value))
						}
					case *ast.AssignStmt: // ev.Basis = adapter.BasisX (also in a tuple)
						for i, lhs := range n.Lhs {
							if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "Basis" && i < len(n.Rhs) && isPromotionBasis(n.Rhs[i]) {
								got = append(got, rel+" "+fn.Name.Name+" "+basisName(n.Rhs[i]))
							}
						}
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
	sort.Strings(got)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("promotion basis setters changed; review each, then update the census.\ngot:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// isPromotionBasis: the value is one of the PromotionBasis constants
// (adapter.BasisX). Known limit: a basis copied from a variable is not seen;
// the bases are only ever set from the constants.
func isPromotionBasis(e ast.Expr) bool {
	sel, ok := e.(*ast.SelectorExpr)
	return ok && strings.HasPrefix(sel.Sel.Name, "Basis")
}

func basisName(e ast.Expr) string {
	if sel, ok := e.(*ast.SelectorExpr); ok {
		return sel.Sel.Name
	}
	return "?"
}
