package volume

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoOtherAssignmentInfoConstruction is the PCDD-STUFFING-001
// AST guard required by T0 sketch §9 / §6.4 rule 5 / §10.7.
//
// Rule: at most ONE adapter.AssignmentInfo composite literal in
// core/host/volume/*.go, and that one literal MUST live inside
// the single named decoder decodeAssignmentFact in subscribe.go.
// Scattered literals anywhere else in this package fail the test.
//
// This is the durable enforcement of the volume-side decode
// boundary (sketch §3.1). Without it, a future patch could
// silently add a second construction site and leave the rule
// code-review-only — which historically drifts within a release.
func TestNoOtherAssignmentInfoConstruction(t *testing.T) {
	fset := token.NewFileSet()
	// Walk every .go file in core/host/volume (the current
	// package directory). Skip _test.go files — tests are allowed
	// to construct adapter.AssignmentInfo in fixtures.
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}

	type hit struct {
		file string
		fn   string
		line int
	}
	var hits []hit

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(".", e.Name())
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		f, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		// Track which function each node is inside of.
		ast.Inspect(f, func(n ast.Node) bool {
			decl, ok := n.(*ast.FuncDecl)
			if !ok {
				return true
			}
			fnName := decl.Name.Name
			ast.Inspect(decl, func(n ast.Node) bool {
				cl, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				if isAssignmentInfoType(cl.Type) {
					pos := fset.Position(cl.Pos())
					hits = append(hits, hit{file: path, fn: fnName, line: pos.Line})
				}
				return true
			})
			return false
		})
	}

	// Require exactly one hit, inside decodeAssignmentFact in
	// subscribe.go. Any other shape fails.
	if len(hits) != 1 {
		t.Fatalf("expected exactly 1 adapter.AssignmentInfo composite literal in core/host/volume (outside tests); got %d: %+v", len(hits), hits)
	}
	h := hits[0]
	if filepath.Base(h.file) != "subscribe.go" {
		t.Fatalf("adapter.AssignmentInfo literal must live in subscribe.go; found in %s at line %d (fn %s)", h.file, h.line, h.fn)
	}
	if h.fn != "decodeAssignmentFact" {
		t.Fatalf("adapter.AssignmentInfo literal must live inside decodeAssignmentFact; found in %s at line %d", h.fn, h.line)
	}
}

// isAssignmentInfoType returns true if the composite-literal type
// expression refers to adapter.AssignmentInfo. Matches
// SelectorExpr{X: Ident("adapter"), Sel: Ident("AssignmentInfo")}.
// Does NOT match unqualified AssignmentInfo — if anyone dot-imports
// the adapter package that is itself a boundary violation and
// would be caught by other tests.
func isAssignmentInfoType(t ast.Expr) bool {
	sel, ok := t.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkgIdent, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return pkgIdent.Name == "adapter" && sel.Sel.Name == "AssignmentInfo"
}
