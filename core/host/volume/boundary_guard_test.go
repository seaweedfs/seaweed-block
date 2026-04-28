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

	// Allowed construction sites (PCDD-STUFFING-001 scope):
	//   1. decodeAssignmentFact in subscribe.go — the master-to-host
	//      fact decoder (T0 sketch §3.1).
	//   2. OnPeerAdded in peer_adapter_registry.go — G5-5C Batch #7
	//      per-peer engine identity priming. The registry constructs
	//      one AssignmentInfo per admitted peer to seed the per-peer
	//      adapter's engine state (so engine.checkReplicaID accepts
	//      probe results targeted at that peer). This is NOT a
	//      master-decode site; it's a peer-state initializer using
	//      data already decoded upstream and routed through
	//      ReplicaTarget.
	//
	// Any literal outside these two named functions fails the test —
	// the rule predates Batch #7 but the per-peer-adapter shape
	// requires a 2nd site that is bounded, named, and AST-pinned
	// here.
	allowed := map[string]string{
		"decodeAssignmentFact": "subscribe.go",
		"OnPeerAdded":          "peer_adapter_registry.go",
	}

	if len(hits) != len(allowed) {
		t.Fatalf("expected exactly %d adapter.AssignmentInfo composite literals in core/host/volume (outside tests); got %d: %+v",
			len(allowed), len(hits), hits)
	}
	for _, h := range hits {
		wantFile, ok := allowed[h.fn]
		if !ok {
			t.Errorf("adapter.AssignmentInfo literal in disallowed function %s (file %s, line %d). Allowed: %v",
				h.fn, h.file, h.line, allowed)
			continue
		}
		if filepath.Base(h.file) != wantFile {
			t.Errorf("adapter.AssignmentInfo literal in %s must live in %s; found in %s at line %d",
				h.fn, wantFile, h.file, h.line)
		}
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
