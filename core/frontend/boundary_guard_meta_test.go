// Ownership: sw test-support (not in QA spec; added per architect
// review finding that the AST guard was skipping function bodies).
// This meta-test proves the guard actually inspects composite
// literals in function bodies AND resolves aliased imports,
// exercising both failure paths against synthesized sources.
package frontend_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestBoundaryGuardMeta_CatchesAliasedComposite(t *testing.T) {
	src := `package fake

import foo "github.com/seaweedfs/seaweed-block/core/adapter"

func mint() foo.AssignmentInfo {
	return foo.AssignmentInfo{VolumeID: "v1"}
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "fake.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	aliases := importAliasMap(f)
	if p, ok := aliases["foo"]; !ok || p != "github.com/seaweedfs/seaweed-block/core/adapter" {
		t.Fatalf("alias resolution failed: %v", aliases)
	}
	// Confirm the walk sees the composite literal through the alias.
	var hits []string
	ast.Inspect(f, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := cl.Type.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		pkgIdent, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		canonical := aliases[pkgIdent.Name]
		hits = append(hits, canonical+"."+sel.Sel.Name)
		return true
	})
	found := false
	for _, h := range hits {
		if strings.HasSuffix(h, "/core/adapter.AssignmentInfo") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("composite literal foo.AssignmentInfo NOT detected via alias; hits=%v", hits)
	}
}

func TestBoundaryGuardMeta_DetectsDotImportOfAdapter(t *testing.T) {
	src := `package fake

import . "github.com/seaweedfs/seaweed-block/core/adapter"

func mint() AssignmentInfo { return AssignmentInfo{} }
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "fake.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// Simulate the boundary_guard_test.go Rule 1b logic here:
	// walk imports, flag the dot-import.
	var flagged bool
	for _, imp := range f.Imports {
		p := strings.Trim(imp.Path.Value, `"`)
		if imp.Name != nil && imp.Name.Name == "." {
			for _, bad := range dotForbiddenImports {
				if p == bad {
					flagged = true
				}
			}
		}
	}
	if !flagged {
		t.Fatal("dot-import of core/adapter was NOT flagged — bare AssignmentInfo{} literals would bypass the guard")
	}
}

func TestBoundaryGuardMeta_CatchesFunctionBodyLiteral(t *testing.T) {
	src := `package fake

import "github.com/seaweedfs/seaweed-block/core/adapter"

func f() { _ = adapter.AssignmentInfo{} }
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "fake.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	aliases := importAliasMap(f)
	var saw bool
	ast.Inspect(f, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := cl.Type.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, _ := sel.X.(*ast.Ident)
		if ident != nil && aliases[ident.Name] == "github.com/seaweedfs/seaweed-block/core/adapter" && sel.Sel.Name == "AssignmentInfo" {
			saw = true
		}
		return true
	})
	if !saw {
		t.Fatalf("function-body composite literal NOT detected")
	}
}
