package csi_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

func TestG15a_CSI_DoesNotImportAuthorityOrConstructAssignmentFacts(t *testing.T) {
	root := "."
	matches, err := filepath.Glob(filepath.Join(root, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range matches {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, imp := range file.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			if strings.Contains(p, "/core/authority") || strings.Contains(p, "/core/adapter") {
				t.Fatalf("%s imports forbidden package %q", path, p)
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			switch typ := cl.Type.(type) {
			case *ast.SelectorExpr:
				name := typ.Sel.Name
				if name == "AssignmentInfo" || name == "AssignmentFact" || name == "AssignmentAsk" {
					t.Fatalf("%s constructs forbidden authority-shaped type %s", path, name)
				}
			case *ast.Ident:
				name := typ.Name
				if name == "AssignmentInfo" || name == "AssignmentFact" || name == "AssignmentAsk" {
					t.Fatalf("%s constructs forbidden authority-shaped type %s", path, name)
				}
			}
			return true
		})
	}
}
