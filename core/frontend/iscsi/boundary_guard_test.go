// Ownership: QA (from sketch-approved test spec v3-phase-15-t2-test-spec.md for T2 First Real Frontends).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t2-test-spec.md
// Maps to ledger rows: PCDD-STUFFING-001 (extended to iSCSI package)
//
// Test layer: Unit (AST)
// Protocol: iSCSI
// Bad-state family: PCDD-STUFFING-001
// Bounded fate: core/frontend/iscsi production code must not
// import core/authority or core/adapter, and must not construct
// any authority-minting composite literal. Covers
// T2.L0.i5 (no authority minting) + T2.L0.i6 (no core/adapter
// import) in one file because the two rules share a single walk.
package iscsi_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// forbiddenImports: plain or dot imports of these packages flag
// an error. Extends T1's frontend-boundary rule to the iSCSI
// package: T2 tightens by forbidding core/adapter as well
// (readiness must come through frontend.Provider, not direct
// adapter access).
var forbiddenImports = []string{
	"github.com/seaweedfs/seaweed-block/core/authority",
	"github.com/seaweedfs/seaweed-block/core/adapter",
}

// forbiddenMinters: composite literal types whose construction
// in production iSCSI code is prohibited. Matched on canonical
// import path (alias-safe). Frontend protocols must NEVER mint
// authority lineage.
type forbiddenMinter struct {
	importPath string
	typeName   string
}

var forbiddenMinters = []forbiddenMinter{
	{importPath: "github.com/seaweedfs/seaweed-block/core/adapter", typeName: "AssignmentInfo"},
	{importPath: "github.com/seaweedfs/seaweed-block/core/adapter", typeName: "AssignmentAsk"},
	{importPath: "github.com/seaweedfs/seaweed-block/core/authority", typeName: "AssignmentAsk"},
	{importPath: "github.com/seaweedfs/seaweed-block/core/authority", typeName: "DurableRecord"},
	{importPath: "github.com/seaweedfs/seaweed-block/core/authority", typeName: "AuthorityBasis"},
}

func TestT2ISCSI_Boundary_NoAuthorityMintingAndNoCoreAdapterImport(t *testing.T) {
	root, err := findISCSIPkgRoot(t)
	if err != nil {
		t.Fatalf("findISCSIPkgRoot: %v", err)
	}

	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		checkISCSIFile(t, path)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
}

func checkISCSIFile(t *testing.T, path string) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		t.Errorf("%s: parse: %v", path, err)
		return
	}

	// Rule 1: no plain import of core/authority or core/adapter.
	// Rule 1b: no dot-import either — a dot import would surface
	// AssignmentInfo{} as a bare (unqualified) literal that the
	// selector-based Rule 2 walk cannot see.
	for _, imp := range f.Imports {
		p := strings.Trim(imp.Path.Value, `"`)
		for _, bad := range forbiddenImports {
			if p == bad {
				t.Errorf("%s: forbidden import %q in core/frontend/iscsi production code (sketch §3, test-spec §4 T2.L0.i6)",
					path, p)
			}
		}
		if imp.Name != nil && imp.Name.Name == "." {
			for _, bad := range forbiddenImports {
				if p == bad {
					t.Errorf("%s: forbidden dot-import %q — bare-literal constructions would bypass the AST guard",
						path, p)
				}
			}
		}
	}

	// Rule 2: no forbidden composite literal, alias-resolved.
	aliases := importAliasMap(f)
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
		canonical, has := aliases[pkgIdent.Name]
		if !has {
			return true
		}
		for _, m := range forbiddenMinters {
			if canonical == m.importPath && sel.Sel.Name == m.typeName {
				t.Errorf("%s: forbidden composite literal %s.%s (resolves to %s.%s) in iSCSI production code (sketch §3)",
					path, pkgIdent.Name, sel.Sel.Name, m.importPath, m.typeName)
			}
		}
		return true
	})
}

func importAliasMap(f *ast.File) map[string]string {
	m := map[string]string{}
	for _, imp := range f.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		var alias string
		if imp.Name != nil {
			if imp.Name.Name == "_" || imp.Name.Name == "." {
				continue
			}
			alias = imp.Name.Name
		} else {
			alias = path
			if i := strings.LastIndex(alias, "/"); i >= 0 {
				alias = alias[i+1:]
			}
		}
		m[alias] = path
	}
	return m
}

func findISCSIPkgRoot(t *testing.T) (string, error) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", &pkgRootError{"runtime.Caller failed"}
	}
	return filepath.Dir(file), nil
}

type pkgRootError struct{ msg string }

func (e *pkgRootError) Error() string { return e.msg }
