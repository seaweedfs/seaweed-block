// Ownership: QA (from sketch-approved test spec v3-phase-15-t2-test-spec.md for T2 First Real Frontends).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t2-test-spec.md
// Maps to ledger rows: PCDD-STUFFING-001 (extended to NVMe package)
//
// Test layer: Unit (AST)
// Protocol: NVMe/TCP
// Bounded fate: core/frontend/nvme production code must not
// import core/authority or core/adapter, and must not construct
// any authority-minting composite literal. Same invariants as
// the iSCSI boundary guard — extends PCDD-STUFFING-001 coverage
// to the NVMe package.
package nvme_test

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

var forbiddenImports = []string{
	"github.com/seaweedfs/seaweed-block/core/authority",
	"github.com/seaweedfs/seaweed-block/core/adapter",
}

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

func TestT2NVMe_Boundary_NoAuthorityMintingAndNoCoreAdapterImport(t *testing.T) {
	root, err := findNVMePkgRoot(t)
	if err != nil {
		t.Fatalf("findNVMePkgRoot: %v", err)
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
		checkNVMeFile(t, path)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
}

func checkNVMeFile(t *testing.T, path string) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		t.Errorf("%s: parse: %v", path, err)
		return
	}

	for _, imp := range f.Imports {
		p := strings.Trim(imp.Path.Value, `"`)
		for _, bad := range forbiddenImports {
			if p == bad {
				t.Errorf("%s: forbidden import %q in core/frontend/nvme production code",
					path, p)
			}
		}
		if imp.Name != nil && imp.Name.Name == "." {
			for _, bad := range forbiddenImports {
				if p == bad {
					t.Errorf("%s: forbidden dot-import %q — bare literals would bypass the AST guard",
						path, p)
				}
			}
		}
	}

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
				t.Errorf("%s: forbidden composite literal %s.%s (resolves to %s.%s)",
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

func findNVMePkgRoot(t *testing.T) (string, error) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", &pkgRootError{"runtime.Caller failed"}
	}
	return filepath.Dir(file), nil
}

type pkgRootError struct{ msg string }

func (e *pkgRootError) Error() string { return e.msg }
