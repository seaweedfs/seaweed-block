// Ownership: QA (from sketch-approved test spec v3-phase-15-t1-test-spec.md for T1 Frontend Contract Smoke).
// sw may NOT modify this file without architect approval via §8B.4 Discovery Bridge.
// See: sw-block/design/v3-phase-15-t1-test-spec.md
// Maps to ledger rows: PCDD-STUFFING-001
//
// Test layer: Unit (AST guard)
// Bad-state family: PCDD-STUFFING-001 (authority stuffing)
// Bounded fate: core/frontend production files must not import
// core/authority nor construct AssignmentInfo / AssignmentAsk /
// DurableRecord / AuthorityBasis composite literals — including
// through aliased imports. Rules enforced at the AST level: the
// guard always performs a full parse (function bodies included)
// and resolves each composite literal's import through a per-
// file alias map before matching against the forbidden list.
package frontend_test

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

// forbiddenImports are packages frontend production code must
// never import. sketch §9 top-of-list.
var forbiddenImports = []string{
	"github.com/seaweedfs/seaweed-block/core/authority",
}

// dotForbiddenImports are packages whose `.` import form is
// specifically banned. A dot-import would make the package's
// exported types addressable as bare identifiers, which the
// selector-based Rule 2 walk cannot see (AssignmentInfo{} would
// parse as an unqualified composite literal). We block both
// authority AND adapter for the frontend boundary.
var dotForbiddenImports = []string{
	"github.com/seaweedfs/seaweed-block/core/authority",
	"github.com/seaweedfs/seaweed-block/core/adapter",
}

// forbiddenMinters are types frontend production code must never
// construct as composite literals (aliased imports included).
// Matched on the CANONICAL import path, not the local alias.
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

func TestFrontendCannotMintAuthority_BoundaryGuard(t *testing.T) {
	root, err := findFrontendPkgRoot(t)
	if err != nil {
		t.Fatalf("findFrontendPkgRoot: %v", err)
	}

	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil // test files may reference types for fakes
		}
		checkFile(t, path)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
}

// checkFile parses one production .go file and runs both rules.
// ALWAYS does a full parse (not imports-only) so function-body
// composite literals are reachable; the prior imports-only fast
// path silently skipped all body-level checks.
func checkFile(t *testing.T, path string) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		t.Errorf("%s: parse: %v", path, err)
		return
	}

	// Rule 1: no forbidden imports (direct match on quoted path).
	for _, imp := range f.Imports {
		p := strings.Trim(imp.Path.Value, `"`)
		for _, bad := range forbiddenImports {
			if p == bad {
				t.Errorf("%s: forbidden import %q in core/frontend production code", path, p)
			}
		}
		// Rule 1b: reject dot-imports of any authority-adjacent
		// package. A `. "core/adapter"` import makes
		// AssignmentInfo{} a bare (unqualified) composite literal,
		// invisible to Rule 2's selector-expression walk. Forbid
		// the dot-import outright so that loophole stays closed.
		if imp.Name != nil && imp.Name.Name == "." {
			for _, dotForbidden := range dotForbiddenImports {
				if p == dotForbidden {
					t.Errorf("%s: forbidden dot-import %q — bare-literal constructions would bypass the AST guard",
						path, p)
				}
			}
		}
	}

	// Rule 2: no composite literal for a forbidden type, where
	// "forbidden" is matched on the canonical import path the
	// local alias resolves to. Build the alias map FIRST, then
	// walk all composite literals in the file.
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
		localName := pkgIdent.Name
		typeName := sel.Sel.Name
		canonical, aliasFound := aliases[localName]
		if !aliasFound {
			return true // identifier isn't a recognized import alias
		}
		for _, m := range forbiddenMinters {
			if canonical == m.importPath && typeName == m.typeName {
				t.Errorf("%s: forbidden composite literal %s.%s (resolves to %s.%s) in core/frontend production code — frontend may not mint authority lineage (sketch §9)",
					path, localName, typeName, m.importPath, m.typeName)
			}
		}
		return true
	})
}

// importAliasMap returns a map from the local alias used inside
// the file (the "X" in X.Type{...}) to the canonical package
// path. For a normal import the alias is the last segment of
// the path; an aliased import uses the explicit name; a "." or
// "_" import does not contribute a usable alias.
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
			// last path segment
			alias = path
			if i := strings.LastIndex(alias, "/"); i >= 0 {
				alias = alias[i+1:]
			}
		}
		m[alias] = path
	}
	return m
}

// findFrontendPkgRoot returns the directory containing the
// core/frontend package. Uses runtime.Caller so the walk starts
// at the package root regardless of the `go test` cwd.
func findFrontendPkgRoot(t *testing.T) (string, error) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", &pkgRootError{"runtime.Caller failed"}
	}
	return filepath.Dir(file), nil
}

type pkgRootError struct{ msg string }

func (e *pkgRootError) Error() string { return e.msg }
