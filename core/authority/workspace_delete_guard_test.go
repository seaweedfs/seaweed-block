package authority

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeletionReservationGuardSites(t *testing.T) {
	// A new field access or grant-bound writer must join the causal controls.
	flagSites := map[string]bool{
		"reserveWorkspaceDeletion": true, "NoLiveWorkspaceLease": true,
		"applyWithWorkspaceMove": true, "MoveWorkspace": true,
		"RenewWorkspaceLease": true, "firstWorkspaceGrant": true,
		"regrantWorkspaceLease": true, "extendWorkspaceLease": true,
	}
	boundSites := map[string]bool{
		"publishIssuedLeaseLocked": true, "setLeaseDeadlineLocked": true,
		"RenewWorkspaceLease": true, "regrantWorkspaceLease": true,
		"extendWorkspaceLease": true,
		// Reply-only copies never increase published validity.
		"WorkspaceLeaseReply": true, "AckWorkspaceRevocation": true,
		// Copies the published horizon into the leaf view under mu -> leaseMu.
		"deriveLeaseLocked": true,
		// Positive lease construction and publication, or a plain clone.
		"newWorkspaceLease": true, "firstWorkspaceGrant": true,
		"issueWorkspaceMove": true, "cloneWorkspace": true,
	}
	seenFlag, seenBound := map[string]bool{}, map[string]bool{}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), filepath.Clean(e.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			name := "package initializer"
			if fn, ok := d.(*ast.FuncDecl); ok {
				name = fn.Name.Name
			}
			ast.Inspect(d, func(n ast.Node) bool {
				if s, ok := n.(*ast.SelectorExpr); ok && s.Sel.Name == "deleteReserved" {
					seenFlag[name] = true
					if !flagSites[name] {
						t.Errorf("unclassified deletion flag reader/writer %s in %s", name, e.Name())
					}
				}
				if a, ok := n.(*ast.AssignStmt); ok {
					for _, lhs := range a.Lhs {
						if s, ok := lhs.(*ast.SelectorExpr); ok && (s.Sel.Name == "deadline" || s.Sel.Name == "horizon" || s.Sel.Name == "ExpiresAtNS" || s.Sel.Name == "Lease") {
							seenBound[name] = true
							if !boundSites[name] {
								t.Errorf("unclassified lease bound writer %s in %s", name, e.Name())
							}
						}
					}
				}
				if kv, ok := n.(*ast.KeyValueExpr); ok {
					if key, ok := kv.Key.(*ast.Ident); ok && (key.Name == "ExpiresAtNS" || key.Name == "Lease" || key.Name == "deadline" || key.Name == "horizon") {
						seenBound[name] = true
						if !boundSites[name] {
							t.Errorf("unclassified lease constructor %s in %s", name, e.Name())
						}
					}
				}
				if call, ok := n.(*ast.CallExpr); ok {
					if fn, ok := call.Fun.(*ast.SelectorExpr); ok && fn.Sel.Name == "newWorkspaceLease" && !boundSites[name] {
						t.Errorf("unclassified lease grant constructor caller %s in %s", name, e.Name())
					}
				}
				return true
			})
		}
	}
	for name := range flagSites {
		if !seenFlag[name] {
			t.Errorf("missing reservation guard at %s", name)
		}
	}
	for name := range boundSites {
		if !seenBound[name] {
			t.Errorf("missing bound classification %s", name)
		}
	}
}
