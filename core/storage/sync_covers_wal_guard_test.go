package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// INV-WAL-FLUSH-SYNC-COVERS-WAL-001, checked in the source: the flusher's
// extent sync and the WAL sync are the same whole-file sync of the store's
// one descriptor. The checkpoint bound rests on it (a checkpoint may pass a
// synced record superseded by an unsynced newer one only because the
// flusher's sync makes the newer record durable too). In the package's
// non-test Go files:
//   - the function that sets syncExtentFile binds exactly one data sync
//     (bindDataFileSync of the store file) and no other function sets it;
//   - every assignment to syncExtentFile assigns exactly that result;
//   - every assignment to syncWALFile is a function that calls that result.
//
// A range sync, a second descriptor for the WAL or the extent, or a WAL sync
// that no longer calls the shared sync fails here first.
func TestSyncCoversWALGuard(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	binds, extentOK, extentBad, walOK, walBad := 0, 0, []string{}, 0, []string{}
	shared := "" // the variable bindDataFileSync's sync result is bound to
	type asg struct {
		field string
		rhs   ast.Expr
		pos   token.Position
	}
	var asgs []asg
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			var fnBinds []string
			setsExtent := false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				a, ok := n.(*ast.AssignStmt)
				if !ok {
					return true
				}
				for i, rhs := range a.Rhs {
					if call, ok := rhs.(*ast.CallExpr); ok {
						if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "bindDataFileSync" && len(a.Lhs) > 0 {
							if lhs, ok := a.Lhs[0].(*ast.Ident); ok {
								fnBinds = append(fnBinds, lhs.Name)
							}
						}
					}
					if i < len(a.Lhs) {
						if sel, ok := a.Lhs[i].(*ast.SelectorExpr); ok && (sel.Sel.Name == "syncExtentFile" || sel.Sel.Name == "syncWALFile") {
							asgs = append(asgs, asg{sel.Sel.Name, rhs, fset.Position(a.Pos())})
							if sel.Sel.Name == "syncExtentFile" {
								setsExtent = true
							}
						}
					}
				}
				return true
			})
			if setsExtent {
				binds += len(fnBinds)
				if len(fnBinds) > 0 {
					shared = fnBinds[0]
				}
			}
		}
	}
	if binds != 1 || shared == "" {
		t.Fatalf("the function setting syncExtentFile binds %d data syncs (first bound to %q); want exactly one: one descriptor, one sync", binds, shared)
	}
	callsShared := func(e ast.Expr) bool {
		found := false
		ast.Inspect(e, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				if id, ok := c.Fun.(*ast.Ident); ok && id.Name == shared {
					found = true
				}
			}
			return !found
		})
		return found
	}
	for _, a := range asgs {
		switch a.field {
		case "syncExtentFile":
			if id, ok := a.rhs.(*ast.Ident); ok && id.Name == shared {
				extentOK++
			} else {
				extentBad = append(extentBad, a.pos.String())
			}
		case "syncWALFile":
			if fl, ok := a.rhs.(*ast.FuncLit); ok && callsShared(fl) {
				walOK++
			} else {
				walBad = append(walBad, a.pos.String())
			}
		}
	}
	if extentOK == 0 || len(extentBad) > 0 {
		t.Fatalf("syncExtentFile assigned the shared sync %q %d times; other assignments at %v: the flusher's sync must be the WAL's whole-file sync", shared, extentOK, extentBad)
	}
	if walOK == 0 || len(walBad) > 0 {
		t.Fatalf("syncWALFile assigned a function calling %q %d times; other assignments at %v", shared, walOK, walBad)
	}
}
