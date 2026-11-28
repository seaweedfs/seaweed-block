package chunkstore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"
)

func TestLayoutRemoteSessionFieldSites(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "layout_remote.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]int{}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if s, ok := n.(*ast.SelectorExpr); ok && s.Sel.Name == "session" {
				fields[fn.Name.Name]++
			}

			return true
		})
	}
	// Begin returns the session it opened; CASHead checks that it survived
	// a concurrent operator HEAD write before retrying, and keys its skip of
	// the object check to the session CheckCommit checked (checked.session).
	if !reflect.DeepEqual(fields, map[string]int{"openSessionLocked": 2, "rollBackLocked": 1, "Begin": 1, "Put": 1, "CASHead": 5, "CheckCommit": 2, "prepareCommitLocked": 1, "CommitPendingSnapshot": 2, "writeRootLocked": 1, "Clean": 1, "syncSessionLocked": 3, "takeAbandonLocked": 1}) {
		t.Fatal("session field sites changed; enumerate and probe them", fields)
	}
}
