package chunkstore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strings"
	"testing"
)

// Every own write of the layout remote records its generation in ownGen.
// One that changes HEAD's image must record the image too (seen), or the
// next write by anyone else that leaves the image in place looks like an
// image change and moves the generation the engine sees (visible) under the
// engine's base. This census lists every assignment to ownGen in
// layout_remote.go with its function and whether the same statement sets
// seen; a new site fails it until it is classified here.
func TestLayoutRemoteOwnWritesCensus(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "layout_remote.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			own, seen := false, false
			for _, lhs := range as.Lhs {
				if sel, ok := lhs.(*ast.SelectorExpr); ok {
					switch sel.Sel.Name {
					case "ownGen":
						own = true
					case "seen":
						seen = true
					}
				}
			}
			if own {
				got = append(got, fn.Name.Name+" seen="+map[bool]string{true: "yes", false: "no"}[seen])
			}
			return true
		})
	}
	sort.Strings(got)
	want := []string{
		// Own writes that change the image: the cut's commit and the
		// snapshot commit of an unchanged (or the first, empty) image.
		"CASHead seen=yes",
		"CommitPendingSnapshot seen=yes",
		// Own writes that change only HEAD's activity, which imageOf
		// ignores: begin and end of a cleaning, a session's begin, and the
		// abort of a dead session or end of a dead cleaning.
		"Clean seen=no",
		"Clean seen=no",
		"openSessionLocked seen=no",
		"rollBackLocked seen=no",
		"rollBackLocked seen=no",
		// Not an own write: the view following a write by anyone else
		// (seen is set in its own statement when the image changed).
		"viewLocked seen=no",
	}
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("ownGen assignments in layout_remote.go:\n%s\nwant:\n%s\nclassify a new site: does its write change HEAD's image (then set seen with it)?", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
