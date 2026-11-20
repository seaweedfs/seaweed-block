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

// publisherMethods parses this package's production files and returns every
// *Publisher method declaration by name.
func publisherMethods(t *testing.T) map[string]*ast.FuncDecl {
	t.Helper()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	out := map[string]*ast.FuncDecl{}
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) != 1 || fn.Body == nil {
				continue
			}
			typ := fn.Recv.List[0].Type
			if star, ok := typ.(*ast.StarExpr); ok {
				typ = star.X
			}
			if id, ok := typ.(*ast.Ident); ok && id.Name == "Publisher" {
				out[fn.Name.Name] = fn
			}
		}
	}
	return out
}

// leaseMuCall reports whether call is <x>.leaseMu.<method>().
func leaseMuCall(call *ast.CallExpr, method string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != method {
		return false
	}
	inner, ok := sel.X.(*ast.SelectorExpr)
	return ok && inner.Sel.Name == "leaseMu"
}

// The issued move's lease view and its initial deadline are published in
// ONE leaseMu hold (INV-LEASE-VIEW-001): a renewal of the new epoch sees both
// or neither. Structural guard: publishIssuedLeaseLocked takes leaseMu
// exactly once, holds it (deferred unlock) across both the derived-view and
// the deadline assignment, and calls no other Publisher method that takes
// leaseMu, directly or transitively. A split into two sections (for example
// refreshLeaseViewLocked then setLeaseDeadlineLocked) fails it.
func TestWorkspaceIssuePublishIsOneLeaseHold(t *testing.T) {
	methods := publisherMethods(t)
	callees := map[string][]string{}
	direct := map[string]bool{}
	for name, fn := range methods {
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if leaseMuCall(call, "Lock") {
				direct[name] = true
			} else if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				if _, isMethod := methods[sel.Sel.Name]; isMethod {
					callees[name] = append(callees[name], sel.Sel.Name)
				}
			}
			return true
		})
	}
	takes := map[string]bool{}
	for name := range direct {
		takes[name] = true
	}
	for changed := true; changed; {
		changed = false
		for name, cs := range callees {
			for _, c := range cs {
				if takes[c] && !takes[name] {
					takes[name], changed = true, true
				}
			}
		}
	}
	// The analysis is not vacuous: the two halves of a split publication
	// are known lease-taking methods.
	for _, name := range []string{"refreshLeaseViewLocked", "setLeaseDeadlineLocked", "leaseDeadlineLocked"} {
		if !takes[name] {
			t.Fatalf("structure scan does not see %s take leaseMu; the guard would be vacuous", name)
		}
	}

	fn := methods["publishIssuedLeaseLocked"]
	if fn == nil {
		t.Fatal("publishIssuedLeaseLocked not found")
	}
	var locks, unlocks, deferredUnlocks int
	var lockPos token.Pos
	var viewSet, deadlineSet []token.Pos
	var lockingCallees []string
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.DeferStmt:
			if leaseMuCall(x.Call, "Unlock") {
				deferredUnlocks++
				return false
			}
		case *ast.CallExpr:
			switch {
			case leaseMuCall(x, "Lock"):
				locks++
				lockPos = x.Pos()
			case leaseMuCall(x, "Unlock"):
				unlocks++
			default:
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok && takes[sel.Sel.Name] {
					lockingCallees = append(lockingCallees, sel.Sel.Name)
				}
			}
		case *ast.AssignStmt:
			for _, lhs := range x.Lhs {
				if sel, ok := lhs.(*ast.SelectorExpr); ok {
					switch sel.Sel.Name {
					case "d":
						viewSet = append(viewSet, x.Pos())
					case "deadline":
						deadlineSet = append(deadlineSet, x.Pos())
					}
				}
			}
		}
		return true
	})
	if locks != 1 || deferredUnlocks != 1 || unlocks != 0 {
		t.Fatalf("publishIssuedLeaseLocked must take leaseMu exactly once and hold it to return (defer Unlock): Lock %d, deferred Unlock %d, explicit Unlock %d", locks, deferredUnlocks, unlocks)
	}
	if len(lockingCallees) != 0 {
		t.Fatalf("publishIssuedLeaseLocked calls methods that take leaseMu (a second lease hold): %v", lockingCallees)
	}
	if len(viewSet) == 0 || len(deadlineSet) == 0 {
		t.Fatalf("publishIssuedLeaseLocked must set both the derived view (lv.d: %d) and the deadline (lv.deadline: %d)", len(viewSet), len(deadlineSet))
	}
	for _, pos := range append(append([]token.Pos{}, viewSet...), deadlineSet...) {
		if pos < lockPos {
			t.Fatal("publishIssuedLeaseLocked sets the view or the deadline before taking leaseMu")
		}
	}
}
