package layout

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// funcCalls maps each function of the package's files (tests or not) to
// the names it calls.
func funcCalls(t *testing.T, tests bool) map[string]map[string]bool {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") != tests {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), f, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) == 1 {
				switch x := fn.Recv.List[0].Type.(type) {
				case *ast.Ident:
					name = x.Name + "." + name
				case *ast.StarExpr:
					name = x.X.(*ast.Ident).Name + "." + name
				}
			}
			calls := map[string]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					switch x := c.Fun.(type) {
					case *ast.SelectorExpr:
						calls[x.Sel.Name] = true
					case *ast.Ident:
						calls[x.Name] = true
					}
				}
				return true
			})
			out[name] = calls
		}
	}
	return out
}

// Guard enumeration for the facts this package writes. It holds at every
// step of the series without edits: a new writer, transition or remover
// fails it until it brings the tests named here.
//
//   - HEAD: the only UpdateHead call is in runTransition (read, one pure
//     step, one conditional update); the store has no way to create a HEAD.
//   - Transitions: every Run<X> through runTransition has a closure test
//     (Test<X>_Closure) and an interrupt test (TestRun<X>_InterruptAtEveryStep).
//   - Lease, claim half: only RunClaim calls OldLeaseEnded, and RunClaim,
//     where it exists, calls it.
//   - Pool: no function removes a store object yet; the removal that comes
//     with cleaning must update this guard and bring its lease re-check.
func TestGuard_WritersTransitionsAndRemovers(t *testing.T) {
	code := funcCalls(t, false)
	tests := funcCalls(t, true)
	var writers []string
	for fn, cs := range code {
		if cs["UpdateHead"] {
			writers = append(writers, fn)
		}
	}
	sort.Strings(writers)
	if !reflect.DeepEqual(writers, []string{"runTransition"}) {
		t.Errorf("functions calling UpdateHead: %v; want only runTransition", writers)
	}
	st := reflect.TypeOf((*HeadStore)(nil)).Elem()
	var methods []string
	for i := 0; i < st.NumMethod(); i++ {
		methods = append(methods, st.Method(i).Name)
	}
	if !reflect.DeepEqual(methods, []string{"ReadHead", "UpdateHead"}) {
		t.Errorf("HeadStore methods %v; want ReadHead and UpdateHead only (no path creates a HEAD)", methods)
	}
	var runs []string
	for fn, cs := range code {
		if !cs["runTransition"] {
			continue
		}
		runs = append(runs, fn)
		x, ok := strings.CutPrefix(fn, "Run")
		if !ok {
			t.Errorf("%s calls runTransition; transitions are exported Run functions", fn)
			continue
		}
		for _, want := range []string{"Test" + x + "_Closure", "TestRun" + x + "_InterruptAtEveryStep"} {
			if _, ok := tests[want]; !ok {
				t.Errorf("transition %s has no %s", fn, want)
			}
		}
	}
	if len(runs) == 0 {
		t.Fatal("no transition found: the scan is broken")
	}
	for fn, cs := range code {
		if cs["OldLeaseEnded"] && fn != "RunClaim" {
			t.Errorf("%s calls OldLeaseEnded; only RunClaim claims", fn)
		}
	}
	if cs, ok := code["RunClaim"]; ok && !cs["OldLeaseEnded"] {
		t.Error("RunClaim does not wait for the old lease")
	}
	for fn, cs := range code {
		for c := range cs {
			if strings.HasPrefix(c, "Delete") || strings.HasPrefix(c, "Remove") {
				t.Errorf("%s calls %s: a store remover needs its lease re-check and its own guard row", fn, c)
			}
		}
	}
}
