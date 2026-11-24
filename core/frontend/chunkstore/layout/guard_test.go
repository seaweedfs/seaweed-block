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

// layoutCalls maps each non-test function of this package to the calls it
// makes, by called name.
func layoutCalls(t *testing.T) map[string]map[string]bool {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
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
				var typ string
				switch x := fn.Recv.List[0].Type.(type) {
				case *ast.Ident:
					typ = x.Name
				case *ast.StarExpr:
					typ = x.X.(*ast.Ident).Name
				}
				name = typ + "." + name
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

// Guard enumeration for HEAD writes: the only call of UpdateHead is in
// runTransition (read, apply one pure step, one conditional update), and
// HeadStore offers no way to create a HEAD. Every Run function that
// writes HEAD goes through runTransition. A new writer fails this test
// until it is listed here with its own guard tests.
func TestHeadWriters_SourceGuard(t *testing.T) {
	calls := layoutCalls(t)
	var writers, runs []string
	for fn, cs := range calls {
		if cs["UpdateHead"] {
			writers = append(writers, fn)
		}
		if cs["runTransition"] {
			runs = append(runs, fn)
		}
	}
	sort.Strings(writers)
	sort.Strings(runs)
	if !reflect.DeepEqual(writers, []string{"runTransition"}) {
		t.Fatalf("functions calling UpdateHead: %v; want only runTransition", writers)
	}
	wantRuns := []string{"RunAbort", "RunBegin", "RunBeginClean", "RunClaim", "RunCommit", "RunEndClean"}
	if !reflect.DeepEqual(runs, wantRuns) {
		t.Fatalf("HEAD transitions: %v; want %v (add a new one here with its closure, replay and interrupt tests)", runs, wantRuns)
	}
	st := reflect.TypeOf((*HeadStore)(nil)).Elem()
	var methods []string
	for i := 0; i < st.NumMethod(); i++ {
		methods = append(methods, st.Method(i).Name)
	}
	if !reflect.DeepEqual(methods, []string{"ReadHead", "UpdateHead"}) {
		t.Fatalf("HeadStore methods %v; want ReadHead and UpdateHead only (no path creates a HEAD)", methods)
	}
	// Claim's lease wait sits in RunClaim, inside its transition.
	if !calls["RunClaim"]["OldLeaseEnded"] {
		t.Fatal("RunClaim does not call OldLeaseEnded")
	}
	for fn, cs := range calls {
		if cs["OldLeaseEnded"] && fn != "RunClaim" {
			t.Fatalf("%s calls OldLeaseEnded; only RunClaim claims", fn)
		}
	}
}

// Every pure transition refuses a caller that is not the holder at HEAD's
// epoch (Claim refuses an older epoch or the same epoch of another server).
func TestTransitions_RefuseAStaleHolder(t *testing.T) {
	stale := HolderID{Epoch: 2, Holder: "srv-a"}
	rn := RootName{Hash: Sum([]byte("r"))}
	for name, step := range map[string]func(Head) error{
		"Begin":      func(h Head) error { _, _, err := h.Begin(stale, "s"); return err },
		"Abort":      func(h Head) error { _, _, err := h.Abort(stale, "s"); return err },
		"Commit":     func(h Head) error { _, _, err := h.Commit(stale, "s", rn, 1, ""); return err },
		"BeginClean": func(h Head) error { _, _, err := h.BeginClean(stale, "c"); return err },
		"EndClean":   func(h Head) error { _, _, err := h.EndClean(stale, "c"); return err },
		"Claim":      func(h Head) error { _, _, err := h.Claim(stale); return err },
	} {
		for _, h := range []Head{idleHead(), snapHead("s"), cleanHead("c")} {
			if err := step(h); err == nil {
				t.Errorf("%s by epoch 2 over a HEAD at epoch 3 (%s) succeeded", name, h.Activity.Kind)
			}
		}
	}
}
