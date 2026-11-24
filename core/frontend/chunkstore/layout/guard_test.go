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

// pkgLevel is the owner of a reference outside any function body: a
// package-level var or const initializer.
const pkgLevel = "<package>"

// refs maps every name used in the given sources (a selector's name, as in
// st.UpdateHead, called or not, or a plain identifier, as in runTransition)
// to the functions that use it: a method value, an alias, a call and a
// package-level initializer all count. Declarations are not uses: function
// and type names, interface method names and struct field names.
func refs(t *testing.T, srcs map[string]string) map[string]map[string]bool {
	t.Helper()
	out := map[string]map[string]bool{}
	add := func(name, owner string) {
		if out[name] == nil {
			out[name] = map[string]bool{}
		}
		out[name][owner] = true
	}
	visit := func(n ast.Node, owner string) {
		ast.Inspect(n, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				add(x.Sel.Name, owner)
				ast.Inspect(x.X, func(n ast.Node) bool {
					if id, ok := n.(*ast.Ident); ok {
						add(id.Name, owner)
					}
					return true
				})
				return false
			case *ast.Ident:
				add(x.Name, owner)
			}
			return true
		})
	}
	for name, src := range srcs {
		file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Body == nil {
					continue
				}
				owner := d.Name.Name
				if d.Recv != nil && len(d.Recv.List) == 1 {
					switch x := d.Recv.List[0].Type.(type) {
					case *ast.Ident:
						owner = x.Name + "." + owner
					case *ast.StarExpr:
						owner = x.X.(*ast.Ident).Name + "." + owner
					}
				}
				visit(d.Body, owner)
			case *ast.GenDecl:
				for _, s := range d.Specs {
					if v, ok := s.(*ast.ValueSpec); ok {
						for _, e := range v.Values {
							visit(e, pkgLevel)
						}
					}
				}
			}
		}
	}
	return out
}

func packageSources(t *testing.T, tests bool) map[string]string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") != tests {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		out[f] = string(b)
	}
	return out
}

func owners(m map[string]bool) []string {
	var out []string
	for o := range m {
		out = append(out, o)
	}
	sort.Strings(out)
	return out
}

// guardFindings checks the package sources against the guard and returns
// every violation.
//
//   - HEAD: UpdateHead is used only in runTransition (read, one pure step,
//     one conditional update): no other call, method value, alias or
//     package-level use. The store has no way to create a HEAD.
//   - Transitions: runTransition is used only by exported Run functions,
//     each with Test<X>_Closure and TestRun<X>_InterruptAtEveryStep.
//   - Lease, claim half: OldLeaseEnded is used only in RunClaim, and
//     RunClaim, when it is declared, uses it.
//   - Every declared exported Run function goes through runTransition.
//   - Pool: no name starting with Delete or Remove is used yet; the
//     removal that comes with cleaning must bring its lease re-check and
//     its own row here.
//
// These are syntactic checks of this package's own files: they see any
// use of these names, but not a store effect reached through a name they
// do not list (for example reflection or another package's helper).
func guardFindings(code map[string]map[string]bool, decl, tests map[string]bool) []string {
	var bad []string
	if got := owners(code["UpdateHead"]); !reflect.DeepEqual(got, []string{"runTransition"}) {
		bad = append(bad, "UpdateHead used by "+strings.Join(got, ", ")+"; want only runTransition")
	}
	var runs []string
	for _, fn := range owners(code["runTransition"]) {
		x, ok := strings.CutPrefix(fn, "Run")
		if !ok {
			bad = append(bad, fn+" uses runTransition; transitions are exported Run functions")
			continue
		}
		runs = append(runs, fn)
		for _, want := range []string{"Test" + x + "_Closure", "TestRun" + x + "_InterruptAtEveryStep"} {
			if !tests[want] {
				bad = append(bad, "transition "+fn+" has no "+want)
			}
		}
	}
	if len(runs) == 0 {
		bad = append(bad, "no transition uses runTransition: the scan is broken")
	}
	for _, fn := range owners(code["OldLeaseEnded"]) {
		if fn != "RunClaim" {
			bad = append(bad, fn+" uses OldLeaseEnded; only RunClaim claims")
		}
	}
	if decl["RunClaim"] && !code["OldLeaseEnded"]["RunClaim"] {
		bad = append(bad, "RunClaim is declared and does not wait for the old lease")
	}
	for fn := range decl {
		if strings.HasPrefix(fn, "Run") && !code["runTransition"][fn] {
			bad = append(bad, fn+" is declared and does not go through runTransition")
		}
	}
	for name, by := range code {
		if strings.HasPrefix(name, "Delete") || strings.HasPrefix(name, "Remove") {
			bad = append(bad, strings.Join(owners(by), ", ")+" uses "+name+": a store remover needs its lease re-check and its own guard row")
		}
	}
	sort.Strings(bad)
	return bad
}

// declared returns the names of the functions the sources declare.
func declared(t *testing.T, srcs map[string]string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for name, src := range srcs {
		file, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv == nil {
				out[fn.Name.Name] = true
			}
		}
	}
	return out
}

func TestGuard_WritersTransitionsAndRemovers(t *testing.T) {
	src := packageSources(t, false)
	code := refs(t, src)
	tests := declared(t, packageSources(t, true))
	if bad := guardFindings(code, declared(t, src), tests); len(bad) != 0 {
		t.Fatalf("guard: %s", strings.Join(bad, "\n"))
	}
	st := reflect.TypeOf((*HeadStore)(nil)).Elem()
	var methods []string
	for i := 0; i < st.NumMethod(); i++ {
		methods = append(methods, st.Method(i).Name)
	}
	if !reflect.DeepEqual(methods, []string{"ReadHead", "UpdateHead"}) {
		t.Fatalf("HeadStore methods %v; want ReadHead and UpdateHead only (no path creates a HEAD)", methods)
	}
}

// Negative controls: each form of an extra HEAD writer, transition or
// remover, added to the package's real sources, is refused; the real
// sources alone pass (the test above).
func TestGuard_Controls(t *testing.T) {
	tests := declared(t, packageSources(t, true))
	// A control with a file name replaces that real file (a changed
	// claim.go); otherwise it is added beside the real sources.
	for name, c := range map[string]struct{ src, want, file string }{
		"direct call": {src: `func w(ctx context.Context, st HeadStore, h Head) error { return st.UpdateHead(ctx, h.Gen, h) }`, want: "UpdateHead used by"},
		"method value alias": {src: `func w(ctx context.Context, st HeadStore, h Head) error {
	update := st.UpdateHead
	return update(ctx, h.Gen, h)
}`, want: "UpdateHead used by"},
		"method expression":        {src: `func w(ctx context.Context, st HeadStore, h Head) error { return HeadStore.UpdateHead(st, ctx, h.Gen, h) }`, want: "UpdateHead used by"},
		"package-level var":        {src: `var w = HeadStore.UpdateHead`, want: "UpdateHead used by"},
		"package-level func lit":   {src: `var w = func(ctx context.Context, st HeadStore, h Head) error { return st.UpdateHead(ctx, h.Gen, h) }`, want: "UpdateHead used by"},
		"inline interface":         {src: `func w(st HeadStore) { var u interface{ UpdateHead(context.Context, uint64, Head) error } = st; _ = u }`, want: "UpdateHead used by"},
		"runTransition alias":      {src: `func w(ctx context.Context, st HeadStore) { f := runTransition; _, _ = f(ctx, st, nil) }`, want: "uses runTransition; transitions are exported"},
		"untested transition":      {src: `func RunZz(ctx context.Context, st HeadStore) (Head, error) { return runTransition(ctx, st, nil) }`, want: "has no TestZz_Closure"},
		"lease wait outside claim": {src: `func w(ctx context.Context, l LeaseWait) error { return l.OldLeaseEnded(ctx, 1) }`, want: "only RunClaim claims"},
		"remover": {src: `type p interface{ DeleteObject(context.Context, string) error }
func w(ctx context.Context, x p) error { return x.DeleteObject(ctx, "o") }`, want: "a store remover"},
		"claim without lease wait": {src: `func RunClaim(ctx context.Context, st HeadStore, lease LeaseWait, me HolderID) (Head, error) {
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h, false, nil })
}`, want: "does not wait for the old lease", file: "claim.go"},
		"transition not through runTransition": {src: `func RunZz(ctx context.Context, st HeadStore) (Head, error) { h, _, err := st.ReadHead(ctx); return h, err }`, want: "does not go through runTransition"},
		"remover method value": {src: `type p interface{ RemoveName(string) }
func w(x p) { f := x.RemoveName; f("o") }`, want: "a store remover"},
	} {
		srcs := packageSources(t, false)
		file := c.file
		if file == "" {
			file = "zz_control.go"
		}
		srcs[file] = "package layout\n\nimport \"context\"\n\nvar _ context.Context\n\n" + c.src + "\n"
		bad := guardFindings(refs(t, srcs), declared(t, srcs), tests)
		if !strings.Contains(strings.Join(bad, "\n"), c.want) {
			t.Errorf("control %q: findings %q; want one containing %q", name, bad, c.want)
		}
	}
}
