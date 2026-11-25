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
//   - Release of a dependent: RunReleaseDependent is used only by
//     DeleteVolume (its own HEAD is deleting) and OperatorReleaseDependent,
//     which reads the target's HEAD before it releases.
//   - Pool: a name starting with Delete or Remove is used only in
//     RemoveUnreachable, where every delete is preceded, in the same loop
//     body, by the remover's own lease check (StillHeld), and in
//     DeleteVolume, whose steps run in the delete's order: no holder, HEAD
//     deleting, the dependents released, the objects deleted, HEAD
//     deleted; see removerOrderFindings and deleteOrderFindings.
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
	for _, fn := range owners(code["RunReleaseDependent"]) {
		if fn != "DeleteVolume" && fn != "OperatorReleaseDependent" {
			bad = append(bad, fn+" uses RunReleaseDependent: only DeleteVolume (target deleting) and OperatorReleaseDependent (target checked) release")
		}
	}
	if decl["OperatorReleaseDependent"] && !code["ReadHead"]["OperatorReleaseDependent"] {
		bad = append(bad, "OperatorReleaseDependent does not read the target's HEAD before releasing")
	}
	for name, by := range code {
		if strings.HasPrefix(name, "Delete") || strings.HasPrefix(name, "Remove") {
			for _, fn := range owners(by) {
				if fn != "RemoveUnreachable" && fn != "DeleteVolume" {
					bad = append(bad, fn+" uses "+name+": only RemoveUnreachable (after its lease re-check) and DeleteVolume (after HEAD deleting and the dependents released) remove")
				}
			}
		}
	}
	sort.Strings(bad)
	return bad
}

// removerOrderFindings checks the removal half of the time premise in the
// sources: in RemoveUnreachable, every statement that calls a method named
// Delete* or Remove* is IMMEDIATELY preceded, in the same block, by a lease
// gate: an if statement whose init calls StillHeld and whose body returns
// on its error. A check whose result is ignored, one check reused for a
// second removal, and a removal method the scan does not list each fail.
func removerOrderFindings(t *testing.T, srcs map[string]string) []string {
	t.Helper()
	removes := func(n ast.Node) bool {
		found := false
		ast.Inspect(n, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				if sel, ok := c.Fun.(*ast.SelectorExpr); ok && (strings.HasPrefix(sel.Sel.Name, "Delete") || strings.HasPrefix(sel.Sel.Name, "Remove")) {
					found = true
				}
			}
			return !found
		})
		return found
	}
	leaseGate := func(st ast.Stmt) bool {
		is, ok := st.(*ast.IfStmt)
		if !ok || is.Init == nil {
			return false
		}
		as, ok := is.Init.(*ast.AssignStmt)
		if !ok || len(as.Rhs) != 1 || len(as.Lhs) != 1 {
			return false
		}
		call, ok := as.Rhs[0].(*ast.CallExpr)
		if !ok {
			return false
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "StillHeld" {
			return false
		}
		errName, ok := as.Lhs[0].(*ast.Ident)
		cond, ok2 := is.Cond.(*ast.BinaryExpr)
		if !ok || !ok2 || cond.Op != token.NEQ {
			return false
		}
		if x, ok := cond.X.(*ast.Ident); !ok || x.Name != errName.Name {
			return false
		}
		for _, b := range is.Body.List {
			if _, ok := b.(*ast.ReturnStmt); ok {
				return true
			}
		}
		return false
	}
	var bad []string
	for fname, src := range srcs {
		file, err := parser.ParseFile(token.NewFileSet(), fname, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name.Name != "RemoveUnreachable" {
				continue
			}
			deletes := 0
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				blk, ok := n.(*ast.BlockStmt)
				if !ok {
					return true
				}
				for i, st := range blk.List {
					switch st.(type) {
					case *ast.BlockStmt, *ast.RangeStmt, *ast.ForStmt:
						continue // their bodies are checked as their own blocks
					}
					if !removes(st) {
						continue
					}
					deletes++
					if i == 0 || !leaseGate(blk.List[i-1]) {
						bad = append(bad, "RemoveUnreachable removes without a lease gate immediately before it (an if on StillHeld's error that returns)")
					}
				}
				return true
			})
			if deletes == 0 {
				bad = append(bad, "RemoveUnreachable is declared and has no delete: the scan is broken")
			}
		}
	}
	return bad
}

// deleteOrderFindings checks DeleteVolume's order in the sources: among its
// top-level statements, the first use of each step comes after the
// previous step's: NoHolder, RunMarkDeleting, RunReleaseDependent,
// DeleteObject, RunMarkDeleted.
func deleteOrderFindings(t *testing.T, srcs map[string]string) []string {
	t.Helper()
	steps := []string{"NoHolder", "RunMarkDeleting", "RunReleaseDependent", "DeleteObject", "RunMarkDeleted"}
	var bad []string
	for fname, src := range srcs {
		file, err := parser.ParseFile(token.NewFileSet(), fname, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name.Name != "DeleteVolume" {
				continue
			}
			first := map[string]int{}
			for i, st := range fn.Body.List {
				ast.Inspect(st, func(n ast.Node) bool {
					var name string
					switch x := n.(type) {
					case *ast.SelectorExpr:
						name = x.Sel.Name
					case *ast.Ident:
						name = x.Name
					}
					if _, seen := first[name]; !seen && name != "" {
						first[name] = i
					}
					return true
				})
			}
			for k, step := range steps {
				at, ok := first[step]
				if !ok {
					bad = append(bad, "DeleteVolume has no "+step)
					continue
				}
				if k > 0 {
					if prev, ok := first[steps[k-1]]; ok && prev >= at {
						bad = append(bad, "DeleteVolume runs "+step+" before "+steps[k-1]+" has run")
					}
				}
			}
		}
	}
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
	if bad := append(guardFindings(code, declared(t, src), tests), removerOrderFindings(t, src)...); len(bad) != 0 {
		t.Fatalf("guard: %s", strings.Join(bad, "\n"))
	}
	if bad := deleteOrderFindings(t, src); len(bad) != 0 {
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
		"remover outside RemoveUnreachable": {src: `type p interface{ DeleteObject(context.Context, string) error }
func w(ctx context.Context, x p) error { return x.DeleteObject(ctx, "o") }`, want: "only RemoveUnreachable (after its lease re-check) and DeleteVolume"},
		"removal without lease check": {file: "remove.go", src: `type p interface{ DeleteObject(context.Context, string) error }
type l interface{ StillHeld(context.Context) error }
func RemoveUnreachable(ctx context.Context, x p, h l, names []string) error {
	for _, n := range names {
		if err := x.DeleteObject(ctx, n); err != nil {
			return err
		}
	}
	return nil
}`, want: "without a lease gate immediately before it"},
		"lease check after the delete": {file: "remove.go", src: `type p interface{ DeleteObject(context.Context, string) error }
type l interface{ StillHeld(context.Context) error }
func RemoveUnreachable(ctx context.Context, x p, h l, names []string) error {
	for _, n := range names {
		if err := x.DeleteObject(ctx, n); err != nil {
			return err
		}
		if err := h.StillHeld(ctx); err != nil {
			return err
		}
	}
	return nil
}`, want: "without a lease gate immediately before it"},
		"lease check once before the loop": {file: "remove.go", src: `type p interface{ DeleteObject(context.Context, string) error }
type l interface{ StillHeld(context.Context) error }
func RemoveUnreachable(ctx context.Context, x p, h l, names []string) error {
	if err := h.StillHeld(ctx); err != nil {
		return err
	}
	for _, n := range names {
		if err := x.DeleteObject(ctx, n); err != nil {
			return err
		}
	}
	return nil
}`, want: "without a lease gate immediately before it"},
		"claim without lease wait": {src: `func RunClaim(ctx context.Context, st HeadStore, lease LeaseWait, me HolderID) (Head, error) {
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h, false, nil })
}`, want: "does not wait for the old lease", file: "claim.go"},
		"transition not through runTransition": {src: `func RunZz(ctx context.Context, st HeadStore) (Head, error) { h, _, err := st.ReadHead(ctx); return h, err }`, want: "does not go through runTransition"},
		"delete removes the pool before releasing dependents": {file: "delete.go", src: `type p interface{ DeleteObject(context.Context, string) error }
type g interface{ NoHolder(context.Context) error }
func RunMarkDeleting(ctx context.Context, st HeadStore) (Head, error) { return runTransition(ctx, st, nil) }
func RunMarkDeleted(ctx context.Context, st HeadStore) (Head, error) { return runTransition(ctx, st, nil) }
func RunReleaseDependent(ctx context.Context, st HeadStore, id VolumeID) (Head, error) { return runTransition(ctx, st, nil) }
func DeleteVolume(ctx context.Context, st HeadStore, x g, id VolumeID, srcs []HeadStore, pool p, objs []string) error {
	_ = x.NoHolder(ctx)
	_, _ = RunMarkDeleting(ctx, st)
	for _, o := range objs {
		_ = pool.DeleteObject(ctx, o)
	}
	for _, s := range srcs {
		_, _ = RunReleaseDependent(ctx, s, id)
	}
	_, _ = RunMarkDeleted(ctx, st)
	return nil
}`, want: "runs DeleteObject before RunReleaseDependent"},
		"delete without the holder check": {file: "delete.go", src: `type p interface{ DeleteObject(context.Context, string) error }
func RunMarkDeleting(ctx context.Context, st HeadStore) (Head, error) { return runTransition(ctx, st, nil) }
func RunMarkDeleted(ctx context.Context, st HeadStore) (Head, error) { return runTransition(ctx, st, nil) }
func RunReleaseDependent(ctx context.Context, st HeadStore, id VolumeID) (Head, error) { return runTransition(ctx, st, nil) }
func DeleteVolume(ctx context.Context, st HeadStore, id VolumeID, srcs []HeadStore, pool p, objs []string) error {
	_, _ = RunMarkDeleting(ctx, st)
	for _, s := range srcs {
		_, _ = RunReleaseDependent(ctx, s, id)
	}
	for _, o := range objs {
		_ = pool.DeleteObject(ctx, o)
	}
	_, _ = RunMarkDeleted(ctx, st)
	return nil
}`, want: "DeleteVolume has no NoHolder"},
		"release outside delete and the operator's check": {src: `func w(ctx context.Context, st HeadStore, id VolumeID) { _, _ = RunReleaseDependent(ctx, st, id) }`, want: "only DeleteVolume (target deleting) and OperatorReleaseDependent"},
		"operator release without reading the target": {file: "delete.go", src: `func OperatorReleaseDependent(ctx context.Context, source HeadStore, id VolumeID) (Head, error) {
	return RunReleaseDependent(ctx, source, id)
}`, want: "does not read the target's HEAD"},
		"ignored lease check":     {file: "remove.go", src: removeVariant(t, "ignored"), want: "without a lease gate immediately before it"},
		"one check, two deletes":  {file: "remove.go", src: removeVariant(t, "second"), want: "without a lease gate immediately before it"},
		"unlisted removal method": {file: "remove.go", src: removeVariant(t, "other"), want: "without a lease gate immediately before it"},
		"remover method value": {src: `type p interface{ RemoveName(string) }
func w(x p) { f := x.RemoveName; f("o") }`, want: "only RemoveUnreachable (after its lease re-check) and DeleteVolume"},
	} {
		srcs := packageSources(t, false)
		file := c.file
		if file == "" {
			file = "zz_control.go"
		}
		srcs[file] = "package layout\n\nimport \"context\"\n\nvar _ context.Context\n\n" + c.src + "\n"
		bad := append(append(guardFindings(refs(t, srcs), declared(t, srcs), tests), removerOrderFindings(t, srcs)...), deleteOrderFindings(t, srcs)...)
		if !strings.Contains(strings.Join(bad, "\n"), c.want) {
			t.Errorf("control %q: findings %q; want one containing %q", name, bad, c.want)
		}
	}
}

// removeVariant returns remove.go's real source changed in one way, minus
// its package clause and imports (the controls add their own header).
func removeVariant(t *testing.T, mode string) string {
	t.Helper()
	b, err := os.ReadFile("remove.go")
	if err != nil {
		t.Fatal(err)
	}
	src := strings.ReplaceAll(string(b), "\r\n", "\n")
	gate := "if err := lease.StillHeld(ctx); err != nil {\n\t\t\treturn removed, fmt.Errorf(\"%w: %v\", ErrLeaseLost, err)\n\t\t}"
	switch mode {
	case "ignored":
		if !strings.Contains(src, gate) {
			t.Fatal("remove.go lease gate not found")
		}
		src = strings.Replace(src, gate, "_ = lease.StillHeld(ctx)", 1)
	case "second":
		src = strings.Replace(src, "removed = append(removed, c)", "_ = pool.DeleteObject(ctx, c.Path())\n\t\tremoved = append(removed, c)", 1)
	case "other":
		src = strings.Replace(src, "removed = append(removed, c)", "_ = pool.RemoveOther(ctx, c.Path())\n\t\tremoved = append(removed, c)", 1)
	}
	i := strings.Index(src, ")\n")
	return src[i+2:]
}
