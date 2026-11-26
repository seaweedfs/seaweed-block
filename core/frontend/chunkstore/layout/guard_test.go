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
//   - Claim: RunClaim is used only by Start, which applies the local-root
//     rules (own epoch: continue only on HEAD's claim id) before claiming.
//   - Release of a dependent: RunReleaseDependent is used only by
//     DeleteVolume (its own HEAD is deleting) and OperatorReleaseDependent,
//     which reads the target's HEAD before it releases.
//   - Pool: a name starting with Delete or Remove is used only in
//     RemoveUnreachable, whose only removal is DeleteMarked, each right
//     after a gate on its Mark's error, and in DeleteVolume, whose steps run
//     in the delete's order: no holder, HEAD deleting, the dependents
//     released, the objects deleted (conditional on the volume id), HEAD
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
	if got := owners(code["CreateVolumeRecord"]); !reflect.DeepEqual(got, []string{"createVolume"}) {
		bad = append(bad, "CreateVolumeRecord used by "+strings.Join(got, ", ")+"; want only createVolume")
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
	for _, fn := range owners(code["NoLiveLease"]) {
		if fn != "RunMarkDeleting" {
			bad = append(bad, fn+" uses NoLiveLease; only RunMarkDeleting checks it, between its HEAD read and update")
		}
	}
	if decl["RunMarkDeleting"] && !code["NoLiveLease"]["RunMarkDeleting"] {
		bad = append(bad, "RunMarkDeleting is declared and does not check the master's live leases")
	}
	for fn := range decl {
		if strings.HasPrefix(fn, "Run") && !code["runTransition"][fn] {
			bad = append(bad, fn+" is declared and does not go through runTransition")
		}
	}
	for _, fn := range owners(code["RunClaim"]) {
		if fn != "Start" {
			bad = append(bad, fn+" uses RunClaim: a claim is made only by Start, which first applies the local-root rules")
		}
	}
	for _, fn := range owners(code["CreateDeletedHead"]) {
		if fn != "OperatorReleaseDependent" {
			bad = append(bad, fn+" uses CreateDeletedHead: only the operator's release writes a tombstone, for a target never created")
		}
	}
	// HEAD creates are exactly three: a new volume's and a restore target's
	// unclaimed HEAD (createVolume, for CreateVolume and CreateTarget) and
	// the operator's tombstone (above).
	for _, fn := range owners(code["CreateHead"]) {
		if fn != "createVolume" {
			bad = append(bad, fn+" uses CreateHead: only createVolume creates a HEAD (unclaimed), besides the operator's tombstone")
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
		if name == "DeleteSnapshot" {
			continue // a HEAD transition (a snapshot's name leaves HEAD), not a store remover
		}
		if strings.HasPrefix(name, "Delete") || strings.HasPrefix(name, "Remove") {
			for _, fn := range owners(by) {
				if fn != "RemoveUnreachable" && fn != "DeleteVolume" {
					bad = append(bad, fn+" uses "+name+": only RemoveUnreachable (mark-gated) and DeleteVolume (after HEAD deleting and the dependents released) remove")
				}
			}
		}
	}
	sort.Strings(bad)
	return bad
}

// removerOrderFindings checks the removal rule (7') in the sources: in
// RemoveUnreachable the only removal call is DeleteMarked (a delete
// conditional on the remover's own mark), and each is IMMEDIATELY
// preceded, in the same block, by a mark gate: an if statement whose init
// calls Mark and whose body leaves on its error (return or continue). An
// unconditional delete, an ignored mark, one mark reused for a second
// delete and an unlisted removal method each fail.
func removerOrderFindings(t *testing.T, srcs map[string]string) []string {
	t.Helper()
	removals := func(n ast.Node) []string {
		var names []string
		ast.Inspect(n, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok {
				if sel, ok := c.Fun.(*ast.SelectorExpr); ok && (strings.HasPrefix(sel.Sel.Name, "Delete") || strings.HasPrefix(sel.Sel.Name, "Remove")) {
					names = append(names, sel.Sel.Name)
				}
			}
			return true
		})
		return names
	}
	markGate := func(st ast.Stmt) bool {
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
		if !ok || sel.Sel.Name != "Mark" {
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
		leaves := false
		ast.Inspect(is.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.ReturnStmt:
				leaves = true
			case *ast.BranchStmt:
				if x.Tok == token.CONTINUE {
					leaves = true
				}
			}
			return !leaves
		})
		return leaves
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
					names := removals(st)
					if len(names) == 0 {
						continue
					}
					deletes++
					for _, name := range names {
						if name != "DeleteMarked" {
							bad = append(bad, "RemoveUnreachable calls "+name+": its only removal is DeleteMarked, conditional on its own mark")
						}
					}
					if i == 0 || !markGate(blk.List[i-1]) {
						bad = append(bad, "RemoveUnreachable removes without a mark gate immediately before it (an if on Mark's error that leaves)")
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
// previous step's: Reserve, RunMarkDeleting, RunReleaseDependent,
// DeleteOfVolume, RunMarkDeleted.
func deleteOrderFindings(t *testing.T, srcs map[string]string) []string {
	t.Helper()
	steps := []string{"Reserve", "RunMarkDeleting", "RunReleaseDependent", "DeleteOfVolume", "RunMarkDeleted"}
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
		"second VOLUME writer":     {src: `func w(ctx context.Context, st NewVolumeStore, v Volume) error { return st.CreateVolumeRecord(ctx,v) }`, want: "CreateVolumeRecord used by"},
		"VOLUME method alias":      {src: `func w(st NewVolumeStore) { write := st.CreateVolumeRecord; _ = write }`, want: "CreateVolumeRecord used by"},
		"VOLUME method expression": {src: `var write = NewVolumeStore.CreateVolumeRecord`, want: "CreateVolumeRecord used by"},
		"direct call":              {src: `func w(ctx context.Context, st HeadStore, h Head) error { return st.UpdateHead(ctx, h.Gen, h) }`, want: "UpdateHead used by"},
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
func w(ctx context.Context, x p) error { return x.DeleteObject(ctx, "o") }`, want: "only RemoveUnreachable (mark-gated) and DeleteVolume"},
		"claim without lease wait": {src: `func RunClaim(ctx context.Context, st HeadStore, lease LeaseWait, me HolderID) (Head, error) {
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h, false, nil })
}`, want: "does not wait for the old lease", file: "claim.go"},
		"transition not through runTransition": {src: `func RunZz(ctx context.Context, st HeadStore) (Head, error) { h, _, err := st.ReadHead(ctx); return h, err }`, want: "does not go through runTransition"},
		"delete removes the pool before releasing dependents": {file: "delete.go", src: `type p interface{ DeleteOfVolume(context.Context, string, VolumeID) error }
type g interface{ Reserve(context.Context) error; NoLiveLease(context.Context) error }
func RunMarkDeleting(ctx context.Context, st HeadStore, x g) (Head, error) { _ = x.NoLiveLease(ctx); return runTransition(ctx, st, nil) }
func RunMarkDeleted(ctx context.Context, st HeadStore) (Head, error) { return runTransition(ctx, st, nil) }
func RunReleaseDependent(ctx context.Context, st HeadStore, id VolumeID) (Head, error) { return runTransition(ctx, st, nil) }
func DeleteVolume(ctx context.Context, st HeadStore, x g, id VolumeID, srcs []HeadStore, pool p, objs []string) error {
	_ = x.Reserve(ctx)
	_, _ = RunMarkDeleting(ctx, st, x)
	for _, o := range objs {
		_ = pool.DeleteOfVolume(ctx, o, id)
	}
	for _, s := range srcs {
		_, _ = RunReleaseDependent(ctx, s, id)
	}
	_, _ = RunMarkDeleted(ctx, st)
	return nil
}`, want: "runs DeleteOfVolume before RunReleaseDependent"},
		"delete without the reservation": {file: "delete.go", src: `type p interface{ DeleteOfVolume(context.Context, string, VolumeID) error }
type g interface{ NoLiveLease(context.Context) error }
func RunMarkDeleting(ctx context.Context, st HeadStore, x g) (Head, error) { _ = x.NoLiveLease(ctx); return runTransition(ctx, st, nil) }
func RunMarkDeleted(ctx context.Context, st HeadStore) (Head, error) { return runTransition(ctx, st, nil) }
func RunReleaseDependent(ctx context.Context, st HeadStore, id VolumeID) (Head, error) { return runTransition(ctx, st, nil) }
func DeleteVolume(ctx context.Context, st HeadStore, x g, id VolumeID, srcs []HeadStore, pool p, objs []string) error {
	_, _ = RunMarkDeleting(ctx, st, x)
	for _, s := range srcs {
		_, _ = RunReleaseDependent(ctx, s, id)
	}
	for _, o := range objs {
		_ = pool.DeleteOfVolume(ctx, o, id)
	}
	_, _ = RunMarkDeleted(ctx, st)
	return nil
}`, want: "DeleteVolume has no Reserve"},
		"mark deleting without the live-lease check": {file: "delete.go", src: `func RunMarkDeleting(ctx context.Context, st HeadStore) (Head, error) {
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h.MarkDeleting() })
}`, want: "does not check the master's live leases"},
		"live-lease check outside mark deleting": {src: `type g interface{ NoLiveLease(context.Context) error }
func w(ctx context.Context, x g) error { return x.NoLiveLease(ctx) }`, want: "only RunMarkDeleting checks it"},
		"release outside delete and the operator's check": {src: `func w(ctx context.Context, st HeadStore, id VolumeID) { _, _ = RunReleaseDependent(ctx, st, id) }`, want: "only DeleteVolume (target deleting) and OperatorReleaseDependent"},
		"operator release without reading the target": {file: "delete.go", src: `func OperatorReleaseDependent(ctx context.Context, source HeadStore, id VolumeID) (Head, error) {
	return RunReleaseDependent(ctx, source, id)
}`, want: "does not read the target's HEAD"},
		"unconditional delete":                     {file: "remove.go", src: removeVariant(t, "unconditional"), want: "its only removal is DeleteMarked"},
		"ignored mark":                             {file: "remove.go", src: removeVariant(t, "ignored"), want: "without a mark gate immediately before it"},
		"one mark, two deletes":                    {file: "remove.go", src: removeVariant(t, "second"), want: "without a mark gate immediately before it"},
		"unlisted removal method":                  {file: "remove.go", src: removeVariant(t, "other"), want: "without a mark gate immediately before it"},
		"claim outside Start":                      {src: `func w(ctx context.Context, st HeadStore, l LeaseWait, me HolderID) { _, _ = RunClaim(ctx, st, l, me) }`, want: "a claim is made only by Start"},
		"a fourth HEAD create":                     {src: `func w(ctx context.Context, st NewVolumeStore, h Head) { _ = st.CreateHead(ctx, h) }`, want: "only createVolume creates a HEAD"},
		"tombstone outside the operator's release": {src: `func w(ctx context.Context, t TargetStore, h Head) { _ = t.CreateDeletedHead(ctx, h) }`, want: "only the operator's release writes a tombstone"},
		"remover method value": {src: `type p interface{ RemoveName(string) }
func w(x p) { f := x.RemoveName; f("o") }`, want: "only RemoveUnreachable (mark-gated) and DeleteVolume"},
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
	gate := "if err := os.Mark(ctx, c.Path(), tag, session); err != nil {"
	if !strings.Contains(src, gate) {
		t.Fatal("remove.go mark gate not found")
	}
	switch mode {
	case "ignored":
		i := strings.Index(src, gate)
		j := strings.Index(src[i:], "\n\t\t}\n") + i + len("\n\t\t}\n")
		src = src[:i] + "_ = os.Mark(ctx, c.Path(), plan, session)\n" + src[j:]
	case "second":
		src = strings.Replace(src, "removed = append(removed, c)", "_ = os.DeleteMarked(ctx, c.Path(), session)\n\t\tremoved = append(removed, c)", 1)
	case "other":
		src = strings.Replace(src, "removed = append(removed, c)", "_ = os.RemoveOther(ctx, c.Path())\n\t\tremoved = append(removed, c)", 1)
	case "unconditional":
		src = strings.Replace(src, "if err := os.DeleteMarked(ctx, c.Path(), session); err != nil {", "if err := os.DeleteObject(ctx, c.Path()); err != nil {", 1)
	}
	i := strings.Index(src, ")\n")
	return src[i+2:]
}
