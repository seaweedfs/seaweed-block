package layout

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Every pure transition refuses a caller that is not the holder at HEAD's
// epoch: an older epoch of the same server, and the same epoch held by
// another server, in idle, snap and clean. The table must name every
// method of Head that takes a HolderID and returns (Head, bool, error), so
// a new holder transition fails here until it is added.
func TestTransitions_RefuseAStaleHolder(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	steps := map[string]func(Head, HolderID) error{
		"Begin": func(h Head, c HolderID) error { _, _, err := h.Begin(c, "s"); return err },
		"Abort": func(h Head, c HolderID) error { _, _, err := h.Abort(c, "s"); return err },
		"Commit": func(h Head, c HolderID) error {
			_, _, err := h.Commit(c, "s", rn, 1, "", SnapshotMeta{}, StateLive)
			return err
		},
		"BeginClean": func(h Head, c HolderID) error { _, _, err := h.BeginClean(c, "c"); return err },
		"EndClean":   func(h Head, c HolderID) error { _, _, err := h.EndClean(c, "c"); return err },
		"Claim":      func(h Head, c HolderID) error { _, _, err := h.Claim(c); return err },
	}
	callers := map[string]HolderID{
		"older epoch, same server":                {Epoch: 2, Holder: "srv-a"},
		"same epoch, another server":              {Epoch: 3, Holder: "srv-b"},
		"same epoch and server, another claim id": {Epoch: 3, Holder: "srv-a", ClaimID: "k-other"},
	}
	for name, step := range steps {
		for who, c := range callers {
			for _, h := range []Head{idleHead(), snapHead("s"), cleanHead("c")} {
				if err := step(h, c); err == nil {
					t.Errorf("%s by %s over a HEAD at epoch 3 held by srv-a (%s) succeeded", name, who, h.Activity.Kind)
				}
			}
		}
	}
	headT, boolT := reflect.TypeOf(Head{}), reflect.TypeOf(false)
	errT := reflect.TypeOf((*error)(nil)).Elem()
	var missing []string
	for i := 0; i < headT.NumMethod(); i++ {
		m := headT.Method(i).Type
		holderT := reflect.TypeOf(HolderID{})
		if m.NumOut() == 3 && m.Out(0) == headT && m.Out(1) == boolT && m.Out(2) == errT && m.NumIn() > 1 && m.In(1) == holderT {
			if _, ok := steps[headT.Method(i).Name]; !ok {
				missing = append(missing, headT.Method(i).Name)
			}
		}
	}
	sort.Strings(missing)
	if len(missing) != 0 {
		t.Fatalf("transitions not in the stale-holder table: %v", missing)
	}
}

// The removal refuses a stale holder too, one dimension at a time: an
// older epoch of the same server, and the same epoch held by another
// server; nothing is deleted. Every function of the package that takes a
// HolderID and is not a Run transition (those go through the pure methods
// above) must be listed here, so a new one fails until it is covered.
func TestHolderFunctions_RefuseAStaleHolder(t *testing.T) {
	callers := map[string]HolderID{
		"older epoch, same server":                {Epoch: me.Epoch - 1, Holder: me.Holder, ClaimID: me.ClaimID},
		"same epoch, another server":              {Epoch: me.Epoch, Holder: "srv-b", ClaimID: me.ClaimID},
		"same epoch and server, another claim id": {Epoch: me.Epoch, Holder: me.Holder, ClaimID: "k-other"},
	}
	for who, c := range callers {
		o, h, _, orphans := removeFixture(t)
		r, err := NewReader(removeBase, "v1", o.fetch)
		if err != nil {
			t.Fatal(err)
		}
		_, err = RemoveUnreachable(context.Background(), &memHead{h: h}, r, o, c, "c1", 0, orphans)
		if !errors.Is(err, ErrNotHolder) || len(o.writes) != 0 {
			t.Errorf("RemoveUnreachable by %s: %v, %d writes; want ErrNotHolder and none", who, err, len(o.writes))
		}
	}
	// Start: an older line, another server at HEAD's epoch and another claim
	// id are its cases in start_test.go (TestStart_Refusals and the same-host
	// forced successor); startCheck is Start's check, run again inside the
	// claim (TestStart_ValidatesAgainstTheHeadItUpdates).
	// DetachedBy: my own committed, idle detach proves nothing to a stale
	// caller in any one dimension.
	mine := Head{Gen: 1, State: StateDetached, Epoch: me.Epoch, Holder: me.Holder, ClaimID: me.ClaimID}
	mine.Activity.Kind = ActIdle
	if !DetachedBy(mine, me) {
		t.Fatal("DetachedBy refused the holder's own detach")
	}
	for who, c := range callers {
		if DetachedBy(mine, c) {
			t.Errorf("DetachedBy accepted %s", who)
		}
	}
	// CheckCommitObjects runs commitChecked (RunCommit's body): Head.Commit
	// refuses a stale caller before any lookup (no object store is given).
	for who, c := range callers {
		h := Head{Gen: 2, State: StateLive, Epoch: me.Epoch, Holder: me.Holder, ClaimID: me.ClaimID}
		h.Activity = Activity{Kind: ActSnap, Session: "s"}
		if err := CheckCommitObjects(context.Background(), h, nil, "v1", c, CommitRequest{Session: "s", Next: map[uint64]Ref{}, State: StateLive}); err == nil {
			t.Errorf("CheckCommitObjects by %s over a snap(s) HEAD held by %s passed", who, me.Holder)
		}
	}
	covered := map[string]bool{"RemoveUnreachable": true, "Start": true, "startCheck": true, "DetachedBy": true, "CheckCommitObjects": true, "commitChecked": true}
	var missing []string
	for fname, src := range packageSources(t, false) {
		file, err := parser.ParseFile(token.NewFileSet(), fname, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || strings.HasPrefix(fn.Name.Name, "Run") {
				continue
			}
			for _, p := range fn.Type.Params.List {
				if id, ok := p.Type.(*ast.Ident); ok && id.Name == "HolderID" && !covered[fn.Name.Name] {
					missing = append(missing, fn.Name.Name)
				}
			}
		}
	}
	sort.Strings(missing)
	if len(missing) != 0 {
		t.Fatalf("functions taking a HolderID not covered by a stale-holder case: %v", missing)
	}
}
