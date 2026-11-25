package layout

import (
	"reflect"
	"sort"
	"testing"
)

// Every pure transition refuses a caller that is not the holder at HEAD's
// epoch: an older epoch of the same server, and the same epoch held by
// another server, in idle, snap and clean. The table must name every
// method of Head that returns (Head, bool, error), so a new transition
// fails here until it is added.
func TestTransitions_RefuseAStaleHolder(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	steps := map[string]func(Head, HolderID) error{
		"Begin":      func(h Head, c HolderID) error { _, _, err := h.Begin(c, "s"); return err },
		"Abort":      func(h Head, c HolderID) error { _, _, err := h.Abort(c, "s"); return err },
		"Commit":     func(h Head, c HolderID) error { _, _, err := h.Commit(c, "s", rn, 1, ""); return err },
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
		if m.NumOut() == 3 && m.Out(0) == headT && m.Out(1) == boolT && m.Out(2) == errT {
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
