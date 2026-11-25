package layout

import (
	"context"
	"errors"
	"testing"
)

func TestDeleteSnapshot_Closure(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	h := idleHead()
	h.Snapshots = map[string]Snapshot{"daily": {Root: rn}, "pinned": {Root: rn, Dependents: deps(rn, "t1")}}
	next, changed, err := h.DeleteSnapshot("daily")
	if err != nil || !changed || len(next.Snapshots) != 1 {
		t.Fatalf("DeleteSnapshot: %+v %t %v", next.Snapshots, changed, err)
	}
	if _, ok := h.Snapshots["daily"]; !ok {
		t.Fatal("DeleteSnapshot changed its receiver")
	}
	if _, changed, err := next.DeleteSnapshot("daily"); err != nil || changed {
		t.Fatalf("DeleteSnapshot of a missing name: %t %v; want a replay", changed, err)
	}
	if _, _, err := h.DeleteSnapshot("pinned"); !errors.Is(err, ErrHasDependents) {
		t.Fatalf("DeleteSnapshot with a dependent: %v; want ErrHasDependents", err)
	}
	d := deletingHead()
	d.Snapshots = map[string]Snapshot{"daily": {Root: rn}}
	if _, _, err := d.DeleteSnapshot("daily"); !errors.Is(err, ErrVolumeDeleting) {
		t.Fatalf("DeleteSnapshot on a deleting volume: %v", err)
	}
}

func TestRunDeleteSnapshot_InterruptAtEveryStep(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	h := idleHead()
	h.Snapshots = map[string]Snapshot{"daily": {Root: rn}}
	interruptHead(t, h, func(st HeadStore) error { _, err := RunDeleteSnapshot(context.Background(), st, "daily"); return err },
		func(t *testing.T, h Head) {
			if _, ok := h.Snapshots["daily"]; ok {
				t.Fatal("snapshot still in HEAD")
			}
		})
}

func TestAddDependent_Closure(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	h := idleHead()
	h.Snapshots = map[string]Snapshot{"daily": {Root: rn}}
	t1 := Dependent{Target: "t1", TargetID: idFor("t1")}
	next, changed, err := h.AddDependent("daily", t1, rn)
	if err != nil || !changed {
		t.Fatalf("AddDependent: %v", err)
	}
	if d := next.Snapshots["daily"].Dependents; len(d) != 1 || d[0].TargetID != idFor("t1") || d[0].Root != rn {
		t.Fatalf("dependents %+v; want t1 with the snapshot's root", d)
	}
	if _, err := next.Encode(); err != nil {
		t.Fatalf("HEAD after AddDependent does not encode: %v", err)
	}
	if _, changed, err := next.AddDependent("daily", t1, rn); err != nil || changed {
		t.Fatalf("AddDependent again: %t %v; want a replay", changed, err)
	}
	if _, _, err := h.AddDependent("weekly", t1, rn); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("AddDependent of a missing snapshot: %v", err)
	}
	if _, _, err := h.AddDependent("daily", t1, RootName{Hash: Sum([]byte("other"))}); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("AddDependent on a snapshot whose root changed since the check: %v", err)
	}
	if _, _, err := h.AddDependent("daily", Dependent{Target: "t1"}, rn); err == nil {
		t.Fatal("AddDependent without the target's id accepted")
	}
	d := deletingHead()
	d.Snapshots = map[string]Snapshot{"daily": {Root: rn}}
	if _, _, err := d.AddDependent("daily", t1, rn); !errors.Is(err, ErrVolumeDeleting) {
		t.Fatalf("AddDependent on a deleting volume: %v", err)
	}
}

// RunAddDependent refuses a snapshot that names another volume's chunks:
// a restore target of a restore target would make the dependency deeper
// than one.
func TestRunAddDependent_RefusesAForeignSource(t *testing.T) {
	ctx := context.Background()
	o, h, _, _ := removeFixture(t)
	foreign := manifest(Ref{Owner: "src", OwnerID: idFor("src"), Chunk: NameChunk(1, []byte("x"))})
	data := map[string][]byte{}
	frn := writeImage(t, data, foreign, nil)
	for p, b := range data {
		o.objs[p] = &memObject{data: b, meta: ObjectMeta{Session: "s0"}}
	}
	h.Snapshots["restored"] = Snapshot{Root: frn}
	r, err := NewReader(removeBase, "v1", o.fetch)
	if err != nil {
		t.Fatal(err)
	}
	st := &memHead{h: h}
	if _, err := RunAddDependent(ctx, st, r, "restored", Dependent{Target: "t", TargetID: idFor("t")}); !errors.Is(err, ErrForeignSource) || st.updates != 0 {
		t.Fatalf("AddDependent on a snapshot naming foreign chunks: %v (%d writes); want ErrForeignSource", err, st.updates)
	}
	if _, err := RunAddDependent(ctx, st, r, "daily", Dependent{Target: "t", TargetID: idFor("t")}); err != nil || len(st.h.Snapshots["daily"].Dependents) != 1 {
		t.Fatalf("AddDependent on an own snapshot: %v", err)
	}
}

func TestRunAddDependent_InterruptAtEveryStep(t *testing.T) {
	o, h, _, _ := removeFixture(t)
	r, err := NewReader(removeBase, "v1", o.fetch)
	if err != nil {
		t.Fatal(err)
	}
	h.Activity = Activity{Kind: ActIdle}
	interruptHead(t, *h, func(st HeadStore) error {
		_, err := RunAddDependent(context.Background(), st, r, "daily", Dependent{Target: "t", TargetID: idFor("t")})
		return err
	}, func(t *testing.T, h Head) {
		if d := h.Snapshots["daily"].Dependents; len(d) != 1 || d[0].TargetID != idFor("t") {
			t.Fatalf("dependents %+v", d)
		}
	})
}
