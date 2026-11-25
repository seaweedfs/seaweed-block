package layout

import (
	"context"
	"errors"
	"fmt"
	"path"
	"testing"
)

func deletingHead() Head {
	h := idleHead()
	h.State = StateDeleting
	return h
}

func TestMarkDeleting_Closure(t *testing.T) {
	next, changed, err := idleHead().MarkDeleting()
	if err != nil || !changed || next.State != StateDeleting {
		t.Fatalf("MarkDeleting from live: %+v %t %v", next, changed, err)
	}
	for _, h := range []Head{deletingHead(), func() Head { h := idleHead(); h.State = StateDeleted; return h }()} {
		if again, changed, err := h.MarkDeleting(); err != nil || changed || again.State != h.State {
			t.Fatalf("MarkDeleting over %s: %+v %t %v; want a replay", h.State, again, changed, err)
		}
	}
	if _, _, err := snapHead("s1").MarkDeleting(); !errors.Is(err, ErrHolderActive) {
		t.Fatalf("MarkDeleting with a snapshot open: %v; want ErrHolderActive", err)
	}
	withDeps := idleHead()
	rn := RootName{Hash: Sum([]byte("r"))}
	withDeps.Snapshots = map[string]Snapshot{"daily": {Root: rn, Dependents: deps(rn, "t1")}}
	if _, _, err := withDeps.MarkDeleting(); !errors.Is(err, ErrHasDependents) {
		t.Fatalf("MarkDeleting with a dependent: %v; want ErrHasDependents", err)
	}
}

func TestMarkDeleted_Closure(t *testing.T) {
	next, changed, err := deletingHead().MarkDeleted()
	if err != nil || !changed || next.State != StateDeleted {
		t.Fatalf("MarkDeleted from deleting: %+v %t %v", next, changed, err)
	}
	if _, changed, err := next.MarkDeleted(); err != nil || changed {
		t.Fatalf("MarkDeleted over deleted: %t %v; want a replay", changed, err)
	}
	if _, _, err := idleHead().MarkDeleted(); err == nil {
		t.Fatal("MarkDeleted over a live HEAD succeeded")
	}
}

func TestReleaseDependent_Closure(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	old := RootName{Hash: Sum([]byte("old"))}
	h := idleHead()
	h.Snapshots = map[string]Snapshot{"daily": {Root: rn, Dependents: deps(rn, "t1", "t2")}, "weekly": {Root: old, Dependents: deps(old, "t1")}}
	next, changed, err := h.ReleaseDependent(idFor("t1"))
	if err != nil || !changed {
		t.Fatalf("ReleaseDependent: %+v %t %v", next, changed, err)
	}
	if d := next.Snapshots["daily"].Dependents; len(d) != 1 || d[0].Target != "t2" || len(next.Snapshots["weekly"].Dependents) != 0 {
		t.Fatalf("dependents after releasing t1: %+v", next.Snapshots)
	}
	if len(h.Snapshots["daily"].Dependents) != 2 {
		t.Fatal("ReleaseDependent changed its receiver")
	}
	if _, changed, err := next.ReleaseDependent(idFor("t1")); err != nil || changed {
		t.Fatalf("ReleaseDependent of an absent target: %t %v; want a replay", changed, err)
	}
	// A volume created again under t2's name has another id and releases nothing.
	if _, changed, err := next.ReleaseDependent(idFor("t2 again")); err != nil || changed {
		t.Fatalf("ReleaseDependent of another volume under the same name: %t %v; want nothing released", changed, err)
	}
	if _, _, err := h.ReleaseDependent(VolumeID{}); err == nil {
		t.Fatal("ReleaseDependent of the zero id accepted")
	}
}

// Once deleting, no holder transition, no claim and no removal applies.
func TestDeleting_RefusesEveryHolderStep(t *testing.T) {
	h := deletingHead()
	rn := RootName{Hash: Sum([]byte("r"))}
	for name, step := range map[string]func() error{
		"Begin":      func() error { _, _, err := h.Begin(me, "s"); return err },
		"Abort":      func() error { _, _, err := h.Abort(me, "s"); return err },
		"Commit":     func() error { _, _, err := h.Commit(me, "s", rn, 1, ""); return err },
		"BeginClean": func() error { _, _, err := h.BeginClean(me, "c"); return err },
		"EndClean":   func() error { _, _, err := h.EndClean(me, "c"); return err },
		"Claim":      func() error { _, _, err := h.Claim(newer); return err },
	} {
		if err := step(); !errors.Is(err, ErrVolumeDeleting) {
			t.Errorf("%s over a deleting HEAD: %v; want ErrVolumeDeleting", name, err)
		}
	}
	o, rh, _, orphans := removeFixture(t)
	rh.State = StateDeleting
	if _, err := removeShard0(t, &memHead{h: rh}, o, &fakeHeld{}, "c1", orphans); !errors.Is(err, ErrVolumeDeleting) || len(o.deletes) != 0 {
		t.Fatalf("removal over a deleting HEAD: %v, %d deletes", err, len(o.deletes))
	}
}

func TestRunMarkDeleting_InterruptAtEveryStep(t *testing.T) {
	interruptHead(t, idleHead(), func(st HeadStore) error { _, err := RunMarkDeleting(context.Background(), st); return err },
		func(t *testing.T, h Head) {
			if h.State != StateDeleting {
				t.Fatalf("state %q", h.State)
			}
		})
}

func TestRunMarkDeleted_InterruptAtEveryStep(t *testing.T) {
	interruptHead(t, deletingHead(), func(st HeadStore) error { _, err := RunMarkDeleted(context.Background(), st); return err },
		func(t *testing.T, h Head) {
			if h.State != StateDeleted {
				t.Fatalf("state %q", h.State)
			}
		})
}

func TestRunReleaseDependent_InterruptAtEveryStep(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	h := idleHead()
	h.Snapshots = map[string]Snapshot{"daily": {Root: rn, Dependents: deps(rn, "t1", "t2")}}
	interruptHead(t, h, func(st HeadStore) error {
		_, err := RunReleaseDependent(context.Background(), st, idFor("t1"))
		return err
	},
		func(t *testing.T, h Head) {
			if d := h.Snapshots["daily"].Dependents; len(d) != 1 || d[0].Target != "t2" {
				t.Fatalf("dependents %+v", d)
			}
		})
}

type gone struct{ err error }

func (g gone) NoHolder(context.Context) error { return g.err }

// deleteWorld: volume t (a restore target of src's snapshot "daily") with
// two objects in its pool; src's HEAD lists t as a dependent.
type deleteWorld struct {
	t, src *memHead
	objs   map[string][]byte
	rels   []string
}

func newDeleteWorld() *deleteWorld {
	rn := RootName{Hash: Sum([]byte("src root"))}
	src := idleHead()
	src.VolumeID = idFor("src")
	src.Snapshots = map[string]Snapshot{"daily": {Root: rn, Dependents: deps(rn, "t")}}
	tgt := idleHead()
	tgt.VolumeID = idFor("t")
	rels := []string{NameChunk(1, []byte("x")).Path(), NameChunk(2, []byte("y")).Path()}
	objs := map[string][]byte{}
	for _, r := range rels {
		objs[path.Join(volRoot, r)] = []byte("o")
	}
	return &deleteWorld{t: &memHead{h: &tgt}, src: &memHead{h: &src}, objs: objs, rels: rels}
}

// A delete is refused while a holder acts and while the volume has
// dependents of its own; otherwise it runs in order and ends deleted with
// its pool empty and itself released from its source.
func TestDeleteVolume(t *testing.T) {
	ctx := context.Background()
	w := newDeleteWorld()
	o := &objPool{objs: w.objs}
	if err := DeleteVolume(ctx, w.t, gone{errors.New("leased to srv-a")}, idFor("t"), []HeadStore{w.src}, o, w.rels); !errors.Is(err, ErrHolderActive) || w.t.updates != 0 {
		t.Fatalf("delete while held: %v (%d HEAD writes)", err, w.t.updates)
	}
	if err := DeleteVolume(ctx, w.src, gone{}, idFor("src"), nil, o, nil); !errors.Is(err, ErrHasDependents) {
		t.Fatalf("delete of a volume with a dependent: %v; want ErrHasDependents", err)
	}
	if err := DeleteVolume(ctx, w.t, gone{}, idFor("t"), []HeadStore{w.src}, o, w.rels); err != nil {
		t.Fatal(err)
	}
	if w.t.h.State != StateDeleted || len(o.objs) != 0 || len(w.src.h.Snapshots["daily"].Dependents) != 0 {
		t.Fatalf("after the delete: state %q, %d objects, source dependents %+v", w.t.h.State, len(o.objs), w.src.h.Snapshots["daily"].Dependents)
	}
	if err := DeleteVolume(ctx, w.t, gone{}, idFor("t"), []HeadStore{w.src}, o, w.rels); err != nil {
		t.Fatalf("a further re-run: %v", err)
	}
}

// Interrupt at every step of the volume delete (HEAD deleting, the release
// on the source, each object, HEAD deleted), before the write applies and
// after it applied with its reply lost. After every kill no step past the
// kill has run out of order (the pool is touched only once HEAD is
// deleting and the source released); the re-run ends deleted, the pool
// empty and the source released.
func TestDeleteVolume_InterruptAtEveryStep(t *testing.T) {
	ctx := context.Background()
	order := []string{"t HEAD", "src HEAD", "object", "object", "t HEAD"}
	for k := range order {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("step%d %s/after=%t", k+1, order[k], after), func(t *testing.T) {
				w := newDeleteWorld()
				o := &objPool{objs: w.objs}
				nth := 0
				for _, kind := range order[:k+1] {
					if kind == order[k] {
						nth++
					}
				}
				switch order[k] {
				case "t HEAD":
					w.t.killAt, w.t.after = nth, after
				case "src HEAD":
					w.src.killAt, w.src.after = nth, after
				case "object":
					o.killAt, o.after = nth, after
				}
				if err := DeleteVolume(ctx, w.t, gone{}, idFor("t"), []HeadStore{w.src}, o, w.rels); !errors.Is(err, errKilled) {
					t.Fatalf("killed run: %v", err)
				}
				if len(o.objs) < len(w.rels) && (w.t.h.State == StateLive || len(w.src.h.Snapshots["daily"].Dependents) != 0) {
					t.Fatal("the pool was touched before HEAD was deleting and the source released")
				}
				if len(w.src.h.Snapshots["daily"].Dependents) == 0 && w.t.h.State == StateLive {
					t.Fatal("the source released the target before its HEAD was deleting")
				}
				w.t.restart()
				w.src.restart()
				o.dead, o.killAt = false, 0
				if err := DeleteVolume(ctx, w.t, gone{}, idFor("t"), []HeadStore{w.src}, o, w.rels); err != nil {
					t.Fatalf("re-run: %v", err)
				}
				if w.t.h.State != StateDeleted || len(o.objs) != 0 || len(w.src.h.Snapshots["daily"].Dependents) != 0 {
					t.Fatalf("after the re-run: state %q, %d objects, source dependents %+v", w.t.h.State, len(o.objs), w.src.h.Snapshots["daily"].Dependents)
				}
			})
		}
	}
}
