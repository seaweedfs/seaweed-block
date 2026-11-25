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
		"Commit":     func() error { _, _, err := h.Commit(me, "s", rn, 1, "", StateLive); return err },
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
	if _, err := removeShard0(t, &memHead{h: rh}, o, "c1", orphans); !errors.Is(err, ErrVolumeDeleting) || len(o.writes) != 0 {
		t.Fatalf("removal over a deleting HEAD: %v, %d writes", err, len(o.writes))
	}
}

func TestRunMarkDeleting_InterruptAtEveryStep(t *testing.T) {
	interruptHead(t, idleHead(), func(st HeadStore) error {
		_, err := RunMarkDeleting(context.Background(), st, &fakeMaster{})
		return err
	},
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

// fakeMaster is the master's delete authority: a reservation after which
// it grants no line, and the lines holding a live lease.
type fakeMaster struct {
	reserved  bool
	leases    int
	err       error  // NoLiveLease's answer while leases is 0, when set
	onReserve func() // runs after Reserve recorded the reservation
}

func (m *fakeMaster) Reserve(context.Context) error {
	m.reserved = true
	if f := m.onReserve; f != nil {
		m.onReserve = nil
		f()
	}
	return nil
}

func (m *fakeMaster) NoLiveLease(context.Context) error {
	if m.leases > 0 {
		return fmt.Errorf("%d lines hold a live lease", m.leases)
	}
	return m.err
}

// grant grants a line (a live lease), refused after the reservation.
func (m *fakeMaster) grant() error {
	if m.reserved {
		return errors.New("volume reserved for delete")
	}
	m.leases++
	return nil
}

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

// objects returns the target's objects as an object store of volume t.
func (w *deleteWorld) objects() *memObjects {
	o := newMemObjects(volRoot)
	o.volume = idFor("t")
	for p, b := range w.objs {
		o.objs[p] = &memObject{data: b, meta: ObjectMeta{Session: "s0", Volume: o.volume}}
	}
	return o
}

// A late delete of a finished volume delete never removes the object of a
// volume created again under the name: the delete is conditional on the
// entry's volume id, and the new volume's entry carries another.
func TestDeleteVolume_ALateDeleteSparesTheNameReused(t *testing.T) {
	ctx := context.Background()
	w := newDeleteWorld()
	o := w.objects()
	if err := DeleteVolume(ctx, w.t, &fakeMaster{}, idFor("t"), []HeadStore{w.src}, o, w.rels); err != nil {
		t.Fatal(err)
	}
	o.volume = idFor("t again") // the name holds a new volume, which writes the same content name
	if err := o.Create(ctx, w.rels[0], []byte("o"), "n1"); err != nil {
		t.Fatal(err)
	}
	if err := o.DeleteOfVolume(ctx, w.rels[0], idFor("t")); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("the old run's late delete: %v; want ErrPrecondition", err)
	}
	if !o.has(w.rels[0]) {
		t.Fatal("the old run's late delete removed the new volume's object")
	}
}

// A delete is refused while a holder acts and while the volume has
// dependents of its own; otherwise it runs in order and ends deleted with
// its pool empty and itself released from its source.
func TestDeleteVolume(t *testing.T) {
	ctx := context.Background()
	w := newDeleteWorld()
	o := w.objects()
	if err := DeleteVolume(ctx, w.t, &fakeMaster{leases: 1}, idFor("t"), []HeadStore{w.src}, o, w.rels); !errors.Is(err, ErrHolderActive) || w.t.updates != 0 {
		t.Fatalf("delete while held: %v (%d HEAD writes)", err, w.t.updates)
	}
	if err := DeleteVolume(ctx, w.src, &fakeMaster{}, idFor("src"), nil, o, nil); !errors.Is(err, ErrHasDependents) {
		t.Fatalf("delete of a volume with a dependent: %v; want ErrHasDependents", err)
	}
	if err := DeleteVolume(ctx, w.t, &fakeMaster{}, idFor("t"), []HeadStore{w.src}, o, w.rels); err != nil {
		t.Fatal(err)
	}
	if w.t.h.State != StateDeleted || len(o.objs) != 0 || len(w.src.h.Snapshots["daily"].Dependents) != 0 {
		t.Fatalf("after the delete: state %q, %d objects, source dependents %+v", w.t.h.State, len(o.objs), w.src.h.Snapshots["daily"].Dependents)
	}
	if err := DeleteVolume(ctx, w.t, &fakeMaster{}, idFor("t"), []HeadStore{w.src}, o, w.rels); err != nil {
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
				o := w.objects()
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
				if err := DeleteVolume(ctx, w.t, &fakeMaster{}, idFor("t"), []HeadStore{w.src}, o, w.rels); !errors.Is(err, errKilled) {
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
				if err := DeleteVolume(ctx, w.t, &fakeMaster{}, idFor("t"), []HeadStore{w.src}, o, w.rels); err != nil {
					t.Fatalf("re-run: %v", err)
				}
				if w.t.h.State != StateDeleted || len(o.objs) != 0 || len(w.src.h.Snapshots["daily"].Dependents) != 0 {
					t.Fatalf("after the re-run: state %q, %d objects, source dependents %+v", w.t.h.State, len(o.objs), w.src.h.Snapshots["daily"].Dependents)
				}
			})
		}
	}
}

// operatorCase: source src (id of "src") lists target t as a dependent;
// t's VOLUME, HEAD and root objects are as the case sets them. When the
// target names chunks, namesSource decides whether one of them is src's.
func operatorCase(t *testing.T, tgtHead *Head, tgtVolume *VolumeID, namesSource bool) (*memHead, *memHead, *Reader) {
	t.Helper()
	w := newDeleteWorld()
	objs := map[string][]byte{}
	put := func(rel string, b []byte) { objs[path.Join(removeBase, "t", rel)] = b }
	if tgtVolume != nil {
		b, err := Volume{ID: *tgtVolume, Name: "t", Format: VolumeFormat}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		put(VolumeName, b)
	}
	if tgtHead != nil {
		m := manifest(Ref{Owner: "t", OwnerID: idFor("t"), Chunk: NameChunk(1, []byte("own"))})
		if namesSource {
			m[2] = Ref{Owner: "src", OwnerID: idFor("src"), Chunk: NameChunk(2, []byte("from src"))}
		}
		root, segs, err := Build(m)
		if err != nil {
			t.Fatal(err)
		}
		for n, b := range segs {
			put(n.Path(), b)
		}
		rn, rb, err := NameRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		put(rn.Path(), rb)
		h := *tgtHead
		h.Current = &rn
		tgtHead = &h
	}
	r, err := NewReader(removeBase, "t", func(_ context.Context, p string) ([]byte, bool, error) {
		b, ok := objs[p]
		return b, ok, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return w.src, &memHead{h: tgtHead}, r
}

// The operator's remove-dependent releases a target only when the target
// exists and is deleted or no longer names the source: its HEAD deleting
// or deleted, its name holding another volume (another VOLUME id), or a
// live HEAD none of whose roots names a chunk of the source. A live target
// that still names the source is refused and the source is unchanged. A
// target without HEAD (never created, or a restore that died between its
// VOLUME and its HEAD) gets a deleted HEAD written exclusively in its
// never-reused directory, so the restore can never create it later, and
// only then is released.
func TestOperatorReleaseDependent(t *testing.T) {
	ctx := context.Background()
	live := idleHead()
	live.VolumeID = idFor("t")
	deleting := live
	deleting.State = StateDeleting
	tid, other := idFor("t"), idFor("t again")
	for name, tc := range map[string]struct {
		head        *Head
		volume      *VolumeID
		namesSource bool
		release     bool
		want        error
	}{
		"live target naming the source":    {&live, &tid, true, false, ErrTargetLive},
		"live target no longer naming it":  {&live, &tid, false, true, nil},
		"restore died between its creates": {nil, &tid, false, true, nil},
		"target never created":             {nil, nil, false, true, nil},
		"target deleting":                  {&deleting, &tid, true, true, nil},
		"name holds another volume":        {nil, &other, false, true, nil},
	} {
		src, tgt, r := operatorCase(t, tc.head, tc.volume, tc.namesSource)
		before := src.updates
		_, err := OperatorReleaseDependent(ctx, src, tgt, r, tid)
		released := len(src.h.Snapshots["daily"].Dependents) == 0
		if tc.head == nil && (tc.volume == nil || *tc.volume == tid) && (tgt.h == nil || tgt.h.State != StateDeleted || tgt.h.VolumeID != tid) {
			t.Errorf("%s: target HEAD %+v; want a deleted HEAD (tombstone) of the target's id written before the release", name, tgt.h)
		}
		if tc.release && (err != nil || !released) {
			t.Errorf("%s: %v, released %t; want released", name, err, released)
		}
		if !tc.release && (!errors.Is(err, tc.want) || released || src.updates != before) {
			t.Errorf("%s: %v, released %t; want %v and the source unchanged", name, err, released, tc.want)
		}
	}
}

// If the restore creates the target's HEAD between the operator's read and
// its tombstone, the exclusive create fails and nothing is released.
func TestOperatorReleaseDependent_TombstoneLosesToTheRestore(t *testing.T) {
	src, tgt, r := operatorCase(t, nil, nil, false)
	live := idleHead()
	live.VolumeID = idFor("t")
	created := &raceTarget{memHead: tgt, appear: &live}
	before := src.updates
	if _, err := OperatorReleaseDependent(context.Background(), src, created, r, idFor("t")); !errors.Is(err, ErrTargetLive) || src.updates != before {
		t.Fatalf("tombstone after the restore created the HEAD: %v (%d source writes); want ErrTargetLive and none", err, src.updates-before)
	}
}

// raceTarget makes a HEAD appear right before the tombstone's create.
type raceTarget struct {
	*memHead
	appear *Head
}

func (r *raceTarget) CreateDeletedHead(ctx context.Context, h Head) error {
	if r.appear != nil {
		n := r.appear.clone()
		r.memHead.h, r.appear = &n, nil
	}
	return r.memHead.CreateDeletedHead(ctx, h)
}

// A volume delete removes only content-named objects: HEAD (which stays,
// deleted), VOLUME and the directory are refused before any write.
func TestDeleteVolume_RemovesOnlyObjects(t *testing.T) {
	for _, bad := range []string{HeadName, VolumeName, "", "pool/", "pool/../HEAD", "other/x"} {
		w := newDeleteWorld()
		o := w.objects()
		if err := DeleteVolume(context.Background(), w.t, &fakeMaster{}, idFor("t"), []HeadStore{w.src}, o, append(append([]string(nil), w.rels...), bad)); !errors.Is(err, ErrMalformed) || w.t.updates != 0 || len(o.writes) != 0 {
			t.Errorf("delete listing %q: %v (%d HEAD writes, %d object writes); want ErrMalformed and nothing written", bad, err, w.t.updates, len(o.writes))
		}
	}
}

// The delete's premise is held by the master, not read once: a grant and
// claim before the reservation is seen as a live lease and refuses; a
// grant after it is refused by the master; a claim landing between the
// HEAD read and the update fails the update. In every case no object is
// deleted from a volume a holder acts on.
func TestDeleteVolume_AGrantRaceNeverDeletesALiveVolume(t *testing.T) {
	ctx := context.Background()
	claim := func(h *memHead) { h.h.Epoch, h.h.Holder, h.h.ClaimID = h.h.Epoch+1, "srv-new", "k-new"; h.h.Gen++ }
	t.Run("granted and claimed before the reservation", func(t *testing.T) {
		w := newDeleteWorld()
		o := w.objects()
		m := &fakeMaster{}
		if err := m.grant(); err != nil {
			t.Fatal(err)
		}
		claim(w.t)
		if err := DeleteVolume(ctx, w.t, m, idFor("t"), []HeadStore{w.src}, o, w.rels); !errors.Is(err, ErrHolderActive) || len(o.writes) != 0 || w.t.h.State != StateLive {
			t.Fatalf("delete of a volume with a granted line: %v, %d object writes, HEAD %s; want ErrHolderActive, nothing deleted", err, len(o.writes), w.t.h.State)
		}
	})
	t.Run("grant attempted after the reservation", func(t *testing.T) {
		w := newDeleteWorld()
		o := w.objects()
		m := &fakeMaster{}
		var grantErr error
		m.onReserve = func() {
			if grantErr = m.grant(); grantErr == nil {
				claim(w.t)
			}
		}
		if err := DeleteVolume(ctx, w.t, m, idFor("t"), []HeadStore{w.src}, o, w.rels); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if grantErr == nil {
			t.Fatal("the master granted a line after the delete reservation")
		}
	})
	t.Run("a claim between the HEAD read and the update", func(t *testing.T) {
		w := newDeleteWorld()
		o := w.objects()
		w.t.afterRead = func() { claim(w.t) }
		if err := DeleteVolume(ctx, w.t, &fakeMaster{}, idFor("t"), []HeadStore{w.src}, o, w.rels); err == nil || len(o.writes) != 0 || w.t.h.State != StateLive {
			t.Fatalf("delete with a claim after its HEAD read: %v, %d object writes, HEAD %s; want the update refused, nothing deleted", err, len(o.writes), w.t.h.State)
		}
	})
}
