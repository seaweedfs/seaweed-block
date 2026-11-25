package layout

import (
	"context"
	"errors"
	"fmt"
	"path"
	"testing"
)

const removeBase = "/swblock/chunks" // volRoot = removeBase/v1

// removeFixture: HEAD clean(c1) at epoch 3; the current root names chunks
// 0 and 3, the snapshot "daily" an older image naming chunks 0 and 2 (all
// in shard 0); chunks 5 and 7 are in the pool but no root names them, and
// chunk 10001 (shard 1) is unreferenced too. Every object was written by
// session s0.
func removeFixture(t *testing.T) (*memObjects, *Head, []ChunkName, []ChunkName) {
	t.Helper()
	data := map[string][]byte{}
	cur := manifest(ref("v1", 0, "a"), ref("v1", 3, "b"))
	old := manifest(ref("v1", 0, "a"), ref("v1", 2, "c"))
	curRoot := writeImage(t, data, cur, map[uint64][]byte{0: []byte("a"), 3: []byte("b")})
	oldRoot := writeImage(t, data, old, map[uint64][]byte{0: []byte("a"), 2: []byte("c")})
	var orphans []ChunkName
	for i, d := range map[uint64]string{5: "e", 7: "f", 10001: "g"} {
		c := NameChunk(i, []byte(d))
		data[path.Join(volRoot, c.Path())] = []byte(d)
		orphans = append(orphans, c)
	}
	vb, err := Volume{ID: idFor("v1"), Name: "v1", Format: VolumeFormat}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	data[path.Join(volRoot, VolumeName)] = vb
	o := newMemObjects(volRoot)
	for p, b := range data {
		o.objs[p] = &memObject{data: b, meta: ObjectMeta{Session: "s0"}}
	}
	h := &Head{Gen: 7, Epoch: 3, Holder: "srv-a", ClaimID: "k-a", VolumeID: idFor("v1"), Activity: Activity{Kind: ActClean, Session: "c1"}, Current: &curRoot,
		Snapshots: map[string]Snapshot{"daily": {Root: oldRoot}}}
	live := []ChunkName{cur[0].Chunk, cur[3].Chunk, old[2].Chunk}
	return o, h, live, orphans
}

func removeShard0(t *testing.T, st HeadStore, o *memObjects, session string, cands []ChunkName) ([]ChunkName, error) {
	t.Helper()
	r, err := NewReader(removeBase, "v1", o.fetch)
	if err != nil {
		t.Fatal(err)
	}
	return RemoveUnreachable(context.Background(), st, r, o, me, session, 0, cands)
}

func inPool(o *memObjects, c ChunkName) bool { return o.has(c.Path()) }

// Only names no root in HEAD reaches are removed, each by a mark then a
// delete conditional on that mark: the current root's and a snapshot's
// chunks stay, shard 0's orphans go, the shard-1 orphan is not touched by
// a shard-0 pass.
func TestRemoveUnreachable_RemovesOnlyWhatNoRootNames(t *testing.T) {
	o, h, live, orphans := removeFixture(t)
	cands := append(append([]ChunkName(nil), live...), orphans...)
	removed, err := removeShard0(t, &memHead{h: h}, o, "c1", cands)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 2 || removed[0].Index != 5 || removed[1].Index != 7 {
		t.Fatalf("removed %v; want chunks 5 and 7", removed)
	}
	for _, c := range live {
		if !inPool(o, c) {
			t.Fatalf("reachable chunk %s was removed", c)
		}
	}
	want := []string{"mark " + removed[0].Path(), "delete " + removed[0].Path(), "mark " + removed[1].Path(), "delete " + removed[1].Path()}
	if fmt.Sprint(o.writes) != fmt.Sprint(want) {
		t.Fatalf("writes %v; want each removal as a mark then its conditional delete: %v", o.writes, want)
	}
	if err := reachableMissing(h, o.data()); err != nil {
		t.Fatalf("after removal HEAD reaches a missing object: %v", err)
	}
	for _, c := range orphans {
		if c.Index == 10001 && !inPool(o, c) {
			t.Fatal("a shard-1 name was removed by a shard-0 pass")
		}
	}
}

// No name is removed while a snapshot is open (or without a cleaning, or
// by another holder, or over an absent HEAD).
func TestRemoveUnreachable_RefusedOutsideItsCleaning(t *testing.T) {
	for name, tc := range map[string]struct {
		edit func(*Head)
		want error
	}{
		"snapshot open":  {func(h *Head) { h.Activity = Activity{Kind: ActSnap, Session: "s1"} }, ErrNoSession},
		"idle":           {func(h *Head) { h.Activity = Activity{Kind: ActIdle} }, ErrNoSession},
		"other cleaning": {func(h *Head) { h.Activity = Activity{Kind: ActClean, Session: "c2"} }, ErrNoSession},
		"newer epoch":    {func(h *Head) { h.Epoch = 4; h.Holder = "srv-b" }, ErrNotHolder},
	} {
		o, h, _, orphans := removeFixture(t)
		tc.edit(h)
		if _, err := removeShard0(t, &memHead{h: h}, o, "c1", orphans); !errors.Is(err, tc.want) || len(o.writes) != 0 {
			t.Errorf("%s: %v, %d writes; want %v and none", name, err, len(o.writes), tc.want)
		}
	}
	o, _, _, orphans := removeFixture(t)
	if _, err := removeShard0(t, &memHead{}, o, "c1", orphans); !errors.Is(err, ErrHeadAbsent) || len(o.writes) != 0 {
		t.Errorf("absent HEAD: %v, %d writes", err, len(o.writes))
	}
}

// The race the conditional writes close: a successor (after a claim that
// voided this cleaning) reuses an orphan the old remover planned to remove,
// between the old plan and its mark, or between its mark and its delete,
// and commits a root naming it. The old mark or the old delete fails its
// condition; the object stays.
func TestRemoveUnreachable_AReuseDefeatsALateMarkOrDelete(t *testing.T) {
	for _, between := range []string{"mark", "delete"} {
		t.Run("reuse before the old "+between, func(t *testing.T) {
			o, h, _, orphans := removeFixture(t)
			var target ChunkName
			for _, c := range orphans {
				if c.Index == 5 {
					target = c
				}
			}
			o.hook = func(op, rel string) {
				if op == between && rel == target.Path() {
					o.hook = nil
					if err := EnsureObject(context.Background(), o, rel, []byte("e"), "successor"); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err := removeShard0(t, &memHead{h: h}, o, "c1", orphans); err != nil {
				t.Fatal(err)
			}
			if !inPool(o, target) || o.objs[path.Join(volRoot, target.Path())].meta != (ObjectMeta{Session: "successor"}) {
				t.Fatalf("chunk %s after a reuse before the old %s: present %t meta %+v; want kept, the successor's", target, between, inPool(o, target), o.objs[path.Join(volRoot, target.Path())])
			}
		})
	}
}

// The window the mark's condition is for: a successor reuses an orphan
// right after the remover read it for its plan. The mark, conditional on
// the metadata of the plan (not a fresh read), fails; the object stays.
func TestRemoveUnreachable_AReuseAfterThePlanDefeatsTheMark(t *testing.T) {
	o, h, _, orphans := removeFixture(t)
	var target ChunkName
	for _, c := range orphans {
		if c.Index == 5 {
			target = c
		}
	}
	o.afterStat = func(rel string) {
		if rel == target.Path() {
			o.afterStat = nil
			if err := EnsureObject(context.Background(), o, rel, []byte("e"), "successor"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := removeShard0(t, &memHead{h: h}, o, "c1", orphans); err != nil {
		t.Fatal(err)
	}
	if !inPool(o, target) {
		t.Fatalf("chunk %s reused after the plan was removed", target)
	}
}

// The stale remover's whole window: right after it reads HEAD, its lease
// ends and a successor claims, reuses an orphan (EnsureObject) and commits
// a root naming it. The remover's tags were read before its HEAD view, so
// the reuse changed the tag and its mark fails; had it read the tags after
// HEAD, it would mark and delete a chunk the successor's root names.
func TestRemoveUnreachable_ASuccessorAfterTheHeadViewDefeatsTheMark(t *testing.T) {
	o, h, _, orphans := removeFixture(t)
	var target ChunkName
	for _, c := range orphans {
		if c.Index == 5 {
			target = c
		}
	}
	st := &memHead{h: h}
	var hookErr error
	st.afterRead = func() {
		if err := EnsureObject(context.Background(), o, target.Path(), []byte("e"), "successor"); err != nil {
			hookErr = err
			return
		}
		// The successor's commit names the reused chunk; the old holder's
		// HEAD view (already taken) still shows its own cleaning.
		next := manifest(Ref{Owner: "v1", OwnerID: idFor("v1"), Chunk: target})
		rn := writeImage(t, map[string][]byte{}, next, map[uint64][]byte{5: []byte("e")})
		root, segs, _ := Build(next)
		_ = root
		for n, b := range segs {
			o.put(n.Path(), b, "successor")
		}
		_, rb, _ := NameRoot(root)
		o.put(rn.Path(), rb, "successor")
		h.Epoch, h.Holder, h.Activity, h.Current = 4, "srv-b", Activity{Kind: ActIdle}, &rn
	}
	if _, err := removeShard0(t, st, o, "c1", orphans); err != nil {
		t.Fatal(err)
	}
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if !inPool(o, target) {
		t.Fatalf("chunk %s, reused and committed by the successor after the remover's HEAD view, was removed", target)
	}
	if err := reachableMissing(h, o.data()); err != nil {
		t.Fatalf("the successor's HEAD reaches a missing object: %v", err)
	}
}

// A successor that acts before the remover's HEAD view (the remover paused
// right after BeginClean) leaves a HEAD that is no longer the remover's:
// the pass stops before any write.
func TestRemoveUnreachable_ASuccessorBeforeTheHeadViewStopsThePass(t *testing.T) {
	o, h, _, orphans := removeFixture(t)
	h.Epoch, h.Holder, h.Activity = 4, "srv-b", Activity{Kind: ActIdle}
	if _, err := removeShard0(t, &memHead{h: h}, o, "c1", orphans); !errors.Is(err, ErrNotHolder) || len(o.writes) != 0 {
		t.Fatalf("remover after a claim: %v, %d writes; want ErrNotHolder and none", err, len(o.writes))
	}
}

// A candidate an earlier session marked and never deleted (it died in
// between) is reclaimed by the next cleaning, and the earlier session's
// late delete, conditional on its own mark, fails once it is re-marked.
func TestRemoveUnreachable_ReclaimsAnAbandonedMark(t *testing.T) {
	ctx := context.Background()
	o, h, _, orphans := removeFixture(t)
	var p5 string
	for _, c := range orphans {
		if c.Index == 5 {
			p5 = path.Join(volRoot, c.Path())
			o.objs[p5].meta.Mark = "c0"
		}
	}
	removed, err := removeShard0(t, &memHead{h: h}, o, "c1", orphans)
	if err != nil || len(removed) != 2 {
		t.Fatalf("removed %v %v; want chunks 5 (abandoned mark) and 7", removed, err)
	}
	o2, _, _, orphans2 := removeFixture(t)
	var c5 ChunkName
	for _, c := range orphans2 {
		if c.Index == 5 {
			c5 = c
			o2.objs[path.Join(volRoot, c.Path())].meta.Mark = "c0"
		}
	}
	tag, _, _ := o2.Stat(ctx, c5.Path())
	if err := o2.Mark(ctx, c5.Path(), tag, "c1"); err != nil {
		t.Fatal(err)
	}
	if err := o2.DeleteMarked(ctx, c5.Path(), "c0"); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("the dead session's late delete after the re-mark: %v; want ErrPrecondition", err)
	}
}

// A root or segment HEAD names that cannot be read stops the pass before
// any write: nothing is removed on a partial view.
func TestRemoveUnreachable_NothingRemovedOnAPartialView(t *testing.T) {
	o, h, _, orphans := removeFixture(t)
	delete(o.objs, path.Join(volRoot, h.Snapshots["daily"].Root.Path()))
	if _, err := removeShard0(t, &memHead{h: h}, o, "c1", orphans); !errors.Is(err, ErrMissing) || len(o.writes) != 0 {
		t.Fatalf("unreadable snapshot root: %v, %d writes; want ErrMissing and none", err, len(o.writes))
	}
}

func (v *memVolume) fetch(_ context.Context, p string) ([]byte, bool, error) {
	if v.dead {
		return nil, false, errKilled
	}
	b, ok := v.objects[p]
	return b, ok, nil
}

// cleaningPass is the whole cleaning operation as the holder runs it after
// a (re)start: a cleaning of its own left open is ended first, then
// BeginClean, the removal of shard 0, EndClean.
func cleaningPass(v *memVolume, session string, cands []ChunkName) error {
	ctx := context.Background()
	h, found, err := v.ReadHead(ctx)
	if err != nil {
		return err
	}
	if !found {
		return ErrHeadAbsent
	}
	if h.Activity.Kind == ActClean && h.heldBy(me) {
		if _, err := RunEndClean(ctx, v, me, h.Activity.Session); err != nil {
			return err
		}
	}
	if _, err := RunBeginClean(ctx, v, me, session); err != nil {
		return err
	}
	r, err := NewReader(removeBase, "v1", v.fetch)
	if err != nil {
		return err
	}
	if _, err := RemoveUnreachable(ctx, v, r, v, me, session, 0, cands); err != nil {
		return err
	}
	_, err = RunEndClean(ctx, v, me, session)
	return err
}

// Interrupt at every step of the cleaning operation: killed at each of its
// writes (BeginClean, each mark, each conditional delete, EndClean), before
// the write applies and after it applied with its reply lost; at no point
// does HEAD reach a missing object, and the restarted holder (a new
// session) ends idle with every orphan of shard 0 removed (also one the
// killed session marked and did not delete) and every reachable object
// present.
func TestCleaningPass_InterruptAtEveryStep(t *testing.T) {
	fresh := func() (*memVolume, []ChunkName, []ChunkName) {
		o, h, live, orphans := removeFixture(t)
		h.Activity = Activity{Kind: ActIdle}
		v := &memVolume{head: &memHead{h: h}, objects: map[string][]byte{}, metas: map[string]ObjectMeta{}}
		for p, ob := range o.objs {
			v.objects[p] = ob.data
			v.metas[p] = ob.meta
		}
		return v, live, orphans
	}
	v, live, orphans := fresh()
	cands := append(append([]ChunkName(nil), live...), orphans...)
	if err := cleaningPass(v, "c1", cands); err != nil {
		t.Fatal(err)
	}
	steps := append([]string(nil), v.writes...)
	if len(steps) != 6 {
		t.Fatalf("the cleaning made %d writes: %v; want 6 (BeginClean, mark and delete twice, EndClean)", len(steps), steps)
	}
	for k := range steps {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("step%d %s/after=%t", k+1, steps[k], after), func(t *testing.T) {
				v, live, orphans := fresh()
				v.killAt, v.after = k+1, after
				if err := cleaningPass(v, "c1", cands); !errors.Is(err, errKilled) {
					t.Fatalf("killed run: %v", err)
				}
				if err := reachableMissing(v.head.h, v.objects); err != nil {
					t.Fatalf("after the kill HEAD reaches a missing object: %v", err)
				}
				v.dead, v.killAt = false, 0
				v.head.restart()
				if err := cleaningPass(v, "c2", cands); err != nil {
					t.Fatalf("re-run: %v", err)
				}
				if v.head.h.Activity.Kind != ActIdle {
					t.Fatalf("activity after the re-run: %+v", v.head.h.Activity)
				}
				for _, c := range orphans {
					p := path.Join(volRoot, c.Path())
					_, present := v.objects[p]
					if c.Index != 10001 && present {
						t.Fatalf("orphan %s still present after the re-run (mark %q)", c, v.meta(p).Mark)
					}
				}
				for _, c := range live {
					if _, ok := v.objects[path.Join(volRoot, c.Path())]; !ok {
						t.Fatalf("reachable %s removed", c)
					}
				}
				if err := reachableMissing(v.head.h, v.objects); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

// The remover tells its own chunks by its VOLUME id: without a readable
// VOLUME it removes nothing.
func TestRemoveUnreachable_NeedsItsOwnVolumeID(t *testing.T) {
	o, h, _, orphans := removeFixture(t)
	delete(o.objs, path.Join(volRoot, VolumeName))
	if _, err := removeShard0(t, &memHead{h: h}, o, "c1", orphans); !errors.Is(err, ErrMissing) || len(o.writes) != 0 {
		t.Fatalf("removal without VOLUME: %v, %d writes; want ErrMissing and none", err, len(o.writes))
	}
}
