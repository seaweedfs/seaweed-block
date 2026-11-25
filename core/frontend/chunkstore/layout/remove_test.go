package layout

import (
	"context"
	"errors"
	"fmt"
	"path"
	"testing"
)

const removeBase = "/swblock/chunks" // volRoot = removeBase/v1

// objPool is a volume's objects for removal tests: Fetch reads them, Pool
// deletes them, with an optional kill point counted over deletes.
type objPool struct {
	objs    map[string][]byte
	deletes []string
	killAt  int
	after   bool
	dead    bool
}

func (o *objPool) fetch(_ context.Context, p string) ([]byte, bool, error) {
	if o.dead {
		return nil, false, errKilled
	}
	b, ok := o.objs[p]
	return b, ok, nil
}

func (o *objPool) DeleteObject(_ context.Context, rel string) error {
	if o.dead {
		return errKilled
	}
	o.deletes = append(o.deletes, rel)
	kill := len(o.deletes) == o.killAt
	if kill && !o.after {
		o.dead = true
		return errKilled
	}
	delete(o.objs, path.Join(volRoot, rel))
	if kill {
		o.dead = true
		return errKilled
	}
	return nil
}

type fakeHeld struct {
	calls  int
	lostAt int // the n-th call (1-based) and later fail; 0 = never
}

func (l *fakeHeld) StillHeld(context.Context) error {
	l.calls++
	if l.lostAt > 0 && l.calls >= l.lostAt {
		return errors.New("lease lapsed")
	}
	return nil
}

// removeFixture: HEAD clean(c1) at epoch 3; the current root names chunks
// 0 and 3, the snapshot "daily" an older image naming chunks 0 and 2 (all
// in shard 0); chunks 5 and 7 are in the pool but no root names them, and
// chunk 10001 (shard 1) is unreferenced too.
func removeFixture(t *testing.T) (*objPool, *Head, []ChunkName, []ChunkName) {
	t.Helper()
	objs := map[string][]byte{}
	vb, err := Volume{ID: idFor("v1"), Name: "v1", Format: VolumeFormat}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	objs[path.Join(volRoot, VolumeName)] = vb
	cur := manifest(ref("v1", 0, "a"), ref("v1", 3, "b"))
	old := manifest(ref("v1", 0, "a"), ref("v1", 2, "c"))
	curRoot := writeImage(t, objs, cur, map[uint64][]byte{0: []byte("a"), 3: []byte("b")})
	oldRoot := writeImage(t, objs, old, map[uint64][]byte{0: []byte("a"), 2: []byte("c")})
	var orphans []ChunkName
	for i, d := range map[uint64]string{5: "e", 7: "f", 10001: "g"} {
		c := NameChunk(i, []byte(d))
		objs[path.Join(volRoot, c.Path())] = []byte(d)
		orphans = append(orphans, c)
	}
	h := &Head{Gen: 7, Epoch: 3, Holder: "srv-a", ClaimID: "k-a", VolumeID: idFor("v1"), Activity: Activity{Kind: ActClean, Session: "c1"}, Current: &curRoot,
		Snapshots: map[string]Snapshot{"daily": {Root: oldRoot}}}
	live := []ChunkName{cur[0].Chunk, cur[3].Chunk, old[2].Chunk}
	return &objPool{objs: objs}, h, live, orphans
}

func removeShard0(t *testing.T, st HeadStore, o *objPool, l LeaseHeld, session string, cands []ChunkName) ([]ChunkName, error) {
	t.Helper()
	r, err := NewReader(removeBase, "v1", o.fetch)
	if err != nil {
		t.Fatal(err)
	}
	return RemoveUnreachable(context.Background(), st, r, o, l, me, session, 0, cands)
}

func inPool(o *objPool, c ChunkName) bool { _, ok := o.objs[path.Join(volRoot, c.Path())]; return ok }

// Only names no root in HEAD reaches are removed: the current root's and
// a snapshot's chunks stay, shard 0's orphans go, the shard-1 orphan is
// not touched by a shard-0 pass; the lease is checked before every delete.
func TestRemoveUnreachable_RemovesOnlyWhatNoRootNames(t *testing.T) {
	o, h, live, orphans := removeFixture(t)
	l := &fakeHeld{}
	cands := append(append([]ChunkName(nil), live...), orphans...)
	removed, err := removeShard0(t, &memHead{h: h}, o, l, "c1", cands)
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
	if l.calls != len(o.deletes) {
		t.Fatalf("lease checks %d for %d deletes; want one before each", l.calls, len(o.deletes))
	}
	if err := reachableMissing(h, o.objs); err != nil {
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
		if _, err := removeShard0(t, &memHead{h: h}, o, &fakeHeld{}, "c1", orphans); !errors.Is(err, tc.want) || len(o.deletes) != 0 {
			t.Errorf("%s: %v, %d deletes; want %v and none", name, err, len(o.deletes), tc.want)
		}
	}
	o, _, _, orphans := removeFixture(t)
	if _, err := removeShard0(t, &memHead{}, o, &fakeHeld{}, "c1", orphans); !errors.Is(err, ErrHeadAbsent) || len(o.deletes) != 0 {
		t.Errorf("absent HEAD: %v, %d deletes", err, len(o.deletes))
	}
}

// A lease that lapses mid-pass stops the removal before the next delete.
func TestRemoveUnreachable_StopsWhenTheLeaseLapses(t *testing.T) {
	o, h, _, orphans := removeFixture(t)
	removed, err := removeShard0(t, &memHead{h: h}, o, &fakeHeld{lostAt: 2}, "c1", orphans)
	if !errors.Is(err, ErrLeaseLost) || len(removed) != 1 || len(o.deletes) != 1 {
		t.Fatalf("lease lapsing before the second delete: removed %v, %v; want one delete then ErrLeaseLost", removed, err)
	}
}

// A root or segment HEAD names that cannot be read stops the pass before
// any delete: nothing is removed on a partial view.
func TestRemoveUnreachable_NothingRemovedOnAPartialView(t *testing.T) {
	o, h, _, orphans := removeFixture(t)
	delete(o.objs, path.Join(volRoot, h.Snapshots["daily"].Root.Path()))
	if _, err := removeShard0(t, &memHead{h: h}, o, &fakeHeld{}, "c1", orphans); !errors.Is(err, ErrMissing) || len(o.deletes) != 0 {
		t.Fatalf("unreadable snapshot root: %v, %d deletes; want ErrMissing and none", err, len(o.deletes))
	}
}

// cleaningPass is the whole cleaning operation as the holder runs it after
// a (re)start: a cleaning of its own left open is ended first, then
// BeginClean, the removal of shard 0, EndClean.
func cleaningPass(st *memVolume, o *objPool, session string, cands []ChunkName) error {
	ctx := context.Background()
	h, found, err := st.ReadHead(ctx)
	if err != nil {
		return err
	}
	if !found {
		return ErrHeadAbsent
	}
	if h.Activity.Kind == ActClean && h.heldBy(me) {
		if _, err := RunEndClean(ctx, st, me, h.Activity.Session); err != nil {
			return err
		}
	}
	if _, err := RunBeginClean(ctx, st, me, session); err != nil {
		return err
	}
	r, err := NewReader(removeBase, "v1", o.fetch)
	if err != nil {
		return err
	}
	if _, err := RemoveUnreachable(ctx, st, r, o, &fakeHeld{}, me, session, 0, cands); err != nil {
		return err
	}
	_, err = RunEndClean(ctx, st, me, session)
	return err
}

// Interrupt at every step of the cleaning operation: killed at each of its
// writes (BeginClean, every delete, EndClean), before the write applies
// and after it applied with its reply lost; at no point does HEAD reach a
// missing object, and the restarted holder ends idle with every orphan of
// shard 0 removed and every reachable object present.
func TestCleaningPass_InterruptAtEveryStep(t *testing.T) {
	fresh := func() (*memVolume, *objPool, []ChunkName, []ChunkName) {
		o, h, live, orphans := removeFixture(t)
		h.Activity = Activity{Kind: ActIdle}
		return &memVolume{head: &memHead{h: h}, objects: o.objs}, o, live, orphans
	}
	v, o, live, orphans := fresh()
	cands := append(append([]ChunkName(nil), live...), orphans...)
	if err := cleaningPass(v, o, "c1", cands); err != nil {
		t.Fatal(err)
	}
	steps := len(v.writes) + len(o.deletes)
	if steps != 4 {
		t.Fatalf("the cleaning made %d writes; want 4 (BeginClean, two deletes, EndClean)", steps)
	}
	// Kill points: HEAD writes are counted by memVolume, deletes by objPool;
	// step k is the k-th write of either kind in run order.
	order := []string{"head", "delete", "delete", "head"}
	for k := range order {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("step%d %s/after=%t", k+1, order[k], after), func(t *testing.T) {
				v, o, live, orphans := fresh()
				nth := 0
				for _, kind := range order[:k+1] {
					if kind == order[k] {
						nth++
					}
				}
				if order[k] == "head" {
					v.killAt, v.after = nth, after
				} else {
					o.killAt, o.after = nth, after
				}
				if err := cleaningPass(v, o, "c1", cands); !errors.Is(err, errKilled) {
					t.Fatalf("killed run: %v", err)
				}
				if err := reachableMissing(v.head.h, o.objs); err != nil {
					t.Fatalf("after the kill HEAD reaches a missing object: %v", err)
				}
				v.dead, v.killAt, o.dead, o.killAt = false, 0, false, 0
				if err := cleaningPass(v, o, "c2", cands); err != nil {
					t.Fatalf("re-run: %v", err)
				}
				if v.head.h.Activity.Kind != ActIdle {
					t.Fatalf("activity after the re-run: %+v", v.head.h.Activity)
				}
				for _, c := range orphans {
					if c.Index != 10001 && inPool(o, c) {
						t.Fatalf("orphan %s still present", c)
					}
				}
				for _, c := range live {
					if !inPool(o, c) {
						t.Fatalf("reachable %s removed", c)
					}
				}
				if err := reachableMissing(v.head.h, o.objs); err != nil {
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
	if _, err := removeShard0(t, &memHead{h: h}, o, &fakeHeld{}, "c1", orphans); !errors.Is(err, ErrMissing) || len(o.deletes) != 0 {
		t.Fatalf("removal without VOLUME: %v, %d deletes; want ErrMissing and none", err, len(o.deletes))
	}
}
