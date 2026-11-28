package layout

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"testing"
)

const volRoot = "/swblock/chunks/v1"

// prepared writes (in the object store, as session) every object a commit
// of next needs: next's own chunks, its segments and its root.
func prepared(t *testing.T, f *memObjects, next map[uint64]Ref, session string) RootName {
	t.Helper()
	root, segs, err := Build(next)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range next {
		if r.Owner == "v1" {
			f.put(next[i].Chunk.Path(), []byte("c"), session)
		}
	}
	for n := range segs {
		f.put(n.Path(), []byte("s"), session)
	}
	rn, _, err := NameRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	f.put(rn.Path(), []byte("r"), session)
	return rn
}

func manifest(entries ...Ref) map[uint64]Ref {
	m := map[uint64]Ref{}
	for _, r := range entries {
		m[r.Chunk.Index] = r
	}
	return m
}

func TestCommit_Closure(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("new"))}
	h := snapHead("s1")
	next, changed, err := h.Commit(me, "s1", rn, 50, "hourly", SnapshotMeta{}, StateLive)
	if err != nil || !changed || next.Activity.Kind != ActIdle || *next.Current != rn || next.Position != 50 || next.Snapshots["hourly"].Root != rn {
		t.Fatalf("Commit: %+v %t %v", next, changed, err)
	}
	if next.Snapshots["daily"].Dependents[0].Target != "t1" || len(next.Snapshots) != 2 {
		t.Fatalf("Commit changed other snapshots: %+v", next.Snapshots)
	}
	if h.Activity.Kind != ActSnap || h.Snapshots["hourly"].Root == rn {
		t.Fatal("Commit changed its receiver")
	}
	if again, changed, err := next.Commit(me, "s1", rn, 50, "hourly", SnapshotMeta{}, StateLive); err != nil || changed || again.Gen != next.Gen {
		t.Fatalf("Commit over its applied HEAD: %+v %t %v; want a replay", again, changed, err)
	}
	if n2, changed, err := h.Commit(me, "s1", rn, 50, "", SnapshotMeta{}, StateLive); err != nil || !changed || len(n2.Snapshots) != 1 {
		t.Fatalf("Commit without a snapshot name: %+v %t %v", n2, changed, err)
	}
	full := snapHead("s1")
	for i := len(full.Snapshots); i < MaxSnapshots; i++ {
		full.Snapshots[fmt.Sprintf("n%03d", i)] = Snapshot{Root: rn}
	}
	for name, tc := range map[string]struct {
		h        Head
		me       HolderID
		snapshot string
		want     error
	}{
		"idle":           {idleHead(), me, "", ErrNoSession},
		"other session":  {snapHead("s2"), me, "", ErrNoSession},
		"cleaning":       {Head{Epoch: 3, Holder: "srv-a", ClaimID: "k-a", VolumeID: idFor("v1"), Activity: Activity{Kind: ActClean, Session: "s1"}}, me, "", ErrNoSession},
		"other epoch":    {h, HolderID{Epoch: 2, Holder: "srv-a"}, "", ErrNotHolder},
		"other holder":   {h, HolderID{Epoch: 3, Holder: "srv-b"}, "", ErrNotHolder},
		"name taken":     {h, me, "daily", ErrSnapshotExists},
		"snapshot limit": {full, me, "one-more", ErrTooManySnapshots},
	} {
		if _, _, err := tc.h.Commit(tc.me, "s1", rn, 50, tc.snapshot, SnapshotMeta{}, StateLive); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v; want %v", name, err, tc.want)
		}
	}
}

// commitFixture: HEAD is snap(s1) at current root = prev's root; prev and
// next share chunk 0 (unchanged), next changes chunk 3, adds chunk 10004
// and names chunk 7 of volume "src".
func commitFixture(t *testing.T) (*memObjects, Head, CommitRequest) {
	t.Helper()
	f := newMemObjects(volRoot)
	prev := manifest(ref("v1", 0, "a"), ref("v1", 3, "b"), ref("src", 7, "d")) // a restore target: it holds a source chunk
	prevRoot := prepared(t, f, prev, "s0")
	next := manifest(ref("v1", 0, "a"), ref("v1", 3, "b2"), ref("v1", 10004, "c"), ref("src", 7, "d"))
	prepared(t, f, next, "s1")
	h := Head{Gen: 7, Epoch: 3, Holder: "srv-a", ClaimID: "k-a", VolumeID: idFor("v1"), Activity: Activity{Kind: ActSnap, Session: "s1"}, Current: &prevRoot, Position: 40}
	return f, h, CommitRequest{Session: "s1", Prev: prev, Next: next, Position: 50, Snapshot: "hourly"}
}

func runCommit(st HeadStore, f *memObjects, req CommitRequest) (Head, error) {
	return RunCommit(context.Background(), st, f, "v1", me, req)
}

func TestRunCommit_LooksUpOnlyWhatIsNew(t *testing.T) {
	f, h, req := commitFixture(t)
	st := &memHead{h: &h}
	f.asked = nil
	got, err := runCommit(st, f, req)
	if err != nil || got.Activity.Kind != ActIdle || got.Position != 50 || st.h.Gen != 8 {
		t.Fatalf("RunCommit: %+v %v", got, err)
	}
	wantRoot, _, _ := rootOf(req.Next)
	if *st.h.Current != wantRoot || st.h.Snapshots["hourly"].Root != wantRoot {
		t.Fatalf("HEAD current %v snapshot %v; want %v", st.h.Current, st.h.Snapshots["hourly"], wantRoot)
	}
	var pool []string
	for _, p := range f.asked {
		if strings.Contains(p, "/"+PoolDir+"/") {
			pool = append(pool, path.Base(p))
		}
	}
	want := []string{req.Next[3].Chunk.String(), req.Next[10004].Chunk.String()}
	sort.Strings(pool) // the commit's lookups run concurrently: compare the set
	sort.Strings(want)
	if strings.Join(pool, ",") != strings.Join(want, ",") {
		t.Fatalf("pool lookups %v; want only the new own chunks %v (not the unchanged chunk 0, not the foreign chunk 7)", pool, want)
	}
}

func TestRunCommit_RefusesUnwrittenObjects(t *testing.T) {
	for _, what := range []string{"new chunk", "segment", "root"} {
		t.Run(what, func(t *testing.T) {
			f, h, req := commitFixture(t)
			root, segs, _ := rootOf(req.Next)
			var p string
			switch what {
			case "new chunk":
				p = req.Next[10004].Chunk.Path()
			case "segment":
				for n := range segs {
					if n.Shard == 1 {
						p = n.Path()
					}
				}
			case "root":
				p = root.Path()
			}
			delete(f.objs, path.Join(volRoot, p))
			st := &memHead{h: &h}
			if _, err := runCommit(st, f, req); !errors.Is(err, ErrNotPrepared) {
				t.Fatalf("commit with its %s missing: %v; want ErrNotPrepared", what, err)
			}
			if st.updates != 0 || st.h.Activity.Kind != ActSnap {
				t.Fatalf("HEAD written by a refused commit: %+v", st.h)
			}
		})
	}
}

func TestRunCommit_StaleBaseAndLookupFailure(t *testing.T) {
	f, h, req := commitFixture(t)
	stale := req
	stale.Prev = manifest(ref("v1", 0, "a")) // not HEAD's current root
	st := &memHead{h: &h}
	if _, err := runCommit(st, f, stale); !errors.Is(err, ErrStaleBase) {
		t.Fatalf("commit against another base: %v; want ErrStaleBase", err)
	}
	empty := req
	empty.Prev = nil // HEAD has a current root, so "nothing before" is stale too
	if _, err := runCommit(st, f, empty); !errors.Is(err, ErrStaleBase) {
		t.Fatalf("commit with no base over a current root: %v; want ErrStaleBase", err)
	}
	f.fail = path.Join(volRoot, req.Next[3].Chunk.Path())
	if _, err := runCommit(st, f, req); err == nil || errors.Is(err, ErrNotPrepared) {
		t.Fatalf("commit with a failing lookup: %v; want an error that is not a refusal", err)
	}
	if st.updates != 0 {
		t.Fatal("HEAD written by a refused commit")
	}
	// The first commit of a volume: no current root, no previous manifest.
	f2 := newMemObjects(volRoot)
	next := manifest(ref("v1", 1, "x"))
	prepared(t, f2, next, "s1")
	h2 := Head{Gen: 1, Epoch: 3, Holder: "srv-a", ClaimID: "k-a", VolumeID: idFor("v1"), Activity: Activity{Kind: ActSnap, Session: "s1"}}
	if got, err := runCommit(&memHead{h: &h2}, f2, CommitRequest{Session: "s1", Next: next, Position: 5}); err != nil || got.Current == nil {
		t.Fatalf("first commit: %+v %v", got, err)
	}
}

func TestRunCommit_InterruptAtEveryStep(t *testing.T) {
	f, h, req := commitFixture(t)
	wantRoot, _, _ := rootOf(req.Next)
	interruptHead(t, h, func(st HeadStore) error {
		_, err := runCommit(st, f, req)
		return err
	}, func(t *testing.T, got Head) {
		if got.Activity.Kind != ActIdle || got.Current == nil || *got.Current != wantRoot || got.Position != 50 || got.Snapshots["hourly"].Root != wantRoot {
			t.Fatalf("HEAD after Commit: %+v", got)
		}
	})
}

// An empty image is a real root: after it is published, a commit whose
// previous manifest is empty builds on it (empty -> written, and empty ->
// empty); a previous manifest that is not HEAD's current root is still
// refused.
func TestRunCommit_AfterAnEmptyPublishedRoot(t *testing.T) {
	ctx := context.Background()
	f := newMemObjects(volRoot)
	prepared(t, f, nil, "s0")
	h := idleHead()
	st := &memHead{h: &h}
	if _, err := RunBegin(ctx, st, me, "s0"); err != nil {
		t.Fatal(err)
	}
	if _, err := runCommit(st, f, CommitRequest{Session: "s0", Snapshot: "empty"}); err != nil {
		t.Fatal(err)
	}
	if st.h.Current == nil {
		t.Fatal("the empty snapshot did not publish its root")
	}
	if _, err := RunBegin(ctx, st, me, "s1"); err != nil {
		t.Fatal(err)
	}
	prepared(t, f, nil, "s1") // each session writes (reuses) what it commits
	if _, err := runCommit(st, f, CommitRequest{Session: "s1", Prev: map[uint64]Ref{}, Position: 1}); err != nil {
		t.Fatalf("empty -> empty: %v", err)
	}
	next := manifest(ref("v1", 1, "x"))
	prepared(t, f, next, "s2")
	if _, err := RunBegin(ctx, st, me, "s2"); err != nil {
		t.Fatal(err)
	}
	stale := manifest(ref("v1", 9, "z"))
	if _, err := runCommit(st, f, CommitRequest{Session: "s2", Prev: stale, Next: next, Position: 2}); !errors.Is(err, ErrStaleBase) {
		t.Fatalf("a non-empty previous manifest over the empty root: %v; want ErrStaleBase", err)
	}
	if _, err := runCommit(st, f, CommitRequest{Session: "s2", Next: next, Position: 2, Snapshot: "written"}); err != nil {
		t.Fatalf("empty -> written: %v", err)
	}
	if want, _, _ := rootOf(next); *st.h.Current != want {
		t.Fatalf("HEAD current %s; want %s", st.h.Current, want)
	}
	// Before any commit there is no current root: a non-empty previous
	// manifest is stale there too.
	h0 := Head{Gen: 1, Epoch: 3, Holder: "srv-a", ClaimID: "k-a", VolumeID: idFor("v1"), Activity: Activity{Kind: ActSnap, Session: "s9"}}
	if _, err := runCommit(&memHead{h: &h0}, f, CommitRequest{Session: "s9", Prev: next, Next: next, Position: 3}); !errors.Is(err, ErrStaleBase) {
		t.Fatalf("a previous manifest before any commit: %v; want ErrStaleBase", err)
	}
}

// Abort after the session committed succeeds without a write and leaves
// the commit in place: its success says only that no activity remains.
func TestAbort_AfterCommitLeavesTheCommit(t *testing.T) {
	f, h, req := commitFixture(t)
	st := &memHead{h: &h}
	if _, err := runCommit(st, f, req); err != nil {
		t.Fatal(err)
	}
	committed := *st.h.Current
	before := st.updates
	if _, err := RunAbort(context.Background(), st, me, "s1"); err != nil {
		t.Fatalf("Abort after the commit: %v", err)
	}
	if st.updates != before || *st.h.Current != committed || st.h.Snapshots["hourly"].Root != committed {
		t.Fatalf("Abort after the commit wrote or changed HEAD: %+v", st.h)
	}
}

// A commit names only objects its own session wrote or reused, unmarked:
// one written by another session, or marked by a cleaning, is not
// prepared until EnsureObject reuses it.
func TestRunCommit_NeedsObjectsOfItsOwnSession(t *testing.T) {
	for _, meta := range []ObjectMeta{{Session: "s0"}, {Session: "s1", Mark: "c9"}} {
		f, h, req := commitFixture(t)
		rel := req.Next[10004].Chunk.Path()
		f.objs[path.Join(volRoot, rel)].meta = meta
		st := &memHead{h: &h}
		if _, err := runCommit(st, f, req); !errors.Is(err, ErrNotPrepared) || st.updates != 0 {
			t.Fatalf("object %+v: %v (%d writes); want ErrNotPrepared, nothing written", meta, err, st.updates)
		}
		if err := EnsureObject(context.Background(), f, rel, []byte("c"), "s1"); err != nil {
			t.Fatal(err)
		}
		if _, err := runCommit(st, f, req); err != nil {
			t.Fatalf("after EnsureObject reused it: %v", err)
		}
	}
}

// A restore target keeps or drops its source's references and never gains
// one: a commit whose manifest holds another volume's chunk at an index
// where the previous manifest did not hold that same chunk is refused, and
// HEAD is not written. Keeping one (the fixture) and dropping one commit.
func TestRunCommit_RefusesAnAddedForeignReference(t *testing.T) {
	for name, add := range map[string]func(next map[uint64]Ref){
		"at a fresh index":                func(next map[uint64]Ref) { next[20] = ref("src", 20, "e") },
		"another chunk at a held index":   func(next map[uint64]Ref) { next[7] = ref("src", 7, "d2") },
		"a third volume at a held index":  func(next map[uint64]Ref) { next[7] = ref("other", 7, "d") },
		"over an own chunk at that index": func(next map[uint64]Ref) { next[3] = ref("src", 3, "b") },
		"this name, another volume id": func(next map[uint64]Ref) {
			r := ref("v1", 20, "e")
			r.OwnerID = idFor("v1 re-created")
			next[20] = r
		},
	} {
		t.Run(name, func(t *testing.T) {
			f, h, req := commitFixture(t)
			add(req.Next)
			prepared(t, f, req.Next, "s1")
			st := &memHead{h: &h}
			if _, err := runCommit(st, f, req); !errors.Is(err, ErrForeignRefAdded) {
				t.Fatalf("commit adding a foreign reference %s: %v; want ErrForeignRefAdded", name, err)
			}
			if st.updates != 0 || st.h.Activity.Kind != ActSnap {
				t.Fatalf("HEAD written by a refused commit: %+v", st.h)
			}
		})
	}
	t.Run("dropping a foreign reference", func(t *testing.T) {
		f, h, req := commitFixture(t)
		delete(req.Next, 7)
		prepared(t, f, req.Next, "s1")
		st := &memHead{h: &h}
		if _, err := runCommit(st, f, req); err != nil {
			t.Fatalf("commit dropping a source reference: %v", err)
		}
	})
}
