package layout

import (
	"context"
	"errors"
	"fmt"
	"path"
	"testing"
)

// memVolume is one volume's store: HEAD (memHead) and its objects, with
// one kill point counted over every write of either kind.
type memVolume struct {
	head    *memHead
	objects map[string][]byte
	metas   map[string]ObjectMeta
	writes  []string // every write, in order
	killAt  int
	after   bool
	dead    bool
}

func (v *memVolume) write(what string) (apply, die bool, err error) {
	if v.dead {
		return false, false, errKilled
	}
	v.writes = append(v.writes, what)
	if len(v.writes) == v.killAt {
		v.dead = true
		return v.after, true, nil
	}
	return true, false, nil
}

func (v *memVolume) ReadHead(ctx context.Context) (Head, bool, error) {
	if v.dead {
		return Head{}, false, errKilled
	}
	return v.head.ReadHead(ctx)
}

func (v *memVolume) UpdateHead(ctx context.Context, gen uint64, next Head) error {
	apply, die, err := v.write("update HEAD")
	if err != nil {
		return err
	}
	if apply {
		if err := v.head.UpdateHead(ctx, gen, next); err != nil {
			return err
		}
	}
	if die {
		return errKilled
	}
	return nil
}

// memVolume is also the volume's ObjectStore: every object write counts
// toward the same kill point as HEAD's. An object a fixture stored without
// metadata reads as written by session "s0".
func (v *memVolume) meta(p string) ObjectMeta {
	if m, ok := v.metas[p]; ok {
		return m
	}
	return ObjectMeta{Session: "s0"}
}

func (v *memVolume) Stat(_ context.Context, rel string) (ObjectMeta, bool, error) {
	if v.dead {
		return ObjectMeta{}, false, errKilled
	}
	p := path.Join(volRoot, rel)
	if _, ok := v.objects[p]; !ok {
		return ObjectMeta{}, false, nil
	}
	return v.meta(p), true, nil
}

// objWrite applies f under the shared kill point.
func (v *memVolume) objWrite(what string, f func() error) error {
	apply, die, err := v.write(what)
	if err != nil {
		return err
	}
	if apply {
		err = f()
	}
	if die {
		return errKilled
	}
	return err
}

func (v *memVolume) Create(_ context.Context, rel string, data []byte, session string) error {
	p := path.Join(volRoot, rel)
	return v.objWrite("create "+rel, func() error {
		if _, ok := v.objects[p]; ok {
			return ErrExists
		}
		v.objects[p] = append([]byte(nil), data...)
		v.metas[p] = ObjectMeta{Session: session}
		return nil
	})
}

func (v *memVolume) Reuse(_ context.Context, rel string, expect ObjectMeta, session string) error {
	p := path.Join(volRoot, rel)
	return v.objWrite("reuse "+rel, func() error {
		if _, ok := v.objects[p]; !ok || v.meta(p) != expect {
			return ErrPrecondition
		}
		v.metas[p] = ObjectMeta{Session: session}
		return nil
	})
}

func (v *memVolume) Mark(_ context.Context, rel string, expect ObjectMeta, mark string) error {
	p := path.Join(volRoot, rel)
	return v.objWrite("mark "+rel, func() error {
		if _, ok := v.objects[p]; !ok || v.meta(p) != expect {
			return ErrPrecondition
		}
		m := v.meta(p)
		m.Mark = mark
		v.metas[p] = m
		return nil
	})
}

func (v *memVolume) DeleteMarked(_ context.Context, rel string, mark string) error {
	p := path.Join(volRoot, rel)
	return v.objWrite("delete "+rel, func() error {
		if _, ok := v.objects[p]; !ok {
			return nil
		}
		if v.meta(p).Mark != mark {
			return ErrPrecondition
		}
		delete(v.objects, p)
		delete(v.metas, p)
		return nil
	})
}

// snapshotSession is the whole snapshot transaction as the holder runs it
// after a (re)start: an open session of its own at HEAD's epoch is rolled
// back first (memory is empty after a crash), then Begin, write every new
// object (chunks, segments, root; present ones skipped), and Commit.
func snapshotSession(v *memVolume, session string, data map[uint64][]byte, req CommitRequest) error {
	ctx := context.Background()
	h, found, err := v.ReadHead(ctx)
	if err != nil {
		return err
	}
	if !found {
		return ErrHeadAbsent
	}
	if h.Activity.Kind == ActSnap && h.heldBy(me) {
		if _, err := RunAbort(ctx, v, me, h.Activity.Session); err != nil {
			return err
		}
	}
	if _, err := RunBegin(ctx, v, me, session); err != nil {
		return err
	}
	// What is new is recomputed against HEAD's current root, never kept
	// from before the crash (a commit may have applied with its reply lost).
	switch cur := h.Current; {
	case cur != nil && *cur == mustRoot(req.Next):
		req.Prev = req.Next
	case cur != nil && *cur != mustRoot(req.Prev):
		return fmt.Errorf("unexpected current root %s", cur)
	}
	for i, r := range newOwn("v1", req.Prev, req.Next) {
		if err := EnsureObject(ctx, v, r.Chunk.Path(), data[i], session); err != nil {
			return err
		}
	}
	root, segs, err := Build(req.Next)
	if err != nil {
		return err
	}
	for n, b := range segs {
		if err := EnsureObject(ctx, v, n.Path(), b, session); err != nil {
			return err
		}
	}
	rn, rb, err := NameRoot(root)
	if err != nil {
		return err
	}
	if err := EnsureObject(ctx, v, rn.Path(), rb, session); err != nil {
		return err
	}
	req.Session = session
	_, err = RunCommit(ctx, v, v, "v1", me, req)
	return err
}

func mustRoot(m map[uint64]Ref) RootName {
	rn, _, err := rootOf(m)
	if err != nil {
		panic(err)
	}
	return rn
}

// sessionFixture: HEAD idle at the root of prev (chunks 0 and 3, written);
// the session commits next (chunk 3 changed, chunk 10004 added, a foreign
// chunk named).
func sessionFixture(t *testing.T) (func() *memVolume, map[uint64][]byte, CommitRequest) {
	t.Helper()
	data := map[uint64][]byte{0: []byte("a"), 3: []byte("b2"), 10004: []byte("c")}
	prev := manifest(ref("v1", 0, "a"), ref("v1", 3, "b"))
	next := manifest(ref("v1", 0, "a"), ref("v1", 3, "b2"), ref("v1", 10004, "c"), ref("src", 7, "d"))
	prevData := map[uint64][]byte{0: []byte("a"), 3: []byte("b")}
	fresh := func() *memVolume {
		objs := map[string][]byte{}
		prevRoot := writeImage(t, objs, prev, prevData)
		h := Head{Gen: 7, Epoch: 3, Holder: "srv-a", Activity: Activity{Kind: ActIdle}, Current: &prevRoot, Position: 40,
			Snapshots: map[string]Snapshot{"daily": {Root: prevRoot}}}
		return &memVolume{head: &memHead{h: &h}, objects: objs, metas: map[string]ObjectMeta{}}
	}
	return fresh, data, CommitRequest{Prev: prev, Next: next, Position: 50, Snapshot: "hourly"}
}

// requireCommitted: HEAD idle at next's root with the snapshot, and every
// object next's root reaches in this volume exists, its bytes as written.
func requireCommitted(t *testing.T, v *memVolume, data map[uint64][]byte, req CommitRequest) {
	t.Helper()
	h := v.head.h
	root, segs, _ := Build(req.Next)
	rn, _, _ := NameRoot(root)
	if h.Activity.Kind != ActIdle || h.Current == nil || *h.Current != rn || h.Position != 50 || h.Snapshots["hourly"].Root != rn {
		t.Fatalf("HEAD after the session: %+v", h)
	}
	for i, r := range req.Next {
		if r.Owner != "v1" {
			continue
		}
		b, ok := v.objects[path.Join(volRoot, r.Chunk.Path())]
		if !ok {
			t.Fatalf("chunk %d the committed root names is missing", i)
		}
		if _, changed := newOwn("v1", req.Prev, req.Next)[i]; changed && string(b) != string(data[i]) {
			t.Fatalf("chunk %d holds %q; want %q", i, b, data[i])
		}
	}
	for n := range segs {
		if _, ok := v.objects[path.Join(volRoot, n.Path())]; !ok {
			t.Fatalf("segment %s the committed root names is missing", n)
		}
	}
	if _, ok := v.objects[path.Join(volRoot, rn.Path())]; !ok {
		t.Fatal("the committed root is missing")
	}
	if err := reachableMissing(h, v.objects); err != nil {
		t.Fatalf("after the session HEAD reaches a missing object: %v", err)
	}
}

// Interrupt at every step of the snapshot transaction: killed at each of
// its writes (Begin's HEAD update, every object create, Commit's HEAD
// update), before the write applies and after it applied with its reply
// lost; the restarted holder rolls back what it finds open, begins a new
// session, reuses written objects by name and commits. The end state is
// the committed image with every object it reaches present; at no point
// does HEAD name an object that is missing.
func TestSnapshotSession_InterruptAtEveryStep(t *testing.T) {
	fresh, data, req := sessionFixture(t)
	count := fresh()
	if err := snapshotSession(count, "s1", data, req); err != nil {
		t.Fatal(err)
	}
	requireCommitted(t, count, data, req)
	steps := append([]string(nil), count.writes...)
	if len(steps) < 5 {
		t.Fatalf("the session made %d writes: %v", len(steps), steps)
	}
	t.Logf("%d step boundaries: %v", len(steps), steps)
	for k := range steps {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("step%d %s/after=%t", k+1, steps[k], after), func(t *testing.T) {
				v := fresh()
				v.killAt, v.after = k+1, after
				if err := snapshotSession(v, "s1", data, req); !errors.Is(err, errKilled) {
					t.Fatalf("killed run: %v", err)
				}
				requireReachablePresent(t, v)
				v.dead, v.killAt = false, 0
				if err := snapshotSession(v, "s2", data, req); err != nil {
					t.Fatalf("re-run after a kill at step %d: %v", k+1, err)
				}
				requireCommitted(t, v, data, req)
			})
		}
	}
}

// writeImage stores a manifest's own chunks (with their bytes), its
// segments and its root, and returns the root's name.
func writeImage(t *testing.T, objs map[string][]byte, m map[uint64]Ref, data map[uint64][]byte) RootName {
	t.Helper()
	root, segs, err := Build(m)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range m {
		if r.Owner == "v1" {
			objs[path.Join(volRoot, r.Chunk.Path())] = data[i]
		}
	}
	for n, b := range segs {
		objs[path.Join(volRoot, n.Path())] = b
	}
	rn, rb, err := NameRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	objs[path.Join(volRoot, rn.Path())] = rb
	return rn
}

// reachableMissing walks every root HEAD names (current and every
// snapshot) through its segments to this volume's chunks, reading the
// stored bytes, and reports the first object that is missing or does not
// hash to its name.
func reachableMissing(h *Head, objs map[string][]byte) error {
	roots := map[RootName]bool{}
	if h.Current != nil {
		roots[*h.Current] = true
	}
	for _, s := range h.Snapshots {
		roots[s.Root] = true
	}
	get := func(rel string, want Hash) ([]byte, error) {
		b, ok := objs[path.Join(volRoot, rel)]
		if !ok {
			return nil, fmt.Errorf("%s is missing", rel)
		}
		if Sum(b) != want {
			return nil, fmt.Errorf("%s does not hash to its name", rel)
		}
		return b, nil
	}
	for rn := range roots {
		rb, err := get(rn.Path(), rn.Hash)
		if err != nil {
			return err
		}
		root, err := DecodeRoot(rb)
		if err != nil {
			return err
		}
		for _, sn := range root.Segments {
			sb, err := get(sn.Path(), sn.Hash)
			if err != nil {
				return err
			}
			seg, err := DecodeSegment(sb)
			if err != nil {
				return err
			}
			for _, r := range seg.Chunks {
				if r.Owner != "v1" {
					continue // the source's pool holds it
				}
				if _, err := get(r.Chunk.Path(), r.Chunk.Hash); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// requireReachablePresent: every object any root in HEAD reaches in this
// volume exists with its bytes (checked after a kill, before the restart).
func requireReachablePresent(t *testing.T, v *memVolume) {
	t.Helper()
	if err := reachableMissing(v.head.h, v.objects); err != nil {
		t.Fatalf("after the kill HEAD reaches a missing object: %v", err)
	}
}

// The reachability observer's negative controls: a reachable own chunk,
// segment or snapshot root deleted (the current root kept) is reported.
func TestReachableMissing_Controls(t *testing.T) {
	fresh, _, req := sessionFixture(t)
	if err := reachableMissing(fresh().head.h, fresh().objects); err != nil {
		t.Fatalf("control: the fixture itself: %v", err)
	}
	_, segs, _ := Build(req.Prev)
	var seg0 SegName
	for n := range segs {
		seg0 = n
	}
	for name, rel := range map[string]string{
		"own chunk": req.Prev[3].Chunk.Path(),
		"segment":   seg0.Path(),
	} {
		v := fresh()
		delete(v.objects, path.Join(volRoot, rel))
		if err := reachableMissing(v.head.h, v.objects); err == nil {
			t.Errorf("%s deleted: not reported", name)
		}
	}
	v := fresh()
	other := RootName{Hash: Sum([]byte("missing root"))}
	v.head.h.Snapshots["weekly"] = Snapshot{Root: other}
	if err := reachableMissing(v.head.h, v.objects); err == nil {
		t.Error("a snapshot root that is missing: not reported")
	}
	v = fresh()
	v.objects[path.Join(volRoot, req.Prev[0].Chunk.Path())] = []byte("tampered")
	if err := reachableMissing(v.head.h, v.objects); err == nil {
		t.Error("a chunk whose bytes do not match its name: not reported")
	}
}
