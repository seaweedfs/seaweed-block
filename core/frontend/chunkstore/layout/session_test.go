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

func (v *memVolume) look(_ context.Context, dir, name string) (bool, error) {
	if v.dead {
		return false, errKilled
	}
	_, ok := v.objects[path.Join(dir, name)]
	return ok, nil
}

// put writes an object if absent (a content name that exists is complete,
// so a present one is skipped).
func (v *memVolume) put(rel string, b []byte) error {
	p := path.Join(volRoot, rel)
	if v.dead {
		return errKilled
	}
	if _, ok := v.objects[p]; ok {
		return nil
	}
	apply, die, err := v.write("create " + rel)
	if err != nil {
		return err
	}
	if apply {
		v.objects[p] = append([]byte(nil), b...)
	}
	if die {
		return errKilled
	}
	return nil
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
		if err := v.put(r.Chunk.Path(), data[i]); err != nil {
			return err
		}
	}
	root, segs, err := Build(req.Next)
	if err != nil {
		return err
	}
	for n, b := range segs {
		if err := v.put(n.Path(), b); err != nil {
			return err
		}
	}
	rn, rb, err := NameRoot(root)
	if err != nil {
		return err
	}
	if err := v.put(rn.Path(), rb); err != nil {
		return err
	}
	req.Session = session
	_, err = RunCommit(ctx, v, v.look, volRoot, "v1", me, req)
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
	fresh := func() *memVolume {
		f := &fakeStore{have: map[string]bool{}}
		prevRoot := prepared(t, f, prev)
		objs := map[string][]byte{}
		for p := range f.have {
			objs[p] = []byte("x")
		}
		h := Head{Gen: 7, Epoch: 3, Holder: "srv-a", Activity: Activity{Kind: ActIdle}, Current: &prevRoot, Position: 40}
		return &memVolume{head: &memHead{h: &h}, objects: objs}
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

// requireReachablePresent: every own object HEAD's current root reaches
// exists (checked after a kill, before the restart).
func requireReachablePresent(t *testing.T, v *memVolume) {
	t.Helper()
	h := v.head.h
	if h.Current == nil {
		return
	}
	if _, ok := v.objects[path.Join(volRoot, h.Current.Path())]; !ok {
		t.Fatalf("HEAD names root %s, which is missing", h.Current)
	}
}
