package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"path"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// cleanFixture is v with chunk 1 live in the current root and in snapshot
// "s", plus orphans: chunk 2 in shard 0 and chunk ShardSize+3 in shard 1.
func cleanFixture(t *testing.T) (*BCatalog, VolumeRef, *LayoutStore, []layout.ChunkName) {
	t.Helper()
	ctx := context.Background()
	c, err := NewBCatalog(NewMemFiler(), newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	v := newRef(t, "v1")
	if _, err := c.CreateVolume(ctx, v); err != nil {
		t.Fatal(err)
	}
	commitSnapshot(t, c, v, bytes.Repeat([]byte{7}, 4096), "s")
	s, err := c.Store(v)
	if err != nil {
		t.Fatal(err)
	}
	var orphans []layout.ChunkName
	for _, idx := range []uint64{2, layout.ShardSize + 3} {
		data := bytes.Repeat([]byte{byte(idx)}, 4096)
		n := layout.NameChunk(idx, data)
		if err := layout.EnsureObject(ctx, s, n.Path(), data, "dead-session"); err != nil {
			t.Fatal(err)
		}
		orphans = append(orphans, n)
	}
	// A real filer lists the shard directories it created for these
	// entries; the in-memory filer keeps only the entries, so add them.
	pool := path.Join(c.base, v.Dir(), layout.PoolDir)
	for _, d := range []string{"0", "1"} {
		if err := c.f.CreateEntry(ctx, pool, &filerpb.Entry{Name: d, IsDirectory: true}, false); err != nil {
			t.Fatal(err)
		}
	}
	return c, v, s, orphans
}

func present(t *testing.T, s *LayoutStore, n layout.ChunkName) bool {
	t.Helper()
	_, found, err := s.Stat(context.Background(), n.Path())
	if err != nil {
		t.Fatal(err)
	}
	return found
}

var cleanHolder = layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}

// The holder's pass removes every orphan in every shard, keeps what the
// current root and the snapshot name, and leaves HEAD idle. With a page of
// one name the shard cursor is exercised too.
func TestBCatalog_CleanRemovesOnlyUnreachable(t *testing.T) {
	ctx := context.Background()
	prev := cleanPage
	cleanPage = 1
	t.Cleanup(func() { cleanPage = prev })
	c, v, s, orphans := cleanFixture(t)
	live := layout.NameChunk(1, bytes.Repeat([]byte{7}, 4096))

	res, err := c.Clean(ctx, v, cleanHolder, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Shards != 2 || res.Planned != 3 || res.Removed != 2 {
		t.Fatalf("clean: %+v; want 2 shards, 3 planned, 2 removed", res)
	}
	for _, n := range orphans {
		if present(t, s, n) {
			t.Fatalf("orphan %s survived the cleaning", n)
		}
	}
	if !present(t, s, live) {
		t.Fatal("the chunk the roots name was removed")
	}
	h, _, err := c.ReadHead(ctx, v)
	if err != nil || h.Activity.Kind != layout.ActIdle {
		t.Fatalf("HEAD after clean: %+v %v; want idle", h.Activity, err)
	}
	// A second pass finds nothing more.
	if res, err := c.Clean(ctx, v, cleanHolder, "c2"); err != nil || res.Removed != 0 {
		t.Fatalf("second clean: %+v %v", res, err)
	}
}

// Only the holder at HEAD's epoch cleans: another holder, an old epoch or
// another claim is refused at Begin, and nothing is removed.
func TestBCatalog_CleanIsHolderOnly(t *testing.T) {
	ctx := context.Background()
	c, v, s, orphans := cleanFixture(t)
	for name, me := range map[string]layout.HolderID{
		"other holder": {Epoch: 1, Holder: "x/y", ClaimID: "k"},
		"other claim":  {Epoch: 1, Holder: "s/r", ClaimID: "other"},
		"old epoch":    {Epoch: 0, Holder: "s/r", ClaimID: "k"},
	} {
		if _, err := c.Clean(ctx, v, me, "c1"); err == nil {
			t.Fatalf("%s: clean succeeded; want refused", name)
		}
	}
	for _, n := range orphans {
		if !present(t, s, n) {
			t.Fatalf("a refused clean removed %s", n)
		}
	}
	h, _, err := c.ReadHead(ctx, v)
	if err != nil || h.Activity.Kind != layout.ActIdle {
		t.Fatalf("HEAD after refusals: %+v %v; want idle", h.Activity, err)
	}
}

// While a snapshot session is open the cleaning cannot begin (ErrBusy) and
// removes nothing; the open session is left as it was.
func TestBCatalog_CleanWaitsForAnOpenSnapshot(t *testing.T) {
	ctx := context.Background()
	c, v, s, orphans := cleanFixture(t)
	if _, err := layout.RunBegin(ctx, s, cleanHolder, "snap2"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Clean(ctx, v, cleanHolder, "c1"); !errors.Is(err, layout.ErrBusy) {
		t.Fatalf("clean during a snapshot: %v; want ErrBusy", err)
	}
	for _, n := range orphans {
		if !present(t, s, n) {
			t.Fatalf("clean during a snapshot removed %s", n)
		}
	}
	h, _, err := c.ReadHead(ctx, v)
	if err != nil || h.Activity != (layout.Activity{Kind: layout.ActSnap, Session: "snap2"}) {
		t.Fatalf("HEAD after the refused clean: %+v %v; want the snapshot session open", h.Activity, err)
	}
}

// A pass that fails after Begin still closes the cleaning, so snapshots are
// not blocked by it.
func TestBCatalog_CleanClosesOnError(t *testing.T) {
	ctx := context.Background()
	c, v, _, _ := cleanFixture(t)
	s, err := c.Store(v)
	if err != nil {
		t.Fatal(err)
	}
	pool := path.Join(c.base, v.Dir(), layout.PoolDir)
	if err := s.f.CreateEntry(ctx, pool, layoutEntry("x", nil), true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Clean(ctx, v, cleanHolder, "c1"); !errors.Is(err, layout.ErrCorrupt) {
		t.Fatalf("clean over a malformed shard: %v; want ErrCorrupt", err)
	}
	h, _, err := c.ReadHead(ctx, v)
	if err != nil || h.Activity.Kind != layout.ActIdle {
		t.Fatalf("HEAD after a failed clean: %+v %v; want idle", h.Activity, err)
	}
}

// commitImage commits, as cleanHolder in a fresh session, the manifest next
// (prev is HEAD's current manifest), uploading data, at position, with a
// named snapshot of it.
func commitImage(t *testing.T, s *LayoutStore, vol string, session string, prev, next map[uint64]layout.Ref, data map[uint64][]byte, position uint64, snapshot string) {
	t.Helper()
	ctx := context.Background()
	if _, err := layout.RunBegin(ctx, s, cleanHolder, session); err != nil {
		t.Fatal(err)
	}
	for idx, ref := range next {
		if b, ok := data[idx]; ok {
			if err := layout.EnsureObject(ctx, s, ref.Chunk.Path(), b, session); err != nil {
				t.Fatal(err)
			}
		}
	}
	root, segs, err := layout.Build(next)
	if err != nil {
		t.Fatal(err)
	}
	for n, b := range segs {
		if err := layout.EnsureObject(ctx, s, n.Path(), b, session); err != nil {
			t.Fatal(err)
		}
	}
	rn, rb, err := layout.NameRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := layout.EnsureObject(ctx, s, rn.Path(), rb, session); err != nil {
		t.Fatal(err)
	}
	if _, err := layout.RunCommit(ctx, s, s, vol, cleanHolder, layout.CommitRequest{Session: session, Prev: prev, Next: next, Position: position, Snapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
}

// Release-blocking (cleaning end to end on the catalog): snapshot s1 of X,
// overwrite the chunk with Y, snapshot s2, delete s1, clean: X's chunk is
// gone, the pool shrinks by exactly it, and the current image and s2 still
// read Y.
func TestBCatalog_CleanAfterOverwriteAndSnapshotDelete(t *testing.T) {
	ctx := context.Background()
	c, err := NewBCatalog(NewMemFiler(), newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	v := newRef(t, "v1")
	if _, err := c.CreateVolume(ctx, v); err != nil {
		t.Fatal(err)
	}
	s, err := c.Store(v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := layout.Start(ctx, s, okClaimLease{}, layout.HolderID{Epoch: 1, Holder: "s/r"}, layout.LocalRoot{ClaimID: "k", Fresh: true}); err != nil {
		t.Fatal(err)
	}
	x, y := bytes.Repeat([]byte{'X'}, 4096), bytes.Repeat([]byte{'Y'}, 4096)
	cx, cy := layout.NameChunk(1, x), layout.NameChunk(1, y)
	m1 := map[uint64]layout.Ref{1: {Owner: v.Name, OwnerID: v.ID, Chunk: cx}}
	m2 := map[uint64]layout.Ref{1: {Owner: v.Name, OwnerID: v.ID, Chunk: cy}}
	commitImage(t, s, v.Name, "w1", map[uint64]layout.Ref{}, m1, map[uint64][]byte{1: x}, 1, "s1")
	commitImage(t, s, v.Name, "w2", m1, m2, map[uint64][]byte{1: y}, 2, "s2")
	if err := c.f.CreateEntry(ctx, path.Join(c.base, v.Dir(), layout.PoolDir), &filerpb.Entry{Name: "0", IsDirectory: true}, false); err != nil {
		t.Fatal(err)
	}
	// Control: while s1 still names X, the cleaning removes nothing.
	if res, err := c.Clean(ctx, v, cleanHolder, "c0"); err != nil || res.Removed != 0 || !present(t, s, cx) {
		t.Fatalf("clean with s1 held: %+v %v; want nothing removed", res, err)
	}
	if _, err := c.DeleteSnapshot(ctx, v, "s1"); err != nil {
		t.Fatal(err)
	}
	before, err := c.listObjects(ctx, v)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Clean(ctx, v, cleanHolder, "c1")
	if err != nil || res.Removed != 1 {
		t.Fatalf("clean: %+v %v; want exactly X's chunk removed", res, err)
	}
	after, err := c.listObjects(ctx, v)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before)-1 || present(t, s, cx) || !present(t, s, cy) {
		t.Fatalf("objects %d -> %d, X present %v, Y present %v; want one fewer, X gone, Y kept", len(before), len(after), present(t, s, cx), present(t, s, cy))
	}
	h, _, err := c.ReadHead(ctx, v)
	if err != nil {
		t.Fatal(err)
	}
	_, r, err := c.open(v)
	if err != nil {
		t.Fatal(err)
	}
	for name, rn := range map[string]layout.RootName{"current": *h.Current, "s2": h.Snapshots["s2"].Root} {
		img, err := r.Open(ctx, rn)
		if err != nil {
			t.Fatal(err)
		}
		ch, err := img.Chunk(ctx, 1)
		if err != nil || !bytes.Equal(ch.Data, y) {
			t.Fatalf("%s reads chunk 1: %v (Y=%v); want Y intact", name, err, bytes.Equal(ch.Data, y))
		}
	}
}
