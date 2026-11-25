package chunkstore

import (
	"bytes"
	"context"
	"path"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// poolObjects is the number of chunk objects in v's pool.
func poolObjects(t *testing.T, c *BCatalog, v VolumeRef) int {
	t.Helper()
	names, err := c.listObjects(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, p := range names {
		if strings.HasPrefix(p, layout.PoolDir+"/") {
			n++
		}
	}
	return n
}

// Release-blocking cleaning end to end through the engine and the layout
// remote: the engine writes and takes snapshot s1, overwrites a block and
// takes s2; s1 is deleted; the holder's cleaning (LayoutRemote.Clean with
// the catalog's removal step) removes exactly the chunk only s1 named, the
// pool shrinks by it, and a fresh host reads the current image back intact,
// as does s2. Control: while s1 still exists the cleaning removes nothing.
func TestLayoutRemote_CleanAfterOverwriteAndSnapshotDelete(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	defer s.Close()
	want := map[uint32][]byte{0: block(1), 9: block(2), 63: block(4)}
	writeSync(t, s, want)
	r.SetSnapshot("s1", layout.SnapshotMeta{CreatedNs: 1, Class: layout.ClassSemantic})
	if _, err := l.Capture(ctx); err != nil {
		t.Fatalf("capture s1: %v", err)
	}
	want[9] = block(9)
	writeSync(t, s, map[uint32][]byte{9: want[9]})
	r.SetSnapshot("s2", layout.SnapshotMeta{CreatedNs: 2, Class: layout.ClassSemantic})
	if _, err := l.Capture(ctx); err != nil {
		t.Fatalf("capture s2: %v", err)
	}
	// A real filer lists the shard directory it created; the fake keeps entries only.
	if err := lh.cat.f.CreateEntry(ctx, path.Join(lh.cat.base, lh.v.Dir(), layout.PoolDir), &filerpb.Entry{Name: "0", IsDirectory: true}, false); err != nil {
		t.Fatal(err)
	}
	clean := func() CleanResult {
		t.Helper()
		var res CleanResult
		if err := r.Clean(ctx, func(ctx context.Context, me layout.HolderID, session string) error {
			var err error
			res, err = lh.cat.RemoveUnreachableAll(ctx, lh.v, me, session)
			return err
		}); err != nil {
			t.Fatalf("clean: %v", err)
		}
		if h := lh.head(); h.Activity.Kind != layout.ActIdle {
			t.Fatalf("HEAD after the cleaning: %+v; want idle", h.Activity)
		}
		return res
	}

	before := poolObjects(t, lh.cat, lh.v)
	if res := clean(); res.Removed != 0 {
		t.Fatalf("clean with s1 held: %+v; want nothing removed", res)
	}
	if _, err := lh.cat.DeleteSnapshot(ctx, lh.v, "s1"); err != nil {
		t.Fatal(err)
	}
	if res := clean(); res.Removed != 1 {
		t.Fatalf("clean after deleting s1: %+v; want exactly the overwritten chunk", res)
	}
	if after := poolObjects(t, lh.cat, lh.v); after != before-1 {
		t.Fatalf("pool objects %d -> %d; want one fewer", before, after)
	}
	lh.readBack("fresh", want)
	_, rd, err := lh.cat.open(lh.v)
	if err != nil {
		t.Fatal(err)
	}
	img, err := rd.Open(ctx, lh.head().Snapshots["s2"].Root)
	if err != nil {
		t.Fatal(err)
	}
	c, err := img.Chunk(ctx, 1)
	if err != nil || !bytes.Equal(c.Data[1*4096:2*4096], want[9]) {
		t.Fatalf("s2 chunk 1 after the cleaning: %v; want block 9 intact", err)
	}
}
