package chunkstore

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The unreferenced-not-yet-collected bindings come from GC's own Preview:
// a HEAD rewrite leaves the old keys stored but unreferenced; they count
// at once (within GC's grace too), only for their own volume, and the
// preview writes nothing.
func TestCatalog_UsageWithGCCountsUnreferenced(t *testing.T) {
	f, b, c, _ := catalogFixture(t) // v1 HEAD: chunks 0 and 3
	ctx := context.Background()
	full := uint64(catGeo.ChunkBlocks) * uint64(catGeo.BlockSize)
	if _, err := c.EnsureVolume(ctx, "v2", catGeo); err != nil {
		t.Fatal(err)
	}
	publishSource(t, f, b, "v2", map[uint64][]byte{1: chunkBytes(0x31)})
	for _, vol := range []string{"v1", "v2"} {
		if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: vol, IsDirectory: true}, true); err != nil {
			t.Fatal(err)
		}
	}
	// Rewrite both HEADs: v1's two old keys and v2's one become unreferenced.
	publishTagged(t, f, b, "v1", "e2", map[uint64][]byte{0: chunkBytes(0x20), 3: chunkBytes(0x23)})
	publishTagged(t, f, b, "v2", "e2", map[uint64][]byte{1: chunkBytes(0x41)})

	g, err := NewCollector(c, gcBlobs{b}, GCConfig{Grace: time.Hour, IntentTTL: time.Hour, IssuedEpoch: func(context.Context, string) (uint64, error) { return 1, nil }})
	if err != nil {
		t.Fatal(err)
	}
	before := len(f.allWritten())
	u, err := c.UsageWithGC(ctx, "v1", g)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(f.allWritten()); got != before {
		t.Fatalf("usage preview wrote %d entries", got-before)
	}
	if u.Unreferenced == nil || u.Unreferenced.BindingsUpperBound != 2 || u.Unreferenced.BytesUpperBound != 2*full || !u.Unreferenced.UnknownUnbound ||
		strings.Join(u.Unreferenced.Keys, ",") != "chunks/e1/c0/k,chunks/e1/c3/k" {
		t.Fatalf("unreferenced: %+v; want 2 bindings, %d bytes upper bound, unknown unbound", u.Unreferenced, 2*full)
	}
	if u.StoredBindings != 4 || u.LiveChunks != 2 {
		t.Fatalf("stored %d live %d; want 4 stored, 2 live", u.StoredBindings, u.LiveChunks)
	}
	// Plain Usage leaves the GC figure unset.
	if plain, err := c.Usage(ctx, "v1"); err != nil || plain.Unreferenced != nil {
		t.Fatalf("plain usage: %+v %v; want no unreferenced figure", plain.Unreferenced, err)
	}
}

// listHookFiler runs hook before listing number skip+1 of dir, outside the
// fake's lock, so a real caller can act at that point of a GC mark.
type listHookFiler struct {
	*fakeFiler
	dir  string
	skip int
	hook func()
}

func (l *listHookFiler) ListEntries(ctx context.Context, dir, prefix, after string, limit uint32) ([]*filerpb.Entry, error) {
	if dir == l.dir && l.hook != nil && l.skip > 0 {
		l.skip--
	} else if dir == l.dir && l.hook != nil {
		h := l.hook
		l.hook = nil
		h()
	}
	return l.fakeFiler.ListEntries(ctx, dir, prefix, after, limit)
}

// Review of #146: a capture that starts after the mark read the intents and
// before it listed the bindings (PutIntent, then upload) is protected by a
// live intent and must not be counted; the real unreferenced keys still
// are, and nothing is deleted.
func TestCatalog_UsageWithGCSkipsAnInFlightCapture(t *testing.T) {
	f, b, c0, _ := catalogFixture(t) // v1 HEAD: chunks 0 and 3
	ctx := context.Background()
	if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v1", IsDirectory: true}, true); err != nil {
		t.Fatal(err)
	}
	publishTagged(t, f, b, "v1", "e2", map[uint64][]byte{0: chunkBytes(0x20), 3: chunkBytes(0x23)})
	r, err := NewChunkRemote(f, b, catBase+"/v1")
	if err != nil {
		t.Fatal(err)
	}
	// UsageWithGC lists v1's bindings twice: Usage's stored count, then
	// the GC mark (after it read the intents). The hook takes the second.
	lf := &listHookFiler{fakeFiler: f, dir: r.keyDir(), skip: 1}
	c, err := NewCatalog(lf, catBase)
	if err != nil {
		t.Fatal(err)
	}
	_ = c0
	const captured = "chunks/e3/c5/k"
	lf.hook = func() { // the mark has read v1's intents; it lists bindings next
		if _, err := putIntent(ctx, f, catBase+"/v1", storage.ChunkIntent{Gen: 9, Epoch: 1, Holder: "h1", Keys: []string{captured}, DeadlineNs: time.Now().Add(time.Hour).UnixNano()}); err != nil {
			t.Fatal(err)
		}
		if err := r.Put(ctx, captured, chunkBytes(0x55)); err != nil {
			t.Fatal(err)
		}
	}
	blobs := gcBlobs{b}
	g, err := NewCollector(c, blobs, GCConfig{Grace: time.Hour, IntentTTL: time.Hour, IssuedEpoch: func(context.Context, string) (uint64, error) { return 1, nil }})
	if err != nil {
		t.Fatal(err)
	}
	stored := len(b.data)
	u, err := c.UsageWithGC(ctx, "v1", g)
	if err != nil {
		t.Fatal(err)
	}
	if lf.hook != nil {
		t.Fatal("the capture never ran inside the mark")
	}
	if got := strings.Join(u.Unreferenced.Keys, ","); got != "chunks/e1/c0/k,chunks/e1/c3/k" || u.Unreferenced.BindingsUpperBound != 2 {
		t.Fatalf("unreferenced keys %q (%d); want the two rewritten keys, not the in-flight %s", got, u.Unreferenced.BindingsUpperBound, captured)
	}
	if len(b.data) != stored+1 {
		t.Fatalf("blobs %d -> %d; want only the capture's upload added, nothing deleted", stored, len(b.data))
	}
}
