package chunkstore

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
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
	if u.Unreferenced == nil || u.Unreferenced.Bindings != 2 || u.Unreferenced.BytesUpperBound != 2*full || !u.Unreferenced.UnknownUnbound {
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
