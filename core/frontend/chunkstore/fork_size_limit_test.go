package chunkstore

import (
	"context"
	"path"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// A legacy catalog may already contain oversized geometry. A new fork is
// still a new volume: it must not bypass the creation cap by inheritance.
func TestForkRejectsLegacyOversizedGeometry(t *testing.T) {
	for _, blocks := range []uint64{4294967295, 4294967296} {
		f, b, c, _, _ := gcFixture(t)
		ctx := context.Background()
		v, ok, err := c.ReadVolume(ctx, "v1")
		if err != nil || !ok {
			t.Fatal(err)
		}
		v.NumBlocks = blocks
		e, err := volumeEntry(v, "")
		if err != nil {
			t.Fatal(err)
		}
		if err = f.UpdateEntry(ctx, path.Join(catBase, "v1"), e, nil); err != nil {
			t.Fatal(err)
		}
		h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x41)})
		_, err = c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "s"}, h)
		if err != nil {
			t.Fatal(err)
		}
		if err = f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "fork", IsDirectory: true}, true); err != nil {
			t.Fatal(err)
		}
		_, err = c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "fork", RequestID: "f"})
		if blocks == 4294967295 {
			if err != nil {
				t.Fatalf("at-cap fork: %v", err)
			}
			got, ok, e := c.ReadVolume(ctx, "fork")
			if e != nil || !ok || got.NumBlocks != blocks {
				t.Fatalf("at-cap geometry not inherited: %+v %v", got, e)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), "16 TiB - 4 KiB") {
			t.Fatalf("oversized fork must refuse: %v", err)
		}
		if _, ok, e := c.ReadVolume(ctx, "fork"); e != nil || ok {
			t.Fatalf("oversized fork persisted target: %v %v", ok, e)
		}
		// Observe claim cleanup through the public retention operation: an
		// uncompleted fork claim would make this snapshot undeletable.
		if _, deleteErr := c.DeleteSnapshot(ctx, "v1", "s1"); deleteErr != nil {
			t.Fatalf("capacity refusal blocked snapshot deletion: %v", deleteErr)
		}
	}
}
