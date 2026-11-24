package chunkstore

import (
	"bytes"
	"context"
	"path"
	"testing"
)

func TestForkGrandchildReadsEveryBlockWithoutAncestorImages(t *testing.T) {
	f, b, c, source := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "seed", RequestID: "seed"}, source); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "seed", Target: "v2", RequestID: "parent"}); err != nil {
		t.Fatal(err)
	}
	parent, err := NewChunkRemote(f, b, catBase+"/v2")
	if err != nil {
		t.Fatal(err)
	}
	ph, err := parent.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateSnapshot(ctx, "v2", SnapshotRequest{Name: "template", RequestID: "template"}, ph); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v2", Snapshot: "template", Target: "v3", RequestID: "grandchild"}); err != nil {
		t.Fatal(err)
	}
	// Remove only ancestor image metadata, preserving immutable object bindings.
	for vol, name := range map[string]string{"v1": "seed", "v2": "template"} {
		attempt := snapPath(t, f, vol, name)
		for _, entry := range []string{attempt, path.Join(catBase, vol, snapNamesDir, name), path.Join(catBase, vol, headName)} {
			if err := f.DeleteEntry(ctx, path.Dir(entry), path.Base(entry), nil); err != nil {
				t.Fatal(err)
			}
			if _, err := f.LookupEntry(ctx, path.Dir(entry), path.Base(entry)); err == nil {
				t.Fatalf("ancestor image metadata remains: %s", entry)
			}
		}
	}
	// Open a fresh reader: no cached parent HEAD or snapshot can supply data.
	grandchild, err := NewChunkRemote(f, b, catBase+"/v3")
	if err != nil {
		t.Fatal(err)
	}
	h, err := grandchild.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Chunks) != 2 {
		t.Fatalf("grandchild lost its complete image: %d chunks", len(h.Chunks))
	}
	for block := uint64(0); block < uint64(catGeo.NumBlocks); block++ {
		got := make([]byte, catGeo.BlockSize)
		if ref, ok := h.Chunks[block/uint64(catGeo.ChunkBlocks)]; ok {
			data, err := grandchild.Get(ctx, ref.Key)
			if err != nil {
				t.Fatalf("block %d: %v", block, err)
			}
			off := (block % uint64(catGeo.ChunkBlocks)) * uint64(catGeo.BlockSize)
			copy(got, data[off:off+uint64(catGeo.BlockSize)])
		}
		var fill byte
		switch block / uint64(catGeo.ChunkBlocks) {
		case 0:
			fill = 0x10
		case 3:
			fill = 0x13
		}
		if !bytes.Equal(got, bytes.Repeat([]byte{fill}, int(catGeo.BlockSize))) {
			t.Fatalf("block %d differs after ancestor images were removed", block)
		}
	}
}
