package chunkstore

import (
	"context"
	"strings"
	"testing"
)

func TestEnsureVolumeRejectsBeyondUint32LBACap(t *testing.T) {
	f, b, _, _, _ := gcFixture(t)
	c, err := NewCatalog(manifestFilerBlobs{f, b}, catBase)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err = c.EnsureVolume(ctx, "at-cap", Geometry{NumBlocks: 4294967295, BlockSize: 4096, ChunkBlocks: 1024}); err != nil {
		t.Fatal(err)
	}
	for _, blocks := range []uint64{4294967296, ^uint64(0)} {
		_, err = c.EnsureVolume(ctx, "too-large", Geometry{NumBlocks: blocks, BlockSize: 4096, ChunkBlocks: 1024})
		if err == nil || !strings.Contains(err.Error(), "16 TiB - 4 KiB") {
			t.Fatalf("blocks %d: want capacity refusal, got %v", blocks, err)
		}
		if _, ok, e := c.ReadVolume(ctx, "too-large"); e != nil || ok {
			t.Fatalf("refusal changed catalog: exists=%v err=%v", ok, e)
		}
	}
}
