package storage

import (
	"bytes"
	"os"
	"testing"
)

func TestChunkLayerV3_MapOnlyCaptureNeedsNoLegacyBitmap(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 5, 5)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.Bitmap = nil
	cfg.BarrierBound = 1
	layer := mustAttach(t, s, f, cfg)
	first, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.imageOf(t, first.Head)[5*ctBlockSize:6*ctBlockSize], blk(5)) {
		t.Fatal("map-only capture omitted checkpointed bytes")
	}
	if _, err = s.Write(5, blk(8)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	second, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.imageOf(t, second.Head)[5*ctBlockSize:6*ctBlockSize], blk(8)) {
		t.Fatal("map-only checkpoint/capture omitted later bytes")
	}
	if _, err = os.Stat(path + ".chunkbitmap"); !os.IsNotExist(err) {
		t.Fatalf("impl3 created or used a legacy bitmap: %v", err)
	}
	if layer.Stats().DirtyChunks != 0 {
		t.Error("verified map-only capture remains dirty")
	}
}
