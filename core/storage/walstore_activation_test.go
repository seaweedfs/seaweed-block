package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestWALStoreProductionSelectorCreatesMappedV3(t *testing.T) {
	path := filepath.Join(t.TempDir(), "volume")
	s, err := CreateWALStore(path, 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if !s.FormatV3() {
		t.Fatal("production Create must select mapped v3/impl3")
	}
	d := s.v3.pub.current()
	if d.ImplVersion != 3 || d.WMode != WModeLagged || d.MapRootHash == [32]byte{} {
		t.Fatalf("production descriptor: impl=%d mode=%d map=%x", d.ImplVersion, d.WMode, d.MapRootHash)
	}
	if _, err := s.Write(3, bytes.Repeat([]byte{0x73}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Recover(); err != nil {
		_ = r.Close()
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if data, err := ro.Read(3); err != nil || !bytes.Equal(data, bytes.Repeat([]byte{0x73}, 4096)) {
		t.Fatalf("production readonly after reopen: %v", err)
	}
}

func TestWALStoreProductionCreateOptions(t *testing.T) {
	s, err := CreateWALStoreWithOptions(filepath.Join(t.TempDir(), "volume"), 64, 4096, WALStoreOptions{ChunkBlocks: 4, WMode: WModeStrict})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.v3.pub.current().WMode != WModeStrict || s.v3.ownedMap.chunkBlocks != 4 || len(s.v3.mapEntries) != 16 {
		t.Fatal("production create lost the strict mode or map geometry option")
	}
}

func TestWALStoreV3InstrumentationIncludesBatch(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	if _, err := s.Write(0, blk(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBatch(2, [][]byte{blk(2), blk(3)}); err != nil {
		t.Fatal(err)
	}
	i := s.WriteInstrumentation()
	if i.WALCopyOps != 2 || i.WALCopyBytes != 3*4096 || i.WALEncodeOps != 2 || i.WALChecksumOps != 2 ||
		i.WALAppendOps != 2 || i.WALAppendWriteAtCalls != 2 || i.DirtyMapUpdateOps != 3 || s.DurabilityLatency().WALAppend.Count != 2 {
		t.Fatalf("v3 single+8KiB batch instrumentation incomplete: %+v latency=%+v", i, s.DurabilityLatency())
	}
}
