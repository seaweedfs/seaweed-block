package storage

import (
	"bytes"
	"os"
	"testing"
)

func TestWALStoreV3_ReadOnlyMergesWALAndExtentWithoutWrites(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 1, 1)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(5, blk(0x55)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(5, blk(0x77)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mapBefore, err := os.ReadFile(path + ".chunkmap")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := openReadOnlyV3ForTest(path, WALStoreOptions{})
	if err != nil {
		t.Fatalf("actual readonly v3 open refused: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	for lba, want := range map[uint32][]byte{0: blk(0), 1: blk(1), 5: blk(0x77)} {
		got, e := reader.Read(lba)
		if e != nil || !bytes.Equal(got, want) {
			t.Fatalf("readonly LBA%d err=%v equal=%v", lba, e, bytes.Equal(got, want))
		}
	}
	if _, err = reader.fd.WriteAt([]byte{0}, 0); err == nil {
		t.Fatal("readonly descriptor allowed writes")
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mapAfter, err := os.ReadFile(path + ".chunkmap")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || !bytes.Equal(mapBefore, mapAfter) {
		t.Fatal("readonly open/read/close mutated store or map")
	}
}
