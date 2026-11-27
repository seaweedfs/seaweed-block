package storage

import (
	"bytes"
	"testing"
)

// A synced guest write into an absent chunk, then a crash: on reopen the
// replayed write must not reach the extent before the chunk layer attaches,
// or it would not be marked local and the next fetch of its chunk would
// install the snapshot's block over it. The store's owner holds the drain
// (as the durable provider does, before Recover); a flush before attach
// drains nothing, and after attach the block reads back as written and is
// drained as local.
func TestReplayedWriteIsNotDrainedBeforeTheChunkLayerAttaches(t *testing.T) {
	s, path, crash := crashableStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	layer := mustAttach(t, s, f, cfg)
	for lba := uint32(8); lba < 12; lba++ {
		if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(0x80+lba))); err != nil {
			t.Fatal(err)
		}
	}
	mustSyncFlush(t, s)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if err := layer.EvictChunk(2); err != nil {
		t.Fatalf("evict: %v", err)
	}
	newer := makeBlock(ctBlockSize, 0xE9)
	if _, err := s.Write(9, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crash()

	// Reopen as the provider does: hold the drain, then recover.
	s2, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeBounded(t, s2) })
	s2.DisableAutoFlushForRecoveryTest()
	s2.HoldExtentDrainUntilChunkLayer()
	if _, err := s2.Recover(); err != nil {
		t.Fatalf("local recovery: %v", err)
	}
	if err := s2.flusher.flushOnce(); err != nil { // a flusher tick before attach
		t.Fatal(err)
	}
	mustAttach(t, s2, f, cfg)
	if got, err := s2.Read(9); err != nil || !bytes.Equal(got, newer) {
		t.Fatalf("after restart LBA 9 = %#x (err %v); want the synced write %#x", got[0], err, newer[0])
	}
	// After attach the drain runs, marks the block local, and a later read
	// still returns the write (no fetch installs over it).
	if err := s2.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if got, err := s2.Read(9); err != nil || !bytes.Equal(got, newer) {
		t.Fatalf("after the drain LBA 9 = %#x (err %v); want the synced write %#x", got[0], err, newer[0])
	}
}
