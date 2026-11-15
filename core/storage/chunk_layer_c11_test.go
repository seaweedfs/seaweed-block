package storage

// C11 incremental snapshots through a persisted dirty-since-HEAD chunk
// bitmap (chunk_seam.go: chunkBitmapStore, chunkBitmapHeader, chunkDelta,
// BeforeCheckpoint). The bitmap stays off the write hot path: the dirty set
// is the persisted bitmap ∪ the chunks of WAL entries above the checkpoint,
// and the flusher persists the bitmap before the WAL tail advances. It may
// over-report, never under-report. RED until the layer exists.

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

// extentBytes reads one block straight from the local extent region of the
// volume file, bypassing the dirty map and the layer.
func extentBytes(t *testing.T, s *WALStore, lba uint32) []byte {
	t.Helper()
	b := make([]byte, ctBlockSize)
	if _, err := s.fd.ReadAt(b, int64(s.extentBase+uint64(lba)*uint64(ctBlockSize))); err != nil {
		t.Fatalf("read extent LBA %d: %v", lba, err)
	}
	return b
}

// saveLog records, for every Save, the dirty set and the store's checkpoint
// at that moment (inside Save, before it returns).
type saveLog struct {
	mu   sync.Mutex
	rows []savedRow
}

type savedRow struct {
	checkpoint uint64
	dirty      map[uint64]bool
}

func recordSaves(s *WALStore, bm *fakeBitmapStore) *saveLog {
	l := &saveLog{}
	bm.mu.Lock()
	bm.onSave = func(dirty []uint64) {
		row := savedRow{checkpoint: s.CheckpointLSN(), dirty: map[uint64]bool{}}
		for _, idx := range dirty {
			row.dirty[idx] = true
		}
		l.mu.Lock()
		l.rows = append(l.rows, row)
		l.mu.Unlock()
	}
	bm.mu.Unlock()
	return l
}

// firstSaveWith returns the first recorded Save whose dirty set holds idx.
func (l *saveLog) firstSaveWith(idx uint64) (savedRow, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.rows {
		if r.dirty[idx] {
			return r, true
		}
	}
	return savedRow{}, false
}

func (l *saveLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.rows)
}

// crashAndReopen simulates a kill and returns the recovered store.
func crashAndReopen(t *testing.T, s *WALStore, path string) *WALStore {
	t.Helper()
	s.committer.Stop()
	if err := s.fd.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	s2.DisableAutoFlushForRecoveryTest()
	if _, err := s2.Recover(); err != nil {
		t.Fatal(err)
	}
	return s2
}

// C11 ordering and period: writes never touch the bitmap; the flusher saves
// a dirty set covering a chunk before the checkpoint (and so the WAL tail)
// passes that chunk's entries; a cut archives the closed period as a delta
// and starts a new bitmap headed by the new HEAD; a failed save aborts the
// checkpoint, the WAL keeps the entries, and a restart still captures them.
func TestChunkLayer_C11_BitmapPersistedBeforeWALRecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vol.bin")
	s, err := CreateWALStore(path, ctNumBlocks, ctBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	bm := cfg.Bitmap.(*fakeBitmapStore)
	layer := mustAttach(t, s, f, cfg)
	saves := recordSaves(s, bm)
	w := newWriteLog()

	var lsn uint64
	within(t, "write and Sync to chunk 3 (the bitmap is off the write path)", func() error {
		var err error
		if lsn, err = s.Write(12, makeBlock(ctBlockSize, 0x3C)); err != nil {
			return err
		}
		w.record(lsn, 12, makeBlock(ctBlockSize, 0x3C))
		_, err = s.Sync()
		return err
	})
	if n := saves.count(); n != 0 {
		t.Fatalf("a write and Sync saved the bitmap %d times; it must stay off the write path", n)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	row, ok := saves.firstSaveWith(3)
	if !ok {
		t.Fatal("the flusher recycled chunk 3's WAL entries without saving a bitmap that covers chunk 3")
	}
	if row.checkpoint >= lsn {
		t.Fatalf("chunk 3 was saved at checkpoint %d, after the checkpoint passed its entry %d", row.checkpoint, lsn)
	}
	if s.CheckpointLSN() < lsn || !bm.persisted(3) {
		t.Fatalf("after the flush: checkpoint %d (want >= %d), chunk 3 persisted %v", s.CheckpointLSN(), lsn, bm.persisted(3))
	}

	cut, err := layer.Capture(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	head := f.stats().head
	hdr, deltas, _ := bm.snapshot()
	if want := (chunkBitmapHeader{BaseLSN: cut, HeadGen: head.Gen, Epoch: 1}); hdr != want {
		t.Fatalf("bitmap header after the cut %+v, want %+v", hdr, want)
	}
	if len(deltas) != 1 {
		t.Fatalf("%d delta records archived, want 1", len(deltas))
	}
	d := deltas[0]
	if d.FromLSN != 0 || d.ToLSN != cut || d.Gen != head.Gen || len(d.Dirty) != 1 || d.Dirty[0] != 3 || d.NewKeys[3] != head.Chunks[3].Key {
		t.Fatalf("delta %+v, want {from 0 to %d gen %d dirty [3] key %q}", d, cut, head.Gen, head.Chunks[3].Key)
	}
	if bm.persisted(3) {
		t.Fatal("chunk 3 is still marked dirty in the new period although the cut captured it")
	}

	// A failed save aborts the checkpoint; the WAL keeps the entry.
	bm.mu.Lock()
	bm.failSave = errors.New("injected bitmap save failure")
	bm.mu.Unlock()
	lsn20 := w.write(t, s, 20, 0x50) // chunk 5
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err == nil {
		t.Fatal("the flusher advanced although the bitmap could not be saved")
	}
	if cp := s.CheckpointLSN(); cp >= lsn20 {
		t.Fatalf("checkpoint %d passed LSN %d whose chunk was never persisted", cp, lsn20)
	}
	bm.mu.Lock()
	bm.failSave = nil
	bm.mu.Unlock()
	s2 := crashAndReopen(t, s, path)
	layer2 := mustAttach(t, s2, f, cfg)
	before := f.stats()
	if _, err := layer2.Capture(ctTimeout(t)); err != nil {
		t.Fatalf("capture after restart: %v", err)
	}
	if n := f.stats().puts - before.puts; n != 1 {
		t.Fatalf("capture after restart stored %d chunks, want 1 (chunk 5, known only from the WAL)", n)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

// c11RestartFixture: HEAD covers chunks 0 and 5; then chunk 6 is written and
// flushed (its WAL recycled, so only the persisted bitmap remembers it) and
// chunk 7 is written and synced but not flushed (only the WAL remembers
// it); then the host crashes and recovers. Whatever beforeReattach does to
// the persisted bitmap, the next capture must upload exactly chunks 6 and 7
// and keep the refs of the unchanged chunks.
func c11RestartFixture(t *testing.T, beforeReattach func(bm *fakeBitmapStore)) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "vol.bin")
	s, err := CreateWALStore(path, ctNumBlocks, ctBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	bm := cfg.Bitmap.(*fakeBitmapStore)
	layer := mustAttach(t, s, f, cfg)
	w := newWriteLog()
	w.write(t, s, 1, 0x01)  // chunk 0
	w.write(t, s, 21, 0x21) // chunk 5
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 24, 0x24) // chunk 6: flushed, WAL recycled
	mustSyncFlush(t, s)
	w.write(t, s, 28, 0x28) // chunk 7: synced only
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	s2 := crashAndReopen(t, s, path)
	if beforeReattach != nil {
		bm.mu.Lock()
		beforeReattach(bm)
		bm.mu.Unlock()
	}
	layer2 := mustAttach(t, s2, f, cfg)
	before := f.stats()
	if _, err := layer2.Capture(ctTimeout(t)); err != nil {
		t.Fatalf("capture after restart: %v", err)
	}
	after := f.stats()
	if n := after.puts - before.puts; n != 2 {
		t.Fatalf("capture after restart stored %d chunks, want exactly 2 (chunks 6 and 7)", n)
	}
	for _, idx := range []uint64{0, 5} {
		if after.head.Chunks[idx] != before.head.Chunks[idx] {
			t.Fatalf("unchanged chunk %d was re-published under a new ref", idx)
		}
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	for _, h := range f.installedHeads() {
		for idx, ref := range h.Chunks {
			if ref.SHA256 == ([32]byte{}) {
				t.Fatalf("HEAD gen %d chunk %d carries no SHA-256", h.Gen, idx)
			}
		}
	}
}

// C11 restart, header matches HEAD: incremental, dirty = persisted bitmap ∪
// chunks of WAL entries above the checkpoint; nothing missed, nothing
// unchanged re-sent.
func TestChunkLayer_C11_IncrementalAfterRestart(t *testing.T) {
	c11RestartFixture(t, nil)
}

// C11 fallback: a lost bitmap, or a header that differs from HEAD in any
// field, falls back to a per-chunk SHA-256 compare and uploads only the
// chunks whose bytes differ.
func TestChunkLayer_C11_LostOrMismatchedBitmapFallsBackToHashCompare(t *testing.T) {
	for name, mutate := range map[string]func(bm *fakeBitmapStore){
		"lost":              func(bm *fakeBitmapStore) { bm.lostNext = true },
		"head gen mismatch": func(bm *fakeBitmapStore) { bm.hdr.HeadGen += 7; bm.dirty = map[uint64]bool{} },
		"epoch mismatch":    func(bm *fakeBitmapStore) { bm.hdr.Epoch++; bm.dirty = map[uint64]bool{} },
		"base lsn mismatch": func(bm *fakeBitmapStore) { bm.hdr.BaseLSN += 3; bm.dirty = map[uint64]bool{} },
	} {
		t.Run(name, func(t *testing.T) { c11RestartFixture(t, mutate) })
	}
}

// C11 replica: the replica apply path goes through the same flusher, so a
// promoted replica's bitmap covers its applied chunks before their WAL
// entries are recycled.
func TestChunkLayer_C11_ReplicaApplyKeepsBitmap(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	bm := cfg.Bitmap.(*fakeBitmapStore)
	mustAttach(t, s, f, cfg)
	saves := recordSaves(s, bm)
	const applied = 5
	if err := s.ApplyEntry(16, makeBlock(ctBlockSize, 0x16), applied); err != nil { // chunk 4
		t.Fatal(err)
	}
	mustSyncFlush(t, s)
	row, ok := saves.firstSaveWith(4)
	if !ok {
		t.Fatal("replica apply: chunk 4's WAL entry was recycled without a bitmap save covering it")
	}
	if row.checkpoint >= applied {
		t.Fatalf("replica chunk 4 saved at checkpoint %d, after the checkpoint passed the applied entry %d", row.checkpoint, applied)
	}
	if got := extentBytes(t, s, 16); !bytes.Equal(got, makeBlock(ctBlockSize, 0x16)) {
		t.Fatal("applied entry did not reach the extent")
	}
}
