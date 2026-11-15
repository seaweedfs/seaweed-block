package storage

// C11 incremental snapshots through a persisted dirty-since-HEAD chunk
// bitmap (chunk_seam.go: chunkBitmapStore, chunkBitmapHeader, chunkDelta,
// BeforeCheckpointAdvance, BeginBaseInstall). The bitmap stays off the
// write hot path: the dirty set is the persisted bitmap ∪ the chunks of WAL
// entries above the checkpoint. The bitmap is durable before ANY checkpoint
// or WAL-tail advance and before any BASE extent write (REBUILDING header).
// It may over-report, never under-report. RED until the layer exists.

import (
	"bytes"
	"errors"
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

// walLogicalTail is the WAL writer's physical recycling position.
func walLogicalTail(s *WALStore) uint64 {
	s.wal.mu.Lock()
	defer s.wal.mu.Unlock()
	return s.wal.logicalTail
}

// saveLog records, for every Save, the dirty set and the store's checkpoint
// and WAL logical tail at that moment (inside Save, before it returns).
type saveLog struct {
	mu   sync.Mutex
	rows []savedRow
}

type savedRow struct {
	checkpoint uint64
	walTail    uint64
	dirty      map[uint64]bool
}

func recordSaves(s *WALStore, bm *fakeBitmapStore) *saveLog {
	l := &saveLog{}
	bm.mu.Lock()
	bm.onSave = func(dirty []uint64) {
		// Save runs inside persistCheckpoint, under the store's publication
		// lock: read the fields directly, never re-lock the store. Only the
		// flusher/Sync goroutine that holds that lock writes them.
		row := savedRow{checkpoint: s.checkpointLSN, walTail: s.wal.logicalTail, dirty: map[uint64]bool{}}
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

func flushWithin(t *testing.T, s *WALStore, what string) error {
	t.Helper()
	var ferr error
	within(t, what, func() error { ferr = s.flusher.flushOnce(); return nil })
	return ferr
}

// C11 callbacks: OnExtentWrite, for a normal and a BASE block, returns at
// once while bitmap persistence and the remote are held and the store's
// publication lock is taken: no Load/Save/WriteHeader/Reset, no remote call,
// no WALStore re-entry. (A BASE mark must also be a constant-time flag; that
// cost bound is measured in the implementation PR, not proven here.)
func TestChunkLayer_C11_ExtentCallbackIsPureInMemory(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	bm := cfg.Bitmap.(*fakeBitmapStore)
	layer := mustAttach(t, s, f, cfg)
	holdPuts(t, f)
	holdGets(t, f)
	block := make(chan struct{})
	var unblock sync.Once
	bm.mu.Lock()
	bm.block = block
	bm.mu.Unlock()
	release := func() { unblock.Do(func() { close(block) }) }
	t.Cleanup(release)
	onUnblock(t, release)
	callsBefore, remoteBefore := bm.callCount(), f.stats()

	s.mu.Lock() // a callback that re-enters the store would block here
	var unlock sync.Once
	unlockStore := func() { unlock.Do(s.mu.Unlock) }
	t.Cleanup(unlockStore)
	onUnblock(t, unlockStore)
	within(t, "OnExtentWrite for a normal block and a BASE block", func() error {
		layer.OnExtentWrite(12, 7)
		layer.OnExtentWrite(13, 0)
		return nil
	})
	unlock.Do(s.mu.Unlock)

	if n := bm.callCount() - callsBefore; n != 0 {
		t.Fatalf("OnExtentWrite made %d bitmap store calls", n)
	}
	after := f.stats()
	if after.puts != remoteBefore.puts || after.gets != remoteBefore.gets || after.getHeads != remoteBefore.getHeads || after.cas != remoteBefore.cas {
		t.Fatal("OnExtentWrite made remote calls")
	}
}

// C11 ordering and period: writes never touch the bitmap; the flusher saves
// a dirty set covering a chunk before the checkpoint or the physical WAL
// tail passes that chunk's entries; a cut archives the closed period as a
// delta and starts a new bitmap headed by the new HEAD; a failed save
// refuses the advance (checkpoint and WAL tail unchanged), and a restart
// still captures the chunk from the WAL.
func TestChunkLayer_C11_BitmapPersistedBeforeWALRecycle(t *testing.T) {
	s, path, crash := crashableStore(t)
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
	tailBefore := walLogicalTail(s)
	if err := flushWithin(t, s, "flush of chunk 3"); err != nil {
		t.Fatal(err)
	}
	row, ok := saves.firstSaveWith(3)
	if !ok {
		t.Fatal("the flusher recycled chunk 3's WAL entries without saving a bitmap that covers chunk 3")
	}
	if row.checkpoint >= lsn || row.walTail != tailBefore {
		t.Fatalf("chunk 3 saved at checkpoint %d / WAL tail %d; want checkpoint < %d and the tail still at %d", row.checkpoint, row.walTail, lsn, tailBefore)
	}
	if s.CheckpointLSN() < lsn || walLogicalTail(s) <= tailBefore || !bm.persisted(3) {
		t.Fatalf("after the flush: checkpoint %d (want >= %d), WAL tail %d (want > %d), chunk 3 persisted %v", s.CheckpointLSN(), lsn, walLogicalTail(s), tailBefore, bm.persisted(3))
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

	// A failed save refuses the advance; the WAL keeps the entry.
	bm.mu.Lock()
	bm.failSave = errors.New("injected bitmap save failure")
	bm.mu.Unlock()
	lsn20 := w.write(t, s, 20, 0x50) // chunk 5
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	cpBefore, tailBefore := s.CheckpointLSN(), walLogicalTail(s)
	if err := flushWithin(t, s, "flush with a failing bitmap save"); err == nil {
		t.Fatal("the flusher advanced although the bitmap could not be saved")
	}
	if cp, tail := s.CheckpointLSN(), walLogicalTail(s); cp != cpBefore || tail != tailBefore || cp >= lsn20 {
		t.Fatalf("failed save moved checkpoint %d -> %d or WAL tail %d -> %d (LSN %d unprotected)", cpBefore, cp, tailBefore, tail, lsn20)
	}
	bm.mu.Lock()
	bm.failSave = nil
	bm.mu.Unlock()
	crash()
	s2, _ := reopen(t, path)
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

// C11 rollover: while a cut's upload is held, a post-cut write is flushed
// and its WAL recycled (so only the bitmap remembers it). The cut's Reset
// must keep that chunk dirty, and after a crash the next capture uploads it.
func TestChunkLayer_C11_RolloverKeepsPostCutDirt(t *testing.T) {
	s, path, crash := crashableStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	bm := cfg.Bitmap.(*fakeBitmapStore)
	layer := mustAttach(t, s, f, cfg)
	w := newWriteLog()
	w.write(t, s, 4, 0x14) // chunk 1, captured by the held cut

	entered, release := holdPuts(t, f)
	done := startCapture(t, layer)
	waitKey(t, entered, "held cut upload")
	within(t, "post-cut write, sync and flush while the cut is held", func() error {
		data := makeBlock(ctBlockSize, 0x26)
		lsn, err := s.Write(24, data) // chunk 6, after the cut
		if err != nil {
			return err
		}
		w.record(lsn, 24, data)
		if _, err := s.Sync(); err != nil {
			return err
		}
		return s.flusher.flushOnce()
	})
	release()
	if err := waitErr(t, done, "held cut"); err != nil {
		t.Fatalf("held cut: %v", err)
	}
	if !bm.persisted(6) {
		t.Fatal("the cut's Reset cleared chunk 6, dirtied after the cut with its WAL already recycled")
	}
	if bm.persisted(1) {
		t.Fatal("the cut's Reset kept chunk 1, which the cut captured")
	}
	crash()
	s2, _ := reopen(t, path)
	layer2 := mustAttach(t, s2, f, cfg)
	before := f.stats()
	if _, err := layer2.Capture(ctTimeout(t)); err != nil {
		t.Fatalf("capture after restart: %v", err)
	}
	if n := f.stats().puts - before.puts; n != 1 {
		t.Fatalf("capture after restart stored %d chunks, want 1 (chunk 6)", n)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

// c11BaseFixture: HEAD covers chunks 0 and 5; then a BASE rebuild rewrites
// LBA 20 (chunk 5) and LBA 24 (chunk 6) through WriteExtentDirect, advances
// the frontier and Syncs (the direct-frontier checkpoint path).
//   - failSave: the bitmap save on that path fails. Sync must return an
//     error and leave the checkpoint and the physical WAL tail unchanged,
//     and EndBaseInstall must refuse because the frontier is not durable.
//   - otherwise the checkpoint reaches the frontier; complete then also
//     calls EndBaseInstall(frontier), which must durably record the
//     completed install.
//
// Then the host crashes.
func c11BaseFixture(t *testing.T, failSave, complete bool) (path string, f *fakeChunkRemote, cfg chunkLayerConfig, frontier uint64) {
	t.Helper()
	s, path, crash := crashableStore(t)
	f = newFakeChunkRemote()
	cfg = ctConfig(1)
	bm := cfg.Bitmap.(*fakeBitmapStore)
	layer := mustAttach(t, s, f, cfg)
	w := newWriteLog()
	w.write(t, s, 1, 0x01)          // chunk 0
	last := w.write(t, s, 21, 0x21) // chunk 5
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}

	if err := layer.BeginBaseInstall(); err != nil {
		t.Fatalf("BeginBaseInstall: %v", err)
	}
	if hdr, _, _ := bm.snapshot(); hdr.Base != chunkBaseInstalling {
		t.Fatalf("BeginBaseInstall returned before an installing header was durable (header %+v)", hdr)
	}
	if _, err := layer.Capture(ctTimeout(t)); !errors.Is(err, errChunkCaptureRefused) {
		t.Fatalf("Capture during a BASE install: got %v, want errChunkCaptureRefused", err)
	}
	frontier = last + 10
	for lba, fill := range map[uint32]byte{20: 0xB5, 24: 0xB6} {
		if err := s.WriteExtentDirect(lba, makeBlock(ctBlockSize, fill)); err != nil {
			t.Fatal(err)
		}
	}
	if err := layer.EndBaseInstall(frontier); err == nil {
		t.Fatal("EndBaseInstall accepted a frontier that is not durable yet")
	}
	if failSave {
		bm.mu.Lock()
		bm.failSave = errors.New("injected bitmap save failure")
		bm.mu.Unlock()
	}
	s.AdvanceFrontier(frontier)
	cpBefore, tailBefore := s.CheckpointLSN(), walLogicalTail(s)
	_, syncErr := s.Sync() // persists the direct frontier as the checkpoint
	if failSave {
		if syncErr == nil {
			t.Fatal("BASE Sync succeeded although its bitmap save failed")
		}
		if cp, tail := s.CheckpointLSN(), walLogicalTail(s); cp != cpBefore || tail != tailBefore {
			t.Fatalf("failed bitmap save still moved checkpoint %d -> %d or WAL tail %d -> %d", cpBefore, cp, tailBefore, tail)
		}
		if err := layer.EndBaseInstall(frontier); err == nil {
			t.Fatal("EndBaseInstall accepted a frontier whose checkpoint was refused")
		}
		bm.mu.Lock()
		bm.failSave = nil
		bm.mu.Unlock()
	} else {
		if syncErr != nil {
			t.Fatalf("BASE Sync: %v", syncErr)
		}
		if cp := s.CheckpointLSN(); cp < frontier {
			t.Fatalf("BASE Sync returned nil but the checkpoint is %d < frontier %d", cp, frontier)
		}
		if complete {
			if err := layer.EndBaseInstall(frontier); err != nil {
				t.Fatalf("EndBaseInstall after the durable frontier: %v", err)
			}
			if hdr, _, _ := bm.snapshot(); hdr.Base != chunkBaseInstalledAwaitingCut {
				t.Fatalf("EndBaseInstall returned before the completed install was durable (header %+v)", hdr)
			}
		}
	}
	crash()
	return path, f, cfg, frontier
}

// assertBaseCutUploadsChangedChunks runs the first cut after a BASE rebuild:
// a full hash compare that stores exactly chunks 5 and 6, keeps chunk 0's
// ref, publishes the BASE blocks, and leaves a normal header for the new HEAD.
func assertBaseCutUploadsChangedChunks(t *testing.T, layer chunkLayer, f *fakeChunkRemote, cfg chunkLayerConfig) {
	t.Helper()
	before := f.stats()
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatalf("first cut after the rebuild: %v", err)
	}
	after := f.stats()
	if n := after.puts - before.puts; n != 2 {
		t.Fatalf("first cut after the rebuild stored %d chunks, want 2 (chunks 5 and 6)", n)
	}
	if after.head.Chunks[0] != before.head.Chunks[0] {
		t.Fatal("unchanged chunk 0 was re-published")
	}
	for lba, fill := range map[uint32]byte{20: 0xB5, 24: 0xB6} {
		if got := blockOf(f.imageOf(t, after.head), lba); !bytes.Equal(got, makeBlock(ctBlockSize, fill)) {
			t.Fatalf("BASE block LBA %d missing from the post-rebuild HEAD", lba)
		}
	}
	bm := cfg.Bitmap.(*fakeBitmapStore)
	if hdr, _, _ := bm.snapshot(); hdr.Base != chunkBaseNone || hdr.HeadGen != after.head.Gen {
		t.Fatalf("header after the post-rebuild cut %+v, want a normal header for HEAD gen %d", hdr, after.head.Gen)
	}
	assertNoRemoteMisuse(t, f)
}

// rerunBase repeats a complete BASE install of the same blocks after a
// restart, as the owner must after an interrupted one.
func rerunBase(t *testing.T, s *WALStore, layer chunkLayer, frontier uint64) {
	t.Helper()
	if err := layer.BeginBaseInstall(); err != nil {
		t.Fatalf("BeginBaseInstall after restart: %v", err)
	}
	for lba, fill := range map[uint32]byte{20: 0xB5, 24: 0xB6} {
		if err := s.WriteExtentDirect(lba, makeBlock(ctBlockSize, fill)); err != nil {
			t.Fatal(err)
		}
	}
	s.AdvanceFrontier(frontier)
	if _, err := s.Sync(); err != nil {
		t.Fatalf("BASE Sync after restart: %v", err)
	}
	if err := layer.EndBaseInstall(frontier); err != nil {
		t.Fatalf("EndBaseInstall after restart: %v", err)
	}
}

// C11 BASE: recovery never infers that an install finished. An install
// interrupted by a crash (header still installing) keeps cuts refused after
// restart until the owner completes a new install; a completed install
// (EndBaseInstall durable) allows the first cut, which hashes fully. With
// the bitmap save failing on the direct-frontier path, Sync errors and
// neither the checkpoint nor the WAL tail moves.
func TestChunkLayer_C11_BaseRebuildRecovery(t *testing.T) {
	t.Run("interrupted install", func(t *testing.T) {
		path, f, cfg, frontier := c11BaseFixture(t, false, false)
		s2, _ := reopen(t, path)
		layer2 := mustAttach(t, s2, f, cfg)
		before := f.stats()
		if _, err := layer2.Capture(ctTimeout(t)); !errors.Is(err, errChunkCaptureRefused) {
			t.Fatalf("cut after an interrupted install: got %v, want errChunkCaptureRefused", err)
		}
		if after := f.stats(); after.puts != before.puts || after.cas != before.cas {
			t.Fatal("refused cut made remote calls")
		}
		rerunBase(t, s2, layer2, frontier)
		assertBaseCutUploadsChangedChunks(t, layer2, f, cfg)
	})
	t.Run("completed install", func(t *testing.T) {
		path, f, cfg, _ := c11BaseFixture(t, false, true)
		s2, _ := reopen(t, path)
		layer2 := mustAttach(t, s2, f, cfg)
		assertBaseCutUploadsChangedChunks(t, layer2, f, cfg)
	})
	t.Run("bitmap save fails on the direct-frontier path", func(t *testing.T) {
		path, f, cfg, frontier := c11BaseFixture(t, true, false)
		s2, _ := reopen(t, path)
		layer2 := mustAttach(t, s2, f, cfg)
		if _, err := layer2.Capture(ctTimeout(t)); !errors.Is(err, errChunkCaptureRefused) {
			t.Fatalf("cut after an install whose frontier never became durable: got %v, want errChunkCaptureRefused", err)
		}
		rerunBase(t, s2, layer2, frontier)
		assertBaseCutUploadsChangedChunks(t, layer2, f, cfg)
	})
}

// C11 over-report only: the bitmap save succeeds but the checkpoint publish
// then fails; after a crash the next capture may over-report, but its image
// is exact and nothing is missed.
func TestChunkLayer_C11_SaveThenCheckpointFailureOverReportsOnly(t *testing.T) {
	s, path, crash := crashableStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	layer := mustAttach(t, s, f, cfg)
	w := newWriteLog()
	w.write(t, s, 1, 0x01)
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	w.write(t, s, 24, 0x24) // chunk 6
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	s.syncSuperblockMetadata = func() error { return errors.New("injected checkpoint sync failure") }
	if err := flushWithin(t, s, "flush whose checkpoint publish fails"); err == nil {
		t.Fatal("the flush succeeded although its checkpoint publish failed")
	}
	crash()
	s2, _ := reopen(t, path)
	layer2 := mustAttach(t, s2, f, cfg)
	if _, err := layer2.Capture(ctTimeout(t)); err != nil {
		t.Fatalf("capture after restart: %v", err)
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
	s, path, crash := crashableStore(t)
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
	crash()
	s2, _ := reopen(t, path)
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
// field (including a completed BASE awaiting its first cut), falls back to a per-chunk SHA-256
// compare and uploads only the chunks whose bytes differ.
func TestChunkLayer_C11_LostOrMismatchedBitmapFallsBackToHashCompare(t *testing.T) {
	for name, mutate := range map[string]func(bm *fakeBitmapStore){
		"lost":                                  func(bm *fakeBitmapStore) { bm.lostNext = true },
		"head gen mismatch":                     func(bm *fakeBitmapStore) { bm.hdr.HeadGen += 7; bm.dirty = map[uint64]bool{} },
		"epoch mismatch":                        func(bm *fakeBitmapStore) { bm.hdr.Epoch++; bm.dirty = map[uint64]bool{} },
		"base lsn mismatch":                     func(bm *fakeBitmapStore) { bm.hdr.BaseLSN += 3; bm.dirty = map[uint64]bool{} },
		"completed BASE awaiting its first cut": func(bm *fakeBitmapStore) { bm.hdr.Base = chunkBaseInstalledAwaitingCut; bm.dirty = map[uint64]bool{} },
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
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	tailBefore := walLogicalTail(s)
	if err := flushWithin(t, s, "flush of the applied entry"); err != nil {
		t.Fatal(err)
	}
	row, ok := saves.firstSaveWith(4)
	if !ok {
		t.Fatal("replica apply: chunk 4's WAL entry was recycled without a bitmap save covering it")
	}
	if row.checkpoint >= applied || row.walTail != tailBefore {
		t.Fatalf("replica chunk 4 saved at checkpoint %d / WAL tail %d, after the advance (applied %d, tail before %d)", row.checkpoint, row.walTail, applied, tailBefore)
	}
	if got := extentBytes(t, s, 16); !bytes.Equal(got, makeBlock(ctBlockSize, 0x16)) {
		t.Fatal("applied entry did not reach the extent")
	}
}
