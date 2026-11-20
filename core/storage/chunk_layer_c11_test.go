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
	if s.v3 != nil {
		return s.v3.pub.current().RetainedTail
	}
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
	waitReconciledForTest(t, layer) // the BASE is reconciled against HEAD first
}
