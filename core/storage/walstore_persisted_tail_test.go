package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The WAL writer may only overwrite bytes that the PERSISTED superblock tail
// has released (claude05's ruling on the S3 torn-wrap finding, 2026-11-16
// 07:33Z). Otherwise a torn new record can land inside the persisted
// [tail, head) and stop recovery's scan before a synced record behind it.

func diskSuperblockForTest(t *testing.T, path string) superblock {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sb, err := readSuperblock(f)
	if err != nil {
		t.Fatal(err)
	}
	return sb
}

// The S3 trace, deterministic: LSN 2 is appended (and synced) while the
// flush of LSN 1 runs, so the checkpoint covers LSN 1 and the persisted head
// covers LSN 2. Synced LSN 3..5 follow; an unsynced LSN 6 wraps to offset 0
// over the released LSN 1. The crash leaves LSN 6 torn and its end padding
// not on disk (both unsynced pages: a plain crash, no corruption). Synced
// LSN 2..5 must all be recovered.
func TestWALStoreAppendDuringFlushThenTornWrapKeepsSyncedRecords(t *testing.T) {
	const (
		blockSize = 4096
		walSize   = 24 * 1024
	)
	S := uint64(walEntryHeaderSize + blockSize)
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, blockSize, walSize)
	s.DisableAutoFlushForRecoveryTest()
	if _, err := s.Write(0, makeBlock(blockSize, 0x10)); err != nil { // LSN 1
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	s.flusher.afterSnapshotForTest = func() {
		s.flusher.afterSnapshotForTest = nil
		if _, err := s.Write(1, makeBlock(blockSize, 0x11)); err != nil { // LSN 2, not in this flush
			t.Error(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Error(err)
		}
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if sb := diskSuperblockForTest(t, path); sb.WALCheckpointLSN != 1 || sb.WALHead != 2*S {
		t.Fatalf("persisted ckpt=%d head=%d; want the flush of LSN 1 to cover LSN 2 in its head (2S=%d)", sb.WALCheckpointLSN, sb.WALHead, 2*S)
	}
	for lba := uint32(2); lba <= 4; lba++ { // LSN 3..5, synced
		if _, err := s.Write(lba, makeBlock(blockSize, byte(0x10+lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(5, makeBlock(blockSize, 0x66)); err != nil { // LSN 6, unsynced, wraps
		t.Fatal(err)
	}
	ents := recoverySnapshotEntriesByLBA(s.dm.snapshot())
	if ents[5].WALOffset != 0 || ents[1].WALOffset != S {
		t.Fatalf("layout: LSN 6 at %d, LSN 2 at %d; want 0 and S", ents[5].WALOffset, ents[1].WALOffset)
	}
	walOff := s.sb.WALOffset
	if _, err := s.fd.WriteAt(make([]byte, walSize-5*S), int64(walOff+5*S)); err != nil { // padding not on disk
		t.Fatal(err)
	}
	if _, err := s.fd.WriteAt(make([]byte, blockSize/2), int64(walOff+S-blockSize/2)); err != nil { // LSN 6 torn
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	for lba := uint32(1); lba <= 4; lba++ {
		assertRecoveryHardeningBlock(t, r, lba, makeBlock(blockSize, byte(0x10+lba)))
	}
}

// A flush persists the tail it releases: the on-disk tail equals the
// writer's tail after every checkpoint, and head/tail come from one snapshot.
func TestWALStoreFlushPersistsTheReleasedTail(t *testing.T) {
	const blockSize = 4096
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, blockSize, 24*1024)
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })
	for round := 0; round < 3; round++ {
		for lba := uint32(0); lba < 2; lba++ {
			if _, err := s.Write(lba, makeBlock(blockSize, byte(0x20+round))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
		sb := diskSuperblockForTest(t, path)
		if tail, head := s.wal.logicalTailValue(), s.wal.logicalHeadValue(); sb.WALTail != tail || sb.WALHead != head {
			t.Fatalf("round %d: persisted tail=%d head=%d, writer tail=%d head=%d; the writer may reuse bytes the persisted tail still retains", round, sb.WALTail, sb.WALHead, tail, head)
		}
	}
}

// Recovery that moves the writer's bounds persists them before any append,
// so the writer never reuses bytes the on-disk superblock still declares
// retained.
func TestWALStoreRecoverPersistsTheRecoveredBounds(t *testing.T) {
	const blockSize = 4096
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, blockSize, 24*1024)
	s.DisableAutoFlushForRecoveryTest()
	if _, err := s.Write(0, makeBlock(blockSize, 0x30)); err != nil { // LSN 1
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(1, makeBlock(blockSize, 0x31)); err != nil { // LSN 2, synced, unflushed
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	// the superblock as an older flush left it: ckpt 1, tail before LSN 1
	sb := *s.sb
	sb.WALCheckpointLSN, sb.WALTail, sb.WALHead = 1, 0, uint64(walEntryHeaderSize+blockSize)
	buf := newSimpleByteBuf()
	if _, err := sb.writeTo(buf); err != nil {
		t.Fatal(err)
	}
	if _, err := s.fd.WriteAt(buf.bytes(), 0); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	disk := diskSuperblockForTest(t, path)
	if tail, head := r.wal.logicalTailValue(), r.wal.logicalHeadValue(); disk.WALTail != tail || disk.WALHead != head {
		t.Fatalf("after Recover: persisted tail=%d head=%d, writer tail=%d head=%d; want the recovered bounds persisted", disk.WALTail, disk.WALHead, tail, head)
	}
	assertRecoveryHardeningBlock(t, r, 1, makeBlock(blockSize, 0x31))
}

// The writer's space check uses the durable tail: an in-memory release that
// is not yet persisted does not make its bytes reusable.
func TestWALWriterReuseIsBoundedByTheDurableTail(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	const walSize = 4096
	entry := func() *walEntry {
		return &walEntry{LSN: 1, Type: walEntryWrite, LBA: 0, Length: 64, Data: make([]byte, 64)}
	}
	size, err := entry().encodedSize()
	if err != nil {
		t.Fatal(err)
	}
	n := uint64(walSize) / uint64(size)
	w := newWALWriter(f, 0, walSize, 0, 0, nil)
	for i := uint64(0); i < n; i++ {
		if _, err := w.append(entry()); err != nil {
			t.Fatalf("append %d of %d: %v", i, n, err)
		}
	}
	w.advanceTailPastEntry(0, uint64(size)) // released in memory only
	if _, err := w.append(entry()); !errors.Is(err, errWALFull) {
		t.Fatalf("append into bytes released only in memory: err=%v; want errWALFull until the tail is persisted", err)
	}
}

// A full flush persists head == tail (an empty WAL at that position). Sync
// does not persist the head, so synced appends follow it there; recovery must
// scan from the persisted position, not from offset 0 (claude05's S3 trace:
// a torn unsynced record wrapped to 0 must not hide the synced run).
func TestWALStoreFullFlushThenTornWrapKeepsSyncedRecords(t *testing.T) {
	const (
		blockSize = 4096
		walSize   = 24 * 1024
	)
	S := uint64(walEntryHeaderSize + blockSize)
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, blockSize, walSize)
	s.DisableAutoFlushForRecoveryTest()
	if _, err := s.Write(0, makeBlock(blockSize, 0x10)); err != nil { // LSN 1
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	for lba := uint32(1); lba <= 4; lba++ { // LSN 2..5, synced
		if _, err := s.Write(lba, makeBlock(blockSize, byte(0x10+lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(5, makeBlock(blockSize, 0x66)); err != nil { // LSN 6, unsynced, wraps
		t.Fatal(err)
	}
	ents := recoverySnapshotEntriesByLBA(s.dm.snapshot())
	if ents[5].WALOffset != 0 || ents[1].WALOffset != S {
		t.Fatalf("layout: LSN 6 at %d, LSN 2 at %d; want 0 and S", ents[5].WALOffset, ents[1].WALOffset)
	}
	walOff := s.sb.WALOffset
	if _, err := s.fd.WriteAt(make([]byte, walSize-5*S), int64(walOff+5*S)); err != nil { // padding not on disk
		t.Fatal(err)
	}
	if _, err := s.fd.WriteAt(make([]byte, blockSize/2), int64(walOff+S-blockSize/2)); err != nil { // LSN 6 torn
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	for lba := uint32(1); lba <= 4; lba++ {
		assertRecoveryHardeningBlock(t, r, lba, makeBlock(blockSize, byte(0x10+lba)))
	}
}

// codex01 #159 P2-S2: the batch planner (with a padding wrap) counts from the
// durable tail too: refused while the release is only in memory or the
// bounds are recovery-pending, accepted once the tail is persisted.
func TestWALWriterBatchReuseIsBoundedByTheDurableTail(t *testing.T) {
	const walSize = 4096
	mk := func(lsn uint64) walEntry {
		return walEntry{LSN: lsn, Type: walEntryWrite, LBA: lsn, Length: 64, Data: make([]byte, 64)}
	}
	e0 := mk(0)
	size, err := e0.encodedSize()
	if err != nil {
		t.Fatal(err)
	}
	sz := uint64(size)
	n := uint64(walSize) / sz // the last walSize%sz bytes force a padding wrap
	fill := func(t *testing.T) (*walWriter, *os.File) {
		f, err := os.CreateTemp(t.TempDir(), "wal")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.Close() })
		if err := f.Truncate(walSize); err != nil {
			t.Fatal(err)
		}
		w := newWALWriter(f, 0, walSize, 0, 0, nil)
		for i := uint64(0); i < n; i++ {
			e := mk(i + 1)
			if _, err := w.append(&e); err != nil {
				t.Fatalf("fill %d: %v", i, err)
			}
		}
		return w, f
	}
	batch := func() []walEntry { return []walEntry{mk(n + 1), mk(n + 2)} }
	refused := func(t *testing.T, w *walWriter, f *os.File, why string) {
		t.Helper()
		head := w.logicalHeadValue()
		tailBytes := make([]byte, walSize-n*sz)
		if _, err := f.ReadAt(tailBytes, int64(n*sz)); err != nil {
			t.Fatal(err)
		}
		if _, err := w.appendBatch(batch()); !errors.Is(err, errWALFull) {
			t.Fatalf("%s: appendBatch err=%v; want errWALFull", why, err)
		}
		after := make([]byte, len(tailBytes))
		if _, err := f.ReadAt(after, int64(n*sz)); err != nil {
			t.Fatal(err)
		}
		if w.logicalHeadValue() != head || !bytes.Equal(after, tailBytes) {
			t.Fatalf("%s: a refused batch moved the head or wrote padding", why)
		}
	}
	accepted := func(t *testing.T, w *walWriter) {
		t.Helper()
		offs, err := w.appendBatch(batch())
		if err != nil {
			t.Fatalf("after the tail is persisted: appendBatch: %v", err)
		}
		if len(offs) != 2 || offs[0] != 0 || offs[1] != sz {
			t.Fatalf("offsets %v; want the batch wrapped to [0 %d]", offs, sz)
		}
	}

	t.Run("released in memory only", func(t *testing.T) {
		w, f := fill(t)
		w.advanceTailPastEntry(0, 2*sz)
		refused(t, w, f, "in-memory release")
		w.tailPersisted(2 * sz)
		accepted(t, w)
	})
	t.Run("recovered bounds pending", func(t *testing.T) {
		w, f := fill(t)
		w.tailPersisted(2 * sz) // the space is free on disk; only the pending flag refuses it
		if err := w.restoreRecoveredBounds(n*sz, 2*sz); err != nil {
			t.Fatal(err)
		}
		refused(t, w, f, "durablePending")
		single := mk(n + 1)
		if _, err := w.append(&single); !errors.Is(err, errWALFull) {
			t.Fatalf("durablePending: append err=%v; want errWALFull", err)
		}
		w.tailPersisted(2 * sz)
		accepted(t, w)
	})
}

// codex01 #159 P2-S3: when Recover cannot persist the rebuilt bounds (the
// superblock write or its fsync fails), it fails, admits no append, and a
// same-instance retry persists them; later writes survive a reopen exactly.
func TestWALStoreRecoverBoundsPersistFailureRefusesAppendsUntilRetry(t *testing.T) {
	const blockSize = 4096
	S := uint64(walEntryHeaderSize + blockSize)
	errInjected := errors.New("injected superblock failure")
	for _, seam := range []string{"write", "sync"} {
		t.Run(seam, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.bin")
			s := createWALStoreWithWALSizeForTest(t, path, 64, blockSize, 24*1024)
			s.DisableAutoFlushForRecoveryTest()
			crashed := false
			t.Cleanup(func() {
				if !crashed {
					_ = s.Close()
				}
			})
			if _, err := s.Write(0, makeBlock(blockSize, 0x30)); err != nil { // LSN 1
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(1, makeBlock(blockSize, 0x31)); err != nil { // LSN 2, synced, unflushed
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			sb := *s.sb // an older flush's superblock: the bounds Recover must rebuild and persist
			sb.WALCheckpointLSN, sb.WALTail, sb.WALHead = 1, 0, S
			buf := newSimpleByteBuf()
			if _, err := sb.writeTo(buf); err != nil {
				t.Fatal(err)
			}
			if _, err := s.fd.WriteAt(buf.bytes(), 0); err != nil {
				t.Fatal(err)
			}
			crashRecoveryHardeningStore(t, s)
			crashed = true

			r, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			r.DisableAutoFlushForRecoveryTest()
			rClosed := false
			t.Cleanup(func() {
				if !rClosed {
					_ = r.Close()
				}
			})
			realWrite, realSync := r.writeSuperblockMetadata, r.syncSuperblockMetadata
			if seam == "write" {
				r.writeSuperblockMetadata = func([]byte) error { return errInjected }
			} else {
				r.syncSuperblockMetadata = func() error { return errInjected }
			}
			if _, err := r.Recover(); !errors.Is(err, errInjected) {
				t.Fatalf("Recover with a failing superblock %s: err=%v; want the injected failure", seam, err)
			}
			head := r.wal.logicalHeadValue()
			r.admissionTimeout = 200 * time.Millisecond
			if _, err := r.Write(5, makeBlock(blockSize, 0x55)); err == nil {
				t.Fatal("Write after a failed Recover succeeded; want it refused until the bounds are persisted")
			}
			if r.wal.logicalHeadValue() != head {
				t.Fatalf("refused Write moved the WAL head %d -> %d", head, r.wal.logicalHeadValue())
			}

			r.writeSuperblockMetadata, r.syncSuperblockMetadata = realWrite, realSync
			live := r
			if seam == "sync" {
				// A failed fsync is sticky (ErrFsyncFailed): no same-instance
				// retry; the bounds are persisted by the recovery of a reopen.
				if _, err := r.Recover(); !errors.Is(err, ErrFsyncFailed) {
					t.Fatalf("same-instance Recover after a failed fsync: %v; want ErrFsyncFailed", err)
				}
				crashRecoveryHardeningStore(t, r)
				rClosed = true
				r2, err := OpenWALStore(path)
				if err != nil {
					t.Fatal(err)
				}
				r2.DisableAutoFlushForRecoveryTest()
				t.Cleanup(func() {
					if live == r2 && !rClosed {
						_ = r2.Close()
					}
				})
				rClosed = false
				live = r2
			}
			if _, err := live.Recover(); err != nil {
				t.Fatalf("Recover retry: %v", err)
			}
			disk := diskSuperblockForTest(t, path)
			if tail, h := live.wal.logicalTailValue(), live.wal.logicalHeadValue(); disk.WALTail != tail || disk.WALHead != h {
				t.Fatalf("after the retry: persisted tail=%d head=%d, writer tail=%d head=%d", disk.WALTail, disk.WALHead, tail, h)
			}
			live.admissionTimeout = 30 * time.Second
			if _, err := live.Write(5, makeBlock(blockSize, 0x55)); err != nil {
				t.Fatal(err)
			}
			if _, err := live.Sync(); err != nil {
				t.Fatal(err)
			}
			crashRecoveryHardeningStore(t, live)
			rClosed = true

			q, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			q.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = q.Close() })
			if _, err := q.Recover(); err != nil {
				t.Fatalf("reopen Recover: %v", err)
			}
			assertRecoveryHardeningBlock(t, q, 1, makeBlock(blockSize, 0x31))
			assertRecoveryHardeningBlock(t, q, 5, makeBlock(blockSize, 0x55))
		})
	}
}
