package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
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
