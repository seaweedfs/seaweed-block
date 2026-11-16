package storage

import (
	"path/filepath"
	"strings"
	"testing"
)

// Issue #51, walstore: pure
// detection. A WAL hole followed by valid records is logged, counted and
// reported as the typed warning wal_hole_suspected; nothing about what is
// replayed, where replay stops, or whether the volume opens changes.

// writeSyncedEntries writes one block to each of lbas (in order), syncs,
// and returns the store with auto-flush off (nothing checkpointed) and
// each LBA's WAL offset.
func writeSyncedEntries(t *testing.T, lbas []uint32) (*WALStore, string, map[uint64]uint64) {
	t.Helper()
	const blockSize = 4096
	path := filepath.Join(t.TempDir(), "store.bin")
	s, err := CreateWALStore(path, 16, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	for i, lba := range lbas {
		if _, err := s.Write(lba, makeBlock(blockSize, byte(0xB0+i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	return s, path, walOffsetsByLBA(s)
}

func walOffsetsByLBA(s *WALStore) map[uint64]uint64 {
	offs := map[uint64]uint64{}
	for _, e := range s.dm.snapshot() {
		offs[e.LBA] = e.WALOffset
	}
	return offs
}

// flipDataByte corrupts one payload byte of the entry at walOffset without
// fixing its CRC.
func flipDataByte(t *testing.T, s *WALStore, walOffset uint64) {
	t.Helper()
	off := int64(s.sb.WALOffset+walOffset) + int64(walEntryHeaderSize) + 100
	b := make([]byte, 1)
	if _, err := s.fd.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if _, err := s.fd.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

func reopenAfterCrash(t *testing.T, s *WALStore, path string) (*WALStore, uint64, error) {
	t.Helper()
	crashRecoveryHardeningStore(t, s)
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	lsn, rerr := r.Recover()
	return r, lsn, rerr
}

func holeWarning(s *WALStore) bool {
	for _, w := range s.RecoveryWarnings() {
		if strings.HasPrefix(w, WarningWALHoleSuspected+":") {
			return true
		}
	}
	return false
}

// (a) A corrupted mid-history synced record: recovery opens as before
// (this range stops at it), and warns (typed, counted).
func TestWALStoreRecoverWarnsOnMidHistoryHole(t *testing.T) {
	s, path, offs := writeSyncedEntries(t, []uint32{0, 1, 2, 3})
	flipDataByte(t, s, offs[1])
	r, lsn, err := reopenAfterCrash(t, s, path)
	if err != nil || lsn != 1 {
		t.Fatalf("Recover after a mid-history hole = %d, %v; want 1, nil (no outage)", lsn, err)
	}
	if !holeWarning(r) || r.HolesSuspected() != 1 {
		t.Fatalf("warnings %v holes %d; want %s, counted once", r.RecoveryWarnings(), r.HolesSuspected(), WarningWALHoleSuspected)
	}
	if got := r.dm.len(); got != 1 {
		t.Fatalf("dirty entries replayed = %d; want 1 (nothing after the hole)", got)
	}
	assertRecoveryHardeningBlock(t, r, 0, makeBlock(4096, 0xB0))
}

// (b) A power loss with an unsynced, out-of-order tail: an earlier
// unsynced record torn, a later one written back. No outage: recovery
// succeeds with the synced prefix intact.
func TestWALStoreRecoverSurvivesAnOutOfOrderUnsyncedTail(t *testing.T) {
	s, path, _ := writeSyncedEntries(t, []uint32{0, 1}) // synced
	for i, lba := range []uint32{2, 3} {                // unsynced
		if _, err := s.Write(lba, makeBlock(4096, byte(0xC0+i))); err != nil {
			t.Fatal(err)
		}
	}
	offs := walOffsetsByLBA(s)
	flipDataByte(t, s, offs[2]) // LBA 2 torn, LBA 3 reached the disk
	r, lsn, err := reopenAfterCrash(t, s, path)
	if err != nil || lsn != 2 {
		t.Fatalf("Recover after an out-of-order unsynced tail = %d, %v; want 2, nil", lsn, err)
	}
	for i, lba := range []uint32{0, 1} {
		assertRecoveryHardeningBlock(t, r, lba, makeBlock(4096, byte(0xB0+i)))
	}
}

// A torn tail (nothing valid after it) recovers without a warning.
func TestWALStoreRecoverKeepsATornTail(t *testing.T) {
	s, path, offs := writeSyncedEntries(t, []uint32{0, 1, 2, 3})
	flipDataByte(t, s, offs[3])
	r, lsn, err := reopenAfterCrash(t, s, path)
	if err != nil || lsn != 3 || holeWarning(r) {
		t.Fatalf("Recover with a torn tail = %d, %v, warnings %v; want 3, nil, none", lsn, err, r.RecoveryWarnings())
	}
	for i, lba := range []uint32{0, 1, 2} {
		assertRecoveryHardeningBlock(t, r, lba, makeBlock(4096, byte(0xB0+i)))
	}
}

// (c) A hole across wrapped ranges (the older record at the high end
// corrupted, the newer batch wrapped to offset 0) is no outage. Detection
// here is per range: when the newer entries were scanned first there is
// nothing "later" left to find, and telling generations apart across
// ranges (like not replaying past such a hole) is the post-M4 watermark
// work.
func TestWALStoreRecoverWrappedHoleIsNoOutage(t *testing.T) {
	const (
		blockSize = 4096
		walSize   = 24 * 1024
	)
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 64, blockSize, walSize)
	s.DisableAutoFlushForRecoveryTest()
	for lba := uint32(0); lba < 3; lba++ {
		if _, err := s.Write(lba, makeBlock(blockSize, byte(0x10+lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(20, makeBlock(blockSize, 0xA4)); err != nil {
		t.Fatal(err)
	}
	s.enableMultiBlockRecordsForTest(true)
	if _, err := s.WriteBatch(24, [][]byte{makeBlock(blockSize, 0xB5), makeBlock(blockSize, 0xB6)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	offs := walOffsetsByLBA(s)
	flipDataByte(t, s, offs[20])
	r, _, err := reopenAfterCrash(t, s, path)
	if err != nil {
		t.Fatalf("Recover = %v; want no outage", err)
	}
	_ = r
}

// Review finding: a damaged STALE record in a released range (below the
// checkpoint, outside the retained window) must not cost retained data.
// Layout: checkpoint 2, persisted tail at S, head at 3S; LSN 1 (released)
// at 0 damaged, LSN 2 checkpointed at S, LSN 3 synced and unflushed at 2S.
// LSN 3 must be replayed with its exact bytes.
func TestWALStoreRecoverStaleReleasedGarbageKeepsRetainedData(t *testing.T) {
	const blockSize = 4096
	s, path, offs := writeSyncedEntries(t, []uint32{0, 1, 2})
	S := uint64(walEntryHeaderSize + blockSize)
	if offs[0] != 0 || offs[1] != S || offs[2] != 2*S {
		t.Fatalf("offsets %v; want 0, S, 2S", offs)
	}
	sb := *s.sb
	sb.WALCheckpointLSN, sb.WALTail, sb.WALHead = 2, S, 3*S
	buf := newSimpleByteBuf()
	if _, err := sb.writeTo(buf); err != nil {
		t.Fatal(err)
	}
	if _, err := s.fd.WriteAt(buf.bytes(), 0); err != nil {
		t.Fatal(err)
	}
	if err := s.fd.Sync(); err != nil {
		t.Fatal(err)
	}
	flipDataByte(t, s, offs[0]) // the released LSN 1
	r, _, err := reopenAfterCrash(t, s, path)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertRecoveryHardeningBlock(t, r, 2, makeBlock(blockSize, 0xB2)) // LSN 3, exact bytes
}
