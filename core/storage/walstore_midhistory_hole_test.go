package storage

import (
	"path/filepath"
	"strings"
	"testing"
)

// Issue #51, walstore interim (claude05's ruling 2026-11-16 06:14Z): a
// plain power loss must never become an outage, so a WAL hole followed by
// valid records does not fail recovery. It is detected, logged, counted and
// reported as the typed warning wal_hole_suspected, and nothing after the
// hole is replayed in any range.

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

// (a) A corrupted mid-history synced record: recovery succeeds to the
// prefix before it, warns (typed, counted), and replays nothing after it.
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

// (c) No replay past a hole across wrapped ranges: the older record at the
// high end of the WAL is corrupted; the newer batch that wrapped to offset
// 0 must not be replayed.
func TestWALStoreRecoverNoReplayPastAHoleAcrossWrappedRanges(t *testing.T) {
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
	if _, err := s.Write(20, makeBlock(blockSize, 0xA4)); err != nil { // high end
		t.Fatal(err)
	}
	s.enableMultiBlockRecordsForTest(true)
	if _, err := s.WriteBatch(24, [][]byte{makeBlock(blockSize, 0xB5), makeBlock(blockSize, 0xB6)}); err != nil { // wraps to 0
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	offs := walOffsetsByLBA(s)
	if offs[24] != 0 || offs[20] == 0 {
		t.Fatalf("offsets high=%d low=%d; want the batch wrapped to 0", offs[20], offs[24])
	}
	flipDataByte(t, s, offs[20])
	r, _, err := reopenAfterCrash(t, s, path)
	if err != nil {
		t.Fatalf("Recover: %v; want no outage", err)
	}
	if !holeWarning(r) {
		t.Fatalf("warnings %v; want %s", r.RecoveryWarnings(), WarningWALHoleSuspected)
	}
	for _, lba := range []uint32{24, 25} {
		got, err := r.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if got[0] == 0xB5 || got[0] == 0xB6 {
			t.Fatalf("LBA %d replayed from past the hole (byte %02x)", lba, got[0])
		}
	}
}
