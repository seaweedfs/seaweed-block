package memorywal

import (
	"bytes"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

const (
	blockSize = 4096
	numBlocks = 64
)

func mkBlock(marker byte) []byte {
	out := make([]byte, blockSize)
	for i := range out {
		out[i] = marker
	}
	return out
}

// TestWrite_AssignsContemporaneousLSN verifies each Write returns
// a fresh strictly-increasing LSN — the property that BlockStore
// satisfies but ScanLBAs cannot expose later (because BlockStore
// loses per-write LSN at scan time).
func TestWrite_AssignsContemporaneousLSN(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	prev := uint64(0)
	for i := uint32(0); i < 5; i++ {
		lsn, err := s.Write(i, mkBlock(byte(i+1)))
		if err != nil {
			t.Fatalf("Write i=%d: %v", i, err)
		}
		if lsn <= prev {
			t.Fatalf("Write i=%d: lsn=%d not > prev=%d", i, lsn, prev)
		}
		prev = lsn
	}
}

// TestRead_LastWriterWins — multiple writes to the same LBA: read
// returns the latest bytes.
func TestRead_LastWriterWins(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	_, _ = s.Write(0, mkBlock(0xAA))
	_, _ = s.Write(0, mkBlock(0xBB))
	_, _ = s.Write(0, mkBlock(0xCC))
	got, _ := s.Read(0)
	if got[0] != 0xCC {
		t.Fatalf("Read lba=0: got[0]=%02x want CC (last writer)", got[0])
	}
}

// TestScanLBAs_EmitsWriteTimeLSN — the headline property. Each
// emitted RecoveryEntry carries the LSN that was assigned when the
// write happened, not a synthesized scan-time LSN.
func TestScanLBAs_EmitsWriteTimeLSN(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	writeLSN := make([]uint64, 5)
	for i := 0; i < 5; i++ {
		lsn, _ := s.Write(uint32(i), mkBlock(byte(i+1)))
		writeLSN[i] = lsn
	}

	var emitted []storage.RecoveryEntry
	err := s.ScanLBAs(0, func(e storage.RecoveryEntry) error {
		dup := storage.RecoveryEntry{
			LSN: e.LSN, LBA: e.LBA, Flags: e.Flags,
			Data: append([]byte(nil), e.Data...),
		}
		emitted = append(emitted, dup)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanLBAs: %v", err)
	}
	if len(emitted) != 5 {
		t.Fatalf("emitted %d entries want 5", len(emitted))
	}
	for i, e := range emitted {
		if e.LSN != writeLSN[i] {
			t.Errorf("entry %d: LSN=%d want %d (write-time)", i, e.LSN, writeLSN[i])
		}
		if e.LBA != uint32(i) {
			t.Errorf("entry %d: LBA=%d want %d", i, e.LBA, i)
		}
		if e.Data[0] != byte(i+1) {
			t.Errorf("entry %d: Data[0]=%02x want %02x", i, e.Data[0], byte(i+1))
		}
	}
}

// TestScanLBAs_ThreeWritesSameLBA_EmitsThreeEntries — V2-faithful
// per-LSN replay: 3 writes to the same LBA produce 3 entries (not 1
// dedup'd entry like the state-convergence sub-mode).
func TestScanLBAs_ThreeWritesSameLBA_EmitsThreeEntries(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	for _, marker := range []byte{0xAA, 0xBB, 0xCC} {
		_, _ = s.Write(7, mkBlock(marker))
	}
	count := 0
	err := s.ScanLBAs(0, func(e storage.RecoveryEntry) error {
		if e.LBA != 7 {
			t.Errorf("entry %d: LBA=%d want 7", count, e.LBA)
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanLBAs: %v", err)
	}
	if count != 3 {
		t.Fatalf("emitted %d entries want 3 (V2-faithful per-LSN)", count)
	}
}

// TestScanLBAs_FromLSNExclusive — fromLSN is exclusive: an entry
// with LSN == fromLSN is NOT emitted.
func TestScanLBAs_FromLSNExclusive(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	lsn1, _ := s.Write(0, mkBlock(0xA1))
	lsn2, _ := s.Write(1, mkBlock(0xA2))
	lsn3, _ := s.Write(2, mkBlock(0xA3))

	var seen []uint64
	err := s.ScanLBAs(lsn1, func(e storage.RecoveryEntry) error {
		seen = append(seen, e.LSN)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanLBAs: %v", err)
	}
	want := []uint64{lsn2, lsn3}
	if len(seen) != 2 || seen[0] != want[0] || seen[1] != want[1] {
		t.Fatalf("seen=%v want %v (fromLSN exclusive)", seen, want)
	}
}

// TestScanLBAs_AtHead_EmptyEmit — scanning from current head returns
// no entries (scan exhaust, not error).
func TestScanLBAs_AtHead_EmptyEmit(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	for i := 0; i < 3; i++ {
		_, _ = s.Write(uint32(i), mkBlock(byte(i+1)))
	}
	_, _, h := s.Boundaries()
	calls := 0
	err := s.ScanLBAs(h, func(e storage.RecoveryEntry) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanLBAs at head: %v", err)
	}
	if calls != 0 {
		t.Fatalf("at head: callback called %d times, want 0", calls)
	}
}

// TestAdvanceWALTail_RecycledGate — after trimming records below
// newTail, a scan from below returns the typed RecoveryFailure with
// Kind=WALRecycled, and errors.Is(err, ErrWALRecycled) holds for
// callers that haven't migrated to the typed envelope yet.
func TestAdvanceWALTail_RecycledGate(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	_, _ = s.Write(0, mkBlock(0x01))
	_, _ = s.Write(1, mkBlock(0x02))
	_, _ = s.Write(2, mkBlock(0x03))

	// Trim records with LSN < 3, leaving only LSN=3.
	s.AdvanceWALTail(3)

	err := s.ScanLBAs(0, func(e storage.RecoveryEntry) error {
		t.Errorf("callback should not be invoked when scan is recycled")
		return nil
	})
	if err == nil {
		t.Fatal("ScanLBAs(0) after AdvanceWALTail(3): want WALRecycled error")
	}

	// Typed envelope.
	var rf *storage.RecoveryFailure
	if !errors.As(err, &rf) {
		t.Fatalf("expected typed *storage.RecoveryFailure, got %T", err)
	}
	if rf.Kind != storage.StorageRecoveryFailureWALRecycled {
		t.Errorf("Kind=%s want WALRecycled", rf.Kind)
	}

	// Backwards-compat: errors.Is(err, ErrWALRecycled) still works.
	if !errors.Is(err, storage.ErrWALRecycled) {
		t.Error("errors.Is(err, ErrWALRecycled) should hold via Unwrap")
	}

	// Scanning from a non-recycled position still works.
	count := 0
	err = s.ScanLBAs(2, func(e storage.RecoveryEntry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanLBAs(2) after trim to 3: %v", err)
	}
	if count != 1 {
		t.Errorf("ScanLBAs(2) emitted %d, want 1 (only LSN=3)", count)
	}
}

// TestRecoveryMode_WALReplay — memorywal must report
// RecoveryModeWALReplay so callers that need V2-faithful per-LSN
// semantics select it correctly.
func TestRecoveryMode_WALReplay(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	if got := s.RecoveryMode(); got != storage.RecoveryModeWALReplay {
		t.Fatalf("RecoveryMode=%s want %s", got, storage.RecoveryModeWALReplay)
	}
}

// TestAppliedLSNs_TracksHighestPerLBA — unlike BlockStore (which
// returns ErrAppliedLSNsNotTracked), memorywal computes per-LBA
// highest LSN from its log.
func TestAppliedLSNs_TracksHighestPerLBA(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	lsnA1, _ := s.Write(0, mkBlock(0xA1))
	_, _ = s.Write(1, mkBlock(0xB1))
	lsnA2, _ := s.Write(0, mkBlock(0xA2)) // higher LSN on lba 0
	lsnB2, _ := s.Write(1, mkBlock(0xB2))

	got, err := s.AppliedLSNs()
	if err != nil {
		t.Fatalf("AppliedLSNs: %v", err)
	}
	if got[0] != lsnA2 {
		t.Errorf("lba 0: got %d want %d (latest of {%d, %d})", got[0], lsnA2, lsnA1, lsnA2)
	}
	if got[1] != lsnB2 {
		t.Errorf("lba 1: got %d want %d", got[1], lsnB2)
	}
	if got[2] != 0 {
		t.Errorf("lba 2 (never written): got %d want 0", got[2])
	}
}

// TestBoundaries_RSH — R/S/H reflect synced/retain-start/head.
func TestBoundaries_RSH(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	if r, sb, h := s.Boundaries(); r != 0 || sb != 0 || h != 0 {
		t.Fatalf("fresh: R/S/H=%d/%d/%d want 0/0/0", r, sb, h)
	}
	for i := 0; i < 3; i++ {
		_, _ = s.Write(uint32(i), mkBlock(byte(i+1)))
	}
	r, sb, h := s.Boundaries()
	if r != 0 {
		t.Errorf("R after writes (no Sync): got %d want 0", r)
	}
	if sb != 1 {
		t.Errorf("S after writes: got %d want 1", sb)
	}
	if h != 3 {
		t.Errorf("H after writes: got %d want 3", h)
	}

	// Sync advances R.
	rNew, _ := s.Sync()
	if rNew != 3 {
		t.Errorf("Sync: got %d want 3", rNew)
	}
	r, _, _ = s.Boundaries()
	if r != 3 {
		t.Errorf("R post-Sync: got %d want 3", r)
	}

	// AdvanceWALTail moves S forward.
	s.AdvanceWALTail(2)
	_, sb, _ = s.Boundaries()
	if sb != 2 {
		t.Errorf("S post-AdvanceWALTail: got %d want 2", sb)
	}
}

// TestApplyEntry_PreservesGivenLSN — ApplyEntry uses the caller's
// LSN (not a fresh one), modeling the receiver-side replication
// path where entries arrive with primary's LSN.
func TestApplyEntry_PreservesGivenLSN(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	if err := s.ApplyEntry(5, mkBlock(0x55), 100); err != nil {
		t.Fatalf("ApplyEntry: %v", err)
	}
	got, _ := s.Read(5)
	if got[0] != 0x55 {
		t.Errorf("Read(5): got[0]=%02x want 55", got[0])
	}
	// Subsequent Write uses nextLSN > applied LSN.
	lsn, _ := s.Write(6, mkBlock(0x66))
	if lsn <= 100 {
		t.Errorf("Write after ApplyEntry(lsn=100): got lsn=%d, expected > 100", lsn)
	}
	// ScanLBAs emits the applied entry with LSN=100.
	var seen []uint64
	_ = s.ScanLBAs(0, func(e storage.RecoveryEntry) error {
		seen = append(seen, e.LSN)
		return nil
	})
	if len(seen) != 2 || seen[0] != 100 {
		t.Errorf("ScanLBAs: %v want first LSN=100", seen)
	}
}

// TestAllBlocks_LastWriterWins_FiltersZeros — AllBlocks must dedup
// to last-writer-wins per LBA and filter out all-zero blocks (per
// LogicalStorage doc).
func TestAllBlocks_LastWriterWins_FiltersZeros(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	_, _ = s.Write(0, mkBlock(0xAA))
	_, _ = s.Write(1, mkBlock(0xBB))
	_, _ = s.Write(0, mkBlock(0xCC)) // overwrites lba 0
	_, _ = s.Write(2, make([]byte, blockSize)) // zero block — should be filtered

	all := s.AllBlocks()
	if len(all) != 2 {
		t.Fatalf("AllBlocks: got %d want 2 (lba 2 should be filtered as zero)", len(all))
	}
	if !bytes.Equal(all[0], mkBlock(0xCC)) {
		t.Errorf("lba 0: not last-writer-wins")
	}
	if !bytes.Equal(all[1], mkBlock(0xBB)) {
		t.Errorf("lba 1: wrong content")
	}
	if _, ok := all[2]; ok {
		t.Errorf("lba 2: zero block should have been filtered")
	}
}

// TestClose_BlocksWriteAndApply — after Close, mutations error out.
func TestClose_BlocksWriteAndApply(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	_, _ = s.Write(0, mkBlock(0x01))
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := s.Write(1, mkBlock(0x02)); err == nil {
		t.Error("Write after Close: want error")
	}
	if err := s.ApplyEntry(2, mkBlock(0x03), 50); err == nil {
		t.Error("ApplyEntry after Close: want error")
	}
}

// TestArgValidation — out-of-range LBA + wrong-size data are
// rejected at the API boundary.
func TestArgValidation(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	if _, err := s.Write(numBlocks, mkBlock(0x01)); err == nil {
		t.Error("Write past numBlocks: want error")
	}
	if _, err := s.Write(0, make([]byte, blockSize-1)); err == nil {
		t.Error("Write wrong size: want error")
	}
	if _, err := s.Read(numBlocks); err == nil {
		t.Error("Read past numBlocks: want error")
	}
	if err := s.ApplyEntry(numBlocks, mkBlock(0x01), 1); err == nil {
		t.Error("ApplyEntry past numBlocks: want error")
	}
}
