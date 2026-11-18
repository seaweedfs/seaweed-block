package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Regression tests for the two walstore "Open items" in docs/spec/README.md
// (S3), run against walstore format v3:
//
//   Item 1: a corrupted synced WAL record made recovery stop that range and
//   silently drop later synced data while reporting success.
//   Property: every synced write is recovered, or recovery fails closed.
//
//   Item 2 (NoForgedReplay): guest data that decodes as a valid WAL record
//   was replayed after a newer, shorter record wrapped over the start of an
//   old multi-block record. Property: no record the store did not append is
//   replayed.
//
// The same scenarios reproduced both bugs on the v2 format before v3; the
// v2 arms are not kept here because the v2 engine survives only as a frozen
// fixture scheduled for retirement.

// ---------------------------------------------------------------------------
// Item 1: corrupted synced record.

// openFinding1Obs is what a reopen after damaging one synced record shows.
type openFinding1Obs struct {
	recoverErr error
	lost       []uint32 // synced LBAs that do not read their data
}

// check is the I1 property: every synced write is recovered (or
// superseded), or recovery fails closed. A successful recovery that lost a
// synced write is the violation the README recorded.
func (o openFinding1Obs) check() error {
	if o.recoverErr != nil {
		return nil // fail closed
	}
	if len(o.lost) > 0 {
		return fmt.Errorf("recovery reported success but synced LBAs %v lost", o.lost)
	}
	return nil
}

// finding1V3 writes LBAs 1..n (one write per Sync, so one batch each),
// "crashes" (committer stopped, file closed, no further write), flips one
// payload byte of the record with LSN damaged, reopens and recovers.
func finding1V3(t *testing.T, mode WMode, n uint32, damaged uint64) openFinding1Obs {
	t.Helper()
	s, path := newStoreV3(t, mode)
	writeSynced(t, s, 1, n)
	crashRecoveryHardeningStore(t, s)
	damageRecord(t, path, damaged)
	r, err := reopenV3(t, path)
	if err != nil {
		t.Logf("v3 mode %d: Recover: %v (wal_corrupt=%v)", mode, err, isWALCorrupt(err))
		return openFinding1Obs{recoverErr: err}
	}
	defer r.Close()
	var o openFinding1Obs
	for lba := uint32(1); lba <= n; lba++ {
		if !bytes.Equal(readV3(t, r, lba), blk(byte(lba))) {
			o.lost = append(o.lost, lba)
		}
	}
	return o
}

// TestOpenFinding1_CorruptMidSyncedRecord_V3 is item 1 as the README states
// it: synced LSNs 1..5, LSN 2's record corrupted. Recovery must not report
// success while LSNs 3..5 (synced, after the bad record) are dropped. v3
// refuses when the contiguous prefix R is below W* = max(slot W, max trusted
// DurableLSN) (recovery_v3.go decideRecoveryV3): both W modes fail closed
// with a typed wal_corrupt.
func TestOpenFinding1_CorruptMidSyncedRecord_V3(t *testing.T) {
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		t.Run(fmt.Sprintf("mode%d", mode), func(t *testing.T) {
			o := finding1V3(t, mode, 5, 2)
			if err := o.check(); err != nil {
				t.Fatalf("v3 mode %d: %v", mode, err)
			}
			if !isWALCorrupt(o.recoverErr) {
				t.Fatalf("v3 mode %d: want a typed wal_corrupt refusal, got %v", mode, o.recoverErr)
			}
		})
	}
}

// TestOpenFinding1_CorruptNewestSyncedRecord_StrictV3 is the README's harder
// case, "a corrupt *last* synced record leaves no valid witness": synced
// LSNs 1..3, the newest (LSN 3, its own acknowledged batch) corrupted, no
// later Sync or idle refresh. Mode A (Strict) publishes W = H in the slot
// before acknowledging, so the slot is the witness and recovery fails
// closed. Mode C (the default) is not asserted here: there the newest
// acknowledged batch is the designed window of INV-WAL-V3-W-001
// (docs/design/invariants.md), closed by the next Sync, the idle refresh or
// SetWMode(A), and pinned by TestWALStoreV3ModeCWindowIsExactlyTheNewestBatch.
func TestOpenFinding1_CorruptNewestSyncedRecord_StrictV3(t *testing.T) {
	o := finding1V3(t, WModeStrict, 3, 3)
	if err := o.check(); err != nil {
		t.Fatalf("v3 mode A: %v", err)
	}
	if !isWALCorrupt(o.recoverErr) {
		t.Fatalf("v3 mode A: want a typed wal_corrupt refusal, got %v", o.recoverErr)
	}
}

// ---------------------------------------------------------------------------
// Item 2: forged replay.
//
// Layout (WAL 64 KiB, 4 KiB blocks), as in the README repro:
//   1. A 3-block batch (LBAs 4,5,6) is the first record, at WAL offset 0.
//      Its guest data carries, at payload offset 4104 (block 1, byte 8), an
//      encoded single-block v3 write record: LBA 7, data 0xEE.., LSN 999
//      (or, for the placement control, the next LSN). 4104 is where a
//      1-block record that starts at offset 0 ends (62-byte prefix + 4096 +
//      8-byte trailer = 4166 = 62 + 4104).
//   2. Twelve 1-block filler writes (the first writes LBA 7 = 0x77..), each
//      synced and flushed, so the checkpoint and the WAL tail move past the
//      batch.
//   3. The 13th write (LBA 0, 0xAA..) does not fit before the ring end,
//      wraps, and lands at WAL offset 0: the newer 1-block record N now
//      covers the batch's header and first 4104 payload bytes, and the
//      forged bytes start exactly where N ends.
//   4. Crash (or clean Close), reopen, Recover.
// The test checks the placement (N at offset 0, forged bytes intact right
// after N) before it checks the outcome, so a pass is not an artefact of a
// layout that never puts the forged bytes in the scan's path.

const (
	forgedLBA     = 7
	forgedWALSize = 64 * 1024
	forgedRecSize = walEntryHeaderSizeV3 + v3TestBlock // a 1-block write record
)

type forgedObs struct {
	recoverErr error
	frontier   uint64 // Recover's returned frontier
	nextLSN    uint64 // LSN the store assigns next
	lba7       []byte
	nLSN       uint64 // LSN of the wrapping record N
}

// check is NoForgedReplay as the README states it: LBA 7 must not read the
// forged data, and the frontier must not move past N.
func (o forgedObs) check() error {
	if o.recoverErr != nil {
		return nil // fail closed is not a forged replay
	}
	var errs []string
	if bytes.Equal(o.lba7, blk(0xEE)) {
		errs = append(errs, "LBA 7 reads the forged data")
	} else if !bytes.Equal(o.lba7, blk(0x77)) {
		errs = append(errs, fmt.Sprintf("LBA 7 reads neither its data nor the forged data (byte %#x)", o.lba7[0]))
	}
	if o.frontier != o.nLSN {
		errs = append(errs, fmt.Sprintf("recovered frontier %d, want %d (N)", o.frontier, o.nLSN))
	}
	if o.nextLSN != o.nLSN+1 {
		errs = append(errs, fmt.Sprintf("next LSN %d, want %d", o.nextLSN, o.nLSN+1))
	}
	if len(errs) > 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

// forgedRecordV3 encodes the guest-embedded record. The guest knows the v3
// codec and the write history, so it can predict the logical cursor where
// the scan will look (one lap plus N). What it cannot know is the volume's
// random 128-bit nonce, which lives only in the descriptor slots, never in
// the guest-addressable extent. A realistic forger therefore guesses the
// nonce; the oracle (placement control) uses the true one and the next
// contiguous LSN.
func forgedRecordV3(t *testing.T, s *WALStore, oracle bool, nextLSN uint64) []byte {
	t.Helper()
	d := s.v3.pub.current()
	nonce := [16]byte{}
	for i := range nonce {
		nonce[i] = 0x5A
	}
	lsn := uint64(999)
	if oracle {
		nonce, lsn = d.Nonce, nextLSN
	}
	e := walEntryV3{
		LSN: lsn, Type: walEntryWrite, LBA: forgedLBA, Length: v3TestBlock,
		LogicalStart: forgedWALSize + forgedRecSize,
		Nonce:        nonce, Data: blk(0xEE),
	}
	b, err := encodeWALEntryV3(e, d.BlockSize, d.VolumeSize/uint64(d.BlockSize))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// runForgedV3 runs the layout above and returns what the reopened store shows.
func runForgedV3(t *testing.T, mode WMode, oracle, cleanClose bool) forgedObs {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store")
	s, err := createMappedWALStoreV3ForTest(path, 16, v3TestBlock, forgedWALSize, mode, 4)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	const fillers = 12
	// Batch LSNs 1..3, fillers 4..15, N = 16; the oracle record takes 17.
	const wantN = 3 + fillers + 1
	forged := forgedRecordV3(t, s, oracle, wantN+1)
	payload := bytes.Repeat([]byte{0x55}, 3*v3TestBlock)
	const forgedAt = forgedRecSize - walEntryPrefixSizeV3 // payload offset where N ends
	copy(payload[forgedAt:], forged)
	blocks := [][]byte{payload[:v3TestBlock], payload[v3TestBlock : 2*v3TestBlock], payload[2*v3TestBlock:]}
	lsns, err := s.WriteBatch(4, blocks)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if off, _, _, _, ok := s.dm.get(4); !ok || off != 0 {
		t.Fatalf("batch not at WAL offset 0 (off=%d ok=%v)", off, ok)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < fillers; i++ {
		lba, data := uint32(i%4), blk(byte(0x10+i))
		if i == 0 {
			lba, data = forgedLBA, blk(0x77)
		}
		if _, err := s.Write(lba, data); err != nil {
			t.Fatalf("filler %d: %v", i, err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
	}
	nLSN, err := s.Write(0, blk(0xAA))
	if err != nil {
		t.Fatalf("wrapping write N: %v", err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if nLSN != wantN || lsns[0] != 1 {
		t.Fatalf("LSN layout batch=%v N=%d, want batch 1..3 and N=%d", lsns, nLSN, wantN)
	}
	// Placement: N is at WAL offset 0 and the forged bytes start where N ends.
	if nOff, _, _, _, ok := s.dm.get(0); !ok || nOff != 0 {
		t.Fatalf("N did not wrap to WAL offset 0 (off=%d ok=%v)", nOff, ok)
	}
	raw := make([]byte, len(forged))
	rf, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = rf.ReadAt(raw, int64(s.v3.pub.current().WALOffset+forgedRecSize))
	_ = rf.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, forged) {
		t.Fatal("the forged record is not intact right after N")
	}

	if cleanClose {
		if err := s.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	} else {
		crashRecoveryHardeningStore(t, s)
	}
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Logf("reopen: %v", err)
		return forgedObs{recoverErr: err, nLSN: nLSN}
	}
	defer r.Close()
	r.DisableAutoFlushForRecoveryTest()
	frontier, err := r.Recover()
	if err != nil {
		t.Logf("Recover: %v", err)
		return forgedObs{recoverErr: err, nLSN: nLSN}
	}
	o := forgedObs{frontier: frontier, nLSN: nLSN}
	if o.lba7, err = r.Read(forgedLBA); err != nil {
		t.Fatal(err)
	}
	if o.nextLSN, err = r.Write(8, blk(0x88)); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	t.Logf("mode %d oracle=%v clean=%v: frontier=%d next=%d lba7[0]=%#x", mode, oracle, cleanClose, o.frontier, o.nextLSN, o.lba7[0])
	return o
}

// TestOpenFinding2_ForgedReplay_V3 asserts NoForgedReplay on v3 for the
// README repro, after a crash and after a clean close, in both W modes.
// The v3 scan trusts a record only if its header carries the volume nonce
// and a LogicalStart equal to the exact expected absolute cursor (which
// also binds the lap), checked before the payload is read (wal_io_v3.go
// scanWALV3Accepted); it stops at the first record that fails, and the drop
// fence zeroes the rest of the ring.
func TestOpenFinding2_ForgedReplay_V3(t *testing.T) {
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		for _, clean := range []bool{false, true} {
			t.Run(fmt.Sprintf("mode%d/clean=%v", mode, clean), func(t *testing.T) {
				o := runForgedV3(t, mode, false, clean)
				if err := o.check(); err != nil {
					t.Fatalf("forged record replayed: %v", err)
				}
				if o.recoverErr != nil {
					t.Fatalf("want a clean recovery (no acknowledged data is damaged), got %v", o.recoverErr)
				}
			})
		}
	}
}

// TestOpenFinding2_V3PlacementControl_TrueNonceIsReplayed is a control, not
// a finding: the same layout with a forged record that carries the volume's
// TRUE nonce, the exact expected cursor and the next contiguous LSN IS
// replayed. It shows that the test above reaches the forged bytes, and that
// what rejects them is exactly the nonce/cursor binding: forging requires
// the per-volume random 128-bit nonce, which the guest cannot read.
func TestOpenFinding2_V3PlacementControl_TrueNonceIsReplayed(t *testing.T) {
	for _, mode := range []WMode{WModeLagged, WModeStrict} {
		t.Run(fmt.Sprintf("mode%d", mode), func(t *testing.T) {
			o := runForgedV3(t, mode, true, false)
			if o.recoverErr != nil {
				t.Fatalf("control: recover: %v", o.recoverErr)
			}
			if !bytes.Equal(o.lba7, blk(0xEE)) || o.frontier != o.nLSN+1 {
				t.Fatalf("control: true-nonce record not replayed (lba7[0]=%#x frontier=%d); the layout does not reach the forged bytes", o.lba7[0], o.frontier)
			}
		})
	}
}
