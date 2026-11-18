package storage

import (
	"bytes"
	"errors"
	"testing"
)

// v3 WriteBatch controls (DESIGN r17.4 sections 3.1 and 6.7; owner
// claude03): one multi-block record per batch, its replay and reads.

func batchBlocks(first byte, n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = blk(first + byte(i))
	}
	return out
}

func readBatchV3(t *testing.T, s *WALStore, lba uint32, first byte, n int, what string) {
	t.Helper()
	for i := 0; i < n; i++ {
		if got := readV3(t, s, lba+uint32(i)); !bytes.Equal(got, blk(first+byte(i))) {
			t.Fatalf("%s: LBA %d = %x..., want %x", what, lba+uint32(i), got[0], first+byte(i))
		}
	}
}

// A batch is one record with consecutive LSNs; every block reads back in
// the same open (each dirty entry names its offset inside the record) and
// after a crash and reopen (replay of the whole record); the next write
// continues after the batch's last LSN. Mutants: reading every block at
// offset 0, and replaying only a batch's first block, fail here.
func TestWALStoreV3BatchRoundTripAcrossACrash(t *testing.T) {
	for name, mode := range map[string]WMode{"strict": WModeStrict, "lagged": WModeLagged} {
		t.Run(name, func(t *testing.T) {
			s, path := newStoreV3(t, mode)
			lsns, err := s.WriteBatch(10, batchBlocks(0x40, 4))
			if err != nil {
				t.Fatal(err)
			}
			for i, lsn := range lsns {
				if lsn != uint64(1+i) {
					t.Fatalf("LSNs %v, want 1..4", lsns)
				}
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			readBatchV3(t, s, 10, 0x40, 4, "same open")
			if lsn, err := s.Write(12, blk(0x99)); err != nil || lsn != 5 {
				t.Fatalf("write after the batch: LSN %d err %v, want 5", lsn, err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			crashRecoveryHardeningStore(t, s)
			r := mustReopenV3(t, path)
			t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
			for lba, want := range map[uint32]byte{10: 0x40, 11: 0x41, 12: 0x99, 13: 0x43} {
				if got := readV3(t, r, lba); !bytes.Equal(got, blk(want)) {
					t.Fatalf("after reopen LBA %d = %x..., want %x", lba, got[0], want)
				}
			}
		})
	}
}

// A torn, unacknowledged batch is dropped whole: none of its blocks is
// served after the reopen, the earlier acknowledged write stays. A torn,
// acknowledged batch fails closed (wal_corrupt), never a partial image.
func TestWALStoreV3TornBatchIsAllOrNothing(t *testing.T) {
	t.Run("unacknowledged", func(t *testing.T) {
		s, path := newStoreV3(t, WModeStrict)
		writeSynced(t, s, 1, 1)
		if _, err := s.WriteBatch(10, batchBlocks(0x40, 4)); err != nil {
			t.Fatal(err)
		}
		crashRecoveryHardeningStore(t, s)
		damageRecord(t, path, 2) // the batch record (LSNs 2..5)
		r := mustReopenV3(t, path)
		t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
		readAllV3(t, r, 1, 1)
		for lba := uint32(10); lba < 14; lba++ {
			if got := readV3(t, r, lba); !bytes.Equal(got, blk(0)) {
				t.Fatalf("LBA %d served from a torn batch: %x...", lba, got[0])
			}
		}
	})
	t.Run("acknowledged", func(t *testing.T) {
		s, path := newStoreV3(t, WModeStrict)
		if _, err := s.WriteBatch(10, batchBlocks(0x40, 4)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		crashRecoveryHardeningStore(t, s)
		damageRecord(t, path, 1)
		if r, err := reopenV3(t, path); err == nil {
			crashRecoveryHardeningStore(t, r)
			t.Fatal("a torn acknowledged batch reopened")
		} else if !isWALCorrupt(err) {
			t.Fatalf("reopen: err = %v, want a WAL integrity failure", err)
		}
	})
}

// A crash in the middle of a batch's pwrite (half the record written, then
// an error) latches the store: the batch fails, a later Sync refuses and
// acknowledges nothing, and the reopen serves none of the batch. Mutant:
// writing a batch as one record per block keeps the first blocks of a
// half-written batch and fails here.
func TestWALStoreV3CrashMidBatchServesNoneOfIt(t *testing.T) {
	s, path := newStoreV3(t, WModeStrict)
	writeSynced(t, s, 1, 1)
	real := s.v3.w.writeAt
	s.v3.w.writeAt = func(b []byte, off int64) (int, error) {
		if len(b) > 2*v3TestBlock {
			half := len(b) / 2
			n, _ := real(b[:half], off)
			return n, errors.New("injected crash mid-batch")
		}
		return real(b, off)
	}
	if _, err := s.WriteBatch(10, batchBlocks(0x40, 4)); err == nil {
		t.Fatal("a half-written batch succeeded")
	}
	wack := s.v3.wack.Load()
	if _, err := s.Sync(); err == nil {
		t.Fatal("Sync acknowledged after a failed batch pwrite")
	}
	if s.v3.wack.Load() != wack {
		t.Fatal("the acknowledged frontier moved after a failed batch")
	}
	crashRecoveryHardeningStore(t, s)
	r := mustReopenV3(t, path)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
	readAllV3(t, r, 1, 1)
	for lba := uint32(10); lba < 14; lba++ {
		if got := readV3(t, r, lba); !bytes.Equal(got, blk(0)) {
			t.Fatalf("LBA %d served from a half-written batch: %x...", lba, got[0])
		}
	}
}

// The ack boundary: Sync after a batch returns at least its last LSN and
// (strict) the slot's W covers it; in mode C the next refresh publishes it.
func TestWALStoreV3BatchAckCoversItsLastLSN(t *testing.T) {
	for name, mode := range map[string]WMode{"strict": WModeStrict, "lagged": WModeLagged} {
		t.Run(name, func(t *testing.T) {
			s, _ := newStoreV3(t, mode)
			t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
			if _, err := s.WriteBatch(10, batchBlocks(0x40, 4)); err != nil {
				t.Fatal(err)
			}
			f, err := s.Sync()
			if err != nil || f < 4 {
				t.Fatalf("Sync = (%d, %v), want >= 4", f, err)
			}
			if mode == WModeLagged {
				if err := s.refreshW(); err != nil {
					t.Fatal(err)
				}
			}
			if w := s.v3.pub.current().W; w < 4 {
				t.Fatalf("slot W %d does not cover the batch's last LSN 4", w)
			}
		})
	}
}

// The flusher materializes every block of a batch at its offset; after the
// checkpoint the reads come from the extent, also across a reopen. Mutant:
// a flusher that reads every block at offset 0 fails here.
func TestWALStoreV3BatchFlushesEveryBlock(t *testing.T) {
	s, path, crash := checkpointedStoreV3(t, 1)
	if _, err := s.WriteBatch(10, batchBlocks(0x40, 4)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnceInternal(false); err != nil {
		t.Fatal(err)
	}
	if d := s.v3.pub.current(); d.CheckpointLSN < 5 || s.dm.len() != 0 {
		t.Fatalf("after flush: checkpoint %d, dirty %d", d.CheckpointLSN, s.dm.len())
	}
	readBatchV3(t, s, 10, 0x40, 4, "from the extent")
	crash()
	r, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { crashRecoveryHardeningStore(t, r) })
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	readBatchV3(t, r, 10, 0x40, 4, "after reopen")
}

// The dark offline reader (codex03's walstore_readonly_v3.go) indexes and
// reads every block of a batch record. Mutant: indexing only a batch's
// first block fails here.
func TestWALStoreV3ReadOnlyReaderServesBatchBlocks(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t) // the reader validates the owned map
	if _, err := s.WriteBatch(10, batchBlocks(0x40, 4)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	reader, err := openReadOnlyV3ForTest(path, WALStoreOptions{})
	if err != nil {
		t.Fatalf("readonly open: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	for i := 0; i < 4; i++ {
		got, err := reader.Read(10 + uint32(i))
		if err != nil || !bytes.Equal(got, blk(0x40+byte(i))) {
			t.Fatalf("readonly LBA %d: err %v", 10+i, err)
		}
	}
}

// The map coverage follows every block's LSN (DESIGN 9.1). Mutant: a batch
// that records no map writes fails here.
func TestWALStoreV3BatchRecordsMapCoverage(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	if _, err := s.WriteBatch(4, batchBlocks(0x40, 8)); err != nil { // chunks 1 and 2
		t.Fatal(err)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for c, want := range map[int]uint64{1: 4, 2: 8} {
		if got := s.v3.mapEntries[c].LSN; got < want {
			t.Fatalf("chunk %d map LSN %d, want >= %d", c, got, want)
		}
	}
}

// Ordinary replicated ApplyEntry on v3 (DESIGN 8.1, 6.7, r16.1; codex03
// 12a inventory): applies append in LSN order and survive a crash; an LSN
// at or below the highest is a superseded no-op when the LBA holds a higher
// LSN and ErrWALLSNOrder otherwise; an unanchored gap is refused; an
// ordinary apply while installing is refused (the install lane is the
// handle's). Mutants: no order check, no gap check, no installing refusal.
func TestWALStoreV3ReplicatedApply(t *testing.T) {
	s, path := newStoreV3(t, WModeStrict)
	for lsn := uint64(1); lsn <= 3; lsn++ {
		if err := s.ApplyEntry(uint32(lsn), blk(byte(lsn)), lsn); err != nil {
			t.Fatalf("apply %d: %v", lsn, err)
		}
	}
	if err := s.ApplyEntry(2, blk(0x22), 3); err != nil {
		// LBA 2 holds LSN 2 < 3, but 3 is already appended (on LBA 3).
		if !errors.Is(err, ErrWALLSNOrder) {
			t.Fatalf("an appended LSN on another LBA: err = %v, want ErrWALLSNOrder", err)
		}
	} else {
		t.Fatal("an LSN at the highest appended was applied again")
	}
	if err := s.ApplyEntry(3, blk(0x33), 2); err != nil {
		t.Fatalf("a superseded lower LSN for an LBA that holds a higher one: %v, want a no-op", err)
	}
	if err := s.ApplyEntry(9, blk(9), 9); !errors.Is(err, ErrInstallLSNGap) {
		t.Fatalf("a gap above 3: err = %v, want the gap refusal", err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	r := mustReopenV3(t, path)
	readAllV3(t, r, 1, 3)
	beginV3(t, r, 3, idA)
	if err := r.ApplyEntry(4, blk(4), 4); !errors.Is(err, ErrBaseInstalling) {
		t.Fatalf("an ordinary apply while installing: err = %v, want ErrBaseInstalling", err)
	}
	crashRecoveryHardeningStore(t, r)
}

// v3 ScanLBAs, the primary's catch-up source: single and batch records in
// LSN order (a batch one entry per block), from the start and from the
// middle of a batch; a fromLSN the retention gate has recycled is refused
// with the typed WALRecycled failure. Mutants: the v2 scan on a v3 store
// (emits nothing), a batch emitting only its first block, no ordering.
func TestWALStoreV3ScanLBAs(t *testing.T) {
	s, _ := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { crashRecoveryHardeningStore(t, s) })
	writeSynced(t, s, 1, 3)                                           // LSNs 1..3 at LBAs 1..3
	if _, err := s.WriteBatch(10, batchBlocks(0x40, 4)); err != nil { // LSNs 4..7
		t.Fatal(err)
	}
	collect := func(from uint64) []RecoveryEntry {
		var out []RecoveryEntry
		if err := s.ScanLBAs(from, func(e RecoveryEntry) error {
			out = append(out, RecoveryEntry{LSN: e.LSN, LBA: e.LBA, Flags: e.Flags, Data: append([]byte(nil), e.Data...)})
			return nil
		}); err != nil {
			t.Fatalf("ScanLBAs(%d): %v", from, err)
		}
		return out
	}
	all := collect(1)
	if len(all) != 7 {
		t.Fatalf("ScanLBAs(1) emitted %d entries, want 7", len(all))
	}
	for i, e := range all {
		lsn := uint64(i + 1)
		wantLBA, want := uint32(lsn), blk(byte(lsn))
		if lsn >= 4 {
			wantLBA, want = 10+uint32(lsn-4), blk(0x40+byte(lsn-4))
		}
		if e.LSN != lsn || e.LBA != wantLBA || !bytes.Equal(e.Data, want) || e.Flags != RecoveryEntryWrite {
			t.Fatalf("entry %d: LSN %d LBA %d data %x..., want LSN %d LBA %d", i, e.LSN, e.LBA, e.Data[0], lsn, wantLBA)
		}
	}
	mid := collect(6)
	if len(mid) != 2 || mid[0].LSN != 6 || mid[0].LBA != 12 || mid[1].LSN != 7 {
		t.Fatalf("ScanLBAs(6) = %+v, want LSNs 6 and 7 at LBAs 12 and 13", mid)
	}
	c, _, _ := checkpointedStoreV3(t, 4)
	err := c.ScanLBAs(1, func(RecoveryEntry) error { return nil })
	if !errors.Is(err, ErrWALRecycled) {
		t.Fatalf("ScanLBAs below the checkpoint: err = %v, want ErrWALRecycled", err)
	}
}
