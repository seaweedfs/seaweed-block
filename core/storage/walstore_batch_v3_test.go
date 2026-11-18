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
