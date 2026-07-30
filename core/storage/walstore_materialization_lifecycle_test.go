package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWALStoreSharedMaterializationLargeConcurrentLifecycle(t *testing.T) {
	const (
		blockSize = 4096
		batchSize = 16
	)
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 512, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	s.enableMultiBlockRecordsForTest(true)
	s.enableSharedRecordMaterializationForTest(true)
	t.Cleanup(func() { _ = s.Close() })

	for base := uint32(0); base < 256; base += batchSize {
		blocks := make([][]byte, batchSize)
		for index := range blocks {
			blocks[index] = makeBlock(blockSize, byte((base+uint32(index))%251+1))
		}
		if _, err := s.WriteBatch(base, blocks); err != nil {
			t.Fatal(err)
		}
	}
	for lba := uint32(300); lba < 332; lba++ {
		if _, err := s.Write(lba, makeBlock(blockSize, byte(lba%251+1))); err != nil {
			t.Fatal(err)
		}
	}
	if stable, err := s.Sync(); err != nil || stable != 288 {
		t.Fatalf("initial Sync=%d err=%v want 288,nil", stable, err)
	}

	firstExtentLock := &s.extentMu[0]
	firstExtentLock.Lock()
	flushDone := make(chan error, 1)
	go func() { flushDone <- s.flusher.flushOnce() }()
	waitForMaterializationRead(t, s, firstExtentLock)

	newZero := makeBlock(blockSize, 0xD0)
	if _, err := s.Write(0, newZero); err != nil {
		firstExtentLock.Unlock()
		t.Fatal(err)
	}
	newBatch := make([][]byte, 8)
	for index := range newBatch {
		newBatch[index] = makeBlock(blockSize, byte(0xD8+index))
	}
	if _, err := s.WriteBatch(8, newBatch); err != nil {
		firstExtentLock.Unlock()
		t.Fatal(err)
	}
	newThreeHundred := makeBlock(blockSize, 0xE3)
	finalLSN, err := s.Write(300, newThreeHundred)
	if err != nil {
		firstExtentLock.Unlock()
		t.Fatal(err)
	}
	if stable, err := s.Sync(); err != nil || stable != finalLSN {
		firstExtentLock.Unlock()
		t.Fatalf("concurrent Sync=%d err=%v want %d,nil", stable, err, finalLSN)
	}
	firstExtentLock.Unlock()
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}

	if got := s.CheckpointLSN(); got != 288 {
		t.Fatalf("first checkpoint=%d want 288", got)
	}
	_, tail, head := s.Boundaries()
	if tail != 289 || head != finalLSN {
		t.Fatalf("first tail/head=%d/%d want 289/%d", tail, head, finalLSN)
	}
	if got := s.dm.len(); got != 10 {
		t.Fatalf("newer dirty entries after first cycle=%d want 10", got)
	}
	assertCurrentBlock(t, s, 0, newZero)
	for index, want := range newBatch {
		assertCurrentBlock(t, s, uint32(8+index), want)
	}
	assertCurrentBlock(t, s, 300, newThreeHundred)

	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if got := s.CheckpointLSN(); got != finalLSN || s.dm.len() != 0 {
		t.Fatalf("final checkpoint/dirty=%d/%d want %d/0",
			got, s.dm.len(), finalLSN)
	}
	assertExtentBlock(t, s, 0, newZero)
	for index, want := range newBatch {
		assertExtentBlock(t, s, uint32(8+index), want)
	}
	assertExtentBlock(t, s, 300, newThreeHundred)

	got := s.FlusherInstrumentation()
	if got.CyclesSucceeded != 2 ||
		got.SnapshotEntries != 298 ||
		got.SnapshotUniqueWALRecords != 51 ||
		got.WALRecordReadOps != 51 ||
		got.MaterializationRecordReuseHits != 247 ||
		got.SupersededEntries != 10 {
		t.Fatalf("cycles/entries/unique/reads/reuse/superseded=%d/%d/%d/%d/%d/%d want 2/298/51/51/247/10",
			got.CyclesSucceeded, got.SnapshotEntries,
			got.SnapshotUniqueWALRecords, got.WALRecordReadOps,
			got.MaterializationRecordReuseHits, got.SupersededEntries)
	}
}

func TestWALStoreSharedMaterializationCannotOverwriteDirectBase(t *testing.T) {
	const blockSize = 4096
	path := filepath.Join(t.TempDir(), "store.bin")
	s, err := CreateWALStore(path, 64, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	s.enableMultiBlockRecordsForTest(true)
	s.enableSharedRecordMaterializationForTest(true)

	blocks := make([][]byte, 16)
	for index := range blocks {
		blocks[index] = makeBlock(blockSize, byte(0x40+index))
	}
	if _, err := s.WriteBatch(32, blocks); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}

	firstExtentLock := &s.extentMu[32%uint32(len(s.extentMu))]
	firstExtentLock.Lock()
	flushDone := make(chan error, 1)
	go func() { flushDone <- s.flusher.flushOnce() }()
	waitForMaterializationRead(t, s, firstExtentLock)

	base := makeBlock(blockSize, 0xB5)
	if err := s.WriteExtentDirect(37, base); err != nil {
		firstExtentLock.Unlock()
		t.Fatal(err)
	}
	firstExtentLock.Unlock()
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}
	if got := s.CheckpointLSN(); got != 16 {
		t.Fatalf("checkpoint=%d want 16", got)
	}
	if got := s.FlusherInstrumentation(); got.WALRecordReadOps != 1 ||
		got.MaterializationRecordReuseHits != 15 ||
		got.SupersededEntries != 1 {
		t.Fatalf("reads/reuse/superseded=%d/%d/%d want 1/15/1",
			got.WALRecordReadOps, got.MaterializationRecordReuseHits,
			got.SupersededEntries)
	}
	assertCurrentBlock(t, s, 37, base)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	assertCurrentBlock(t, reopened, 37, base)
}

func TestWALStoreSharedMaterializationRespectsRecycleFloorAcrossRestart(t *testing.T) {
	const blockSize = 4096
	path := filepath.Join(t.TempDir(), "store.bin")
	s, err := CreateWALStore(path, 32, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	s.enableMultiBlockRecordsForTest(true)
	s.enableSharedRecordMaterializationForTest(true)

	blocks := make([][]byte, 16)
	for index := range blocks {
		blocks[index] = makeBlock(blockSize, byte(0x60+index))
	}
	if _, err := s.WriteBatch(0, blocks); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	s.SetRecycleFloorSource(&fakeRecycleFloorSource{floor: 8, anyActive: true})
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if got := s.CheckpointLSN(); got != 8 {
		t.Fatalf("pinned checkpoint=%d want 8", got)
	}
	if got := s.wal.logicalTailValue(); got != 0 {
		t.Fatalf("physical WAL tail under pin=%d want 0", got)
	}
	_, tail, head := s.Boundaries()
	if tail != 1 || head != 16 {
		t.Fatalf("logical tail/head under pin=%d/%d want 1/16", tail, head)
	}
	var retained []RecoveryEntry
	if err := s.ScanLBAs(9, func(entry RecoveryEntry) error {
		retained = append(retained, entry)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(retained) != 8 {
		t.Fatalf("retained entries above pin=%d want 8", len(retained))
	}
	crashGeometryTestStore(t, s)

	reopened, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = reopened.Close() })
	if recovered, err := reopened.Recover(); err != nil || recovered != 16 {
		t.Fatalf("Recover=%d err=%v want 16,nil", recovered, err)
	}
	wantRecoveredHead := uint64(walEntryHeaderSize + 16*blockSize)
	if tail, head := reopened.wal.logicalTailValue(), reopened.wal.logicalHeadValue(); tail != 0 || head != wantRecoveredHead {
		t.Fatalf("recovered physical tail/head=%d/%d want 0/%d",
			tail, head, wantRecoveredHead)
	}
	reopened.enableSharedRecordMaterializationForTest(true)
	newBlock := makeBlock(blockSize, 0xF1)
	newLSN, err := reopened.Write(20, newBlock)
	if err != nil {
		t.Fatal(err)
	}
	newEntry := snapshotEntriesByLBA(reopened.dm.snapshot())[20]
	if newEntry.WALOffset != wantRecoveredHead {
		t.Fatalf("post-recovery append offset=%d want %d",
			newEntry.WALOffset, wantRecoveredHead)
	}
	if _, err := reopened.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if got := reopened.CheckpointLSN(); got != newLSN {
		t.Fatalf("checkpoint after recovery=%d want %d", got, newLSN)
	}
	got := reopened.FlusherInstrumentation()
	if got.WALRecordReadOps != 2 || got.MaterializationRecordReuseHits != 7 {
		t.Fatalf("recovered reads/reuse=%d/%d want 2/7",
			got.WALRecordReadOps, got.MaterializationRecordReuseHits)
	}
	for index, want := range blocks {
		assertCurrentBlock(t, reopened, uint32(index), want)
	}
	assertCurrentBlock(t, reopened, 20, newBlock)
}

func TestWALStoreSharedMaterializationCloseLifecycle(t *testing.T) {
	t.Run("final flush", func(t *testing.T) {
		const blockSize = 4096
		path := filepath.Join(t.TempDir(), "store.bin")
		s, err := CreateWALStore(path, 32, blockSize)
		if err != nil {
			t.Fatal(err)
		}
		replaceFlusherForTest(s, time.Hour)
		s.enableMultiBlockRecordsForTest(true)
		s.enableSharedRecordMaterializationForTest(true)
		blocks := make([][]byte, 16)
		for index := range blocks {
			blocks[index] = makeBlock(blockSize, byte(0x20+index))
		}
		if _, err := s.WriteBatch(0, blocks); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		got := s.FlusherInstrumentation()
		if got.CyclesSucceeded != 1 || got.WALRecordReadOps != 1 ||
			got.MaterializationRecordReuseHits != 15 {
			t.Fatalf("close cycles/reads/reuse=%d/%d/%d want 1/1/15",
				got.CyclesSucceeded, got.WALRecordReadOps,
				got.MaterializationRecordReuseHits)
		}

		reopened, err := OpenWALStore(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = reopened.Close() })
		if _, err := reopened.Recover(); err != nil {
			t.Fatal(err)
		}
		if got := reopened.CheckpointLSN(); got != 16 {
			t.Fatalf("reopened checkpoint=%d want 16", got)
		}
		for index, want := range blocks {
			assertCurrentBlock(t, reopened, uint32(index), want)
		}
	})

	t.Run("checkpoint failure recovers", func(t *testing.T) {
		const blockSize = 4096
		path := filepath.Join(t.TempDir(), "store.bin")
		s, err := CreateWALStore(path, 32, blockSize)
		if err != nil {
			t.Fatal(err)
		}
		replaceFlusherForTest(s, time.Hour)
		s.enableMultiBlockRecordsForTest(true)
		s.enableSharedRecordMaterializationForTest(true)
		blocks := make([][]byte, 16)
		for index := range blocks {
			blocks[index] = makeBlock(blockSize, byte(0x90+index))
		}
		if _, err := s.WriteBatch(0, blocks); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		s.writeSuperblockMetadata = func([]byte) error {
			return errors.New("injected shared checkpoint failure")
		}
		err = s.Close()
		if err == nil || !strings.Contains(err.Error(), "injected shared checkpoint failure") {
			t.Fatalf("Close error=%v want injected failure", err)
		}
		if got := s.CheckpointLSN(); got != 0 {
			t.Fatalf("checkpoint after failed Close=%d want 0", got)
		}
		if got := s.wal.logicalTailValue(); got != 0 {
			t.Fatalf("physical WAL tail after failed Close=%d want 0", got)
		}
		if s.fd != nil {
			t.Fatal("failed Close left file open")
		}

		reopened, err := OpenWALStore(path)
		if err != nil {
			t.Fatal(err)
		}
		reopened.DisableAutoFlushForRecoveryTest()
		t.Cleanup(func() { _ = reopened.Close() })
		if recovered, err := reopened.Recover(); err != nil || recovered != 16 {
			t.Fatalf("Recover=%d err=%v want 16,nil", recovered, err)
		}
		reopened.enableSharedRecordMaterializationForTest(true)
		if err := reopened.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
		if got := reopened.CheckpointLSN(); got != 16 {
			t.Fatalf("recovered checkpoint=%d want 16", got)
		}
		for index, want := range blocks {
			assertCurrentBlock(t, reopened, uint32(index), want)
		}
	})
}

func TestWALStoreRecoverRejectsOverflowingBatchGeometry(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{
			name: "reserved multiplication overflow",
			mutate: func(record []byte) {
				binary.LittleEndian.PutUint64(record[8:16], (uint64(1)<<52)+3)
			},
		},
		{
			name: "LSN range overflow",
			mutate: func(record []byte) {
				binary.LittleEndian.PutUint64(record[0:8], ^uint64(0)-1)
			},
		},
		{
			name: "reserved length mismatch",
			mutate: func(record []byte) {
				binary.LittleEndian.PutUint64(record[8:16], 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const blockSize = 4096
			path := filepath.Join(t.TempDir(), "store.bin")
			s, err := CreateWALStore(path, 16, blockSize)
			if err != nil {
				t.Fatal(err)
			}
			s.DisableAutoFlushForRecoveryTest()
			s.enableMultiBlockRecordsForTest(true)
			if _, err := s.WriteBatch(4, [][]byte{
				makeBlock(blockSize, 0xA1),
				makeBlock(blockSize, 0xA2),
				makeBlock(blockSize, 0xA3),
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			entry := snapshotEntriesByLBA(s.dm.snapshot())[4]
			rewriteRecordForTest(
				t, s, entry, int64(s.sb.WALOffset+entry.WALOffset), tt.mutate,
			)
			crashGeometryTestStore(t, s)

			reopened, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			reopened.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = reopened.Close() })
			recovered, recoverErr := reopened.Recover()
			if recovered != 0 ||
				!errors.Is(recoverErr, ErrWALIntegrityFault) {
				t.Fatalf("Recover=%d err=%v want 0,WALIntegrity", recovered, recoverErr)
			}
			if got := reopened.dm.len(); got != 0 {
				t.Fatalf("dirty entries from malformed batch=%d want 0", got)
			}
			if got := reopened.NextLSN(); got != 1 {
				t.Fatalf("next LSN after malformed batch=%d want 1", got)
			}
		})
	}
}

func TestWALStoreRecoverReconstructsLegacyWrappedRetainedWindow(t *testing.T) {
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

	high := makeBlock(blockSize, 0xA4)
	if _, err := s.Write(20, high); err != nil {
		t.Fatal(err)
	}
	s.enableMultiBlockRecordsForTest(true)
	low := [][]byte{
		makeBlock(blockSize, 0xB5),
		makeBlock(blockSize, 0xB6),
	}
	if _, err := s.WriteBatch(24, low); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	entries := snapshotEntriesByLBA(s.dm.snapshot())
	if entries[20].WALOffset != uint64(3*(walEntryHeaderSize+blockSize)) ||
		entries[24].WALOffset != 0 {
		t.Fatalf("high/low offsets=%d/%d want %d/0",
			entries[20].WALOffset, entries[24].WALOffset,
			3*(walEntryHeaderSize+blockSize))
	}

	legacy := *s.sb
	legacy.WALHead = 0
	legacy.WALTail = 0
	buf := newSimpleByteBuf()
	if _, err := legacy.writeTo(buf); err != nil {
		t.Fatal(err)
	}
	if _, err := s.fd.WriteAt(buf.bytes(), 0); err != nil {
		t.Fatal(err)
	}
	if err := s.fd.Sync(); err != nil {
		t.Fatal(err)
	}
	crashGeometryTestStore(t, s)

	reopened, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = reopened.Close() })
	if recovered, err := reopened.Recover(); err != nil || recovered != 6 {
		t.Fatalf("Recover=%d err=%v want 6,nil", recovered, err)
	}
	wantTail := entries[20].WALOffset
	wantHead := walSize + entries[24].RecordSize
	if tail, head := reopened.wal.logicalTailValue(), reopened.wal.logicalHeadValue(); tail != wantTail || head != wantHead {
		t.Fatalf("recovered wrapped tail/head=%d/%d want %d/%d",
			tail, head, wantTail, wantHead)
	}
	assertCurrentBlock(t, reopened, 20, high)
	assertCurrentBlock(t, reopened, 24, low[0])
	assertCurrentBlock(t, reopened, 25, low[1])

	reopened.enableSharedRecordMaterializationForTest(true)
	postRecovery := makeBlock(blockSize, 0xC7)
	finalLSN, err := reopened.Write(30, postRecovery)
	if err != nil {
		t.Fatal(err)
	}
	postEntry := snapshotEntriesByLBA(reopened.dm.snapshot())[30]
	if postEntry.WALOffset != entries[24].RecordSize {
		t.Fatalf("post-recovery append offset=%d want %d",
			postEntry.WALOffset, entries[24].RecordSize)
	}
	if _, err := reopened.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if got := reopened.CheckpointLSN(); got != finalLSN {
		t.Fatalf("checkpoint=%d want %d", got, finalLSN)
	}
	if got := reopened.dm.len(); got != 0 {
		t.Fatalf("dirty entries after wrapped flush=%d want 0", got)
	}
	wantFinalBoundary := wantHead + uint64(walEntryHeaderSize+blockSize)
	if tail, head := reopened.wal.logicalTailValue(), reopened.wal.logicalHeadValue(); tail != wantFinalBoundary || head != wantFinalBoundary {
		t.Fatalf("final wrapped tail/head=%d/%d want %d/%d",
			tail, head, wantFinalBoundary, wantFinalBoundary)
	}
	assertCurrentBlock(t, reopened, 20, high)
	assertCurrentBlock(t, reopened, 24, low[0])
	assertCurrentBlock(t, reopened, 25, low[1])
	assertCurrentBlock(t, reopened, 30, postRecovery)
}

func waitForMaterializationRead(t *testing.T, s *WALStore, lock *sync.RWMutex) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for s.FlusherInstrumentation().WALRecordReadOps == 0 {
		if time.Now().After(deadline) {
			lock.Unlock()
			t.Fatal("flusher did not materialize the first record")
		}
		time.Sleep(time.Millisecond)
	}
}

func assertCurrentBlock(t *testing.T, s *WALStore, lba uint32, want []byte) {
	t.Helper()
	got, err := s.Read(lba)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("current LBA %d mismatch", lba)
	}
}
