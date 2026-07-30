package storage

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWALStoreSharedRecordMaterializationReadsEachRecordOnce(t *testing.T) {
	s := createSingleReadTestStore(t)
	s.enableSharedRecordMaterializationForTest(true)
	s.enableMultiBlockRecordsForTest(true)

	blocksA := [][]byte{
		makeBlock(4096, 0x11),
		makeBlock(4096, 0x12),
		makeBlock(4096, 0x13),
	}
	ordinary := makeBlock(4096, 0x21)
	blocksB := [][]byte{
		makeBlock(4096, 0x31),
		makeBlock(4096, 0x32),
	}
	if _, err := s.WriteBatch(0, blocksA); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(10, ordinary); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBatch(5, blocksB); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}

	got := s.FlusherInstrumentation()
	if got.SnapshotEntries != 6 ||
		got.SnapshotUniqueWALRecords != 3 ||
		got.SnapshotRecordReuseCandidates != 3 ||
		got.ValidatedRecords != 6 ||
		got.WALHeaderReadOps != 0 ||
		got.WALRecordReadOps != 3 ||
		got.MaterializationReadOps != 3 ||
		got.MaterializationRecordReuseHits != 3 {
		t.Fatalf("entries/unique/candidates/validated/header/record/materialization/hits=%d/%d/%d/%d/%d/%d/%d/%d want 6/3/3/6/0/3/3/3",
			got.SnapshotEntries, got.SnapshotUniqueWALRecords,
			got.SnapshotRecordReuseCandidates, got.ValidatedRecords,
			got.WALHeaderReadOps, got.WALRecordReadOps,
			got.MaterializationReadOps, got.MaterializationRecordReuseHits)
	}
	assertExtentBlock(t, s, 0, blocksA[0])
	assertExtentBlock(t, s, 1, blocksA[1])
	assertExtentBlock(t, s, 2, blocksA[2])
	assertExtentBlock(t, s, 10, ordinary)
	assertExtentBlock(t, s, 5, blocksB[0])
	assertExtentBlock(t, s, 6, blocksB[1])
}

func TestWALStoreSharedRecordMaterializationReadsRangeTrimOnce(t *testing.T) {
	const blockSize = 4096
	s := createSingleReadTestStore(t)
	s.enableSharedRecordMaterializationForTest(true)
	for lba := uint32(4); lba < 7; lba++ {
		if err := s.WriteExtentDirect(lba, makeBlock(blockSize, 0x35)); err != nil {
			t.Fatal(err)
		}
	}

	trim := &walEntry{LSN: 1, Type: walEntryTrim, LBA: 4, Length: 3 * blockSize}
	walOffset, err := s.wal.append(trim)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint32(0); index < 3; index++ {
		s.dm.putAt(
			4+uint64(index), walOffset, index*blockSize,
			1, blockSize, walEntryHeaderSize,
		)
	}
	s.mu.Lock()
	s.nextLSN = 2
	s.walHead = 1
	s.walTail = 1
	s.mu.Unlock()
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}

	got := s.FlusherInstrumentation()
	if got.ValidatedRecords != 3 ||
		got.WALHeaderReadOps != 0 ||
		got.WALRecordReadOps != 1 ||
		got.MaterializationReadOps != 1 ||
		got.MaterializationRecordReuseHits != 2 {
		t.Fatalf("validated/header/record/materialization/reuse=%d/%d/%d/%d/%d want 3/0/1/1/2",
			got.ValidatedRecords, got.WALHeaderReadOps, got.WALRecordReadOps,
			got.MaterializationReadOps, got.MaterializationRecordReuseHits)
	}
	for lba := uint32(4); lba < 7; lba++ {
		assertExtentBlock(t, s, lba, make([]byte, blockSize))
	}
}

func TestWALStoreSharedRecordMaterializationConcurrentPartialOverwrite(t *testing.T) {
	s := createSingleReadTestStore(t)
	s.enableSharedRecordMaterializationForTest(true)
	s.enableMultiBlockRecordsForTest(true)
	blocks := [][]byte{
		makeBlock(4096, 0x41),
		makeBlock(4096, 0x42),
		makeBlock(4096, 0x43),
	}
	if _, err := s.WriteBatch(5, blocks); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}

	firstExtentLock := &s.extentMu[5%uint32(len(s.extentMu))]
	firstExtentLock.Lock()
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- s.flusher.flushOnce()
	}()
	deadline := time.Now().Add(2 * time.Second)
	for s.FlusherInstrumentation().WALRecordReadOps == 0 {
		if time.Now().After(deadline) {
			firstExtentLock.Unlock()
			t.Fatal("flusher did not materialize shared record")
		}
		time.Sleep(time.Millisecond)
	}

	newBlock := makeBlock(4096, 0x52)
	newLSN, err := s.Write(6, newBlock)
	if err != nil {
		firstExtentLock.Unlock()
		t.Fatal(err)
	}
	firstExtentLock.Unlock()
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}

	got := s.FlusherInstrumentation()
	if got.WALRecordReadOps != 1 ||
		got.MaterializationRecordReuseHits != 2 ||
		got.SupersededEntries != 1 {
		t.Fatalf("record reads/reuse/superseded=%d/%d/%d want 1/2/1",
			got.WALRecordReadOps, got.MaterializationRecordReuseHits,
			got.SupersededEntries)
	}
	if got := s.CheckpointLSN(); got != 3 {
		t.Fatalf("checkpoint after old snapshot=%d want 3", got)
	}
	_, tail, head := s.Boundaries()
	if tail != 4 || head != newLSN {
		t.Fatalf("boundaries after old snapshot tail/head=%d/%d want 4/%d",
			tail, head, newLSN)
	}
	if got, want := s.wal.logicalTailValue(), uint64(walEntryHeaderSize+3*4096); got != want {
		t.Fatalf("physical WAL tail after old snapshot=%d want %d", got, want)
	}
	entries := snapshotEntriesByLBA(s.dm.snapshot())
	if len(entries) != 1 || entries[6].LSN != newLSN {
		t.Fatalf("dirty entries after overwrite=%+v want only LBA 6 LSN %d",
			entries, newLSN)
	}
	assertExtentBlock(t, s, 5, blocks[0])
	assertExtentBlock(t, s, 7, blocks[2])
	gotBlock, err := s.Read(6)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotBlock, newBlock) {
		t.Fatal("newer overwritten block was not retained in WAL")
	}

	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if got := s.CheckpointLSN(); got != newLSN || s.dm.len() != 0 {
		t.Fatalf("final checkpoint/dirty=%d/%d want %d/0",
			got, s.dm.len(), newLSN)
	}
	assertExtentBlock(t, s, 6, newBlock)
}

func TestWALStoreSharedRecordMaterializationSurvivesLegalRingWrap(t *testing.T) {
	const (
		blockSize = 4096
		walSize   = 16 * 1024
	)
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 16, blockSize, walSize)
	s.DisableAutoFlushForRecoveryTest()
	for lba := uint32(0); lba < 3; lba++ {
		if _, err := s.Write(lba, makeBlock(blockSize, byte(0x70+lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}

	s.enableMultiBlockRecordsForTest(true)
	blocks := [][]byte{
		makeBlock(blockSize, 0x81),
		makeBlock(blockSize, 0x82),
		makeBlock(blockSize, 0x83),
	}
	if _, err := s.WriteBatch(7, blocks); err != nil {
		t.Fatal(err)
	}
	entries := snapshotEntriesByLBA(s.dm.snapshot())
	if entries[7].WALOffset != 0 {
		t.Fatalf("wrapped shared WAL offset=%d want 0", entries[7].WALOffset)
	}
	if _, err := s.Sync(); err != nil {
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
	reopened.enableSharedRecordMaterializationForTest(true)
	if err := reopened.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}

	got := reopened.FlusherInstrumentation()
	if got.ValidatedRecords != 3 ||
		got.WALRecordReadOps != 1 ||
		got.MaterializationRecordReuseHits != 2 {
		t.Fatalf("validated/record/reuse=%d/%d/%d want 3/1/2",
			got.ValidatedRecords, got.WALRecordReadOps,
			got.MaterializationRecordReuseHits)
	}
	if got := reopened.CheckpointLSN(); got != 6 {
		t.Fatalf("checkpoint=%d want 6", got)
	}
	if tail, head := reopened.wal.logicalTailValue(), reopened.wal.logicalHeadValue(); tail != head {
		t.Fatalf("physical WAL tail/head=%d/%d want equal", tail, head)
	}
	for index, want := range blocks {
		assertExtentBlock(t, reopened, uint32(7+index), want)
	}
}

func TestWALStoreSharedRecordMaterializationFailsClosedOnMalformedRecord(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*testing.T, *WALStore, snapshotEntry, int64)
		wantErr string
	}{
		{
			name: "reserved",
			mutate: func(t *testing.T, s *WALStore, entry snapshotEntry, absoluteOffset int64) {
				rewriteRecordForTest(t, s, entry, absoluteOffset, func(record []byte) {
					binary.LittleEndian.PutUint64(record[8:16], 4)
				})
			},
			wantErr: "dirty WAL batch",
		},
		{
			name: "reserved multiplication overflow",
			mutate: func(t *testing.T, s *WALStore, entry snapshotEntry, absoluteOffset int64) {
				rewriteRecordForTest(t, s, entry, absoluteOffset, func(record []byte) {
					binary.LittleEndian.PutUint64(record[8:16], (uint64(1)<<52)+3)
				})
			},
			wantErr: "dirty WAL batch",
		},
		{
			name: "total length",
			mutate: func(t *testing.T, s *WALStore, entry snapshotEntry, absoluteOffset int64) {
				rewriteRecordForTest(t, s, entry, absoluteOffset, func(record []byte) {
					binary.LittleEndian.PutUint32(record[26:30], 2*4096)
				})
			},
			wantErr: "record size",
		},
		{
			name: "shared data offset",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 6, func(entry *dirtyEntry) {
					entry.dataOffset = 2 * 4096
				})
			},
			wantErr: "WAL slot mismatch",
		},
		{
			name: "identity size disagreement",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 6, func(entry *dirtyEntry) {
					entry.recordSize++
				})
			},
			wantErr: "record size",
		},
		{
			name: "wrapped geometry",
			mutate: func(t *testing.T, s *WALStore, _ snapshotEntry, _ int64) {
				mutateDirtyEntryForTest(t, s, 6, func(entry *dirtyEntry) {
					entry.walOffset = s.sb.WALSize - 10
				})
			},
			wantErr: "record size",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createSingleReadTestStore(t)
			s.enableSharedRecordMaterializationForTest(true)
			s.enableMultiBlockRecordsForTest(true)
			if _, err := s.WriteBatch(5, [][]byte{
				makeBlock(4096, 0x61),
				makeBlock(4096, 0x62),
				makeBlock(4096, 0x63),
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			entry := snapshotEntriesByLBA(s.dm.snapshot())[6]
			absoluteOffset := int64(s.sb.WALOffset + entry.WALOffset)
			tt.mutate(t, s, entry, absoluteOffset)

			err := s.flusher.flushOnce()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("flush error=%v want substring %q", err, tt.wantErr)
			}
			assertSingleReadFailureState(t, s, 3, 0)
		})
	}
}

func assertExtentBlock(t *testing.T, s *WALStore, lba uint32, want []byte) {
	t.Helper()
	got, err := s.readFromExtent(lba)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("extent LBA %d mismatch", lba)
	}
}
