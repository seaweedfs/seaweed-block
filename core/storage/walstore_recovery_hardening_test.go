package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"path/filepath"
	"testing"
)

func TestWALStoreRecoverRetainsMultiBlockSuffixAcrossRestart(t *testing.T) {
	const blockSize = 4096
	path := filepath.Join(t.TempDir(), "store.bin")
	s, err := CreateWALStore(path, 32, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	s.enableMultiBlockRecordsForTest(true)

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
	crashRecoveryHardeningStore(t, s)

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
	newBlock := makeBlock(blockSize, 0xF1)
	newLSN, err := reopened.Write(20, newBlock)
	if err != nil {
		t.Fatal(err)
	}
	newEntry := recoverySnapshotEntriesByLBA(reopened.dm.snapshot())[20]
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
	for index, want := range blocks {
		assertRecoveryHardeningBlock(t, reopened, uint32(index), want)
	}
	assertRecoveryHardeningBlock(t, reopened, 20, newBlock)
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
			const (
				blockSize = 4096
				blocks    = 3
			)
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
			entry := recoverySnapshotEntriesByLBA(s.dm.snapshot())[4]
			rewriteRecoveryRecordForTest(
				t, s, uint64(walEntryHeaderSize+blocks*blockSize),
				int64(s.sb.WALOffset+entry.WALOffset), tt.mutate,
			)
			crashRecoveryHardeningStore(t, s)

			reopened, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			reopened.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = reopened.Close() })
			recovered, recoverErr := reopened.Recover()
			if recovered != 0 || !errors.Is(recoverErr, ErrWALIntegrityFault) {
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
		blockSize     = 4096
		walSize       = 24 * 1024
		lowRecordSize = uint64(walEntryHeaderSize + 2*blockSize)
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
	entries := recoverySnapshotEntriesByLBA(s.dm.snapshot())
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
	crashRecoveryHardeningStore(t, s)

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
	wantHead := walSize + lowRecordSize
	if tail, head := reopened.wal.logicalTailValue(), reopened.wal.logicalHeadValue(); tail != wantTail || head != wantHead {
		t.Fatalf("recovered wrapped tail/head=%d/%d want %d/%d",
			tail, head, wantTail, wantHead)
	}
	assertRecoveryHardeningBlock(t, reopened, 20, high)
	assertRecoveryHardeningBlock(t, reopened, 24, low[0])
	assertRecoveryHardeningBlock(t, reopened, 25, low[1])

	postRecovery := makeBlock(blockSize, 0xC7)
	finalLSN, err := reopened.Write(30, postRecovery)
	if err != nil {
		t.Fatal(err)
	}
	postEntry := recoverySnapshotEntriesByLBA(reopened.dm.snapshot())[30]
	if postEntry.WALOffset != lowRecordSize {
		t.Fatalf("post-recovery append offset=%d want %d",
			postEntry.WALOffset, lowRecordSize)
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
	assertRecoveryHardeningBlock(t, reopened, 20, high)
	assertRecoveryHardeningBlock(t, reopened, 24, low[0])
	assertRecoveryHardeningBlock(t, reopened, 25, low[1])
	assertRecoveryHardeningBlock(t, reopened, 30, postRecovery)
}

func crashRecoveryHardeningStore(t *testing.T, s *WALStore) {
	t.Helper()
	s.committer.Stop()
	if err := s.fd.Close(); err != nil {
		t.Fatal(err)
	}
}

func recoverySnapshotEntriesByLBA(entries []snapshotEntry) map[uint64]snapshotEntry {
	result := make(map[uint64]snapshotEntry, len(entries))
	for _, entry := range entries {
		result[entry.LBA] = entry
	}
	return result
}

func rewriteRecoveryRecordForTest(
	t *testing.T,
	s *WALStore,
	recordSize uint64,
	absoluteOffset int64,
	mutate func([]byte),
) {
	t.Helper()
	record := make([]byte, recordSize)
	if _, err := s.fd.ReadAt(record, absoluteOffset); err != nil {
		t.Fatal(err)
	}
	mutate(record)
	binary.LittleEndian.PutUint32(
		record[len(record)-8:],
		crc32.ChecksumIEEE(record[:len(record)-8]),
	)
	if _, err := s.fd.WriteAt(record, absoluteOffset); err != nil {
		t.Fatal(err)
	}
}

func assertRecoveryHardeningBlock(t *testing.T, s *WALStore, lba uint32, want []byte) {
	t.Helper()
	got, err := s.Read(lba)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("current LBA %d mismatch", lba)
	}
}
