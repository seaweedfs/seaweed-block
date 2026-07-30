package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestDirtyMapSnapshotCarriesRecordGeometry(t *testing.T) {
	dm := newDirtyMap(4)
	dm.put(7, 91, 12, 4096, 4134)
	dm.putAt(8, 200, 4096, 20, 4096, 8230)

	entries := snapshotEntriesByLBA(dm.snapshot())
	if got := entries[7].RecordSize; got != 4134 {
		t.Fatalf("ordinary record size=%d want 4134", got)
	}
	if got := entries[8].RecordSize; got != 8230 {
		t.Fatalf("shared record size=%d want 8230", got)
	}
}

func TestWALStoreDirtyRecordGeometryAppendPaths(t *testing.T) {
	const blockSize = 4096
	const ordinaryRecordSize = uint64(walEntryHeaderSize + blockSize)

	t.Run("ordinary", func(t *testing.T) {
		s := createGeometryTestStore(t)
		if _, err := s.Write(3, makeBlock(blockSize, 0x31)); err != nil {
			t.Fatal(err)
		}

		entry := snapshotEntriesByLBA(s.dm.snapshot())[3]
		if entry.RecordSize != ordinaryRecordSize {
			t.Fatalf("record size=%d want %d", entry.RecordSize, ordinaryRecordSize)
		}
	})

	t.Run("independent batch", func(t *testing.T) {
		s := createGeometryTestStore(t)
		if _, err := s.WriteBatch(4, [][]byte{
			makeBlock(blockSize, 0x41),
			makeBlock(blockSize, 0x42),
			makeBlock(blockSize, 0x43),
		}); err != nil {
			t.Fatal(err)
		}

		entries := snapshotEntriesByLBA(s.dm.snapshot())
		offsets := make(map[uint64]struct{})
		for lba := uint64(4); lba < 7; lba++ {
			entry := entries[lba]
			if entry.RecordSize != ordinaryRecordSize {
				t.Fatalf("LBA %d record size=%d want %d", lba, entry.RecordSize, ordinaryRecordSize)
			}
			offsets[entry.WALOffset] = struct{}{}
		}
		if len(offsets) != 3 {
			t.Fatalf("independent batch unique offsets=%d want 3", len(offsets))
		}
	})

	t.Run("multi block", func(t *testing.T) {
		s := createGeometryTestStore(t)
		s.enableMultiBlockRecordsForTest(true)
		if _, err := s.WriteBatch(8, [][]byte{
			makeBlock(blockSize, 0x51),
			makeBlock(blockSize, 0x52),
			makeBlock(blockSize, 0x53),
		}); err != nil {
			t.Fatal(err)
		}

		entries := snapshotEntriesByLBA(s.dm.snapshot())
		wantSize := uint64(walEntryHeaderSize + 3*blockSize)
		wantOffset := entries[8].WALOffset
		for index, lba := range []uint64{8, 9, 10} {
			entry := entries[lba]
			if entry.WALOffset != wantOffset || entry.RecordSize != wantSize {
				t.Fatalf("LBA %d identity=(%d,%d) want=(%d,%d)",
					lba, entry.WALOffset, entry.RecordSize, wantOffset, wantSize)
			}
			if wantDataOffset := uint32(index * blockSize); entry.DataOffset != wantDataOffset {
				t.Fatalf("LBA %d data offset=%d want %d", lba, entry.DataOffset, wantDataOffset)
			}
		}
	})

	t.Run("apply entry", func(t *testing.T) {
		s := createGeometryTestStore(t)
		if err := s.ApplyEntry(12, makeBlock(blockSize, 0x61), 41); err != nil {
			t.Fatal(err)
		}

		entry := snapshotEntriesByLBA(s.dm.snapshot())[12]
		if entry.RecordSize != ordinaryRecordSize {
			t.Fatalf("record size=%d want %d", entry.RecordSize, ordinaryRecordSize)
		}
	})
}

func TestWALStoreRecoverReconstructsRecordGeometry(t *testing.T) {
	const blockSize = 4096
	path := filepath.Join(t.TempDir(), "store.bin")

	func() {
		s, err := CreateWALStore(path, 32, blockSize)
		if err != nil {
			t.Fatal(err)
		}
		s.DisableAutoFlushForRecoveryTest()
		if _, err := s.Write(1, makeBlock(blockSize, 0x71)); err != nil {
			t.Fatal(err)
		}
		s.enableMultiBlockRecordsForTest(true)
		if _, err := s.WriteBatch(4, [][]byte{
			makeBlock(blockSize, 0x72),
			makeBlock(blockSize, 0x73),
			makeBlock(blockSize, 0x74),
		}); err != nil {
			t.Fatal(err)
		}
		if stable, err := s.Sync(); err != nil || stable != 4 {
			t.Fatalf("Sync stable=%d err=%v want 4,nil", stable, err)
		}
		crashGeometryTestStore(t, s)
	}()

	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	if recovered, err := s.Recover(); err != nil || recovered != 4 {
		t.Fatalf("Recover=%d err=%v want 4,nil", recovered, err)
	}

	entries := snapshotEntriesByLBA(s.dm.snapshot())
	if got, want := entries[1].RecordSize, uint64(walEntryHeaderSize+blockSize); got != want {
		t.Fatalf("ordinary recovered record size=%d want %d", got, want)
	}
	wantMultiSize := uint64(walEntryHeaderSize + 3*blockSize)
	wantMultiOffset := entries[4].WALOffset
	for _, lba := range []uint64{4, 5, 6} {
		entry := entries[lba]
		if entry.WALOffset != wantMultiOffset || entry.RecordSize != wantMultiSize {
			t.Fatalf("LBA %d recovered identity=(%d,%d) want=(%d,%d)",
				lba, entry.WALOffset, entry.RecordSize, wantMultiOffset, wantMultiSize)
		}
	}
}

func TestWALStoreRecoverReconstructsLegacyTrimRecordGeometry(t *testing.T) {
	const (
		blockSize  = 4096
		trimBlocks = 3
	)
	path := filepath.Join(t.TempDir(), "store.bin")

	func() {
		s, err := CreateWALStore(path, 16, blockSize)
		if err != nil {
			t.Fatal(err)
		}
		s.DisableAutoFlushForRecoveryTest()
		for lba := uint32(3); lba < 3+trimBlocks; lba++ {
			if err := s.WriteExtentDirect(lba, makeBlock(blockSize, 0xA5)); err != nil {
				t.Fatal(err)
			}
		}
		if stable, err := s.Sync(); err != nil || stable != 0 {
			t.Fatalf("base Sync stable=%d err=%v want 0,nil", stable, err)
		}
		trim := &walEntry{
			LSN:    1,
			Type:   walEntryTrim,
			LBA:    3,
			Length: trimBlocks * blockSize,
		}
		if _, err := s.wal.append(trim); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		s.nextLSN = 2
		s.walHead = 1
		s.walTail = 1
		s.mu.Unlock()
		if stable, err := s.Sync(); err != nil || stable != 1 {
			t.Fatalf("Sync stable=%d err=%v want 1,nil", stable, err)
		}
		crashGeometryTestStore(t, s)
	}()

	reader, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	for lba := uint32(3); lba < 3+trimBlocks; lba++ {
		got, err := reader.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, make([]byte, blockSize)) {
			t.Fatalf("read-only trim LBA %d did not return zeros", lba)
		}
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	if recovered, err := s.Recover(); err != nil || recovered != 1 {
		t.Fatalf("Recover=%d err=%v want 1,nil", recovered, err)
	}

	entries := snapshotEntriesByLBA(s.dm.snapshot())
	if len(entries) != trimBlocks {
		t.Fatalf("trim dirty entries=%d want %d", len(entries), trimBlocks)
	}
	wantOffset := entries[3].WALOffset
	for index, lba := range []uint64{3, 4, 5} {
		entry := entries[lba]
		if entry.WALOffset != wantOffset ||
			entry.RecordSize != uint64(walEntryHeaderSize) ||
			entry.DataOffset != uint32(index*blockSize) {
			t.Fatalf("trim LBA %d identity=(%d,%d,%d) want=(%d,%d,%d)",
				lba, entry.WALOffset, entry.RecordSize, entry.DataOffset,
				wantOffset, walEntryHeaderSize, index*blockSize)
		}
		got, err := s.Read(uint32(lba))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, make([]byte, blockSize)) {
			t.Fatalf("dirty trim LBA %d did not return zeros", lba)
		}
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if got := s.dm.len(); got != 0 {
		t.Fatalf("trim dirty entries after flush=%d want 0", got)
	}
	if got := s.CheckpointLSN(); got != 1 {
		t.Fatalf("trim checkpoint=%d want 1", got)
	}
	for lba := uint32(3); lba < 3+trimBlocks; lba++ {
		got, err := s.readFromExtent(lba)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, make([]byte, blockSize)) {
			t.Fatalf("flushed trim LBA %d did not zero extent", lba)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALStoreRecordGeometrySurvivesRingWrap(t *testing.T) {
	const (
		blockSize = 4096
		walSize   = 16 * 1024
	)
	path := filepath.Join(t.TempDir(), "store.bin")
	s := createWALStoreWithWALSizeForTest(t, path, 16, blockSize, walSize)
	s.DisableAutoFlushForRecoveryTest()

	for lba := uint32(0); lba < 3; lba++ {
		if _, err := s.Write(lba, makeBlock(blockSize, byte(0x90+lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if got := s.dm.len(); got != 0 {
		t.Fatalf("dirty entries after first flush=%d want 0", got)
	}

	if _, err := s.Write(7, makeBlock(blockSize, 0x99)); err != nil {
		t.Fatal(err)
	}
	entry := snapshotEntriesByLBA(s.dm.snapshot())[7]
	if entry.WALOffset != 0 {
		t.Fatalf("wrapped WAL offset=%d want 0", entry.WALOffset)
	}
	if got, want := entry.RecordSize, uint64(walEntryHeaderSize+blockSize); got != want {
		t.Fatalf("wrapped record size=%d want %d", got, want)
	}
	if got := s.WriteInstrumentation().WALAppendWrapCount; got != 1 {
		t.Fatalf("WAL wraps=%d want 1", got)
	}
	if stable, err := s.Sync(); err != nil || stable != 4 {
		t.Fatalf("wrapped Sync stable=%d err=%v want 4,nil", stable, err)
	}
	crashGeometryTestStore(t, s)

	reopened, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened.DisableAutoFlushForRecoveryTest()
	if recovered, err := reopened.Recover(); err != nil || recovered != 4 {
		t.Fatalf("wrapped Recover=%d err=%v want 4,nil", recovered, err)
	}
	recoveredEntry := snapshotEntriesByLBA(reopened.dm.snapshot())[7]
	if recoveredEntry.WALOffset != entry.WALOffset ||
		recoveredEntry.RecordSize != entry.RecordSize {
		t.Fatalf("recovered wrapped identity=(%d,%d) want=(%d,%d)",
			recoveredEntry.WALOffset, recoveredEntry.RecordSize,
			entry.WALOffset, entry.RecordSize)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPhase172ScopedMaterializationProbe(t *testing.T) {
	path := os.Getenv("SW_BLOCK_PHASE172_SCOPED_PROBE_PATH")
	if path == "" {
		t.Skip("set SW_BLOCK_PHASE172_SCOPED_PROBE_PATH to run the syscall probe")
	}
	const (
		blockSize = 4096
		records   = 1024
	)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	s, err := CreateWALStore(path, records, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })
	mode := os.Getenv("SW_BLOCK_PHASE172_SCOPED_PROBE_MODE")
	if mode == "" {
		mode = "default"
		if os.Getenv("SW_BLOCK_PHASE172_SCOPED_PROBE_SINGLE_READ") == "true" {
			mode = "single-read"
		}
	}
	if err := configurePhase172BenchmarkMaterialization(s, mode); err != nil {
		t.Fatal(err)
	}
	singleRead := s.singleReadMaterialization.Load()

	for lba := uint32(0); lba < records; lba++ {
		if _, err := s.Write(lba, makeBlock(blockSize, byte(lba%251+1))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}

	got := s.FlusherInstrumentation()
	t.Logf("phase172_probe_mode=%s", mode)
	t.Logf("phase172_probe_single_read=%t", singleRead)
	t.Logf("phase172_probe_shared_record=%t", s.sharedRecordMaterialization.Load())
	t.Logf("phase172_probe_validated_records=%d", got.ValidatedRecords)
	t.Logf("phase172_probe_header_read_ops=%d", got.WALHeaderReadOps)
	t.Logf("phase172_probe_record_read_ops=%d", got.WALRecordReadOps)
	t.Logf("phase172_probe_materialization_read_ops=%d", got.MaterializationReadOps)
	wantHeaderReads := uint64(records)
	wantMaterializationReads := uint64(2 * records)
	if singleRead {
		wantHeaderReads = 0
		wantMaterializationReads = records
	}
	if got.ValidatedRecords != records ||
		got.WALHeaderReadOps != wantHeaderReads ||
		got.WALRecordReadOps != records ||
		got.MaterializationReadOps != uint64(wantMaterializationReads) {
		t.Fatalf("validated/header/record/materialization=%d/%d/%d/%d want %d/%d/%d/%d",
			got.ValidatedRecords, got.WALHeaderReadOps, got.WALRecordReadOps,
			got.MaterializationReadOps, records, wantHeaderReads, records,
			wantMaterializationReads)
	}
}

func TestPhase172ScopedSharedRecordProbe(t *testing.T) {
	path := os.Getenv("SW_BLOCK_PHASE172_SHARED_PROBE_PATH")
	if path == "" {
		t.Skip("set SW_BLOCK_PHASE172_SHARED_PROBE_PATH to run the shared-record probe")
	}
	const (
		blockSize       = 4096
		logicalBlocks   = 1024
		blocksPerRecord = 16
		physicalRecords = logicalBlocks / blocksPerRecord
	)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	s, err := CreateWALStore(path, logicalBlocks, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	s.enableMultiBlockRecordsForTest(true)
	mode := os.Getenv("SW_BLOCK_PHASE172_SHARED_PROBE_MODE")
	if mode == "" {
		mode = "shared-record"
	}
	if err := configurePhase172BenchmarkMaterialization(s, mode); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	for base := uint32(0); base < logicalBlocks; base += blocksPerRecord {
		blocks := make([][]byte, blocksPerRecord)
		for index := range blocks {
			lba := base + uint32(index)
			blocks[index] = makeBlock(blockSize, byte(lba%251+1))
		}
		if _, err := s.WriteBatch(base, blocks); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}

	got := s.FlusherInstrumentation()
	t.Logf("phase172_shared_probe_mode=%s", mode)
	t.Logf("phase172_shared_probe_enabled=%t", s.sharedRecordMaterialization.Load())
	t.Logf("phase172_shared_probe_snapshot_entries=%d", got.SnapshotEntries)
	t.Logf("phase172_shared_probe_unique_records=%d", got.SnapshotUniqueWALRecords)
	t.Logf("phase172_shared_probe_reuse_candidates=%d", got.SnapshotRecordReuseCandidates)
	t.Logf("phase172_shared_probe_validated_records=%d", got.ValidatedRecords)
	t.Logf("phase172_shared_probe_header_read_ops=%d", got.WALHeaderReadOps)
	t.Logf("phase172_shared_probe_record_read_ops=%d", got.WALRecordReadOps)
	t.Logf("phase172_shared_probe_materialization_read_ops=%d", got.MaterializationReadOps)
	t.Logf("phase172_shared_probe_reuse_hits=%d", got.MaterializationRecordReuseHits)
	wantHeaderReads := uint64(0)
	wantRecordReads := uint64(physicalRecords)
	wantMaterializationReads := uint64(physicalRecords)
	wantReuseHits := uint64(logicalBlocks - physicalRecords)
	if mode == "default" {
		wantHeaderReads = logicalBlocks
		wantRecordReads = logicalBlocks
		wantMaterializationReads = 2 * logicalBlocks
		wantReuseHits = 0
	} else if mode != "shared-record" {
		t.Fatalf("shared probe mode=%q want default or shared-record", mode)
	}
	if got.SnapshotEntries != logicalBlocks ||
		got.SnapshotUniqueWALRecords != physicalRecords ||
		got.SnapshotRecordReuseCandidates != logicalBlocks-physicalRecords ||
		got.ValidatedRecords != logicalBlocks ||
		got.WALHeaderReadOps != wantHeaderReads ||
		got.WALRecordReadOps != wantRecordReads ||
		got.MaterializationReadOps != wantMaterializationReads ||
		got.MaterializationRecordReuseHits != wantReuseHits {
		t.Fatalf("entries/unique/candidates/validated/header/record/materialization/reuse=%d/%d/%d/%d/%d/%d/%d/%d want 1024/64/960/1024/%d/%d/%d/%d",
			got.SnapshotEntries, got.SnapshotUniqueWALRecords,
			got.SnapshotRecordReuseCandidates, got.ValidatedRecords,
			got.WALHeaderReadOps, got.WALRecordReadOps,
			got.MaterializationReadOps, got.MaterializationRecordReuseHits,
			wantHeaderReads, wantRecordReads, wantMaterializationReads, wantReuseHits)
	}
	if got := s.CheckpointLSN(); got != logicalBlocks {
		t.Fatalf("checkpoint=%d want %d", got, logicalBlocks)
	}
	if got := s.dm.len(); got != 0 {
		t.Fatalf("dirty entries=%d want 0", got)
	}
}

func createGeometryTestStore(t *testing.T) *WALStore {
	t.Helper()
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store.bin"), 32, 4096)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func crashGeometryTestStore(t *testing.T, s *WALStore) {
	t.Helper()
	s.committer.Stop()
	if err := s.fd.Close(); err != nil {
		t.Fatal(err)
	}
}

func snapshotEntriesByLBA(entries []snapshotEntry) map[uint64]snapshotEntry {
	result := make(map[uint64]snapshotEntry, len(entries))
	for _, entry := range entries {
		result[entry.LBA] = entry
	}
	return result
}
