package storage

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"
)

func TestWALMultiBlock_EncodeDecode(t *testing.T) {
	block0 := makeBlock(4096, 0xA0)
	block1 := makeBlock(4096, 0xB0)
	data := append(append([]byte{}, block0...), block1...)

	entry := walEntry{
		LSN:      10,
		Reserved: 2,
		Type:     walEntryWriteBatch,
		LBA:      7,
		Length:   uint32(len(data)),
		Data:     data,
	}
	encoded, err := entry.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := decodeWALEntry(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Type != walEntryWriteBatch {
		t.Fatalf("type=%d want %d", decoded.Type, walEntryWriteBatch)
	}
	if decoded.LSN != 10 || decoded.Reserved != 2 || decoded.LBA != 7 {
		t.Fatalf("identity mismatch: %+v", decoded)
	}
	if !bytes.Equal(decoded.Data[:4096], block0) || !bytes.Equal(decoded.Data[4096:], block1) {
		t.Fatal("decoded batch payload mismatch")
	}
}

func TestWALStore_MultiBlockFeatureDisabledByDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.flusher.Stop()

	if _, err := s.WriteBatch(2, [][]byte{
		makeBlock(4096, 0xA1),
		makeBlock(4096, 0xA2),
		makeBlock(4096, 0xA3),
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	entries := s.dm.snapshot()
	if len(entries) != 3 {
		t.Fatalf("dirty entries=%d want 3", len(entries))
	}
	seenOffsets := map[uint64]bool{}
	header := make([]byte, walEntryHeaderSize)
	for _, e := range entries {
		seenOffsets[e.WALOffset] = true
		if e.DataOffset != 0 {
			t.Fatalf("default dirty data offset=%d want 0", e.DataOffset)
		}
		if _, err := s.fd.ReadAt(header, int64(s.sb.WALOffset+e.WALOffset)); err != nil {
			t.Fatalf("read header: %v", err)
		}
		if typ := header[16]; typ != walEntryWrite {
			t.Fatalf("default entry type=%d want walEntryWrite", typ)
		}
	}
	if len(seenOffsets) != 3 {
		t.Fatalf("default WriteBatch offsets=%d want 3 independent records", len(seenOffsets))
	}
}

func TestWALStore_MultiBlockDirtyRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.flusher.Stop()
	s.enableMultiBlockRecordsForTest(true)

	blocks := [][]byte{
		makeBlock(4096, 0xB1),
		makeBlock(4096, 0xB2),
		makeBlock(4096, 0xB3),
	}
	lsns, err := s.WriteBatch(4, blocks)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	for i, lsn := range lsns {
		if want := uint64(i + 1); lsn != want {
			t.Fatalf("lsn[%d]=%d want %d", i, lsn, want)
		}
	}
	entries := s.dm.snapshot()
	if len(entries) != 3 {
		t.Fatalf("dirty entries=%d want 3", len(entries))
	}
	offset := entries[0].WALOffset
	for _, e := range entries {
		if e.WALOffset != offset {
			t.Fatalf("batch dirty offset changed: %d vs %d", e.WALOffset, offset)
		}
	}
	for i, want := range blocks {
		got, err := s.Read(uint32(4 + i))
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Read %d returned wrong block", i)
		}
	}
}

func TestWALStore_MultiBlockRecoverSplitsPerBlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	func() {
		s, err := CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		s.flusher.Stop()
		s.enableMultiBlockRecordsForTest(true)
		if _, err := s.WriteBatch(1, [][]byte{
			makeBlock(4096, 0xC1),
			makeBlock(4096, 0xC2),
			makeBlock(4096, 0xC3),
		}); err != nil {
			t.Fatalf("WriteBatch: %v", err)
		}
		if stable, err := s.Sync(); err != nil || stable != 3 {
			t.Fatalf("Sync stable=%d err=%v, want stable=3", stable, err)
		}
		s.committer.Stop()
		_ = s.fd.Close()
	}()

	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if recovered, err := s.Recover(); err != nil || recovered != 3 {
		t.Fatalf("Recover=%d err=%v, want 3", recovered, err)
	}
	for i, marker := range []byte{0xC1, 0xC2, 0xC3} {
		got, err := s.Read(uint32(1 + i))
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if got[0] != marker {
			t.Fatalf("Read %d marker=%02x want %02x", i, got[0], marker)
		}
	}
}

func TestWALStore_DisableAutoFlushForRecoveryTestPreservesReplayAfterCrash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	blocks := [][]byte{
		makeBlock(4096, 0xD1),
		makeBlock(4096, 0xD2),
		makeBlock(4096, 0xD3),
		makeBlock(4096, 0xD4),
	}

	func() {
		s, err := CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		s.DisableAutoFlushForRecoveryTest()
		s.enableMultiBlockRecordsForTest(true)
		if _, err := s.WriteBatch(2, blocks); err != nil {
			t.Fatalf("WriteBatch: %v", err)
		}
		if stable, err := s.Sync(); err != nil || stable != 4 {
			t.Fatalf("Sync stable=%d err=%v, want stable=4", stable, err)
		}
		if got := s.CheckpointLSN(); got != 0 {
			t.Fatalf("checkpoint=%d want 0 before crash", got)
		}
		s.committer.Stop()
		if err := s.fd.Close(); err != nil {
			t.Fatalf("close fd: %v", err)
		}
	}()

	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if recovered, err := s.Recover(); err != nil || recovered != 4 {
		t.Fatalf("Recover=%d err=%v, want 4", recovered, err)
	}
	if got := s.CheckpointLSN(); got != 0 {
		t.Fatalf("checkpoint after recovery=%d want 0", got)
	}
	for i, want := range blocks {
		got, err := s.Read(uint32(2 + i))
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Read %d returned wrong block", i)
		}
	}
}

func TestWALStore_MultiBlockRecoveryHeadLSNUsesRecoveredFrontier(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	blocks := [][]byte{
		makeBlock(4096, 0xE1),
		makeBlock(4096, 0xE2),
		makeBlock(4096, 0xE3),
		makeBlock(4096, 0xE4),
	}
	var persistedWALHeadBytes uint64

	func() {
		s, err := CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		s.DisableAutoFlushForRecoveryTest()
		s.enableMultiBlockRecordsForTest(true)
		if _, err := s.WriteBatch(2, blocks); err != nil {
			t.Fatalf("WriteBatch: %v", err)
		}
		if stable, err := s.Sync(); err != nil || stable != 4 {
			t.Fatalf("Sync stable=%d err=%v, want stable=4", stable, err)
		}
		_, _, headLSN := s.Boundaries()
		if headLSN != 4 {
			t.Fatalf("pre-close HeadLSN=%d want 4", headLSN)
		}
		persistedWALHeadBytes = s.wal.logicalHeadValue()
		if persistedWALHeadBytes <= headLSN {
			t.Fatalf("test setup did not create distinct WAL byte head: walHeadBytes=%d headLSN=%d", persistedWALHeadBytes, headLSN)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()

	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	recovered, err := s.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if recovered != 4 {
		t.Fatalf("Recover=%d want 4", recovered)
	}
	_, _, headLSN := s.Boundaries()
	if headLSN != recovered {
		t.Fatalf("HeadLSN=%d want recovered frontier %d, not WAL byte offset %d", headLSN, recovered, persistedWALHeadBytes)
	}
	if next := s.NextLSN(); next != recovered+1 {
		t.Fatalf("NextLSN=%d want %d", next, recovered+1)
	}
}

func TestWALStore_MultiBlockFlusherSplitsPerBlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.enableMultiBlockRecordsForTest(true)

	blocks := [][]byte{
		makeBlock(4096, 0xD1),
		makeBlock(4096, 0xD2),
		makeBlock(4096, 0xD3),
	}
	if _, err := s.WriteBatch(6, blocks); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if stable, err := s.Sync(); err != nil || stable != 3 {
		t.Fatalf("Sync stable=%d err=%v, want stable=3", stable, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.CheckpointLSN() >= 3 && s.dm.len() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if s.CheckpointLSN() < 3 {
		t.Fatalf("checkpoint=%d want >=3", s.CheckpointLSN())
	}
	if got := s.dm.len(); got != 0 {
		t.Fatalf("dirty map len=%d want 0 after flush", got)
	}
	for i, want := range blocks {
		got, err := s.readFromExtent(uint32(6 + i))
		if err != nil {
			t.Fatalf("read extent %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("extent block %d mismatch", i)
		}
	}
}

func TestWALStore_MultiBlockScanLBAsSplitsPerLSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.bin")
	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.flusher.Stop()
	s.enableMultiBlockRecordsForTest(true)

	if _, err := s.WriteBatch(8, [][]byte{
		makeBlock(4096, 0xE1),
		makeBlock(4096, 0xE2),
		makeBlock(4096, 0xE3),
	}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	var got []RecoveryEntry
	if err := s.ScanLBAs(1, func(e RecoveryEntry) error {
		dup := append([]byte(nil), e.Data...)
		e.Data = dup
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("ScanLBAs: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("entries=%d want 3", len(got))
	}
	for i, e := range got {
		if wantLSN := uint64(i + 1); e.LSN != wantLSN {
			t.Fatalf("entry %d LSN=%d want %d", i, e.LSN, wantLSN)
		}
		if wantLBA := uint32(8 + i); e.LBA != wantLBA {
			t.Fatalf("entry %d LBA=%d want %d", i, e.LBA, wantLBA)
		}
		if wantMarker := byte(0xE1 + i); e.Data[0] != wantMarker {
			t.Fatalf("entry %d marker=%02x want %02x", i, e.Data[0], wantMarker)
		}
	}

	header := make([]byte, walEntryHeaderSize)
	entries := s.dm.snapshot()
	if len(entries) == 0 {
		t.Fatal("dirty map empty")
	}
	if _, err := s.fd.ReadAt(header, int64(s.sb.WALOffset+entries[0].WALOffset)); err != nil {
		t.Fatalf("read header: %v", err)
	}
	if gotType := header[16]; gotType != walEntryWriteBatch {
		t.Fatalf("entry type=%d want walEntryWriteBatch", gotType)
	}
	if count := binary.LittleEndian.Uint64(header[8:16]); count != 3 {
		t.Fatalf("batch count=%d want 3", count)
	}
}
