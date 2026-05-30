package smartwal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInspectLayoutReadsSmartWALGeometry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.sw")
	s, err := CreateStoreWithSlots(path, 4, 4096, 8)
	if err != nil {
		t.Fatalf("CreateStoreWithSlots: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	layout, err := InspectLayout(path)
	if err != nil {
		t.Fatalf("InspectLayout: %v", err)
	}

	if layout.HeaderSize != 4096 {
		t.Fatalf("HeaderSize=%d want 4096", layout.HeaderSize)
	}
	if layout.RecordSize != 32 {
		t.Fatalf("RecordSize=%d want 32", layout.RecordSize)
	}
	if layout.WALOffset != 4096 {
		t.Fatalf("WALOffset=%d want 4096", layout.WALOffset)
	}
	if layout.WALLength != 8*32 {
		t.Fatalf("WALLength=%d want %d", layout.WALLength, 8*32)
	}
	if layout.ExtentStart != 4096+8*32 {
		t.Fatalf("ExtentStart=%d want %d", layout.ExtentStart, 4096+8*32)
	}
	if layout.ExtentBytes != 4*4096 {
		t.Fatalf("ExtentBytes=%d want %d", layout.ExtentBytes, 4*4096)
	}
	if layout.FileSize != layout.ExtentStart+layout.ExtentBytes {
		t.Fatalf("FileSize=%d want %d", layout.FileSize, layout.ExtentStart+layout.ExtentBytes)
	}

	off, err := layout.RecordOffset(9)
	if err != nil {
		t.Fatalf("RecordOffset: %v", err)
	}
	// slot = 9 % 8 = 1
	if off != layout.WALOffset+layout.RecordSize {
		t.Fatalf("RecordOffset(9)=%d want %d", off, layout.WALOffset+layout.RecordSize)
	}
	if !layout.ContainsWALOffset(off) {
		t.Fatalf("record offset %d should be inside WAL ring", off)
	}
	if !layout.ContainsExtentOffset(layout.ExtentStart) {
		t.Fatalf("extent start should be inside extent")
	}
}

func TestInspectLayoutRejectsNonSmartWAL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "not-smartwal.bin")
	if err := os.WriteFile(path, []byte("not a smartwal file"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := InspectLayout(path); err == nil {
		t.Fatal("InspectLayout should reject non-SmartWAL files")
	}
}

func TestInspectLayoutRejectsTruncatedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.sw")
	s, err := CreateStoreWithSlots(path, 4, 4096, 8)
	if err != nil {
		t.Fatalf("CreateStoreWithSlots: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := os.Truncate(path, 4096+8*32+1); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if _, err := InspectLayout(path); err == nil {
		t.Fatal("InspectLayout should reject truncated SmartWAL files")
	}
}

func TestInspectRecordsFindsLatestValidRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.sw")
	s, err := CreateStoreWithSlots(path, 8, 4096, 8)
	if err != nil {
		t.Fatalf("CreateStoreWithSlots: %v", err)
	}
	for i := uint32(0); i < 3; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		if _, err := s.Write(i, data); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	layout, records, err := InspectRecords(path)
	if err != nil {
		t.Fatalf("InspectRecords: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("records=%d want 3", len(records))
	}
	for i, rec := range records {
		wantLSN := uint64(i + 1)
		if rec.LSN != wantLSN {
			t.Fatalf("record[%d].LSN=%d want %d", i, rec.LSN, wantLSN)
		}
		if !layout.ContainsWALOffset(rec.Offset) {
			t.Fatalf("record[%d] offset %d should be inside WAL", i, rec.Offset)
		}
		if rec.BytesHex == "" {
			t.Fatalf("record[%d] missing byte sample", i)
		}
	}

	_, latest, err := LatestRecord(path)
	if err != nil {
		t.Fatalf("LatestRecord: %v", err)
	}
	if latest.LSN != 3 || latest.LBA != 2 {
		t.Fatalf("latest=%+v want LSN=3 LBA=2", latest)
	}
}
