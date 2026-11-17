package storage

import (
	"io"
	"path/filepath"
	"testing"
)

func TestMultiBlockRecordsRequireDistinctOnDiskImplVersion(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "store"), 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.flusher.Stop()
	s.SetMultiBlockRecords(true)
	if _, err := s.WriteBatch(2, [][]byte{makeBlock(4096, 0x41), makeBlock(4096, 0x42)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, walEntryHeaderSize)
	if _, err := s.fd.ReadAt(header, int64(s.sb.WALOffset)); err != nil {
		t.Fatal(err)
	}
	if header[16] != walEntryWriteBatch {
		t.Fatalf("setup record type=%d, want batch", header[16])
	}
	disk, err := readSuperblock(io.NewSectionReader(s.fd, 0, superblockSize))
	if err != nil {
		t.Fatal(err)
	}
	if disk.ImplVersion == 1 {
		t.Fatal("synced type0x04 multi-block record still advertises legacy impl version1; a pre-multiblock opener cannot reject the incompatible file by version")
	}
}
