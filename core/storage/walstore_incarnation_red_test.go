package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

// This is existing-caller RED evidence for the format-v3 nonce/cursor
// contract. It copies real, CRC-valid WAL bytes without re-encoding them.
// Recovery may reject the damaged image, but must never serve a record
// belonging to another store incarnation as this store's data.
func TestFormatV3_RedCrossVolumeRecordCannotReplay(t *testing.T) {
	const blockSize = 4096
	dir := t.TempDir()
	donor, err := CreateWALStore(filepath.Join(dir, "donor"), 32, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	donor.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = donor.Close() })
	poison := makeBlock(blockSize, 0xD7)
	if _, err := donor.Write(3, poison); err != nil {
		t.Fatal(err)
	}
	if _, err := donor.Sync(); err != nil {
		t.Fatal(err)
	}
	// The existing codec's overhead is used only to copy the complete physical
	// record. No fields, LSN or CRC are forged by this fixture.
	record := make([]byte, walEntryHeaderSize+blockSize)
	if _, err := donor.fd.ReadAt(record, int64(donor.sb.WALOffset)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "target")
	target, err := CreateWALStore(path, 32, blockSize)
	if err != nil {
		t.Fatal(err)
	}
	target.DisableAutoFlushForRecoveryTest()
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = target.Close()
		}
	})
	if _, err := target.fd.WriteAt(record, int64(target.sb.WALOffset)); err != nil {
		t.Fatal(err)
	}
	if err := target.fd.Sync(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, target)
	closed = true
	reopened, err := OpenWALStore(path)
	if err != nil {
		t.Fatalf("fixture must open before recovery: %v", err)
	}
	reopened.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.Recover(); err != nil {
		return
	} // fail-closed is safe
	got, err := reopened.Read(3)
	if err != nil {
		return
	} // no wrong bytes served
	if !bytes.Equal(got, make([]byte, blockSize)) {
		t.Fatal("foreign-incarnation WAL record replayed: target served donor bytes")
	}
}
