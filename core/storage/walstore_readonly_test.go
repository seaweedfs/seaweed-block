package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestOpenReadOnly_RoundTripWriteThenRead — pin G5-5: a value
// written through the R/W path is observable via OpenReadOnly+Read.
// Closes #2 acceptance criterion's "storage abstraction is the only
// authoritative read path" claim by exercising the same merge logic.
func TestOpenReadOnly_RoundTripWriteThenRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vol.walstore")

	// Write phase: open R/W, write LBAs 0/1/2 with distinct payloads.
	{
		w, err := CreateWALStore(path, 8 /*blocks*/, 4096 /*bs*/)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		for i := 0; i < 3; i++ {
			data := bytes.Repeat([]byte{byte(0x10 + i)}, 4096)
			if _, err := w.Write(uint32(i), data); err != nil {
				t.Fatalf("write lba=%d: %v", i, err)
			}
		}
		if _, err := w.Sync(); err != nil {
			t.Fatalf("sync: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close R/W: %v", err)
		}
	}

	// Read phase: open read-only, verify each LBA's bytes.
	r, err := OpenReadOnly(path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer func() { _ = r.Close() }()
	for i := 0; i < 3; i++ {
		got, err := r.Read(uint32(i))
		if err != nil {
			t.Fatalf("read lba=%d: %v", i, err)
		}
		want := bytes.Repeat([]byte{byte(0x10 + i)}, 4096)
		if !bytes.Equal(got, want) {
			t.Fatalf("lba=%d bytes mismatch: got[0]=%02x want[0]=%02x", i, got[0], want[0])
		}
	}

	// Close idempotency.
	if err := r.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
}

// TestOpenReadOnly_FailsOnMissingFile — cheap fence.
func TestOpenReadOnly_FailsOnMissingFile(t *testing.T) {
	r, err := OpenReadOnly(filepath.Join(t.TempDir(), "does-not-exist"))
	if err == nil {
		_ = r.Close()
		t.Fatal("OpenReadOnly on missing file: want error")
	}
	if _, ok := err.(*os.PathError); ok {
		// fine — wrapped os error
	}
}
