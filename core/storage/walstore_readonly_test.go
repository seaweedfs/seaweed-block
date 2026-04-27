package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// runtimeNumGoroutine wraps runtime.NumGoroutine so the test file's
// import set stays minimal at the top of the file.
func runtimeNumGoroutine() int { return runtime.NumGoroutine() }

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

// TestOpenReadOnly_NoBackgroundGoroutines — architect REVIEW round 52
// MEDIUM fix: the read-only path must NOT start flusher / committer /
// admission goroutines (the original implementation did). Pin the
// fix by snapshotting goroutine count before/after Open+Read+Close.
//
// Tolerance is a small constant (Go runtime spins up housekeeping
// goroutines that are unrelated to walstore). The pin is "no growth
// proportional to walstore writer machinery" — flusher + committer
// alone would add 2 goroutines per Open.
func TestOpenReadOnly_NoBackgroundGoroutines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vol.walstore")
	w, err := CreateWALStore(path, 4, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(0, bytes.Repeat([]byte{0x42}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	before := goroutineCount()
	r, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read(0); err != nil {
		t.Fatal(err)
	}
	during := goroutineCount()
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// Expect ZERO net goroutine growth from Open. Tolerance of 1 for
	// transient runtime jitter; failing this would mean a goroutine
	// (flusher / committer / admission / etc.) was started.
	delta := during - before
	if delta > 1 {
		t.Errorf("OpenReadOnly leaked %d goroutines (before=%d during=%d) — read-only path must not start writer machinery",
			delta, before, during)
	}
}

func goroutineCount() int {
	// runtime.NumGoroutine — but we use a small wrapper to keep the
	// import locality with this test file.
	return runtimeNumGoroutine()
}
