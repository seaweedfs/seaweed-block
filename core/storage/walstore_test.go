package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// TestWALStore_WriteSyncCloseReopenRead is the headline acceptance:
// a clean Sync → Close → Open → Recover round-trip preserves every
// acked block.
func TestWALStore_WriteSyncCloseReopenRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	{
		s, err := CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 4; i++ {
			data := makeBlock(4096, byte(0xA0+i))
			if _, err := s.Write(i, data); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}

	{
		s, err := OpenWALStore(path)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if _, err := s.Recover(); err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 4; i++ {
			got, err := s.Read(i)
			if err != nil {
				t.Fatal(err)
			}
			want := makeBlock(4096, byte(0xA0+i))
			if !bytes.Equal(got, want) {
				t.Fatalf("LBA %d: bytes did not survive close+reopen", i)
			}
		}
	}
}

// TestWALStore_AckedWritesSurviveSimulatedCrash is the real
// crash-consistency proof. A clean Close persists superblock state,
// which would mask any per-write durability bug. This test bypasses
// Close entirely, simulating a kill -9.
//
// Pattern:
//   1. Write blocks 0,1,2 + Sync (acked, must survive)
//   2. Write blocks 3,4   (NOT followed by Sync — may or may not survive)
//   3. Bypass Close — drop the file handle without finalizing
//      anything (no superblock update, no fsync, no group-committer drain)
//   4. Reopen + Recover
//   5. Acked blocks 0,1,2 MUST be present and intact
//   6. Post-Sync blocks 3,4 may be present OR absent — never corrupt
//      versions of acked data
func TestWALStore_AckedWritesSurviveSimulatedCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	// Write + Sync + simulated crash. NO Close call.
	func() {
		s, err := CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 3; i++ {
			if _, err := s.Write(i, makeBlock(4096, byte(0xA0+i))); err != nil {
				t.Fatal(err)
			}
		}
		// Acked: these MUST survive crash.
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		// Unacked: may or may not survive.
		for i := uint32(3); i < 5; i++ {
			if _, err := s.Write(i, makeBlock(4096, byte(0xA0+i))); err != nil {
				t.Fatal(err)
			}
		}
		// Simulate crash: stop the group committer (so it doesn't
		// race the file close), then drop the *os.File handle without
		// going through Close(). The on-disk state is whatever was
		// fsync'd by the Sync call above; nothing else.
		s.committer.Stop()
		_ = s.fd.Close()
	}()

	// Reopen + Recover. The acked blocks must be present.
	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatalf("OpenWALStore after simulated crash: %v", err)
	}
	defer s.Close()

	if _, err := s.Recover(); err != nil {
		t.Fatalf("Recover after simulated crash: %v", err)
	}

	// Acked: must be intact.
	for i := uint32(0); i < 3; i++ {
		got, err := s.Read(i)
		if err != nil {
			t.Fatalf("Read LBA %d: %v", i, err)
		}
		want := makeBlock(4096, byte(0xA0+i))
		if !bytes.Equal(got, want) {
			t.Fatalf("LBA %d: ACKED data did not survive simulated crash; got[0]=%02x want[0]=%02x len(got)=%d",
				i, got[0], want[0], len(got))
		}
	}

	// Unacked: may or may not be present, but if present must be
	// the bytes we wrote (never some other LBA's data).
	for i := uint32(3); i < 5; i++ {
		got, err := s.Read(i)
		if err != nil {
			t.Fatal(err)
		}
		want := makeBlock(4096, byte(0xA0+i))
		zero := make([]byte, 4096)
		if !bytes.Equal(got, want) && !bytes.Equal(got, zero) {
			t.Fatalf("LBA %d: post-Sync write recovered to non-write/non-zero state — corruption: got[0]=%02x",
				i, got[0])
		}
	}
}

// TestWALStore_RecoverIsIdempotent: calling Recover twice on the same
// on-disk state must yield identical results, per the LogicalStorage
// contract.
func TestWALStore_RecoverIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	{
		s, err := CreateWALStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 3; i++ {
			_, _ = s.Write(i, makeBlock(4096, byte(i+1)))
		}
		_, _ = s.Sync()
		_ = s.Close()
	}

	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	r1, err := s.Recover()
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if r1 != r2 {
		t.Fatalf("Recover not idempotent: r1=%d r2=%d", r1, r2)
	}
}

// TestWALStore_OpenRejectsBadMagic: corruption-detection sanity.
func TestWALStore_OpenRejectsBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bogus.bin")
	if err := os.WriteFile(path, []byte("not_a_valid_store"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := OpenWALStore(path)
	if err == nil {
		t.Fatal("OpenWALStore on bogus file should error")
	}
}
