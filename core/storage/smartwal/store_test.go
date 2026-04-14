package smartwal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// All the same contract assertions used in core/storage/contract_test.go
// — we duplicate the small subset here rather than depending on a
// test-helper export so this experimental subpackage stays
// self-contained and removable without touching the main storage
// package's public surface.

func makeBlock(blockSize int, fillByte byte) []byte {
	b := make([]byte, blockSize)
	for i := range b {
		b[i] = fillByte
	}
	return b
}

func freshStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")
	s, err := CreateStore(path, 16, 4096)
	if err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// --- LogicalStorage contract surface (the same shape as the main
// core/storage contract tests). ---

func TestSmartWAL_SatisfiesLogicalStorageInterface(t *testing.T) {
	// Compile-time check is in store.go; this is a runtime sanity that
	// the type can be assigned to the interface.
	var s storage.LogicalStorage = freshStore(t)
	_ = s
}

func TestSmartWAL_WriteReadRoundTrip(t *testing.T) {
	s := freshStore(t)
	want := makeBlock(s.BlockSize(), 0xAB)
	lsn, err := s.Write(0, want)
	if err != nil {
		t.Fatal(err)
	}
	if lsn == 0 {
		t.Fatal("Write returned LSN 0")
	}
	got, err := s.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Read returned wrong bytes")
	}
}

func TestSmartWAL_ReadUnwrittenLBAReturnsZeros(t *testing.T) {
	s := freshStore(t)
	got, err := s.Read(7)
	if err != nil {
		t.Fatal(err)
	}
	zero := make([]byte, s.BlockSize())
	if !bytes.Equal(got, zero) {
		t.Fatal("unread LBA should be zeros")
	}
}

func TestSmartWAL_WriteAdvancesLSN(t *testing.T) {
	s := freshStore(t)
	var last uint64
	for i := uint32(0); i < 5; i++ {
		lsn, err := s.Write(i, makeBlock(s.BlockSize(), byte(i)))
		if err != nil {
			t.Fatal(err)
		}
		if lsn <= last {
			t.Fatalf("LSN did not advance: prev=%d got=%d", last, lsn)
		}
		last = lsn
	}
}

func TestSmartWAL_SyncReturnsHighestWritten(t *testing.T) {
	s := freshStore(t)
	var highest uint64
	for i := uint32(0); i < 3; i++ {
		lsn, err := s.Write(i, makeBlock(s.BlockSize(), 1))
		if err != nil {
			t.Fatal(err)
		}
		highest = lsn
	}
	synced, err := s.Sync()
	if err != nil {
		t.Fatal(err)
	}
	if synced != highest {
		t.Fatalf("Sync returned %d, want %d", synced, highest)
	}
}

func TestSmartWAL_BoundariesReflectWrites(t *testing.T) {
	s := freshStore(t)
	R, S, H := s.Boundaries()
	if R != 0 || S != 0 || H != 0 {
		t.Fatalf("fresh: R=%d S=%d H=%d, all want 0", R, S, H)
	}
	_, _ = s.Write(0, makeBlock(s.BlockSize(), 1))
	_, _ = s.Sync()
	R, _, H = s.Boundaries()
	if R == 0 || H == 0 || R != H {
		t.Fatalf("after sync of one write: R=%d H=%d, want R==H>0", R, H)
	}
}

func TestSmartWAL_ApplyEntryUsesSuppliedLSN(t *testing.T) {
	s := freshStore(t)
	want := makeBlock(s.BlockSize(), 0xCD)
	if err := s.ApplyEntry(3, want, 100); err != nil {
		t.Fatal(err)
	}
	got, err := s.Read(3)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("ApplyEntry did not round-trip")
	}
	_, _, H := s.Boundaries()
	if H != 100 {
		t.Fatalf("ApplyEntry should bump H to supplied LSN, got %d", H)
	}
	next, err := s.Write(4, makeBlock(s.BlockSize(), 0xEF))
	if err != nil {
		t.Fatal(err)
	}
	if next <= 100 {
		t.Fatalf("Write after ApplyEntry must allocate beyond supplied LSN, got %d", next)
	}
}

func TestSmartWAL_AdvanceFrontierBumpsH(t *testing.T) {
	s := freshStore(t)
	s.AdvanceFrontier(50)
	_, _, H := s.Boundaries()
	if H != 50 {
		t.Fatalf("AdvanceFrontier(50) should set H=50, got %d", H)
	}
	lsn, _ := s.Write(0, makeBlock(s.BlockSize(), 1))
	if lsn <= 50 {
		t.Fatalf("Write after AdvanceFrontier(50): lsn=%d, want > 50", lsn)
	}
}

func TestSmartWAL_AdvanceWALTailMovesS(t *testing.T) {
	s := freshStore(t)
	_, _ = s.Write(0, makeBlock(s.BlockSize(), 1))
	s.AdvanceWALTail(20)
	_, S, _ := s.Boundaries()
	if S != 20 {
		t.Fatalf("AdvanceWALTail(20): S=%d, want 20", S)
	}
	s.AdvanceWALTail(5)
	_, S, _ = s.Boundaries()
	if S != 20 {
		t.Fatalf("AdvanceWALTail(5) after 20 must not regress: S=%d", S)
	}
}

func TestSmartWAL_CloseIdempotent(t *testing.T) {
	s := freshStore(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSmartWAL_RecoverFreshReturnsZero(t *testing.T) {
	s := freshStore(t)
	r, err := s.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if r != 0 {
		t.Fatalf("fresh Recover: got %d, want 0", r)
	}
}

// --- SmartWAL-specific tests: persistence + crash consistency. ---

func TestSmartWAL_WriteSyncCloseReopenRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	{
		s, err := CreateStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 4; i++ {
			if _, err := s.Write(i, makeBlock(4096, byte(0xA0+i))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
	}
	{
		s, err := OpenStore(path)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if _, err := s.Recover(); err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 4; i++ {
			got, _ := s.Read(i)
			want := makeBlock(4096, byte(0xA0+i))
			if !bytes.Equal(got, want) {
				t.Fatalf("LBA %d: bytes did not survive close+reopen", i)
			}
		}
	}
}

// TestSmartWAL_AckedWritesSurviveSimulatedCrash mirrors the WALStore
// equivalent: drop the file handle without going through Close (no
// finalizing fsync in our path either, since Sync was the durability
// boundary). Acked blocks must come back; unacked may or may not but
// must not corrupt acked ones.
func TestSmartWAL_AckedWritesSurviveSimulatedCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	func() {
		s, err := CreateStore(path, 16, 4096)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 3; i++ {
			if _, err := s.Write(i, makeBlock(4096, byte(0xA0+i))); err != nil {
				t.Fatal(err)
			}
		}
		// Acked: must survive.
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		// Unacked: may or may not survive.
		for i := uint32(3); i < 5; i++ {
			if _, err := s.Write(i, makeBlock(4096, byte(0xA0+i))); err != nil {
				t.Fatal(err)
			}
		}
		// Drop file handle without Close — no implicit fsync, no
		// shutdown housekeeping.
		_ = s.fd.Close()
	}()

	s, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore after simulated crash: %v", err)
	}
	defer s.Close()
	if _, err := s.Recover(); err != nil {
		t.Fatalf("Recover after simulated crash: %v", err)
	}

	for i := uint32(0); i < 3; i++ {
		got, err := s.Read(i)
		if err != nil {
			t.Fatal(err)
		}
		want := makeBlock(4096, byte(0xA0+i))
		if !bytes.Equal(got, want) {
			t.Fatalf("LBA %d: ACKED data did not survive simulated crash; got[0]=%02x want[0]=%02x",
				i, got[0], want[0])
		}
	}
	// Unacked: present-as-written or absent (zero); never a different
	// LBA's data, never garbage.
	for i := uint32(3); i < 5; i++ {
		got, _ := s.Read(i)
		want := makeBlock(4096, byte(0xA0+i))
		zero := make([]byte, 4096)
		if !bytes.Equal(got, want) && !bytes.Equal(got, zero) {
			t.Fatalf("LBA %d: post-Sync write recovered to neither write nor zero — corruption: got[0]=%02x",
				i, got[0])
		}
	}
}

// TestSmartWAL_RecoverIsIdempotent: calling Recover twice on the
// same on-disk state yields identical results.
func TestSmartWAL_RecoverIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	{
		s, err := CreateStore(path, 8, 4096)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 3; i++ {
			_, _ = s.Write(i, makeBlock(4096, byte(i+1)))
		}
		_, _ = s.Sync()
		_ = s.Close()
	}
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r1, _ := s.Recover()
	r2, _ := s.Recover()
	if r1 != r2 {
		t.Fatalf("Recover not idempotent: r1=%d r2=%d", r1, r2)
	}
}

// TestSmartWAL_OpenRejectsBadMagic.
func TestSmartWAL_OpenRejectsBadMagic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bogus.bin")
	if err := os.WriteFile(path, []byte("not_a_smartwal_store"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := OpenStore(path)
	if err == nil {
		t.Fatal("OpenStore on bogus file must error")
	}
}

// TestSmartWAL_RingWrapPreservesLatestPerLBA: with a tiny ring (4
// slots), write 12 distinct LBAs once each. The ring overwrites the
// oldest 8 records, leaving 4. Recovery sees only 4 records but
// extent has all 12 blocks. Verified extents match those 4 records;
// the other 8 LBAs read as whatever extent has (which IS the data
// we wrote, but the metadata is gone). This proves the design's
// "metadata is bounded by ring size, but extent retains all data"
// property.
func TestSmartWAL_RingWrapPreservesLatestPerLBA(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.bin")

	const ringSlots = 4
	{
		s, err := CreateStoreWithSlots(path, 32, 4096, ringSlots)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint32(0); i < 12; i++ {
			if _, err := s.Write(i, makeBlock(4096, byte(0x10+i))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
	}

	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	frontier, err := s.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if frontier == 0 {
		t.Fatal("Recover should report a non-zero frontier from the ring")
	}
	// Every LBA's extent bytes should match what we wrote, regardless
	// of whether its metadata record survived ring overwrite.
	for i := uint32(0); i < 12; i++ {
		got, _ := s.Read(i)
		want := makeBlock(4096, byte(0x10+i))
		if !bytes.Equal(got, want) {
			t.Fatalf("LBA %d extent contents lost", i)
		}
	}
}

