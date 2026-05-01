package smartwal

// Completion oracle: recover(a,b) band — NOT recover(a) closure.
// See sw-block/design/recover-semantics-adjustment-plan.md §8.1.

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

// TestSmartWAL_WriteExtentDirect — INV-RECV-BITMAP-CORE (§6.10).
// smartwal-specific contract pin: WriteExtentDirect writes to the
// extent without touching the ring (no LSN appended) and without
// advancing nextLSN / walHead. The recovery receiver pairs it with
// AdvanceFrontier(targetLSN) at MarkBaseComplete; this test asserts
// the substrate-side half of that contract.
func TestSmartWAL_WriteExtentDirect(t *testing.T) {
	s := freshStore(t)
	bs := s.BlockSize()
	baseBytes := makeBlock(bs, 0xAA)

	// Pre-condition: fresh store, frontier zero.
	_, _, h0 := s.Boundaries()
	if h0 != 0 {
		t.Fatalf("fresh smartwal H=%d want 0", h0)
	}
	if next := s.NextLSN(); next != 1 {
		t.Fatalf("fresh smartwal nextLSN=%d want 1", next)
	}

	// WriteExtentDirect succeeds and Read returns the bytes (smartwal
	// reads always come from the extent — the ring is not consulted).
	if err := s.WriteExtentDirect(3, baseBytes); err != nil {
		t.Fatalf("WriteExtentDirect: %v", err)
	}
	got, err := s.Read(3)
	if err != nil {
		t.Fatalf("Read after WriteExtentDirect: %v", err)
	}
	if !bytes.Equal(got, baseBytes) {
		t.Errorf("Read(3) returned %02x... want %02x...", got[:2], baseBytes[:2])
	}

	// Frontier MUST NOT have advanced — BASE has no LSN at this layer.
	_, _, h1 := s.Boundaries()
	if h1 != 0 {
		t.Errorf("WriteExtentDirect advanced H to %d (must NOT touch frontier on smartwal)", h1)
	}
	if next := s.NextLSN(); next != 1 {
		t.Errorf("WriteExtentDirect advanced nextLSN to %d (smartwal ring should be untouched)", next)
	}

	// A subsequent ApplyEntry at LSN > 0 goes through the ring + writeAt
	// path and shadows the BASE bytes — smartwal's reads always pull
	// from the extent, and ApplyEntry's writeAt overwrites the same
	// extent offset, so the WAL bytes win at the byte level.
	walBytes := makeBlock(bs, 0xBB)
	if err := s.ApplyEntry(3, walBytes, 5); err != nil {
		t.Fatalf("ApplyEntry after WriteExtentDirect: %v", err)
	}
	got2, err := s.Read(3)
	if err != nil {
		t.Fatalf("Read after ApplyEntry: %v", err)
	}
	if !bytes.Equal(got2, walBytes) {
		t.Errorf("Read(3) after ApplyEntry returned %02x... want %02x...", got2[:2], walBytes[:2])
	}
	_, _, h2 := s.Boundaries()
	if h2 < 5 {
		t.Errorf("after ApplyEntry(lsn=5) H=%d want >= 5 (ApplyEntry's normal frontier side effect)", h2)
	}

	// Bypass-vs-WAL ordering — running another WriteExtentDirect for
	// the SAME LBA AFTER an ApplyEntry would naively overwrite the
	// extent. The receiver's bitmap discipline (§6.10) prevents this
	// at the application layer; but at the substrate layer, the call
	// IS technically still allowed and observable (smartwal does not
	// enforce arbitration — bitmap is the sole arbiter, by design).
	// Pin that observation so a future "let's silently dedupe at
	// substrate" change doesn't quietly subvert §6.10.
	staleBase := makeBlock(bs, 0xCC)
	if err := s.WriteExtentDirect(3, staleBase); err != nil {
		t.Fatalf("WriteExtentDirect (post-ApplyEntry, substrate must allow): %v", err)
	}
	got3, _ := s.Read(3)
	if !bytes.Equal(got3, staleBase) {
		t.Errorf("substrate-level: WriteExtentDirect after ApplyEntry must overwrite (bitmap arbitration is at receiver layer); got %02x want %02x", got3[0], staleBase[0])
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

// TestSmartWAL_CrashFamily_AbruptKillAtMultipleWindows is the
// SmartWAL parallel of WALStore's crash family. SmartWAL's
// architecture has fewer "windows" than WALStore because it has no
// separate WAL→extent flush step (writes go directly to extent, one
// fsync per Sync covers both extent and metadata). The meaningful
// windows here are:
//
//   - kill_pre_sync: writes returned but no Sync was called yet.
//     Nothing required to survive. Reads of those LBAs return
//     either the written bytes (if the extent write reached disk)
//     or zeros (if it did not) — never garbage.
//   - kill_post_sync: writes followed by a successful Sync, then
//     immediate kill. Acked writes must survive.
//   - kill_after_acked_then_unacked_overwrite: write A to LBA 0 +
//     Sync; overwrite LBA 0 with B (no Sync); kill. The
//     failure-atomicity claim is that LBA 0 reads either A or B —
//     never garbage, never some other LBA's data, never a third
//     value that was never written.
//
// "Abrupt kill" is simulated by dropping the file handle without
// going through Close — no implicit fsync, no housekeeping.
func TestSmartWAL_CrashFamily_AbruptKillAtMultipleWindows(t *testing.T) {
	t.Run("kill_pre_sync", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "store.bin")

		patterns := [][]byte{
			makeBlock(4096, 0xA0),
			makeBlock(4096, 0xA1),
			makeBlock(4096, 0xA2),
		}
		func() {
			s, err := CreateStore(path, 16, 4096)
			if err != nil {
				t.Fatal(err)
			}
			for i, data := range patterns {
				if _, err := s.Write(uint32(i), data); err != nil {
					t.Fatal(err)
				}
			}
			_ = s.fd.Close() // bypass Close — no fsync, no metadata persist
		}()

		s, err := OpenStore(path)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if _, err := s.Recover(); err != nil {
			t.Fatal(err)
		}
		zero := make([]byte, 4096)
		for i, want := range patterns {
			got, err := s.Read(uint32(i))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) && !bytes.Equal(got, zero) {
				t.Fatalf("LBA %d after pre-Sync kill: got neither write nor zero — corruption: got[0]=%02x",
					i, got[0])
			}
		}
	})

	t.Run("kill_post_sync", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "store.bin")

		patterns := [][]byte{
			makeBlock(4096, 0xB0),
			makeBlock(4096, 0xB1),
			makeBlock(4096, 0xB2),
		}
		func() {
			s, err := CreateStore(path, 16, 4096)
			if err != nil {
				t.Fatal(err)
			}
			for i, data := range patterns {
				if _, err := s.Write(uint32(i), data); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			_ = s.fd.Close()
		}()

		s, err := OpenStore(path)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if _, err := s.Recover(); err != nil {
			t.Fatal(err)
		}
		for i, want := range patterns {
			got, err := s.Read(uint32(i))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("LBA %d ACKED bytes did not survive post-Sync kill: got[0]=%02x want[0]=%02x",
					i, got[0], want[0])
			}
		}
	})

	t.Run("kill_after_acked_then_unacked_overwrite", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "store.bin")

		acked := makeBlock(4096, 0xCC)     // pattern A — acked via Sync
		overwrite := makeBlock(4096, 0xDD) // pattern B — written, never Sync'd

		func() {
			s, err := CreateStore(path, 16, 4096)
			if err != nil {
				t.Fatal(err)
			}
			// Acked: A on LBA 0.
			if _, err := s.Write(0, acked); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			// Unacked overwrite: B on LBA 0. Extent has B (atomic at
			// 4KB-on-4KB-sector); metadata for LSN=2 may or may not
			// have hit disk.
			if _, err := s.Write(0, overwrite); err != nil {
				t.Fatal(err)
			}
			_ = s.fd.Close()
		}()

		s, err := OpenStore(path)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if _, err := s.Recover(); err != nil {
			t.Fatal(err)
		}
		got, err := s.Read(0)
		if err != nil {
			t.Fatal(err)
		}
		// Failure atomicity: read returns either the acked bytes or
		// the unacked-overwrite bytes — never garbage, never another
		// LBA's data, never a third value that was never written.
		if !bytes.Equal(got, acked) && !bytes.Equal(got, overwrite) {
			t.Fatalf("LBA 0 after acked+unacked-overwrite kill: got neither A nor B — corruption: got[0]=%02x acked[0]=%02x overwrite[0]=%02x",
				got[0], acked[0], overwrite[0])
		}
		// Other LBAs must remain zero (no cross-LBA contamination).
		zero := make([]byte, 4096)
		for i := uint32(1); i < 4; i++ {
			other, _ := s.Read(i)
			if !bytes.Equal(other, zero) {
				t.Fatalf("LBA %d should be untouched, got non-zero bytes: got[0]=%02x",
					i, other[0])
			}
		}
	})
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

