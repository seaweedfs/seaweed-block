package memorywal

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// G7-redo priority 2.5 — memorywal AdvanceWALTail consumes
// `MinPinAcrossActiveSessions()` from an external coordinator so the
// in-memory substrate honors the same retention inequality the on-disk
// walstore does. Without this, component-level dual-lane tests
// (`core/replication/component`) would be free to recycle past pinned
// LSNs and the gate's correctness wouldn't be exercised end-to-end in
// the in-memory test path.
//
// Pinned by: INV-RECYCLE-GATED-BY-MIN-ACTIVE-PIN.

// fakeRecycleFloorSource — substrate-level test stub. Same shape as
// the walstore test; duplicated to keep the memorywal test package
// self-contained (no `internal/testfakes` exists yet, and the stub
// is trivial).
type fakeRecycleFloorSource struct {
	floor     uint64
	anyActive bool
}

func (f *fakeRecycleFloorSource) MinPinAcrossActiveSessions() (uint64, bool) {
	return f.floor, f.anyActive
}

// seedRecords appends `n` writes so AdvanceWALTail has something to
// trim. Returns the highest LSN observed so the caller can target a
// recycle position above it.
func seedRecords(t *testing.T, s *Store, n int) uint64 {
	t.Helper()
	var lastLSN uint64
	for i := 0; i < n; i++ {
		lsn, err := s.Write(uint32(i), mkBlock(byte(i+1)))
		if err != nil {
			t.Fatalf("Write i=%d: %v", i, err)
		}
		lastLSN = lsn
	}
	return lastLSN
}

// retainStartOf is a test-only accessor that uses Boundaries() — S is
// memorywal's exposed retainStart.
func retainStartOf(s *Store) uint64 {
	_, sboundary, _ := s.Boundaries()
	return sboundary
}

// TestStore_RecycleGate_NilSource_LegacyBehavior pins fallback: when
// no source is installed AdvanceWALTail trims as before. This is the
// pre-priority-2.5 baseline; the gate must not change it.
func TestStore_RecycleGate_NilSource_LegacyBehavior(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	_ = seedRecords(t, s, 5) // LSNs 1..5

	// No source set; trim to 4 → only LSN 5 remains.
	s.AdvanceWALTail(4)

	if got := retainStartOf(s); got != 4 {
		t.Errorf("retainStart after AdvanceWALTail(4) = %d, want 4 (nil source = no clamp)", got)
	}

	// memorywal's gate fires when fromLSN+1 < recycleFloor. With
	// recycleFloor=4, fromLSN=0 (0+1 < 4) MUST be recycled — pins
	// that the trim actually reached the substrate (not silently
	// no-op'd by some unintended clamp from the gate code path).
	err := s.ScanLBAs(0, func(storage.RecoveryEntry) error { return nil })
	if err == nil {
		t.Fatal("ScanLBAs(0) after recycle to 4: want WALRecycled error")
	}
}

// TestStore_RecycleGate_SourceInactive_LegacyBehavior pins the
// `(_, false)` branch: source reports no active session → trim
// proceeds unmodified.
func TestStore_RecycleGate_SourceInactive_LegacyBehavior(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	_ = seedRecords(t, s, 5)

	src := &fakeRecycleFloorSource{floor: 2, anyActive: false}
	s.SetRecycleFloorSource(src)

	// anyActive=false → src ignored; trim to 4.
	s.AdvanceWALTail(4)
	if got := retainStartOf(s); got != 4 {
		t.Errorf("retainStart = %d, want 4 (anyActive=false bypasses gate)", got)
	}
}

// TestStore_RecycleGate_SourceActive_ClampsAtFloor pins the headline
// behavior: source reports floor=2; flusher proposes trim to 4. The
// gate clamps the effective newTail to 2 so records 2..3 are
// retained.
func TestStore_RecycleGate_SourceActive_ClampsAtFloor(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	_ = seedRecords(t, s, 5)

	src := &fakeRecycleFloorSource{floor: 2, anyActive: true}
	s.SetRecycleFloorSource(src)

	// Proposed newTail=4 → clamped to 2. retainStart should land at 2.
	s.AdvanceWALTail(4)
	if got := retainStartOf(s); got != 2 {
		t.Errorf("retainStart = %d, want 2 (clamped to active pin floor)", got)
	}

	// ScanLBAs(1) is below the new floor (recycleFloor=2).
	// memorywal's gate fires when fromLSN+1 < recycleFloor. With
	// recycleFloor=2, fromLSN=0 (0+1 < 2) returns recycled;
	// fromLSN=1 (1+1 == 2) does NOT — it's the first "still-retained"
	// position. This pins the inequality we ship.
	err := s.ScanLBAs(0, func(storage.RecoveryEntry) error { return nil })
	if err == nil {
		t.Error("ScanLBAs(0) after clamp to 2: want WALRecycled (0+1 < 2)")
	}
	calls := 0
	err = s.ScanLBAs(1, func(storage.RecoveryEntry) error {
		calls++
		return nil
	})
	if err != nil {
		t.Errorf("ScanLBAs(1) after clamp to 2: want success, got %v", err)
	}
	if calls == 0 {
		t.Error("ScanLBAs(1): want at least one entry emitted (LSNs 2..5 retained)")
	}
}

// TestStore_RecycleGate_FloorAboveProposed_NoClamp pins the
// "floor doesn't pull recycle forward" rule. If source reports
// floor=10 but the proposed newTail is 4, the effective newTail
// stays at 4 — the gate is an upper bound, not a target.
func TestStore_RecycleGate_FloorAboveProposed_NoClamp(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	_ = seedRecords(t, s, 5)

	src := &fakeRecycleFloorSource{floor: 10, anyActive: true}
	s.SetRecycleFloorSource(src)

	s.AdvanceWALTail(4) // proposed 4 < floor 10 → no clamp; trim to 4.
	if got := retainStartOf(s); got != 4 {
		t.Errorf("retainStart = %d, want 4 (floor >= proposed → no clamp)", got)
	}
}

// TestStore_RecycleGate_SetRecycleFloorSourceNil_Disables pins the
// disable path: after SetRecycleFloorSource(nil) AdvanceWALTail
// behaves as if no source were ever installed.
func TestStore_RecycleGate_SetRecycleFloorSourceNil_Disables(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	_ = seedRecords(t, s, 5)

	src := &fakeRecycleFloorSource{floor: 2, anyActive: true}
	s.SetRecycleFloorSource(src)

	// First call clamps to 2.
	s.AdvanceWALTail(4)
	if got := retainStartOf(s); got != 2 {
		t.Fatalf("retainStart = %d, want 2 (gated)", got)
	}

	// Seed more records so a second AdvanceWALTail has room to move.
	_, _ = s.Write(0, mkBlock(0xEE))
	_, _ = s.Write(1, mkBlock(0xFF))

	// Disable the gate. Now AdvanceWALTail can move past the old floor.
	s.SetRecycleFloorSource(nil)
	s.AdvanceWALTail(6)
	if got := retainStartOf(s); got != 6 {
		t.Errorf("retainStart = %d, want 6 (gate disabled)", got)
	}
}

// TestStore_RecycleGate_SatisfiesGateInterface pins the compile-time
// assertion that *Store implements storage.RecycleFloorGate. The
// daemon wiring relies on this type assertion succeeding.
func TestStore_RecycleGate_SatisfiesGateInterface(t *testing.T) {
	s := NewStore(numBlocks, blockSize)
	var gate storage.RecycleFloorGate = s
	gate.SetRecycleFloorSource(nil) // smoke
}
