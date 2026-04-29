package storage

import (
	"testing"
)

// G7-redo priority 2.5 — WAL recycle path consumes
// `MinPinAcrossActiveSessions()` from an external coordinator (today
// `core/recovery.PeerShipCoordinator`). These tests pin the substrate
// gate directly: with no source set the legacy recycle proceeds; with
// a source reporting an active session the checkpoint cannot advance
// past the reported floor.
//
// Pinned by: INV-RECYCLE-GATED-BY-MIN-ACTIVE-PIN.
//
// Per docs/recovery-wiring-plan.md §6: "a new
// walstore_recycle_pin_test.go confirms recycle floor is gated by min
// pin when source is set; falls back to existing logic when nil."

// fakeRecycleFloorSource is a hand-written stub for
// storage.RecycleFloorSource. The substrate-level test uses this
// rather than depending on `core/recovery` (substrate stays orthogonal
// to engine — same import discipline as `core/storage` itself).
type fakeRecycleFloorSource struct {
	floor     uint64
	anyActive bool
}

func (f *fakeRecycleFloorSource) MinPinAcrossActiveSessions() (uint64, bool) {
	return f.floor, f.anyActive
}

// TestWALStore_RecycleGate_NilSource_LegacyBehavior pins fallback:
// when no source is installed (the daemon is in legacy mode, or
// dual-lane wiring opted out for this volume), persistCheckpoint
// advances exactly as it did before priority 2.5 landed.
func TestWALStore_RecycleGate_NilSource_LegacyBehavior(t *testing.T) {
	ws := freshWALStore(t)
	advanceCheckpointForTest(t, ws, 50) // start at checkpoint=50

	// No source set → checkpoint advances to 100 unmodified.
	if err := ws.persistCheckpoint(100); err != nil {
		t.Fatalf("persistCheckpoint(100): %v", err)
	}
	if got := ws.CheckpointLSN(); got != 100 {
		t.Errorf("CheckpointLSN = %d, want 100 (nil source = no clamp)", got)
	}
}

// TestWALStore_RecycleGate_SourceInactive_LegacyBehavior pins the
// `(_, false)` branch: a source that reports "no active session"
// MUST NOT clamp. This is the steady-state (no rebuild in flight)
// path; legacy behavior is preserved.
func TestWALStore_RecycleGate_SourceInactive_LegacyBehavior(t *testing.T) {
	ws := freshWALStore(t)
	advanceCheckpointForTest(t, ws, 50)

	src := &fakeRecycleFloorSource{floor: 60, anyActive: false}
	ws.SetRecycleFloorSource(src)

	// Even though src.floor=60 (< 100), anyActive=false → no clamp.
	if err := ws.persistCheckpoint(100); err != nil {
		t.Fatalf("persistCheckpoint(100): %v", err)
	}
	if got := ws.CheckpointLSN(); got != 100 {
		t.Errorf("CheckpointLSN = %d, want 100 (anyActive=false bypasses gate)", got)
	}
}

// TestWALStore_RecycleGate_SourceActive_ClampsAtFloor pins the
// headline behavior: an active session with floor=70 forces the
// effective checkpoint to 70 even though the flusher proposed 100.
// This is what holds the WAL retained for a replica that has
// committed pin_floor=70 in its rebuild session.
func TestWALStore_RecycleGate_SourceActive_ClampsAtFloor(t *testing.T) {
	ws := freshWALStore(t)
	advanceCheckpointForTest(t, ws, 50)

	src := &fakeRecycleFloorSource{floor: 70, anyActive: true}
	ws.SetRecycleFloorSource(src)

	// Flusher proposes 100; gate clamps to 70.
	if err := ws.persistCheckpoint(100); err != nil {
		t.Fatalf("persistCheckpoint(100): %v", err)
	}
	if got := ws.CheckpointLSN(); got != 70 {
		t.Errorf("CheckpointLSN = %d, want 70 (clamped to active pin floor)", got)
	}
}

// TestWALStore_RecycleGate_FloorAboveProposed_NoClamp pins the
// "floor doesn't pull checkpoint forward" rule: when the source
// reports floor=200 but the flusher only has 100 ready, the
// effective checkpoint is the proposed 100 — the floor is an
// upper-bound gate, not a target.
func TestWALStore_RecycleGate_FloorAboveProposed_NoClamp(t *testing.T) {
	ws := freshWALStore(t)
	advanceCheckpointForTest(t, ws, 50)

	src := &fakeRecycleFloorSource{floor: 200, anyActive: true}
	ws.SetRecycleFloorSource(src)

	// floor=200 >= proposed=100 → no clamp; checkpoint advances to 100.
	if err := ws.persistCheckpoint(100); err != nil {
		t.Fatalf("persistCheckpoint(100): %v", err)
	}
	if got := ws.CheckpointLSN(); got != 100 {
		t.Errorf("CheckpointLSN = %d, want 100 (floor >= proposed → no clamp)", got)
	}
}

// TestWALStore_RecycleGate_FloorBelowCurrentCheckpoint_NoRegress pins
// the monotonic invariant: even if the source reports a floor BELOW
// the current checkpoint (e.g., transient race during session
// install), persistCheckpoint must not regress checkpointLSN. The
// `if highestLSN <= s.checkpointLSN { return nil }` guard handles
// this — the post-clamp value just becomes a no-op rather than a
// rollback.
func TestWALStore_RecycleGate_FloorBelowCurrentCheckpoint_NoRegress(t *testing.T) {
	ws := freshWALStore(t)
	advanceCheckpointForTest(t, ws, 80) // current checkpoint = 80

	// Active session reports floor=30 (below current). The clamp
	// would set highestLSN=30, but the no-regress guard returns
	// without changing checkpointLSN.
	src := &fakeRecycleFloorSource{floor: 30, anyActive: true}
	ws.SetRecycleFloorSource(src)

	if err := ws.persistCheckpoint(100); err != nil {
		t.Fatalf("persistCheckpoint(100): %v", err)
	}
	if got := ws.CheckpointLSN(); got != 80 {
		t.Errorf("CheckpointLSN = %d, want 80 (must not regress under floor)", got)
	}
}

// TestWALStore_RecycleGate_SetRecycleFloorSourceNil_Disables pins the
// disable path: passing nil to SetRecycleFloorSource removes the gate
// (used when the daemon transitions back to legacy mode, or when a
// volume is closing down a coordinator). After Set(nil), behavior
// matches the nil-source default.
func TestWALStore_RecycleGate_SetRecycleFloorSourceNil_Disables(t *testing.T) {
	ws := freshWALStore(t)
	advanceCheckpointForTest(t, ws, 50)

	src := &fakeRecycleFloorSource{floor: 70, anyActive: true}
	ws.SetRecycleFloorSource(src)

	// First call clamps to 70.
	if err := ws.persistCheckpoint(100); err != nil {
		t.Fatalf("persistCheckpoint(100) [gated]: %v", err)
	}
	if got := ws.CheckpointLSN(); got != 70 {
		t.Fatalf("CheckpointLSN = %d, want 70 (gated)", got)
	}

	// Disable the gate.
	ws.SetRecycleFloorSource(nil)

	// Second call advances unmodified.
	if err := ws.persistCheckpoint(150); err != nil {
		t.Fatalf("persistCheckpoint(150) [ungated]: %v", err)
	}
	if got := ws.CheckpointLSN(); got != 150 {
		t.Errorf("CheckpointLSN = %d, want 150 (gate disabled)", got)
	}
}
