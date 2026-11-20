package storage

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
