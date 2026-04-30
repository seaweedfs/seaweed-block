package transport

// P1 — BlockExecutor ↔ WalShipper integration tests.
//
// Spec drivers:
//   - v3-recovery-wal-shipper-spec.md §3 INV-SINGLE — at most one
//     WalShipper instance per (volume, replicaID).
//   - v3-recovery-wal-shipper-mini-plan.md §3 P1 — "BlockExecutor
//     registry: create/destroy WalShipper with StartRebuild/
//     EndSession/steady attach; Ship delegates Emit".
//   - mini-plan §4 row "INV-NO-DOUBLE-LIVE (CHK surrogate) — Two
//     entrypoints: call WalShipper + legacy Ship mock — after P1,
//     Ship must not bypass (single emit counter)".
//
// Test scope (architect 2026-04-29 ratification):
//   - 3a: Ship() delegates to per-replica WalShipper — observable
//     via WalShipper.Cursor() advancing per Ship() call.
//   - 3b: Registry idempotent — WalShipperFor returns same instance
//     for same replicaID; different instances for different replicaIDs.
//   - 3c (deferred): cross-component "Sender + Ship same replica
//     INV-NO-DOUBLE-LIVE" integration — nightly / follow-up per
//     architect.

import (
	"io"
	"net"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// TestBlockExecutor_WalShipperRegistry_GetOrCreate — spec §3 INV-SINGLE
// at executor level.
//
// Idempotent get-or-create:
//   - First WalShipperFor(r1) creates and returns instance A.
//   - Second WalShipperFor(r1) returns SAME instance A.
//   - WalShipperFor(r2) returns DIFFERENT instance B.
//   - Concurrent WalShipperFor(r1) from N goroutines returns the
//     SAME instance to all callers.
//
// "INV-SINGLE: at any moment, at most one WalShipper per
// (volumeID, replicaID)" — get-or-create satisfies this trivially.
func TestBlockExecutor_WalShipperRegistry_GetOrCreate(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	exec := NewBlockExecutor(primary, "127.0.0.1:0")

	// First-time creation for r1.
	s1a := exec.WalShipperFor("r1")
	if s1a == nil {
		t.Fatal("WalShipperFor(r1) returned nil")
	}

	// Second call for same replicaID returns SAME instance.
	s1b := exec.WalShipperFor("r1")
	if s1b != s1a {
		t.Errorf("WalShipperFor(r1) idempotency violated: first=%p second=%p", s1a, s1b)
	}

	// Different replicaID returns DIFFERENT instance.
	s2 := exec.WalShipperFor("r2")
	if s2 == s1a {
		t.Error("WalShipperFor(r2) returned same instance as r1 — registry collision")
	}
	if s2 == nil {
		t.Fatal("WalShipperFor(r2) returned nil")
	}

	// Concurrent get-or-create for same replicaID — all goroutines
	// must observe the same instance (or at most one goroutine wins
	// the create race; others see its result).
	const goroutines = 16
	var wg sync.WaitGroup
	results := make([]*WalShipper, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			results[idx] = exec.WalShipperFor("r3")
		}(i)
	}
	wg.Wait()

	// All concurrent calls must return the same instance.
	first := results[0]
	if first == nil {
		t.Fatal("concurrent WalShipperFor(r3): goroutine 0 got nil")
	}
	for i, got := range results {
		if got != first {
			t.Errorf("concurrent WalShipperFor(r3): goroutine %d got %p, want %p (registry race)", i, got, first)
		}
	}
}

// TestBlockExecutor_Ship_AdvancesWalShipperCursor — mini-plan §3 P1
// "Ship delegates Emit" + spec §3 INV-MONOTONIC-CURSOR.
//
// After Ship(replicaID, lineage, lba, lsn, data) returns nil, the
// per-replica WalShipper's cursor MUST be lsn (or higher if a later
// Ship has come in). Idempotent retries (same lsn) must NOT regress
// cursor.
//
// This pins "Ship delegates to WalShipper" — observable via cursor —
// without asserting on the wire format or transport. Wire-level
// "exactly one emit per LSN" is mini-plan §4 INV-NO-DOUBLE-LIVE
// (3c, deferred to integration).
func TestBlockExecutor_Ship_AdvancesWalShipperCursor(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	exec := NewBlockExecutor(primary, "")

	// Set up a session with attached conn so Ship's lineage check
	// passes and the lazy-dial path is bypassed.
	lineage := RecoveryLineage{
		SessionID:       42,
		Epoch:           1,
		EndpointVersion: 1,
		TargetLSN:       1000,
	}
	session, err := exec.registerSession(lineage)
	if err != nil {
		t.Fatalf("registerSession: %v", err)
	}

	// Real net.Pipe with drainer on the other side so Ship's wire
	// writes succeed. Drainer reads-and-discards; we don't assert on
	// the wire format here (that's existing ship_sender_test).
	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()
	go io.Copy(io.Discard, replicaConn)

	if err := exec.attachConn(session, primaryConn); err != nil {
		t.Fatalf("attachConn: %v", err)
	}

	const replicaID = "r1"

	// Ship LSN=10 → walShipper cursor must reach 10.
	if err := exec.Ship(replicaID, lineage, 0, 10, []byte{0xAA, 0xBB}); err != nil {
		t.Fatalf("Ship lsn=10: %v", err)
	}
	s := exec.WalShipperFor(replicaID)
	if got := s.Cursor(); got != 10 {
		t.Errorf("after Ship lsn=10: walShipper.Cursor()=%d want 10 (Ship must delegate to WalShipper)", got)
	}

	// Ship LSN=20 → walShipper cursor advances.
	if err := exec.Ship(replicaID, lineage, 1, 20, []byte{0xCC}); err != nil {
		t.Fatalf("Ship lsn=20: %v", err)
	}
	if got := s.Cursor(); got != 20 {
		t.Errorf("after Ship lsn=20: walShipper.Cursor()=%d want 20", got)
	}

	// Idempotent Ship with lsn=15 (less than current cursor=20) —
	// walShipper's NotifyAppend in Realtime drops it; cursor unchanged.
	// Ship should still return nil (caller can't tell idempotent skip
	// from successful emit at this layer).
	if err := exec.Ship(replicaID, lineage, 2, 15, []byte{0xDD}); err != nil {
		t.Fatalf("Ship lsn=15 (idempotent): %v", err)
	}
	if got := s.Cursor(); got != 20 {
		t.Errorf("after idempotent Ship lsn=15: walShipper.Cursor()=%d want 20 (no regress)", got)
	}
}

// TestBlockExecutor_WalShipperFor_StableUnderConcurrent_Ship —
// strong INV-SINGLE under concurrent Ship() to same replica.
//
// Concurrent Ship() calls for replicaID=r1 must all funnel through
// the SAME WalShipper instance. If the registry races and creates a
// second WalShipper, INV-SINGLE is violated.
func TestBlockExecutor_WalShipperFor_StableUnderConcurrent_Ship(t *testing.T) {
	primary := memorywal.NewStore(64, 4096)
	exec := NewBlockExecutor(primary, "")

	lineage := RecoveryLineage{
		SessionID:       7,
		Epoch:           1,
		EndpointVersion: 1,
		TargetLSN:       10000,
	}
	session, err := exec.registerSession(lineage)
	if err != nil {
		t.Fatalf("registerSession: %v", err)
	}
	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()
	go io.Copy(io.Discard, replicaConn)
	if err := exec.attachConn(session, primaryConn); err != nil {
		t.Fatalf("attachConn: %v", err)
	}

	const replicaID = "r1"
	const writers = 8
	const perWriter = 25

	// First, capture the walShipper instance from main goroutine.
	expected := exec.WalShipperFor(replicaID)

	// Drive concurrent Ship() — each writer ships distinct LSNs.
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// LSNs: 1..200, distinct per (worker, i).
				lsn := uint64(workerID*perWriter + i + 1)
				if err := exec.Ship(replicaID, lineage, uint32(i), lsn, []byte{byte(workerID)}); err != nil {
					t.Errorf("Ship: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// After concurrent Ships, the WalShipper for r1 must STILL be
	// the same instance — registry must have linearized.
	got := exec.WalShipperFor(replicaID)
	if got != expected {
		t.Errorf("concurrent Ship: WalShipperFor(r1) returned different instance after load (registry race)")
	}

	// Cursor should be at the highest LSN any Ship saw.
	const maxLSN = uint64(writers * perWriter)
	if got := expected.Cursor(); got != maxLSN {
		t.Errorf("after %d concurrent Ships: cursor=%d want %d (some emit was lost or out of order)",
			writers*perWriter, got, maxLSN)
	}
}
