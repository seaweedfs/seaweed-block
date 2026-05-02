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

	// §6.3 migration: sequential LSN starting at cursor+1=1; no gaps.
	// Test claim is "Ship advances WalShipper cursor" — sequential LSN
	// is the production-mirror pattern (cursor advances by 1 per Ship).
	// Ship LSN=1 → cursor must reach 1.
	if err := exec.Ship(replicaID, lineage, 0, 1, []byte{0xAA, 0xBB}); err != nil {
		t.Fatalf("Ship lsn=1: %v", err)
	}
	s := exec.WalShipperFor(replicaID)
	if got := s.Cursor(); got != 1 {
		t.Errorf("after Ship lsn=1: walShipper.Cursor()=%d want 1 (Ship must delegate to WalShipper)", got)
	}

	// Ship LSN=2 → cursor advances.
	if err := exec.Ship(replicaID, lineage, 1, 2, []byte{0xCC}); err != nil {
		t.Fatalf("Ship lsn=2: %v", err)
	}
	if got := s.Cursor(); got != 2 {
		t.Errorf("after Ship lsn=2: walShipper.Cursor()=%d want 2", got)
	}

	// Idempotent Ship with lsn=1 (less than cursor=2) — walShipper's
	// drive() drops it (in.lsn <= cursor); cursor unchanged. Ship
	// returns nil regardless.
	if err := exec.Ship(replicaID, lineage, 2, 1, []byte{0xDD}); err != nil {
		t.Fatalf("Ship lsn=1 (idempotent): %v", err)
	}
	if got := s.Cursor(); got != 2 {
		t.Errorf("after idempotent Ship lsn=1: walShipper.Cursor()=%d want 2 (no regress)", got)
	}
}

// TestBlockExecutor_WalShipperRegistry_StableUnderConcurrent_Ship —
// REGISTRY stability under concurrent Ship() load. NOT a lossless-
// emit test.
//
// Architect P1 review (2026-04-29 #2): concurrent Ship() calls for
// the SAME replicaID is NOT the production contract — production
// serializes fan-out via `ReplicationVolume.OnLocalWrite`'s `v.mu`
// held across the whole peer loop ("Condition A" in volume.go).
// Realtime's `if lsn <= s.cursor { return nil }` is correct under
// that serialization but DROPS out-of-order concurrent ships in
// this test. Specifically: if goroutine A ships LSN=5 after
// goroutine B already shipped LSN=10, A's emit is silently dropped
// and cursor stays at 10.
//
// What this test pins:
//   - INV-SINGLE registry: WalShipperFor returns the SAME instance
//     under concurrent Ship load (no registry race / second instance).
//   - cursor advances to SOMETHING ≤ maxLSN (cannot be 0 — at least
//     one Ship landed).
//
// What this test does NOT pin:
//   - "All 200 LSNs reach the wire" — would require serialized Ship
//     (production-mirror pattern) OR a recording EmitFunc that counts
//     successful WriteMsg. Lossless-emit is INV-NO-DOUBLE-LIVE +
//     production volume.mu serialization, NOT the registry.
//
// Replacement test (production-mirror, serialized) lives in P2 e2e
// when recovery.Sender + walShipper integrate; cursor==maxLSN there
// IS lossless because LSN order matches Ship order.
func TestBlockExecutor_WalShipperRegistry_StableUnderConcurrent_Ship(t *testing.T) {
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

	// §6.3 migration (architect Path B ruling): concurrent Ship with
	// out-of-order LSN is no longer a survivable pattern — under the
	// fail-closed tail-gap default, out-of-order LSN returns an error
	// per §6.3 / §6.9 NEGATIVE-EQUITY. The test's own header
	// acknowledges concurrent Ship is "NOT production" — production
	// serializes via volume.mu. The registry-stability claim
	// (INV-SINGLE: WalShipperFor returns the same instance) is
	// independent of concurrency and holds under sequential Ship.
	//
	// Drive sequential Ship from a single goroutine but call
	// WalShipperFor from N concurrent goroutines to still exercise the
	// registry-stability concurrency claim — the part of the test
	// whose intent survives the §6.3 collapse.
	var wg sync.WaitGroup
	const lookups = 8
	for r := 0; r < lookups; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				if got := exec.WalShipperFor(replicaID); got != expected {
					t.Errorf("WalShipperFor concurrent lookup returned different instance (registry race)")
					return
				}
			}
		}()
	}
	// Sequential Ship with cursor+1 LSN — production-mirror.
	for i := 0; i < writers*perWriter; i++ {
		lsn := uint64(i + 1)
		if err := exec.Ship(replicaID, lineage, uint32(i%64), lsn, []byte{byte(i & 0xFF)}); err != nil {
			t.Fatalf("Ship lsn=%d: %v", lsn, err)
		}
	}
	wg.Wait()

	// After Ships + concurrent lookups, the WalShipper for r1 must
	// still be the same instance — registry stability claim survives
	// concurrent WalShipperFor lookups.
	got := exec.WalShipperFor(replicaID)
	if got != expected {
		t.Errorf("WalShipperFor returned different instance after load (registry race)")
	}

	// Sequential Ship: cursor MUST equal maxLSN (lossless under §6.3
	// when LSN sequence is dense and serialized — the production-mirror
	// pattern).
	const maxLSN = uint64(writers * perWriter)
	cursor := expected.Cursor()
	if cursor != maxLSN {
		t.Errorf("after sequential Ships: cursor=%d want %d (lossless under §6.3 + serialized)", cursor, maxLSN)
	}
}
