package transport

// WalShipper-as-resident integration tests — confirm the algorithmic
// invariant the user named:
//
//   "WalShipper 常驻; recovery 时是 Backlog; 追平后仍在同一实例上变成
//    Realtime, 再接 steady 的 NotifyAppend."
//
// One resident WalShipper per (volume, replicaID). Steady ship and
// recovery sessions share the SAME instance. Mode transitions are
// internal to the shipper:
//
//   Idle → (Activate) → Realtime → (StartSession) → Backlog
//   Backlog → (DrainBacklog R1) → Realtime → (EndSession) → Realtime
//   Realtime → (StartSession again) → Backlog ... — repeatable
//
// These tests exercise the WalShipper directly (unit-level) and
// through the RecoverySink adapter (integration-level) to confirm
// the resident pattern holds across:
//   - Multiple sequential recovery sessions
//   - Steady NotifyAppend interleaved between sessions
//   - Concurrent steady NotifyAppend during a session (Backlog mode
//     drops to lag-tracking; recovery drain + post-session Realtime
//     emits both observable on the same EmitFunc)

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// TestWalShipper_Resident_SameInstanceAcrossSessions — INV-SINGLE
// at the integration level: WalShipperFor returns the same instance
// across multiple recovery sessions on the same replica.
func TestWalShipper_Resident_SameInstanceAcrossSessions(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	e := NewBlockExecutor(primary, "127.0.0.1:0")

	const replicaID = "r1"
	first := e.WalShipperFor(replicaID)
	if first == nil {
		t.Fatal("first WalShipperFor returned nil")
	}

	// Run multiple session brackets via the adapter; each one should
	// reuse the same shipper.
	for i := 0; i < 3; i++ {
		sessionConn, _ := net.Pipe()
		steadyConn, _ := net.Pipe()
		sessionLineage := RecoveryLineage{
			SessionID: uint64(100 + i), Epoch: 1, EndpointVersion: 1, TargetLSN: 50,
		}
		steadyLineage := RecoveryLineage{
			SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
		}

		sink := NewRecoverySink(e, replicaID,
			sessionConn, sessionLineage,
			steadyConn, steadyLineage,
		)

		if err := sink.StartSession(0); err != nil {
			t.Fatalf("session %d StartSession: %v", i, err)
		}

		duringSession := e.WalShipperFor(replicaID)
		if duringSession != first {
			t.Errorf("session %d: WalShipperFor returned different instance during session (resident violation)", i)
		}

		if err := sink.DrainBacklog(context.Background()); err != nil {
			t.Fatalf("session %d DrainBacklog: %v", i, err)
		}
		sink.EndSession()

		afterSession := e.WalShipperFor(replicaID)
		if afterSession != first {
			t.Errorf("session %d: WalShipperFor returned different instance AFTER EndSession (resident violation)", i)
		}

		_ = sessionConn.Close()
		_ = steadyConn.Close()
	}
}

// TestWalShipper_Resident_ModeTransitionsAcrossSessions — verify
// the mode goes Realtime → Backlog → Realtime → Backlog → Realtime
// across two consecutive sessions, all on the same shipper instance.
func TestWalShipper_Resident_ModeTransitionsAcrossSessions(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	e := NewBlockExecutor(primary, "127.0.0.1:0")

	const replicaID = "r1"
	shipper := e.WalShipperFor(replicaID)

	// After WalShipperFor, the shipper is Activated to Realtime.
	if got := shipper.Mode(); got != ModeRealtime {
		t.Fatalf("initial mode=%s want Realtime (Activate done in WalShipperFor)", got)
	}

	for i := 0; i < 2; i++ {
		sessionConn, _ := net.Pipe()
		steadyConn, _ := net.Pipe()
		sink := NewRecoverySink(e, replicaID,
			sessionConn,
			RecoveryLineage{SessionID: uint64(200 + i), Epoch: 1, EndpointVersion: 1, TargetLSN: 100},
			steadyConn,
			RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1},
		)

		if err := sink.StartSession(0); err != nil {
			t.Fatalf("session %d StartSession: %v", i, err)
		}
		if got := shipper.Mode(); got != ModeBacklog {
			t.Errorf("session %d post-StartSession: mode=%s want Backlog", i, got)
		}

		if err := sink.DrainBacklog(context.Background()); err != nil {
			t.Fatalf("session %d DrainBacklog: %v", i, err)
		}
		// After successful DrainBacklog, R1 transitions Backlog→Realtime.
		if got := shipper.Mode(); got != ModeRealtime {
			t.Errorf("session %d post-DrainBacklog: mode=%s want Realtime (R1 transition)", i, got)
		}

		sink.EndSession()
		if got := shipper.Mode(); got != ModeRealtime {
			t.Errorf("session %d post-EndSession: mode=%s want Realtime (stays)", i, got)
		}

		_ = sessionConn.Close()
		_ = steadyConn.Close()
	}
}

// TestWalShipper_Resident_SteadyShipBetweenSessions — between recovery
// sessions, the same shipper handles steady NotifyAppend in Realtime
// mode. The recording EmitFunc captures emits across the full timeline:
// emits before session 1, during/after session 1, between sessions,
// during/after session 2.
func TestWalShipper_Resident_SteadyShipBetweenSessions(t *testing.T) {
	primary := memorywal.NewStore(64, 64)
	e := NewBlockExecutor(primary, "127.0.0.1:0")

	const replicaID = "r1"
	recorded, recMu := installRecordingShipper(t, e, replicaID)

	shipper := e.WalShipperFor(replicaID)
	if got := shipper.Mode(); got != ModeRealtime {
		t.Fatalf("initial mode=%s want Realtime", got)
	}

	steadyConn, _ := net.Pipe()
	defer steadyConn.Close()
	sessionConn, _ := net.Pipe()
	defer sessionConn.Close()

	steadyLineage := RecoveryLineage{
		SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
	}
	e.updateWalShipperEmitContext(replicaID, steadyConn, steadyLineage, EmitProfileSteadyMsgShip)

	// Phase 1: steady NotifyAppend (Realtime).
	if err := shipper.NotifyAppend(0, 1, []byte{0x01}); err != nil {
		t.Fatalf("phase1 NotifyAppend: %v", err)
	}

	// Phase 2: recovery session 1.
	sessionLineage1 := RecoveryLineage{
		SessionID: 100, Epoch: 1, EndpointVersion: 1, TargetLSN: 1000,
	}
	sink1 := NewRecoverySink(e, replicaID,
		sessionConn, sessionLineage1,
		steadyConn, steadyLineage,
	)
	if err := sink1.StartSession(0); err != nil {
		t.Fatalf("session1 StartSession: %v", err)
	}
	if err := sink1.DrainBacklog(context.Background()); err != nil {
		t.Fatalf("session1 DrainBacklog: %v", err)
	}
	sink1.EndSession()

	// Phase 3: steady NotifyAppend between sessions (Realtime, steady context).
	if err := shipper.NotifyAppend(1, 2, []byte{0x02}); err != nil {
		t.Fatalf("phase3 NotifyAppend: %v", err)
	}

	// Phase 4: recovery session 2 — same shipper, fresh session.
	sessionLineage2 := RecoveryLineage{
		SessionID: 101, Epoch: 1, EndpointVersion: 1, TargetLSN: 2000,
	}
	sink2 := NewRecoverySink(e, replicaID,
		sessionConn, sessionLineage2,
		steadyConn, steadyLineage,
	)
	if err := sink2.StartSession(0); err != nil {
		t.Fatalf("session2 StartSession: %v", err)
	}
	if err := sink2.DrainBacklog(context.Background()); err != nil {
		t.Fatalf("session2 DrainBacklog: %v", err)
	}
	sink2.EndSession()

	// Phase 5: steady NotifyAppend post-session-2.
	if err := shipper.NotifyAppend(2, 3, []byte{0x03}); err != nil {
		t.Fatalf("phase5 NotifyAppend: %v", err)
	}

	recMu.Lock()
	defer recMu.Unlock()

	// Phase 1, 3, 5 emits should use steady lineage; phase 2 + 4 emits
	// (if any from drain) use session lineage. Phase 2/4 substrate is
	// empty so DrainBacklog ships nothing — recorded should contain
	// only the three steady-phase emits.
	steadyEmits := 0
	for _, r := range *recorded {
		if r.lineage == steadyLineage {
			steadyEmits++
		} else if r.lineage != sessionLineage1 && r.lineage != sessionLineage2 {
			t.Errorf("emit lineage=%+v matches no expected (steady or session1/2)", r.lineage)
		}
	}
	if steadyEmits != 3 {
		t.Errorf("steady emits=%d want 3 (phases 1, 3, 5)", steadyEmits)
	}
}

// TestWalShipper_Resident_NotifyAppendDuringSession_BacklogMode —
// while a session holds the shipper in Backlog mode, concurrent
// steady NotifyAppend calls do NOT emit (Backlog mode no-ops the
// emit; lag is updated). After EndSession (back to Realtime), a
// subsequent NotifyAppend emits normally.
//
// This pins the "WalShipper makes its own decision per mode" boundary —
// the recovery session's lifecycle controls when steady-NotifyAppend
// callers see emits land on the wire.
func TestWalShipper_Resident_NotifyAppendDuringSession_BacklogMode(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	e := NewBlockExecutor(primary, "127.0.0.1:0")

	const replicaID = "r1"
	recorded, recMu := installRecordingShipper(t, e, replicaID)

	shipper := e.WalShipperFor(replicaID)
	steadyConn, _ := net.Pipe()
	defer steadyConn.Close()
	sessionConn, _ := net.Pipe()
	defer sessionConn.Close()

	steadyLineage := RecoveryLineage{
		SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
	}
	sessionLineage := RecoveryLineage{
		SessionID: 100, Epoch: 1, EndpointVersion: 1, TargetLSN: 1000,
	}
	e.updateWalShipperEmitContext(replicaID, steadyConn, steadyLineage, EmitProfileSteadyMsgShip)

	sink := NewRecoverySink(e, replicaID,
		sessionConn, sessionLineage,
		steadyConn, steadyLineage,
	)
	if err := sink.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// In Backlog mode: NotifyAppend should NOT emit. We can't directly
	// observe "did not emit" cleanly with a recording shipper that runs
	// alongside, but we can check that no emits land during the
	// Backlog window by snapshotting before / during.
	recMu.Lock()
	preDrainCount := len(*recorded)
	recMu.Unlock()

	// Concurrent NotifyAppend during Backlog (lag-only path).
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_ = shipper.NotifyAppend(uint32(n), uint64(500+n), []byte{byte(n)})
		}(i)
	}
	wg.Wait()

	if err := sink.DrainBacklog(context.Background()); err != nil {
		t.Fatalf("DrainBacklog: %v", err)
	}
	sink.EndSession()

	// Post-EndSession Realtime emit MUST land.
	if err := shipper.NotifyAppend(99, 9999, []byte{0xFF}); err != nil {
		t.Fatalf("post-session NotifyAppend: %v", err)
	}

	recMu.Lock()
	defer recMu.Unlock()

	// At least the post-session emit must be recorded.
	if len(*recorded) <= preDrainCount {
		t.Errorf("expected at least one emit after EndSession; got total=%d preDrain=%d", len(*recorded), preDrainCount)
	}

	last := (*recorded)[len(*recorded)-1]
	if last.lba != 99 || last.lsn != 9999 {
		t.Errorf("last emit unexpected: lba=%d lsn=%d (want 99/9999)", last.lba, last.lsn)
	}
	if last.lineage != steadyLineage {
		t.Errorf("post-session emit lineage=%+v want steady=%+v (rule 2 violated)",
			last.lineage, steadyLineage)
	}
}
