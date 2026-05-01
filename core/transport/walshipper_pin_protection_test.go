package transport

// Completion oracle: recover(a,b) band — NOT recover(a) closure.
// See sw-block/design/recover-semantics-adjustment-plan.md §8.1.
// migrate-candidate: depends on primary.H semantics, see §8.1 Tier-5 migration

// Pin protection + advance + release across a resident WalShipper
// recovery session — pins the user-stated architectural invariant:
//
//   "primary 需要 walshipper 平时 handle 接收和对 peer 发送，
//    在 recovery session 需要保护 pin，但是 walshipper 会推进 pin
//    以后数据 sync，通过反馈更改 pin"
//
// Translation:
//   - Steady: WalShipper handles NotifyAppend (recv) + emit (peer)
//   - Recovery session: pin floor is PROTECTED at fromLSN so the
//     substrate's WAL retention can't recycle entries the session
//     still needs.
//   - Pin advances via FEEDBACK: replica's BaseBatchAck →
//     recovery.Sender's readerLoop → coord.SetPinFloor → pin floor
//     advances to the acked LSN; substrate may now recycle below.
//   - Session end: pin released (back to 0).
//
// The actual readerLoop translation is exercised by recovery
// package's TestE2E_PinFloorAdvancesIncrementally (substrate-side).
// This test focuses on the WalShipper-side: same shipper instance
// participates in a session whose pin is correctly managed by the
// coordinator, and the shipper's mode transitions don't disturb
// the pin invariant.

import (
	"context"
	"net"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// TestPin_ProtectedDuringSession_AdvancesViaFeedback_ReleasedOnEnd —
// full pin lifecycle around a WalShipper-routed recovery session:
//
//   1. coord.StartSession → pin = fromLSN
//   2. RecoverySink.StartSession → WalShipper enters Backlog;
//      pin still = fromLSN (protected; shipper mode change doesn't
//      perturb pin).
//   3. Simulate ack feedback: coord.SetPinFloor(replica, ackLSN, S);
//      pin advances to ackLSN. (In production this is what the
//      Sender's readerLoop does on each BaseBatchAck.)
//   4. RecoverySink.EndSession → WalShipper back to Realtime;
//      pin still at the advanced value (EndSession on the SHIPPER
//      doesn't release the coord pin — coord.EndSession does).
//   5. coord.EndSession → pin released (back to 0).
func TestPin_ProtectedDuringSession_AdvancesViaFeedback_ReleasedOnEnd(t *testing.T) {
	primary := memorywal.NewStore(64, 4096)
	e := NewBlockExecutor(primary, "127.0.0.1:0")

	const replicaID recovery.ReplicaID = "r1"
	const sessionID = uint64(42)
	const fromLSN = uint64(10)
	const targetLSN = uint64(1000)

	coord := recovery.NewPeerShipCoordinator()

	// --- Step 1: coord.StartSession → pin = fromLSN.
	if err := coord.StartSession(replicaID, sessionID, fromLSN, targetLSN); err != nil {
		t.Fatalf("coord.StartSession: %v", err)
	}
	if got := coord.PinFloor(replicaID); got != fromLSN {
		t.Errorf("after coord.StartSession: PinFloor=%d want fromLSN=%d", got, fromLSN)
	}

	// --- Step 2: WalShipper bracket via RecoverySink.
	shipper := e.WalShipperFor(string(replicaID))
	if got := shipper.Mode(); got != ModeRealtime {
		t.Fatalf("initial WalShipper mode=%s want Realtime", got)
	}

	steadyConn, _ := net.Pipe()
	defer steadyConn.Close()
	sessionConn, _ := net.Pipe()
	defer sessionConn.Close()

	sessionLineage := RecoveryLineage{
		SessionID: sessionID, Epoch: 1, EndpointVersion: 1, TargetLSN: targetLSN,
	}
	steadyLineage := RecoveryLineage{
		SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
	}

	sink := NewRecoverySink(e, string(replicaID),
		sessionConn, sessionLineage,
		steadyConn, steadyLineage,
	)

	if err := sink.StartSession(fromLSN); err != nil {
		t.Fatalf("sink.StartSession: %v", err)
	}
	if got := shipper.Mode(); got != ModeBacklog {
		t.Errorf("post-sink.StartSession: mode=%s want Backlog", got)
	}
	// Pin is still fromLSN — the shipper's mode change must NOT
	// perturb the coord's pin floor.
	if got := coord.PinFloor(replicaID); got != fromLSN {
		t.Errorf("after sink.StartSession: PinFloor=%d want fromLSN=%d (shipper mode change must not move pin)", got, fromLSN)
	}

	// --- Step 3: simulate ack feedback. In production, recovery.Sender's
	// readerLoop reads BaseBatchAck and calls coord.SetPinFloor with
	// (ack.AcknowledgedLSN, primaryStore.Boundaries().S). Here we
	// simulate that translation directly.
	const ack1 = uint64(50)
	primaryR, primaryS, _ := primary.Boundaries()
	_ = primaryR
	if err := coord.SetPinFloor(replicaID, ack1, primaryS); err != nil {
		t.Fatalf("coord.SetPinFloor(%d): %v", ack1, err)
	}
	if got := coord.PinFloor(replicaID); got != ack1 {
		t.Errorf("after first ack=%d: PinFloor=%d want %d", ack1, got, ack1)
	}

	// Multiple acks: monotonic advance.
	const ack2 = uint64(75)
	if err := coord.SetPinFloor(replicaID, ack2, primaryS); err != nil {
		t.Fatalf("coord.SetPinFloor(%d): %v", ack2, err)
	}
	if got := coord.PinFloor(replicaID); got != ack2 {
		t.Errorf("after second ack=%d: PinFloor=%d want %d", ack2, got, ack2)
	}

	// Lower-LSN ack (out-of-order): pin must NOT regress.
	if err := coord.SetPinFloor(replicaID, ack1, primaryS); err != nil {
		// SetPinFloor may return an error or silently ignore; either
		// behavior is acceptable as long as pin doesn't regress.
		t.Logf("regress ack=%d returned: %v (acceptable)", ack1, err)
	}
	if got := coord.PinFloor(replicaID); got != ack2 {
		t.Errorf("after lower-LSN ack: PinFloor=%d regressed below ack2=%d", got, ack2)
	}

	// Drain backlog (substrate is empty so this returns immediately).
	if err := sink.DrainBacklog(context.Background()); err != nil {
		t.Fatalf("sink.DrainBacklog: %v", err)
	}

	// --- Step 4: sink.EndSession → shipper to Realtime; coord pin
	// still at ack2 (sink.EndSession does NOT touch coord).
	sink.EndSession()
	if got := shipper.Mode(); got != ModeRealtime {
		t.Errorf("post-sink.EndSession: mode=%s want Realtime", got)
	}
	if got := coord.PinFloor(replicaID); got != ack2 {
		t.Errorf("after sink.EndSession: PinFloor=%d want %d (coord pin must survive shipper EndSession)", got, ack2)
	}

	// --- Step 5: coord.EndSession → pin released.
	coord.EndSession(replicaID)
	if got := coord.PinFloor(replicaID); got != 0 {
		t.Errorf("after coord.EndSession: PinFloor=%d want 0 (released)", got)
	}
	// Phase back to Idle.
	if got := coord.Phase(replicaID); got != recovery.PhaseIdle {
		t.Errorf("after coord.EndSession: Phase=%s want Idle", got)
	}
}

// TestPin_MultiSession_MinPinAcrossActiveSessions — production
// invariant: when multiple replicas have active sessions, the WAL
// retention is gated by the MINIMUM pin across all of them. Verifies
// MinPinAcrossActiveSessions reflects the slowest replica's pin even
// as others advance via ack feedback.
//
// This is the cluster-wide pin guarantee: WAL recycling can't outrun
// the slowest active recovery session, so no session ever gets
// ErrWALRecycled mid-flight from a peer's progress alone.
func TestPin_MultiSession_MinPinAcrossActiveSessions(t *testing.T) {
	primary := memorywal.NewStore(64, 4096)
	coord := recovery.NewPeerShipCoordinator()
	_, primaryS, _ := primary.Boundaries()

	// Two replicas, two sessions, different fromLSNs and ack timing.
	if err := coord.StartSession("r1", 100, 5, 1000); err != nil {
		t.Fatalf("StartSession r1: %v", err)
	}
	if err := coord.StartSession("r2", 200, 20, 1000); err != nil {
		t.Fatalf("StartSession r2: %v", err)
	}

	// Initial: min(5, 20) = 5; both active.
	if got, active := coord.MinPinAcrossActiveSessions(); got != 5 || !active {
		t.Errorf("initial MinPin=%d active=%v want 5/true", got, active)
	}

	// r1 acks LSN=50; r2 still at 20.
	if err := coord.SetPinFloor("r1", 50, primaryS); err != nil {
		t.Fatalf("SetPinFloor r1: %v", err)
	}
	// MinPin = min(50, 20) = 20 — gated by slow r2.
	if got, active := coord.MinPinAcrossActiveSessions(); got != 20 || !active {
		t.Errorf("after r1 ack: MinPin=%d active=%v want 20/true (gated by r2)", got, active)
	}

	// r2 acks LSN=40; now MinPin = min(50, 40) = 40.
	if err := coord.SetPinFloor("r2", 40, primaryS); err != nil {
		t.Fatalf("SetPinFloor r2: %v", err)
	}
	if got, active := coord.MinPinAcrossActiveSessions(); got != 40 || !active {
		t.Errorf("after r2 ack: MinPin=%d active=%v want 40/true", got, active)
	}

	// r1 ends → only r2 active → MinPin = 40 (r2 alone).
	coord.EndSession("r1")
	if got, active := coord.MinPinAcrossActiveSessions(); got != 40 || !active {
		t.Errorf("after r1 EndSession: MinPin=%d active=%v want 40/true (r2 alone)", got, active)
	}

	// r2 ends → no active sessions → anyActive=false. The substrate
	// is free to recycle; the floor value when !anyActive is not a
	// valid minimum to enforce.
	coord.EndSession("r2")
	_, active := coord.MinPinAcrossActiveSessions()
	if active {
		t.Errorf("after all-ended: anyActive=true; want false (no sessions left)")
	}
	t.Logf("after all-ended: anyActive=false (no floor; substrate free to recycle)")
}
