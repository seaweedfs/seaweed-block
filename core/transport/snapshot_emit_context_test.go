package transport

// SnapshotEmitContext — pre-decision integration helper (P2d-parallel).
//
// This accessor lets production wiring code capture the steady-state
// emit context before constructing a RecoverySink, so EndSession's
// rule-2 restore lands the right values rather than zero / nil.

import (
	"net"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// TestSnapshotEmitContext_NoEntry_ReturnsZero — honest signal: when
// no WalShipperEntry exists for replicaID (no Ship has run yet),
// SnapshotEmitContext returns (nil conn, zero lineage). Caller can
// pass these through to NewRecoverySink — there is no steady ship
// to restore on top of.
func TestSnapshotEmitContext_NoEntry_ReturnsZero(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	e := NewBlockExecutor(primary, "127.0.0.1:0")

	conn, lineage, profile := e.SnapshotEmitContext("never-shipped")
	if conn != nil {
		t.Errorf("SnapshotEmitContext on absent replica: conn=%p want nil", conn)
	}
	if lineage != (RecoveryLineage{}) {
		t.Errorf("SnapshotEmitContext on absent replica: lineage=%+v want zero", lineage)
	}
	if profile != EmitProfileSteadyMsgShip {
		t.Errorf("SnapshotEmitContext on absent replica: profile=%s want SteadyMsgShip", profile)
	}
}

// TestSnapshotEmitContext_AfterUpdate_ReturnsCurrent — basic
// round-trip: updateWalShipperEmitContext writes (conn, lineage);
// SnapshotEmitContext reads them back. Same lock discipline as
// production EmitFunc.
func TestSnapshotEmitContext_AfterUpdate_ReturnsCurrent(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	e := NewBlockExecutor(primary, "127.0.0.1:0")

	const replicaID = "r1"
	steadyConn, _ := net.Pipe()
	defer steadyConn.Close()
	steadyLineage := RecoveryLineage{
		SessionID: 7, Epoch: 2, EndpointVersion: 3, TargetLSN: 100,
	}

	// Ensure entry exists, then update.
	_ = e.WalShipperFor(replicaID)
	e.updateWalShipperEmitContext(replicaID, steadyConn, steadyLineage, EmitProfileSteadyMsgShip)

	gotConn, gotLineage, gotProfile := e.SnapshotEmitContext(replicaID)
	if gotConn != steadyConn {
		t.Errorf("SnapshotEmitContext conn=%p want %p", gotConn, steadyConn)
	}
	if gotLineage != steadyLineage {
		t.Errorf("SnapshotEmitContext lineage=%+v want %+v", gotLineage, steadyLineage)
	}
	if gotProfile != EmitProfileSteadyMsgShip {
		t.Errorf("SnapshotEmitContext profile=%s want SteadyMsgShip", gotProfile)
	}
}

// TestSnapshotEmitContext_ThenRecoverySink_RestoresCaptured —
// integration: snapshot the steady context, hand it to RecoverySink
// as the steady-restore values, run the session, verify the restore
// matches what was snapshotted.
//
// This is the canonical pre-decision wiring pattern that production
// callers will use once P2d resolves the wire format.
func TestSnapshotEmitContext_ThenRecoverySink_RestoresCaptured(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	e := NewBlockExecutor(primary, "127.0.0.1:0")

	const replicaID = "r1"
	steadyConn, _ := net.Pipe()
	defer steadyConn.Close()
	steadyLineage := RecoveryLineage{
		SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 5,
	}

	// Production-shape step: pre-existing steady ship has set context.
	_ = e.WalShipperFor(replicaID)
	e.updateWalShipperEmitContext(replicaID, steadyConn, steadyLineage, EmitProfileSteadyMsgShip)

	// Caller pattern: snapshot BEFORE constructing RecoverySink, pass
	// to the constructor as the steady-restore values.
	snappedConn, snappedLineage, _ := e.SnapshotEmitContext(replicaID)

	sessionConn, _ := net.Pipe()
	defer sessionConn.Close()
	sessionLineage := RecoveryLineage{
		SessionID: 99, Epoch: 5, EndpointVersion: 7, TargetLSN: 9000,
	}

	sink := NewRecoverySink(e, replicaID,
		sessionConn, sessionLineage,
		snappedConn, snappedLineage,
	)

	if err := sink.StartSession(0); err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	// Mid-session: context is session.
	mid, midLin := readEntryEmitContext(t, e, replicaID)
	if mid != sessionConn || midLin != sessionLineage {
		t.Fatalf("mid-session ctx wrong: conn=%p lineage=%+v", mid, midLin)
	}

	sink.EndSession()

	// After EndSession: restored to snapshotted steady values.
	got, gotLin := readEntryEmitContext(t, e, replicaID)
	if got != steadyConn {
		t.Errorf("restored conn=%p want steadyConn=%p", got, steadyConn)
	}
	if gotLin != steadyLineage {
		t.Errorf("restored lineage=%+v want %+v", gotLin, steadyLineage)
	}
}
