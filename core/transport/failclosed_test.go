package transport

import (
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// These tests prove the data-sync institution's fail-closed
// contract under stale, overlapping, restart, and observation
// traffic. They intentionally bypass StartCatchUp / StartRebuild
// and drive raw frames so they can exercise wire-level boundaries
// that the executor abstracts away.

// TestFailClosed_OverlappingSessions_OldCannotPolluteNew proves
// that once a higher-lineage session establishes activeLineage on
// the replica, in-flight frames from the previous (lower-lineage)
// session are rejected — even if they arrive after the higher
// lineage was admitted.
func TestFailClosed_OverlappingSessions_OldCannotPolluteNew(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)

	low := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 5}
	high := RecoveryLineage{SessionID: 2, Epoch: 2, EndpointVersion: 2, TargetLSN: 10}

	lowData := makeData(0x11)
	highData := makeData(0xAA)

	// Session A (low) opens and sends LBA 0.
	connA, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer connA.Close()
	if err := WriteMsg(connA, MsgRebuildBlock, EncodeRebuildBlock(low, 0, lowData)); err != nil {
		t.Fatalf("session A first block: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	// Session B (high) opens on a separate conn and sends LBA 0.
	connB, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer connB.Close()
	if err := WriteMsg(connB, MsgRebuildBlock, EncodeRebuildBlock(high, 0, highData)); err != nil {
		t.Fatalf("session B block: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	// Session A tries to send another block AFTER B has bumped the
	// active lineage. This frame must be rejected at the data plane.
	if err := WriteMsg(connA, MsgRebuildBlock, EncodeRebuildBlock(low, 1, lowData)); err != nil {
		// Connection may already be closed by the replica — that is
		// itself a fail-closed signal. The data check below is the
		// real assertion.
		t.Logf("session A second block (expected to fail): %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	got, err := replica.Read(0)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 0xAA {
		t.Fatalf("LBA 0: stale session A overwrote session B (got 0x%02x, want 0xAA)", got[0])
	}
	got1, err := replica.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if got1[0] != 0 {
		t.Fatalf("LBA 1: stale session A wrote despite newer lineage (got 0x%02x, want 0x00)", got1[0])
	}
}

// TestFailClosed_PrimaryHalfCrash_NextSessionConverges proves that
// if a rebuild aborts mid-stream (primary crash, conn drop) without
// reaching MsgRebuildDone, a subsequent session at higher lineage
// successfully completes, the achieved frontier reflects the NEW
// session's target, and replica data matches the new primary.
//
// Rebuild blocks are BASE-lane bytes: a partial rebuild may write
// extent data, but it must not advance the replica frontier until
// RebuildDone. The convergence guarantee is at the next complete
// session's data + achieved-frontier level.
func TestFailClosed_PrimaryHalfCrash_NextSessionConverges(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)

	first := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 100}

	// Session 1: open, send one block, drop the conn — simulating
	// primary crash before sending RebuildDone.
	connA, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteMsg(connA, MsgRebuildBlock, EncodeRebuildBlock(first, 0, makeData(0x11))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	connA.Close()

	// Session 2: a fresh primary at higher lineage runs a full
	// rebuild via the executor and observes a clean callback.
	second := RecoveryLineage{SessionID: 2, Epoch: 2, EndpointVersion: 2, TargetLSN: 200}
	primary := storage.NewBlockStore(64, 4096)
	writeTestBlocks(primary, 3)

	exec := NewBlockExecutor(primary, listener.Addr())
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	if err := exec.StartRebuild("r1",
		second.SessionID, second.Epoch, second.EndpointVersion, second.TargetLSN); err != nil {
		t.Fatal(err)
	}

	select {
	case r := <-closeCh:
		if !r.Success {
			t.Fatalf("second session must succeed: %+v", r)
		}
		if r.AchievedLSN != 200 {
			t.Fatalf("achieved frontier: got %d want 200", r.AchievedLSN)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second session callback")
	}

	// Replica's data must reflect the new primary, not the aborted
	// half-write from session 1.
	assertDataMatch(t, "after primary half-crash recovery", primary, replica, 3)
}

// TestFailClosed_ReplicaHalfCrash_NoFalseSuccess proves the primary
// surfaces a session failure when the replica accepts blocks +
// RebuildDone but disconnects before sending BarrierResp. There is
// no path that fabricates a frontier and reports Success=true.
func TestFailClosed_ReplicaHalfCrash_NoFalseSuccess(t *testing.T) {
	primary := storage.NewBlockStore(8, 4096)
	writeTestBlocks(primary, 2)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// Mock replica: accept all frames including RebuildDone, then
	// close the conn instead of sending BarrierResp.
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			msgType, _, err := ReadMsg(conn)
			if err != nil {
				return
			}
			if msgType == MsgRebuildDone {
				return // close without responding
			}
		}
	}()

	exec := NewBlockExecutor(primary, ln.Addr().String())
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	if err := exec.StartRebuild("r1", 7, 1, 1, 50); err != nil {
		t.Fatal(err)
	}

	select {
	case r := <-closeCh:
		if r.Success {
			t.Fatalf("replica half-crash must not report success, got %+v", r)
		}
		if r.AchievedLSN != 0 {
			t.Fatalf("failed session must not carry a fabricated frontier, got AchievedLSN=%d", r.AchievedLSN)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("timeout: executor did not surface failure")
	}
}

// TestFailClosed_StaleShipAfterNewerRebuild_Rejected proves that
// stale Ship traffic is rejected after a higher-lineage Rebuild has
// taken over — cross-message-type lineage check, not just within
// the same message family.
func TestFailClosed_StaleShipAfterNewerRebuild_Rejected(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)

	low := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 5}
	high := RecoveryLineage{SessionID: 2, Epoch: 2, EndpointVersion: 2, TargetLSN: 10}

	// Step 1: low Ship establishes activeLineage = low.
	connA, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer connA.Close()
	shipPayload := EncodeShipEntry(ShipEntry{Lineage: low, LBA: 0, LSN: 5, Data: makeData(0x11)})
	if err := WriteMsg(connA, MsgShipEntry, shipPayload); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	// Step 2: high RebuildBlock bumps activeLineage = high.
	connB, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer connB.Close()
	if err := WriteMsg(connB, MsgRebuildBlock, EncodeRebuildBlock(high, 1, makeData(0xAA))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	// Step 3: another stale Ship at low lineage on a fresh conn.
	connC, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer connC.Close()
	stalePayload := EncodeShipEntry(ShipEntry{Lineage: low, LBA: 2, LSN: 5, Data: makeData(0x22)})
	if err := WriteMsg(connC, MsgShipEntry, stalePayload); err != nil {
		t.Logf("stale ship write (expected closed): %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	got, err := replica.Read(2)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 0 {
		t.Fatalf("stale Ship after newer Rebuild lineage was applied: LBA 2 = 0x%02x, want 0x00", got[0])
	}
}

// TestFailClosed_StaleProbeDuringRebuild_RejectedAndRebuildContinues
// proves probe is now authority-bearing (T4c-1 round-26 symmetric-pair
// rule): a probe carrying a stale lineage is rejected at
// acceptMutationLineage (conn dropped without echo), so it cannot
// disturb the replica's activeLineage. Subsequent rebuild frames at
// the original (higher) lineage continue to be accepted.
//
// Pre-T4c-1 this test asserted "probe is observation only, no lineage."
// That premise is invalidated by T4c-1 — probe IS authority-bearing.
// The new contract: stale probe → fail-closed (drop without echo);
// fresh probe → activeLineage advances and stale subsequent traffic
// is correctly rejected.
func TestFailClosed_StaleProbeDuringRebuild_RejectedAndRebuildContinues(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)

	lin := RecoveryLineage{SessionID: 5, Epoch: 3, EndpointVersion: 3, TargetLSN: 99}

	connA, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer connA.Close()
	if err := WriteMsg(connA, MsgRebuildBlock, EncodeRebuildBlock(lin, 0, makeData(0xBE))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	// Stale probe lineage (older epoch than the in-flight rebuild).
	// Replica's acceptMutationLineage rejects → conn dropped without
	// echo → primary observes EOF (in real flow, surfaces as probe
	// failure).
	stale := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}
	connP, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer connP.Close()
	if err := WriteMsg(connP, MsgProbeReq, EncodeProbeReq(ProbeRequest{Lineage: stale})); err != nil {
		t.Fatal(err)
	}
	// Stale probe must NOT receive an echo; conn drops on read.
	if _, _, err := ReadMsg(connP); err == nil {
		t.Fatalf("stale probe must be rejected without echo; got a response")
	}

	// Continue rebuild on the original conn — must still be accepted
	// (probe was rejected, did not disturb activeLineage).
	if err := WriteMsg(connA, MsgRebuildBlock, EncodeRebuildBlock(lin, 1, makeData(0xEF))); err != nil {
		t.Fatalf("rebuild after probe: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	got0, _ := replica.Read(0)
	if got0[0] != 0xBE {
		t.Fatalf("LBA 0 corrupted by stale probe: 0x%02x", got0[0])
	}
	got1, _ := replica.Read(1)
	if got1[0] != 0xEF {
		t.Fatalf("LBA 1 not applied after stale probe — stale probe disturbed activeLineage: 0x%02x", got1[0])
	}
}

func makeData(b byte) []byte {
	d := make([]byte, 4096)
	d[0] = b
	return d
}
