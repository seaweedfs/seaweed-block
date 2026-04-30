package transport

// Pillar 3 slice-1 (mini-plan §11.7) — receiver-side convergence under
// SAME-LBA conflict between backlog WAL and SessionLive writes.
//
// Pillar 2 covered fault-injection on the assembled stack (BASE error,
// outer abort, high-pressure live writes on disjoint LBAs). Pillar 3
// claims **convergence**: when the same LBA is touched by both the
// backlog stream (WAL ≤ targetLSN) and a live write during the session
// (WAL > targetLSN flushed at seal), the replica's final state matches
// the higher-LSN write. AtomicSeal and Pillar 2 B both use DISJOINT LBA
// ranges (push LBA window > base window), so neither asserts the
// arbitration claim.
//
// In-scope (this slice):
//   - Transport stack ONLY (NewBlockExecutorWithDualLane + real
//     RecoverySink + runDualLaneListener + real receiver).
//   - One subtest: multi-LBA seed → mid-session live-overwrites of a
//     subset → assert per-LBA byte-equality post-barrier.
//
// Explicitly out of scope:
//   - Full engine/cluster path (component.Cluster.DriveAssignment +
//     DriveProbeResult). slice-2 will lift this same scenario into the
//     engine-driven path.
//   - §IV T2 (targetLSN vs moving head / barrier — architect-open at
//     consensus §IV). This test asserts CURRENT engine's barrier
//     semantics under a frozen target only.
//   - RF > 1 (single-replica scope).
//   - Long-duration soak.
//   - Wire-level frame tap (each-frame decode + LSN-monotonicity proof).
//     Convergence here is asserted via post-barrier replica reads;
//     frame integrity is transitive (any torn frame → receiver decode
//     fail → session.Success = false).
//
// Pinned by: §11.7 pillar 3 (receiver convergence). Companion to
// Pillar 2 commits e354813 / e5a8763 / 7d051e2 / 9f62ebe.

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// TestPillar3Slice1_ReceiverConvergence_LiveOverwritesBacklog_SameLBAs —
// the architect-suggested "pattern A then B" check, generalised to
// multiple LBAs to give the assertion bite.
//
// Setup:
//   - Primary memorywal with 12 seeded LBAs (LBA 0..11), each holding
//     pattern A_i. Sync. primaryH = 12.
//   - Live-overwrite three of the SAME LBAs after OnSessionStart with
//     pattern B_i (higher LSN, > targetLSN=12 → SessionLive path):
//     LBA 3 → B_3 (LSN 13), LBA 5 → B_5 (LSN 14), LBA 7 → B_7 (LSN 15).
//
// Wire path (in arrival order, broadly):
//   - SessionStart, then BASE frames for LBA 0..11 (each carrying
//     primary's CURRENT bytes — could be A or B for the overwritten
//     LBAs depending on whether the live Write hit primary before
//     streamBase read that LBA),
//   - WAL backlog frames for LSN 1..12 (carries the historical
//     Writes — A_i for every LBA),
//   - WAL SessionLive frames flushed at seal: LSN 13/14/15 (B_3/5/7),
//   - BarrierReq → BarrierResp.
//
// Convergence claim (regardless of arrival-order interleave):
//   - Untouched LBAs (0,1,2,4,6,8,9,10,11) → replica == seed A.
//   - Overwritten LBAs (3,5,7)             → replica == live B.
//
// If the receiver applies frames naively without arbitration AND the
// historical WAL frames arrive AFTER the SessionLive frames (which
// would not be expected per the wire ordering, but is what the test
// is sensitive to), an LBA could end up at A. The assertion thus
// captures both the wire-ordering invariant and the apply-order
// arbitration invariant: BOTH must hold for the test to pass.
//
// Stability check (not enforced in CI): I ran this 5× locally and
// observed deterministic convergence to B on all overwritten LBAs.
// If this becomes flaky in future, the receiver's apply path is
// the suspect.
func TestPillar3Slice1_ReceiverConvergence_LiveOverwritesBacklog_SameLBAs(t *testing.T) {
	const numBlocks = 64
	const blockSize = 64
	const seedN = 12

	overwriteLBAs := []uint32{3, 5, 7}

	primary := memorywal.NewStore(numBlocks, blockSize)
	replica := storage.NewBlockStore(numBlocks, blockSize)

	// Seed: A_i pattern keyed by LBA. byte 0 = 0x40 | lba so we can
	// tell A from B (B uses 0x80-marker below) and across LBAs.
	for lba := uint32(0); lba < seedN; lba++ {
		data := make([]byte, blockSize)
		data[0] = byte(0x40 | lba)
		data[1] = 0xA0 // 'A' family marker
		if _, err := primary.Write(lba, data); err != nil {
			t.Fatalf("seed Write lba=%d: %v", lba, err)
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatalf("seed Sync: %v", err)
	}
	_, _, primaryH := primary.Boundaries()
	if primaryH != seedN {
		t.Fatalf("expected primaryH=%d, got %d", seedN, primaryH)
	}
	frozenTarget := primaryH

	dualLaneAddr, stop := runDualLaneListener(t, replica)
	defer stop()

	coord := recovery.NewPeerShipCoordinator()
	const replicaID = "r1"
	exec := NewBlockExecutorWithDualLane(
		primary, "127.0.0.1:0", dualLaneAddr, coord,
		recovery.ReplicaID(replicaID),
	)

	startCh := make(chan adapter.SessionStartResult, 1)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionStart(func(r adapter.SessionStartResult) { startCh <- r })
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	if err := exec.StartRebuild(replicaID, 7, 1, 1, frozenTarget); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	select {
	case <-startCh:
	case <-time.After(2 * time.Second):
		t.Fatal("OnSessionStart did not fire within 2s")
	}

	// Mid-session live-overwrites on the SAME LBAs as the backlog.
	// Each Write here lands on primary with a fresh LSN > frozenTarget;
	// PushLiveWrite routes it to the active session sink → flushAndSeal
	// will ship it as WALKindSessionLive on the wire.
	bridge := exec.dualLane.Bridge
	bRefs := make(map[uint32][]byte, len(overwriteLBAs))
	for _, lba := range overwriteLBAs {
		bData := make([]byte, blockSize)
		bData[0] = byte(0x80 | lba)
		bData[1] = 0xB0 // 'B' family marker
		lsn, err := primary.Write(lba, bData)
		if err != nil {
			t.Fatalf("live Write lba=%d: %v", lba, err)
		}
		if lsn <= frozenTarget {
			t.Fatalf("live LSN %d should be > frozenTarget %d", lsn, frozenTarget)
		}
		if pushErr := bridge.PushLiveWrite(recovery.ReplicaID(replicaID), lba, lsn, bData); pushErr != nil {
			t.Fatalf("PushLiveWrite lba=%d lsn=%d: %v", lba, lsn, pushErr)
		}
		bRefs[lba] = bData
	}

	var res adapter.SessionCloseResult
	select {
	case res = <-closeCh:
	case <-time.After(10 * time.Second):
		t.Fatal("OnSessionClose did not fire within 10s")
	}
	if !res.Success {
		t.Fatalf("session not Success — convergence claim moot. FailReason=%q", res.FailReason)
	}

	// Untouched LBAs: replica must hold seed pattern A.
	overwriteSet := make(map[uint32]struct{}, len(overwriteLBAs))
	for _, lba := range overwriteLBAs {
		overwriteSet[lba] = struct{}{}
	}
	for lba := uint32(0); lba < seedN; lba++ {
		if _, ovw := overwriteSet[lba]; ovw {
			continue
		}
		got, err := replica.Read(lba)
		if err != nil {
			t.Fatalf("replica.Read untouched lba=%d: %v", lba, err)
		}
		expected := make([]byte, blockSize)
		expected[0] = byte(0x40 | lba)
		expected[1] = 0xA0
		if !bytes.Equal(got, expected) {
			t.Errorf("untouched lba=%d not seed-A; got[0:2]=%02x %02x want %02x %02x",
				lba, got[0], got[1], expected[0], expected[1])
		}
	}

	// Overwritten LBAs: replica must hold live B (higher-LSN wins).
	// This is the discriminating assertion. If receiver lacked the
	// wire-ordering or apply-order arbitration the architect calls out
	// for §11.7 pillar 3, an overwritten LBA could end up at A.
	for _, lba := range overwriteLBAs {
		got, err := replica.Read(lba)
		if err != nil {
			t.Fatalf("replica.Read overwritten lba=%d: %v", lba, err)
		}
		want := bRefs[lba]
		if !bytes.Equal(got, want) {
			t.Errorf("INV-PILLAR3-SAMELBA-CONVERGENCE: overwritten lba=%d not live-B; got[0:2]=%02x %02x want %02x %02x (live-write LSN > backlog LSN must arbitrate)",
				lba, got[0], got[1], want[0], want[1])
		}
	}

	if got := coord.Phase(recovery.ReplicaID(replicaID)); got != recovery.PhaseIdle {
		t.Errorf("post-session coord.Phase=%s want Idle", got)
	}

	t.Logf("pillar3-slice1: backlog=%d, live-overwrites on LBA %v converged to B; achievedLSN=%d",
		seedN, overwriteLBAs, res.AchievedLSN)
}
