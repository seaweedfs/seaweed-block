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
// Phase 0 / C3 / Pillar 2 commits:
//   e354813 (transport: unify replicaID)
//   e5a8763 (test harness: replica-%d alignment)
//   7d051e2 (recovery: C3 fault-injection — spy sink)
//   9f62ebe (transport: pillar 2 fault-injection on assembled stack)
//   291e652 (THIS slice — pillar 3 slice-1 same-LBA arbitration).

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
// Stability check (not enforced in CI): 10 consecutive runs locally
// showed deterministic convergence to B on all overwritten LBAs.
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

	// Pillar-3 polish: assert AchievedLSN against post-session primary.H.
	// Live writes pushed LSNs 13/14/15 onto primary; primary.H should be
	// 15 (= frozenTarget + len(overwrites)). The receiver's barrier-ack
	// AchievedLSN MUST equal that — anything less means a SessionLive
	// frame didn't apply (silent gap), anything more means we drifted
	// past the actually-shipped tail. Tighter than "achieved >= target".
	_, _, postPrimaryH := primary.Boundaries()
	expectedAchieved := frozenTarget + uint64(len(overwriteLBAs))
	if postPrimaryH != expectedAchieved {
		t.Fatalf("post-session primaryH=%d want %d (frozenTarget %d + %d live writes)",
			postPrimaryH, expectedAchieved, frozenTarget, len(overwriteLBAs))
	}
	if res.AchievedLSN != postPrimaryH {
		t.Errorf("AchievedLSN=%d != postPrimaryH=%d (barrier ack should reflect last-shipped LSN)",
			res.AchievedLSN, postPrimaryH)
	}

	// Untouched LBAs: replica must hold seed pattern A AND must equal
	// primary's bytes for that LBA (defence-in-depth — we expect both
	// to be the seed pattern, but tying to primary directly catches
	// any future receiver bug that diverges from primary even if it
	// "happens to" still match the seed cache).
	overwriteSet := make(map[uint32]struct{}, len(overwriteLBAs))
	for _, lba := range overwriteLBAs {
		overwriteSet[lba] = struct{}{}
	}
	for lba := uint32(0); lba < seedN; lba++ {
		if _, ovw := overwriteSet[lba]; ovw {
			continue
		}
		gotR, err := replica.Read(lba)
		if err != nil {
			t.Fatalf("replica.Read untouched lba=%d: %v", lba, err)
		}
		gotP, err := primary.Read(lba)
		if err != nil {
			t.Fatalf("primary.Read untouched lba=%d: %v", lba, err)
		}
		if !bytes.Equal(gotR, gotP) {
			t.Errorf("untouched lba=%d: replica != primary (got_replica[0:2]=%02x %02x got_primary[0:2]=%02x %02x)",
				lba, gotR[0], gotR[1], gotP[0], gotP[1])
		}
		// Sanity: must still be seed-A pattern (untouched).
		if gotR[0] != byte(0x40|lba) || gotR[1] != 0xA0 {
			t.Errorf("untouched lba=%d not seed-A; got[0:2]=%02x %02x", lba, gotR[0], gotR[1])
		}
	}

	// Overwritten LBAs: replica MUST equal primary (== live B), AND
	// MUST equal the cached bRefs (sanity that the live-write payload
	// itself wasn't dropped silently). The discriminating assertion
	// is the replica == primary direction; if receiver lacked the
	// wire-ordering or apply-order arbitration §11.7 pillar 3 calls
	// out, an overwritten LBA could end up at A — replica.Read would
	// then differ from primary.Read.
	for _, lba := range overwriteLBAs {
		gotR, err := replica.Read(lba)
		if err != nil {
			t.Fatalf("replica.Read overwritten lba=%d: %v", lba, err)
		}
		gotP, err := primary.Read(lba)
		if err != nil {
			t.Fatalf("primary.Read overwritten lba=%d: %v", lba, err)
		}
		if !bytes.Equal(gotR, gotP) {
			t.Errorf("INV-PILLAR3-SAMELBA-CONVERGENCE: overwritten lba=%d replica != primary (got_replica[0:2]=%02x %02x got_primary[0:2]=%02x %02x); live-write LSN > backlog LSN must arbitrate",
				lba, gotR[0], gotR[1], gotP[0], gotP[1])
		}
		want := bRefs[lba]
		if !bytes.Equal(gotR, want) {
			t.Errorf("overwritten lba=%d replica differs from cached live-write payload bRefs (test-internal sanity)", lba)
		}
	}

	if got := coord.Phase(recovery.ReplicaID(replicaID)); got != recovery.PhaseIdle {
		t.Errorf("post-session coord.Phase=%s want Idle", got)
	}

	t.Logf("pillar3-slice1: backlog=%d, live-overwrites on LBA %v converged to B; achievedLSN=%d",
		seedN, overwriteLBAs, res.AchievedLSN)
}
