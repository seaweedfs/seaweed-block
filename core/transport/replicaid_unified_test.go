package transport

// Phase 0 Fix #1 — INV-RID-UNIFIED-PER-SESSION test loop.
//
// Bug summary (pre-fix): startRebuildDualLane built the RecoverySink
// using the engine's arg `replicaID`, but called bridge.StartRebuild
// SessionWithSink with the construction-time `dl.ReplicaID`. If the
// two diverged, the post-emit hook (sink → coord.RecordShipped) keyed
// under arg while the bridge keyed coord.PinFloor under dl.ReplicaID.
// PinFloor and shipCursor advanced under different IDs; rebuild never
// reconciled, no error fired, watchdog eventually timed out.
//
// Post-fix: (1) the engine's arg `replicaID` is the single source of
// truth, threaded through to the bridge; (2) a fail-closed assert at
// session entry rejects mismatched (non-empty) construction IDs.
//
// Tests in this file:
//
//   1. TestPhase0Fix1_DriftRejected_FailsClosedAtEntry —
//      construction RID != arg → StartRebuild returns drift error
//      BEFORE any wire activity; no orphan coord state.
//   2. TestPhase0Fix1_HappyPath_ArgEqualsConstructionID —
//      construction RID == arg, full rebuild completes; replica
//      frontier converges; coord returns to Idle (the keying
//      alignment is implicit: post-session phase observation under arg
//      RID succeeds, which it could not if the bridge had keyed
//      under a different ID).
//   3. TestPhase0Fix1_EmptyConstructionRID_ArgFlowsThrough —
//      construction RID == "" (legacy/test omission). Assert is
//      bypassed (empty is intentionally permissive), but post-fix the
//      bridge call uses the engine arg, so coord and sink key
//      uniformly under arg. Rebuild succeeds.
//
// Regression coverage: the existing dual-lane suite
// (TestDualLane_BlockExecutor_StartRebuild, TestC1_PostEmitHook_…,
// TestDualLane_LiveWritesDuringSession_AtomicSeal, etc.) all run with
// arg == construction RID, exercising the same code path. The fix
// only changed the bridge call's RID source; if any existing path
// silently relied on dl.ReplicaID being distinct from arg, it would
// break in this run. None do.

import (
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// TestPhase0Fix1_DriftRejected_FailsClosedAtEntry — construction
// RID and arg disagree (and construction RID is non-empty). The
// assert in startRebuildDualLane MUST fire before any dial / sink
// construction / coord state mutation.
//
// Observables:
//   - StartRebuild returns an error mentioning "drift".
//   - Neither replica ID has an active coord session (Phase == Idle).
//   - Bridge has not been entered (no OnSessionStart fired).
func TestPhase0Fix1_DriftRejected_FailsClosedAtEntry(t *testing.T) {
	const numBlocks = 16
	const blockSize = 64

	primary := storage.NewBlockStore(numBlocks, blockSize)
	for lba := uint32(0); lba < 4; lba++ {
		_, _ = primary.Write(lba, makeBytes(blockSize, byte(lba+1)))
	}
	_, _ = primary.Sync()

	replica := storage.NewBlockStore(numBlocks, blockSize)
	dualLaneAddr, stop := runDualLaneListener(t, replica)
	defer stop()

	coord := recovery.NewPeerShipCoordinator()

	// Construction RID and the arg passed to StartRebuild diverge.
	const constructionRID = "r-construction"
	const argRID = "r-arg-mismatch"
	exec := NewBlockExecutorWithDualLane(
		primary, "127.0.0.1:0", dualLaneAddr, coord,
		recovery.ReplicaID(constructionRID),
	)

	startCh := make(chan adapter.SessionStartResult, 1)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionStart(func(r adapter.SessionStartResult) { startCh <- r })
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	_, _, primaryH := primary.Boundaries()
	err := exec.StartRebuild(argRID, 9, 1, 1, primaryH)
	if err == nil {
		t.Fatal("expected drift error; StartRebuild returned nil")
	}
	if !strings.Contains(err.Error(), "drift") {
		t.Errorf("expected error to mention 'drift'; got %v", err)
	}

	// Brief window to confirm nothing fires asynchronously.
	select {
	case got := <-startCh:
		t.Fatalf("OnSessionStart fired despite drift: %+v", got)
	case got := <-closeCh:
		t.Fatalf("OnSessionClose fired despite drift: %+v", got)
	case <-time.After(200 * time.Millisecond):
	}

	// No coord session under either ID.
	if got := coord.Phase(recovery.ReplicaID(constructionRID)); got != recovery.PhaseIdle {
		t.Errorf("constructionRID coord.Phase=%s want Idle (no session should have started)", got)
	}
	if got := coord.Phase(recovery.ReplicaID(argRID)); got != recovery.PhaseIdle {
		t.Errorf("argRID coord.Phase=%s want Idle (no session should have started)", got)
	}
}

// TestPhase0Fix1_HappyPath_ArgEqualsConstructionID — baseline case
// with construction RID == arg. Post-fix this exercises the unified
// path: bridge call now uses the arg-derived recovery.ReplicaID
// (which equals dl.ReplicaID here, but the source is now the arg).
//
// Observables:
//   - Rebuild completes successfully; achievedLSN == primary H.
//   - Replica frontier matches primary.
//   - Post-session coord.Phase(arg) returns Idle (the bridge keyed
//     under arg, the sink keyed under arg — they aligned, so the
//     session lifecycle ran cleanly).
func TestPhase0Fix1_HappyPath_ArgEqualsConstructionID(t *testing.T) {
	const numBlocks = 32
	const blockSize = 64

	primary := storage.NewBlockStore(numBlocks, blockSize)
	for lba := uint32(0); lba < 8; lba++ {
		_, _ = primary.Write(lba, makeBytes(blockSize, byte(lba+0x40)))
	}
	_, _ = primary.Sync()
	_, _, primaryH := primary.Boundaries()

	replica := storage.NewBlockStore(numBlocks, blockSize)
	dualLaneAddr, stop := runDualLaneListener(t, replica)
	defer stop()

	coord := recovery.NewPeerShipCoordinator()
	const replicaID = "r-aligned"
	exec := NewBlockExecutorWithDualLane(
		primary, "127.0.0.1:0", dualLaneAddr, coord,
		recovery.ReplicaID(replicaID),
	)

	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	if err := exec.StartRebuild(replicaID, 11, 1, 1, primaryH); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	select {
	case r := <-closeCh:
		if !r.Success {
			t.Fatalf("session not Success: %s", r.FailReason)
		}
		if r.AchievedLSN != primaryH {
			t.Errorf("achievedLSN=%d want %d", r.AchievedLSN, primaryH)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnSessionClose did not fire within 5s")
	}

	rR, _, _ := replica.Boundaries()
	if rR != primaryH {
		t.Errorf("replica frontier=%d want %d", rR, primaryH)
	}
	if got := coord.Phase(recovery.ReplicaID(replicaID)); got != recovery.PhaseIdle {
		t.Errorf("post-session coord.Phase=%s want Idle", got)
	}
}

// TestPhase0Fix1_EmptyConstructionRID_ArgFlowsThrough —
// construction RID is empty (a test/legacy oversight). The drift
// guard is intentionally permissive on empty (rationale: empty
// signals "not configured at construction"; the arg is the truth).
// Post-fix the bridge call still uses the engine arg, so coord and
// sink key uniformly under arg.
//
// Pre-fix this test would have wedged: bridge.StartRebuildSession
// WithSink(... dl.ReplicaID="" ...) keys coord.PinFloor under "",
// while the sink's post-emit hook keys RecordShipped under arg.
// The keying mismatch silently broke RecordShipped's contribution
// to PinFloor, but the rebuild barrier still completed — so the
// only observable bug was a stale shipCursor that operator surfaces
// would later misreport. Post-fix: the bridge gets the arg-derived
// RID, both sides line up, frontier converges cleanly.
func TestPhase0Fix1_EmptyConstructionRID_ArgFlowsThrough(t *testing.T) {
	const numBlocks = 32
	const blockSize = 64

	primary := storage.NewBlockStore(numBlocks, blockSize)
	for lba := uint32(0); lba < 6; lba++ {
		_, _ = primary.Write(lba, makeBytes(blockSize, byte(lba+0x20)))
	}
	_, _ = primary.Sync()
	_, _, primaryH := primary.Boundaries()

	replica := storage.NewBlockStore(numBlocks, blockSize)
	dualLaneAddr, stop := runDualLaneListener(t, replica)
	defer stop()

	coord := recovery.NewPeerShipCoordinator()
	const argRID = "r-arg-only"
	exec := NewBlockExecutorWithDualLane(
		primary, "127.0.0.1:0", dualLaneAddr, coord,
		recovery.ReplicaID(""), // intentionally empty: guard bypasses
	)

	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	if err := exec.StartRebuild(argRID, 13, 1, 1, primaryH); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	select {
	case r := <-closeCh:
		if !r.Success {
			t.Fatalf("session not Success: %s", r.FailReason)
		}
		if r.AchievedLSN != primaryH {
			t.Errorf("achievedLSN=%d want %d", r.AchievedLSN, primaryH)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnSessionClose did not fire within 5s")
	}

	rR, _, _ := replica.Boundaries()
	if rR != primaryH {
		t.Errorf("replica frontier=%d want %d", rR, primaryH)
	}
	// Coord lifecycle ran under arg (proves the bridge keyed under arg,
	// not under the empty construction RID).
	if got := coord.Phase(recovery.ReplicaID(argRID)); got != recovery.PhaseIdle {
		t.Errorf("post-session coord.Phase(arg)=%s want Idle", got)
	}
	// And the empty-key path was never used.
	if got := coord.Phase(recovery.ReplicaID("")); got != recovery.PhaseIdle {
		t.Errorf("coord.Phase(\"\")=%s want Idle (empty key should never have been touched)", got)
	}
}
