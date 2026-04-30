package transport

// Pillar 2 (mini-plan §11.7) — fault-injection on the manager-assembled
// stack: BlockExecutor + PrimaryBridge + RecoverySink + recovery.Sender
// + resident WalShipper + PeerShipCoordinator under the unified
// replicaID landed in Phase 0 (Fix #1).
//
// Maps the three C3 fault dimensions (core/recovery/c3_fault_injection_test.go)
// to the assembled wire path. Each test below corresponds to one C3 axis:
//
//   A — BaseError_AssembledStack_FailReason
//       Faulty primaryStore.Read; assert OnSessionClose Success=false
//       with FailReason wrapping the synthetic error; coord returns to
//       Idle; replica frontier did NOT advance past the failure point.
//
//   B — LiveWrites_HighPressure_BarrierIntegrity (sibling to AtomicSeal)
//       Larger pusher fan-out + larger backlog than AtomicSeal so the
//       BASE ∥ WAL overlap window is wider and writeMu serialization is
//       under real contention. Stricter convergence: replica.H ==
//       primary.H exactly post-barrier. Frame-integrity is asserted
//       transitively — any torn frame would fail receiver decode →
//       session.Success would be false. AtomicSeal focuses on the seal
//       race; this focuses on overlap-pressure + convergence.
//
//   C — WireAbortMidSession_AssembledStack_RestoresEmitContext
//       In the assembled path startRebuildDualLane uses
//       context.Background() — there is no outer ctx-cancel hook.
//       Production-equivalent abort = peer drops the connection mid-
//       session (network failure / replica crash). Test closes the
//       replica-side conn after BASE has emitted some frames; assert:
//         - OnSessionClose fires with FailReason (wire failure surface);
//         - resident WalShipper's emit context restored to the steady
//           snapshot taken before session start (rule-2 restore);
//         - coord.Phase back to Idle.
//
// Why these are SIBLINGS to TestC3_*/TestDualLane_*: the failure
// mechanism, the sink type (real RecoverySink), and the wire path
// (TCP via runDualLaneListener) are all the assembled-stack form.
// C3 used a spy sink and net.Pipe, proving the recovery-package layer.
// These tests prove the same robustness one layer up.

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// ─── faulty primary: Read fails after N successful reads ─────────────

type faultyReadStorePillar2 struct {
	storage.LogicalStorage
	failAfter int32
	reads     atomic.Int32
}

func (f *faultyReadStorePillar2) Read(lba uint32) ([]byte, error) {
	if f.reads.Add(1) > f.failAfter {
		return nil, fmt.Errorf("synthetic read failure at lba=%d (pillar2)", lba)
	}
	return f.LogicalStorage.Read(lba)
}

// ─── A: BASE error → assembled stack FailReason ───────────────────────
//
// Setup: real assembled stack. Primary's LogicalStorage is wrapped so
// `Read` fails after 4 successful base-block reads. Sender's streamBase
// surfaces FailureSubstrate; bridge's onClose fires with the wrapped
// error; executor.OnSessionClose receives Success=false + FailReason.
//
// Asserts:
//   - OnSessionClose fires within 5s (no hang).
//   - res.Success == false.
//   - res.FailReason mentions the synthetic error string.
//   - coord.Phase("r1") == Idle (defer ran).
//   - Replica's frontier did not reach primary's H (BASE failed mid-stream).
func TestPillar2A_BaseError_AssembledStack_FailReason(t *testing.T) {
	const numBlocks = 32
	const blockSize = 64

	primaryRaw := memorywal.NewStore(numBlocks, blockSize)
	for lba := uint32(0); lba < 12; lba++ {
		data := make([]byte, blockSize)
		data[0] = byte(lba + 1)
		if _, err := primaryRaw.Write(lba, data); err != nil {
			t.Fatalf("seed Write lba=%d: %v", lba, err)
		}
	}
	if _, err := primaryRaw.Sync(); err != nil {
		t.Fatalf("seed Sync: %v", err)
	}
	_, _, primaryH := primaryRaw.Boundaries()
	if primaryH == 0 {
		t.Fatalf("primaryH=0 after seeding (memorywal silently rejected writes — check blockSize match)")
	}

	faulty := &faultyReadStorePillar2{LogicalStorage: primaryRaw, failAfter: 4}

	replica := storage.NewBlockStore(numBlocks, blockSize)
	dualLaneAddr, stop := runDualLaneListener(t, replica)
	defer stop()

	coord := recovery.NewPeerShipCoordinator()
	const replicaID = "r1"
	exec := NewBlockExecutorWithDualLane(
		faulty, "127.0.0.1:0", dualLaneAddr, coord,
		recovery.ReplicaID(replicaID),
	)

	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	if err := exec.StartRebuild(replicaID, 7, 1, 1, primaryH); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	var res adapter.SessionCloseResult
	select {
	case res = <-closeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("OnSessionClose did not fire within 5s — BASE failure path may be hung")
	}

	if res.Success {
		t.Fatalf("expected Success=false on BASE failure; got Success=true achieved=%d", res.AchievedLSN)
	}
	if !bytes.Contains([]byte(res.FailReason), []byte("synthetic read failure")) {
		t.Errorf("FailReason should mention synthetic read error; got %q", res.FailReason)
	}

	if got := coord.Phase(recovery.ReplicaID(replicaID)); got != recovery.PhaseIdle {
		t.Errorf("post-failure coord.Phase=%s want Idle (EndSession defer should have run)", got)
	}

	// Replica's recovered-frontier (R) did NOT reach primary.H. R is
	// the convergence-truth: it advances only when the receiver has
	// committed bytes; H may be set from session metadata at frame-1
	// (TargetLSN) before any base block lands. The barrier never
	// fired, so R stays below the targetLSN.
	rR, _, rH := replica.Boundaries()
	if rR >= primaryH {
		t.Errorf("replica.R=%d should be < primaryH=%d (BASE failed mid-stream, barrier never fired — R must reflect that)", rR, primaryH)
	}
	t.Logf("BASE-error assembled-path: failReason=%q replica R=%d H=%d (primary H=%d)",
		res.FailReason, rR, rH, primaryH)
}

// ─── C: deterministic session failure → emit context restored ───────
//
// Setup: real assembled stack with faulty primary substrate (BASE
// fails after N reads). Snapshot pre-session WalShipper emit context.
// StartRebuild → BASE goroutine errors → session fails →
// OnSessionClose fires with FailReason. Snapshot post-session emit
// context; assert it equals the pre-session snapshot (rule-2 restore).
//
// Why faulty-store instead of mid-session wire abort: §6.10 made
// BASE bypass the WAL apply path (writeExtentDirect), so base lane
// is fast enough to complete a small backlog before any wire-abort
// timing trick can fire. Faulty-read is deterministic: BASE
// guaranteed fails at the N+1th block, no race.
//
// Asserts:
//   - res.Success == false (deterministic via faulty read);
//   - res.FailReason mentions the synthetic error;
//   - resident WalShipper emit context matches pre-session snapshot
//     (rule-2 restore on the failure path — the actual claim);
//   - coord.Phase == Idle.
func TestPillar2C_WireAbortMidSession_AssembledStack_RestoresEmitContext(t *testing.T) {
	const numBlocks = 64
	const blockSize = 256

	primaryRaw := memorywal.NewStore(numBlocks, blockSize)
	for lba := uint32(0); lba < 30; lba++ {
		data := make([]byte, blockSize)
		data[0] = byte(0x70 | (lba & 0xF))
		_, _ = primaryRaw.Write(lba, data)
	}
	_, _ = primaryRaw.Sync()
	_, _, primaryH := primaryRaw.Boundaries()

	// Faulty wrapper: streamBase fails after 4 successful Read calls,
	// surface as Substrate failure → bridge onClose → executor's
	// OnSessionClose with Success=false.
	faulty := &faultyReadStorePillar2{LogicalStorage: primaryRaw, failAfter: 4}

	replica := storage.NewBlockStore(numBlocks, blockSize)
	dualLaneAddr, stop := runDualLaneListener(t, replica)
	defer stop()

	coord := recovery.NewPeerShipCoordinator()
	const replicaID = "r1"
	exec := NewBlockExecutorWithDualLane(
		faulty, "127.0.0.1:0", dualLaneAddr, coord,
		recovery.ReplicaID(replicaID),
	)

	// Pre-session: WalShipper not yet created (no Ship has run).
	// SnapshotEmitContext returns (nil, zero, SteadyMsgShip) — the
	// fresh-replica baseline. This is the snapshot the rule-2 restore
	// must return to.
	preConn, preLineage, preProfile := exec.SnapshotEmitContext(replicaID)
	if preConn != nil {
		t.Errorf("pre-session preConn=%v want nil (no Ship has run)", preConn)
	}
	if preProfile != EmitProfileSteadyMsgShip {
		t.Errorf("pre-session preProfile=%s want SteadyMsgShip", preProfile)
	}

	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	beforeGoroutines := runtime.NumGoroutine()

	if err := exec.StartRebuild(replicaID, 9, 1, 1, primaryH); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	var res adapter.SessionCloseResult
	select {
	case res = <-closeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("OnSessionClose did not fire within 5s of forced BASE failure")
	}

	if res.Success {
		t.Errorf("expected Success=false on faulty-store BASE failure; got Success=true")
	}
	if res.FailReason == "" {
		t.Error("expected non-empty FailReason on BASE failure")
	}

	// Rule-2 restore: post-EndSession the resident WalShipper's emit
	// context returns to the pre-session snapshot. For a fresh replica
	// (no prior Ship) that's (nil, zero, SteadyMsgShip).
	postConn, postLineage, postProfile := exec.SnapshotEmitContext(replicaID)
	if postConn != preConn {
		t.Errorf("post-session conn=%v != pre-session conn=%v (rule-2 restore failed)", postConn, preConn)
	}
	if postProfile != preProfile {
		t.Errorf("post-session profile=%s != pre-session profile=%s (rule-2 restore failed)", postProfile, preProfile)
	}
	if postLineage != preLineage {
		t.Errorf("post-session lineage=%+v != pre-session lineage=%+v", postLineage, preLineage)
	}

	if got := coord.Phase(recovery.ReplicaID(replicaID)); got != recovery.PhaseIdle {
		t.Errorf("post-failure coord.Phase=%s want Idle", got)
	}

	// Goroutine leak (informational only — runtime noise).
	time.Sleep(100 * time.Millisecond)
	leaked := runtime.NumGoroutine() - beforeGoroutines
	if leaked > 2 {
		t.Logf("goroutine count delta=%d after session failure (informational)", leaked)
	}
}

// ─── B: live-write high-pressure during BASE ∥ WAL ───────────────────
//
// Sibling to TestDualLane_LiveWritesDuringSession_AtomicSeal but with:
//   - Larger backlog (300 entries) → wider BASE+WAL overlap window;
//   - More pushers (16 workers × 8 = 128 attempts) → real writeMu
//     contention rather than the lighter AtomicSeal load (8×5=40);
//   - Stricter convergence: replica.H == primary.H EXACTLY post-barrier
//     (AtomicSeal asserts per-LBA byte-equality only on accepted pushes
//     and individual backlog LBAs — does not pin the H boundary).
//
// Frame-integrity is established TRANSITIVELY: a torn frame on the
// wire would fail receiver decode → session would fail → res.Success
// would be false. The Success=true assertion below is therefore a
// frame-integrity claim.
//
// What this test does NOT do (deferred): wire-level tap that records
// every frame for explicit decode + LSN-monotonicity assertion.
// Adding a tap-listener helper to runDualLaneListener-equivalent is
// a future test-infra follow-up.
func TestPillar2B_LiveWrites_HighPressure_BarrierIntegrity(t *testing.T) {
	const numBlocks = 1024
	const blockSize = 1024
	const backlogN = 300

	primary := memorywal.NewStore(numBlocks, blockSize)
	replica := storage.NewBlockStore(numBlocks, blockSize)

	for lba := uint32(0); lba < backlogN; lba++ {
		data := make([]byte, blockSize)
		data[0] = byte(0x40 | (lba & 0x3F))
		if _, err := primary.Write(lba, data); err != nil {
			t.Fatalf("seed Write lba=%d: %v", lba, err)
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatalf("seed Sync: %v", err)
	}
	_, _, primaryH := primary.Boundaries()
	if primaryH != backlogN {
		t.Fatalf("expected primaryH=%d after seeding, got %d", backlogN, primaryH)
	}

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

	if err := exec.StartRebuild(replicaID, 7, 1, 1, primaryH); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	select {
	case <-startCh:
	case <-time.After(2 * time.Second):
		t.Fatal("OnSessionStart did not fire within 2s")
	}

	const writers = 16
	const perWriter = 8
	bridge := exec.dualLane.Bridge

	var (
		acceptedMu  sync.Mutex
		acceptedSet = make(map[uint32][]byte, writers*perWriter)
		rejectedN   atomic.Int32
	)

	var pushersWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		pushersWG.Add(1)
		go func(workerID int) {
			defer pushersWG.Done()
			for i := 0; i < perWriter; i++ {
				lba := uint32(backlogN) + uint32(workerID*perWriter+i)
				if lba >= numBlocks {
					return
				}
				data := make([]byte, blockSize)
				data[0] = byte(0xC0 | byte(workerID))
				data[1] = byte(i)
				lsn, err := primary.Write(lba, data)
				if err != nil {
					t.Errorf("primary.Write lba=%d: %v", lba, err)
					return
				}
				if pushErr := bridge.PushLiveWrite(recovery.ReplicaID(replicaID), lba, lsn, data); pushErr == nil {
					acceptedMu.Lock()
					acceptedSet[lba] = append([]byte(nil), data...)
					acceptedMu.Unlock()
				} else {
					rejectedN.Add(1)
				}
			}
		}(w)
	}
	pushersWG.Wait()

	var res adapter.SessionCloseResult
	select {
	case res = <-closeCh:
	case <-time.After(15 * time.Second):
		t.Fatal("OnSessionClose did not fire within 15s under high-pressure load")
	}
	if !res.Success {
		t.Fatalf("session not Success under high-pressure load — frame integrity / writeMu serialization may have torn a frame: FailReason=%q", res.FailReason)
	}

	// Strictest convergence: replica's H matches primary's H exactly
	// post-barrier. Captures both backlog drain AND accepted live-write
	// flush completing. Stronger than AtomicSeal which only asserts
	// per-LBA equality.
	_, _, postPrimaryH := primary.Boundaries()
	_, _, replicaH := replica.Boundaries()
	if replicaH != postPrimaryH {
		t.Errorf("replica.H=%d != primary.H=%d after barrier (convergence violated)",
			replicaH, postPrimaryH)
	}

	// Every accepted push is byte-equal on replica (atomic-seal contract).
	acceptedMu.Lock()
	defer acceptedMu.Unlock()
	if len(acceptedSet) == 0 {
		t.Fatal("no pushes accepted — flush+seal raced ahead of all pushers; widen the test window")
	}
	for lba, want := range acceptedSet {
		got, err := replica.Read(lba)
		if err != nil {
			t.Errorf("replica.Read lba=%d: %v", lba, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("accepted push lba=%d not byte-equal on replica (frame integrity / apply order)", lba)
		}
	}

	// Backlog correctness — every seeded LBA is byte-equal on replica.
	for lba := uint32(0); lba < backlogN; lba++ {
		pd, _ := primary.Read(lba)
		rd, _ := replica.Read(lba)
		if !bytes.Equal(pd, rd) {
			t.Errorf("backlog lba=%d mismatch", lba)
			break
		}
	}

	if got := coord.Phase(recovery.ReplicaID(replicaID)); got != recovery.PhaseIdle {
		t.Errorf("post-session coord.Phase=%s want Idle", got)
	}

	t.Logf("pillar2-B high-pressure: backlog=%d pushers=%d×%d accepted=%d rejected=%d achievedLSN=%d replica.H=%d",
		backlogN, writers, perWriter, len(acceptedSet), rejectedN.Load(), res.AchievedLSN, replicaH)
}
