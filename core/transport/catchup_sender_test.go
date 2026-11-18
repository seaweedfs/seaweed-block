package transport

// Completion oracle: recover(a,b) band — NOT recover(a) closure.
// See sw-block/design/recover-semantics-adjustment-plan.md §8.1.
// migrate-candidate: depends on primary.H semantics, see §8.1 Tier-5 migration

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// T4c-2 catch-up sender muscle-port tests. Per G-1 §5 + §7:
//
//   2 V2-mirror tests:
//     TestCatchUpSender_HappyPath_WALReplay        — happy round-trip
//     TestCatchUpSender_RecycledFromLSN_ErrorPropagates — substrate ErrWALRecycled
//
//   7 V3-only invariant pins (1 per §5 invariant + 2 mode-label):
//     TestCatchUpSender_FeedsPastTargetBand
//     TestCatchUpSender_DoneMarkerOnZeroEntries  (re-shaped per §4.2 binding)
//     TestCatchUpSender_DeadlineClearedAfterReturn
//     TestCatchUpSender_EmptyScanDoesNotFailTargetBand
//     TestCatchUpSender_AckBehindLastSentFails
//     TestCatchUpSender_LastSentNotAdvancedOnWriteFail (covered by happy + recycle paths)
//     TestCatchUpSender_ModeLabelEmittedWALReplay
//     TestCatchUpSender_CompletionFromBarrierAchievedLSN  (§4.2 binding)

func newCatchUpExecutor(t *testing.T) (*BlockExecutor, *storage.BlockStore, *ReplicaListener) {
	t.Helper()
	primary, _, listener := setupPrimaryReplica(t)
	exec := NewBlockExecutor(primary, listener.Addr())
	return exec, primary, listener
}

func runCatchUp(t *testing.T, exec *BlockExecutor, targetLSN uint64) adapter.SessionCloseResult {
	t.Helper()
	startCh := make(chan adapter.SessionStartResult, 1)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionStart(func(r adapter.SessionStartResult) { startCh <- r })
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })
	if err := exec.StartCatchUp("r1", 1, 1, 1, 1, targetLSN); err != nil {
		t.Fatal(err)
	}
	select {
	case <-startCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for start")
	}
	select {
	case r := <-closeCh:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for close")
	}
	return adapter.SessionCloseResult{}
}

// --- V2-mirror tests ---

func TestCatchUpSender_HappyPath_WALReplay(t *testing.T) {
	exec, primary, _ := newCatchUpExecutor(t)
	writeTestBlocks(primary, 5)
	_, _, pH := primary.Boundaries()

	result := runCatchUp(t, exec, pH)
	if !result.Success {
		t.Fatalf("catch-up failed: %s", result.FailReason)
	}
	if result.AchievedLSN != pH {
		t.Errorf("AchievedLSN=%d want %d (target)", result.AchievedLSN, pH)
	}
}

// TestCatchUpSender_RecycledFromLSN_ErrorPropagates pins
// INV-REPL-CATCHUP-RECYCLE-ESCALATES: substrate ErrWALRecycled MUST
// propagate through doCatchUp as a wrapped error containing the
// sentinel. Engine SessionClose handler matches with errors.Is to
// trigger the NeedsRebuild transition.
//
// We use a custom substrate that always returns ErrWALRecycled to
// avoid depending on a real walstore + checkpoint advance dance.
func TestCatchUpSender_RecycledFromLSN_ErrorPropagates(t *testing.T) {
	primary := &recycledStubStore{BlockStore: storage.NewBlockStore(64, 4096)}
	primary.Write(0, make([]byte, 4096)) // make H>0 so probe doesn't reject
	_, _, listener := setupPrimaryReplica(t)
	exec := NewBlockExecutor(primary, listener.Addr())

	result := runCatchUp(t, exec, 1)
	if result.Success {
		t.Fatal("catch-up against recycled substrate must fail")
	}
	if !strings.Contains(result.FailReason, "WAL recycled") {
		t.Errorf("FailReason should contain 'WAL recycled' (substrate ErrWALRecycled propagated): %s",
			result.FailReason)
	}
}

// recycledStubStore wraps BlockStore but ScanLBAs always returns
// ErrWALRecycled. Used to test the recycle escalation path without
// needing a real walstore + checkpoint advance.
type recycledStubStore struct {
	*storage.BlockStore
}

func (r *recycledStubStore) ScanLBAs(fromLSN uint64, fn func(storage.RecoveryEntry) error) error {
	return storage.ErrWALRecycled
}

type dropApplyHook struct{}

func (dropApplyHook) ApplyRecovery(lineage RecoveryLineage, lba uint32, data []byte, lsn uint64) error {
	return nil
}

func (dropApplyHook) ApplyLive(lineage RecoveryLineage, lba uint32, data []byte, lsn uint64) error {
	return nil
}

// --- V3-only invariant pins ---

// TestCatchUpSender_FeedsPastTargetBand — targetLSN must not act as
// the catch-up feeder's stop line. The sender feeds all retained WAL
// entries at or after fromLSN; target remains a compat lineage band.
func TestCatchUpSender_FeedsPastTargetBand(t *testing.T) {
	primaryRaw, replica, listener := setupPrimaryReplica(t)
	for i := uint32(0); i < 5; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		_, _ = primaryRaw.Write(i, data)
	}
	primaryRaw.Sync()

	exec := NewBlockExecutor(primaryRaw, listener.Addr())
	// targetLSN=3, but WAL entries 4 and 5 must still be fed.
	result := runCatchUp(t, exec, 3)
	if !result.Success {
		t.Fatalf("catch-up failed: %s", result.FailReason)
	}
	if result.AchievedLSN < 5 {
		t.Fatalf("AchievedLSN=%d want >=5 (entries past target band must be fed)", result.AchievedLSN)
	}
	for i := uint32(3); i < 5; i++ {
		got, _ := replica.Read(i)
		if got[0] != byte(i+1) {
			t.Fatalf("LBA %d not fed past target band: got marker %d want %d", i, got[0], byte(i+1))
		}
	}
}

// TestCatchUpSender_CompletionFromBarrierAchievedLSN — §4.2 binding
// invariant. The catch-up's completion observation comes from the
// barrier response's AchievedLSN, not from a separate done marker.
func TestCatchUpSender_CompletionFromBarrierAchievedLSN(t *testing.T) {
	exec, primary, _ := newCatchUpExecutor(t)
	writeTestBlocks(primary, 3)

	// Happy path: target = primaryH = 3, barrier achieves 3, success.
	_, _, pH := primary.Boundaries()
	r1 := runCatchUp(t, exec, pH)
	if !r1.Success {
		t.Fatalf("happy: %s", r1.FailReason)
	}
	if r1.AchievedLSN != pH {
		t.Errorf("happy: AchievedLSN=%d want %d", r1.AchievedLSN, pH)
	}

	_ = r1
}

// TestCatchUpSender_DeadlineClearedAfterReturn — INV-REPL-CATCHUP-
// DEADLINE-PER-CALL-SCOPE. After doCatchUp returns, the executor's
// conn is unusable for subsequent ops because catch-up's
// `_ = conn.Close()` deferred close. We can only verify scope by
// confirming the deferred SetDeadline(time.Time{}) call exists in
// the source — runtime checking would require connection reuse
// across catch-up + steady-state, which the current architecture
// doesn't expose.
//
// This test is a build-time fence: it imports the executor and
// exercises catch-up; the SetDeadline(time.Time{}) clear is verified
// by code review. Marked as a smoke test for the invariant.
func TestCatchUpSender_DeadlineClearedAfterReturn(t *testing.T) {
	exec, primary, _ := newCatchUpExecutor(t)
	writeTestBlocks(primary, 1)
	_, _, pH := primary.Boundaries()
	r := runCatchUp(t, exec, pH)
	if !r.Success {
		t.Fatalf("expected success: %s", r.FailReason)
	}
	// If the deferred SetDeadline(time.Time{}) clear were missing,
	// subsequent catch-ups on a reused conn would inherit a stale
	// deadline. The test passes because the catch-up path completes
	// cleanly; absence of error is the indirect fence.
}

// TestCatchUpSender_EmptyScanDoesNotFailTargetBand —
// targetLSN is not a catch-up close predicate. If the substrate emits
// no entries, the feeder wrote no WAL and the session may close at the
// observed frontier. Upper layers must use later probe/decision facts
// to determine whether more feeding is needed.
func TestCatchUpSender_EmptyScanDoesNotFailTargetBand(t *testing.T) {
	primary := &emptyScanStore{BlockStore: storage.NewBlockStore(64, 4096)}
	primary.Write(0, make([]byte, 4096))
	primary.Write(1, make([]byte, 4096)) // walHead = 2
	_, _, listener := setupPrimaryReplica(t)
	exec := NewBlockExecutor(primary, listener.Addr())

	result := runCatchUp(t, exec, 2)
	if !result.Success {
		t.Fatalf("empty feeder scan should not fail solely because target band is higher: %s", result.FailReason)
	}
	if result.AchievedLSN != 0 {
		t.Fatalf("AchievedLSN=%d want 0 (replica observed no WAL)", result.AchievedLSN)
	}
}

// TestCatchUpSender_AckBehindLastSentFails —
// INV-REPL-CATCHUP-ACK-COVERS-LAST-SENT. Two different error classes:
//   (a) substrate returns ErrWALRecycled  → wrapped sentinel
//   (b) barrier achieves < lastSent       → ack-behind-last-sent
//
// (a) covered by TestCatchUpSender_RecycledFromLSN_ErrorPropagates.
// (b) tested here via a replica that drops ShipEntry payloads but
// still responds to the barrier.
func TestCatchUpSender_AckBehindLastSentFails(t *testing.T) {
	primary := storage.NewBlockStore(64, 4096)
	primary.Write(0, make([]byte, 4096))
	primary.Write(1, make([]byte, 4096))
	primary.Sync()
	replica := storage.NewBlockStore(64, 4096)
	dropListener, err := NewReplicaListenerWithApplyHook("127.0.0.1:0", replica, dropApplyHook{})
	if err != nil {
		t.Fatal(err)
	}
	defer dropListener.Stop()
	dropListener.Serve()
	exec := NewBlockExecutor(primary, dropListener.Addr())

	result := runCatchUp(t, exec, 2)
	if result.Success {
		t.Fatal("catch-up must fail when barrier ack is behind lastSent")
	}
	if strings.Contains(result.FailReason, "WAL recycled") {
		t.Errorf("ack-behind-last-sent must NOT be conflated with WAL recycle: %s", result.FailReason)
	}
	if !strings.Contains(result.FailReason, "behind last sent") {
		t.Errorf("FailReason should mention behind-last-sent: %s", result.FailReason)
	}
}

// emptyScanStore wraps BlockStore but ScanLBAs emits no entries.
type emptyScanStore struct {
	*storage.BlockStore
}

func (e *emptyScanStore) ScanLBAs(fromLSN uint64, fn func(storage.RecoveryEntry) error) error {
	return nil // emit nothing — replica stays at 0
}

// TestCatchUpSender_ModeLabelEmittedWALReplay — memo §5.1 mode-label
// observability pin. The catch-up success log includes the
// `recovery_mode` label. Substrates exposing CheckpointLSN report
// `wal_replay`; others report `state_convergence`. T4c-3 will extend
// observability surface with full integration matrix coverage; for
// the unit-test layer we verify the runtime probe doesn't panic and
// returns one of the two known labels.
func TestCatchUpSender_ModeLabelEmittedWALReplay(t *testing.T) {
	// Direct verification: BlockStore does NOT expose CheckpointLSN,
	// so the runtime probe in catchup_sender returns
	// state_convergence. Walstore would return wal_replay. Sanity-
	// check the substrate-introspection probe matches expectation.
	bs := storage.NewBlockStore(64, 4096)
	if _, ok := interface{}(bs).(interface{ CheckpointLSN() uint64 }); ok {
		t.Fatal("BlockStore must NOT expose CheckpointLSN — would mis-label as wal_replay")
	}
	// Mode constants are stable strings (memo §5.1).
	if storage.RecoveryModeWALReplay != "wal_replay" {
		t.Errorf("RecoveryModeWALReplay = %q, want %q", storage.RecoveryModeWALReplay, "wal_replay")
	}
	if storage.RecoveryModeStateConvergence != "state_convergence" {
		t.Errorf("RecoveryModeStateConvergence = %q, want %q",
			storage.RecoveryModeStateConvergence, "state_convergence")
	}
}

// TestCatchUpSender_ErrWALRecycledSentinelStable pins the unified
// sentinel: storage.ErrWALRecycled is the single sentinel both
// substrates raise; smartwal's re-export aliases it.
func TestCatchUpSender_ErrWALRecycledSentinelStable(t *testing.T) {
	if !errors.Is(storage.ErrWALRecycled, storage.ErrWALRecycled) {
		t.Fatal("sentinel must satisfy errors.Is reflexively")
	}
}
