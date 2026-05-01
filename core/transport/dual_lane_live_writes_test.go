package transport

// E2E test for the gap the recovery package's e2e_test.go disclaimed
// at the dual-lane (cross-process-shape) level: live writes pushed via
// PrimaryBridge during an active dual-lane rebuild session.
//
// This exercises:
//   - BlockExecutor.StartRebuild (dual-lane path)
//   - PrimaryBridge.PushLiveWrite during session
//   - senderBacklogSink.NotifyAppend buffer + flushAndSeal atomic-seal
//     boundary (pushes that race with seal either land buffered or
//     return "sink sealed" error)
//   - Receiver applying both Backlog (from streamBacklog) and
//     SessionLive (from flushAndSeal) WAL frames
//   - OnSessionClose fires; coordinator returns to Idle
//
// Substrate: memorywal on primary (preserves real write-time LSNs so
// live writes get LSN > frozenTarget and streamBacklog filters them
// for flushAndSeal to ship as SessionLive). Replica stays BlockStore.

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// TestDualLane_LiveWritesDuringSession_AtomicSeal — live writes injected
// via PrimaryBridge.PushLiveWrite during a dual-lane rebuild session
// must respect the atomic-seal contract: every push that returns nil
// MUST result in the entry being byte-equal on the replica after barrier.
//
// Race shape (intrinsic in B-2 lifecycle): pushers race against
// senderBacklogSink.flushAndSeal. Pushes before seal → buffered → shipped
// as WALKindSessionLive → applied on replica. Pushes after seal → error
// → caller knows session is closing.
func TestDualLane_LiveWritesDuringSession_AtomicSeal(t *testing.T) {
	const numBlocks = 256
	const blockSize = 4096
	const backlogN = 100 // backlog scan length — large enough to give pushers a window

	primary := memorywal.NewStore(numBlocks, blockSize)
	replica := storage.NewBlockStore(numBlocks, blockSize)

	// Seed primary with backlog.
	for lba := uint32(0); lba < backlogN; lba++ {
		data := make([]byte, blockSize)
		data[0] = byte(0x40 | (lba & 0x3F)) // distinguishable backlog byte
		if _, err := primary.Write(lba, data); err != nil {
			t.Fatalf("seed Write lba=%d: %v", lba, err)
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatalf("seed Sync: %v", err)
	}
	_, _, primaryH := primary.Boundaries()
	if primaryH != backlogN {
		t.Fatalf("expected primaryH=%d after %d seeded writes, got %d", backlogN, backlogN, primaryH)
	}

	// Replica-side dual-lane listener.
	dualLaneAddr, stop := runDualLaneListener(t, replica)
	defer stop()

	coord := recovery.NewPeerShipCoordinator()
	exec := NewBlockExecutorWithDualLane(
		primary,
		"127.0.0.1:0",
		dualLaneAddr,
		coord,
		recovery.ReplicaID("r1"),
	)

	startCh := make(chan adapter.SessionStartResult, 1)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionStart(func(r adapter.SessionStartResult) { startCh <- r })
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	frozenTarget := primaryH
	if err := exec.StartRebuild("r1", 7, 1, 1, frozenTarget); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	// Wait for session-start signal so PushLiveWrite has a sender to
	// route through.
	select {
	case <-startCh:
	case <-time.After(2 * time.Second):
		t.Fatal("OnSessionStart did not fire within 2s")
	}

	// §6.3 / Path B: pusher fan-out runs concurrently against the
	// session, but the contract being tested is "post-barrier replica
	// converges to primary" — NOT "every successful push reaches
	// replica byte-equal at the per-pusher granularity". The pre-§6.3
	// per-pusher acceptedSet vs replica.Read comparison was racy under
	// §6.3 single drive() dispatch (concurrent CASE A scans interleave
	// against producer state); architect Path B ruling: drop that
	// per-pusher cache; assert whole-array equivalence post-barrier.
	const writers = 8
	const perWriter = 5

	bridge := exec.dualLane.Bridge
	var (
		pushedAccepted atomic.Int32
		pushedRejected atomic.Int32
	)
	var pushersWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		pushersWG.Add(1)
		go func(workerID int) {
			defer pushersWG.Done()
			for i := 0; i < perWriter; i++ {
				lba := uint32(backlogN + workerID*perWriter + i)
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
				if pushErr := bridge.PushLiveWrite(recovery.ReplicaID("r1"), lba, lsn, data); pushErr == nil {
					pushedAccepted.Add(1)
				} else {
					pushedRejected.Add(1)
				}
			}
		}(w)
	}
	pushersWG.Wait()

	// Wait for OnSessionClose.
	var closeRes adapter.SessionCloseResult
	select {
	case closeRes = <-closeCh:
	case <-time.After(10 * time.Second):
		t.Fatal("OnSessionClose did not fire within 10s")
	}
	if !closeRes.Success {
		t.Fatalf("session not Success; FailReason=%q", closeRes.FailReason)
	}

	// §6.3 Path B post-barrier convergence: every LBA that primary
	// has must equal replica. This is the architect's "barrier 后整块
	// replica.Read == primary.Read" contract. Iterates across the
	// full LBA range any pusher could have written (backlog + push
	// window).
	maxLBA := uint32(backlogN + writers*perWriter)
	if maxLBA > numBlocks {
		maxLBA = numBlocks
	}
	for lba := uint32(0); lba < maxLBA; lba++ {
		pd, err := primary.Read(lba)
		if err != nil {
			t.Fatalf("primary.Read lba=%d: %v", lba, err)
		}
		rd, err := replica.Read(lba)
		if err != nil {
			t.Errorf("replica.Read lba=%d: %v", lba, err)
			continue
		}
		if !bytes.Equal(pd, rd) {
			t.Errorf("post-barrier convergence violated: lba=%d primary[0:2]=%02x %02x replica[0:2]=%02x %02x",
				lba, pd[0], pd[1], rd[0], rd[1])
		}
	}

	// Coordinator returned to Idle (session ended cleanly via defer).
	if got := coord.Phase("r1"); got != recovery.PhaseIdle {
		t.Errorf("post-session phase=%s want Idle", got)
	}

	t.Logf("§6.3 Path B atomic-seal: %d/%d pushes accepted (%d rejected); "+
		"post-barrier replica == primary across LBA [0,%d)",
		pushedAccepted.Load(), int32(writers*perWriter), pushedRejected.Load(), maxLBA)
}

// TestDualLane_LiveWritesDuringSession_AchievedLSN — sanity:
// achievedLSN reported by OnSessionClose must reflect at least the
// frozenTarget (backlog drain); if pushers landed before seal, the
// replica's H advances further. We don't assert on the exact value
// (race-dependent) but pin a non-degeneracy lower bound.
func TestDualLane_LiveWritesDuringSession_AchievedLSN(t *testing.T) {
	const numBlocks = 64
	const blockSize = 4096
	const backlogN = 30

	primary := memorywal.NewStore(numBlocks, blockSize)
	replica := storage.NewBlockStore(numBlocks, blockSize)

	for lba := uint32(0); lba < backlogN; lba++ {
		data := make([]byte, blockSize)
		data[0] = byte(0x80 | lba)
		_, _ = primary.Write(lba, data)
	}
	_, _ = primary.Sync()
	_, _, primaryH := primary.Boundaries()

	dualLaneAddr, stop := runDualLaneListener(t, replica)
	defer stop()

	coord := recovery.NewPeerShipCoordinator()
	exec := NewBlockExecutorWithDualLane(
		primary, "127.0.0.1:0", dualLaneAddr, coord, recovery.ReplicaID("r1"),
	)

	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	if err := exec.StartRebuild("r1", 11, 1, 1, primaryH); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	select {
	case res := <-closeCh:
		if !res.Success {
			t.Fatalf("session not Success; FailReason=%q", res.FailReason)
		}
		if res.AchievedLSN < primaryH {
			t.Errorf("achievedLSN=%d < frozenTarget=%d (backlog drain incomplete)", res.AchievedLSN, primaryH)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnSessionClose did not fire within 5s")
	}
}

// TestDualLane_PushLiveWrite_NoSession_Errors — without an active
// session, PrimaryBridge.PushLiveWrite returns an error. Exercises
// the bridge's "no sender for replica" guard.
func TestDualLane_PushLiveWrite_NoSession_Errors(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	replica := storage.NewBlockStore(8, 4096)

	dualLaneAddr, stop := runDualLaneListener(t, replica)
	defer stop()

	coord := recovery.NewPeerShipCoordinator()
	exec := NewBlockExecutorWithDualLane(
		primary, "127.0.0.1:0", dualLaneAddr, coord, recovery.ReplicaID("r1"),
	)

	bridge := exec.dualLane.Bridge
	err := bridge.PushLiveWrite(recovery.ReplicaID("r1"), 0, 1, []byte("x"))
	if err == nil {
		t.Fatal("PushLiveWrite without active session: want error, got nil")
	}
	wantSubstr := "no active session"
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Errorf("error=%q does not contain %q", err.Error(), wantSubstr)
	}
}
