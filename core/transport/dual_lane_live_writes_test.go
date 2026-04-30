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

	// Pusher fan-out: 8 workers × 5 LBAs = 40 attempted pushes.
	// LBAs 100..139, distinct per worker. Each push:
	//   1. primary.Write to allocate a fresh LSN > frozenTarget
	//   2. bridge.PushLiveWrite → sink.NotifyAppend
	// Race: scan + flush race against pushers; some land before seal,
	// some after.
	const writers = 8
	const perWriter = 5

	var (
		acceptedMu  sync.Mutex
		acceptedSet = map[uint32][]byte{}
	)

	bridge := exec.dualLane.Bridge
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
					// Successful push — the atomic-seal contract demands
					// this entry MUST land on replica.
					acceptedMu.Lock()
					acceptedSet[lba] = append([]byte(nil), data...)
					acceptedMu.Unlock()
				}
				// pushErr != nil → "sink sealed" — caller knows session
				// is closing. Production would re-route to steady-live;
				// test just drops.
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

	// Atomic-seal contract: every successful push is byte-equal on replica.
	acceptedMu.Lock()
	defer acceptedMu.Unlock()

	if len(acceptedSet) == 0 {
		t.Fatal("no pushes succeeded — flush+seal raced ahead of all pushers; widen the test window")
	}

	for lba, want := range acceptedSet {
		got, err := replica.Read(lba)
		if err != nil {
			t.Errorf("replica.Read lba=%d: %v", lba, err)
			continue
		}
		if !bytes.Equal(got, want) {
			t.Errorf("INV-SEAL-ATOMIC violated: lba=%d successful push but replica bytes differ (got[0:2]=%02x %02x want[0:2]=%02x %02x)",
				lba, got[0], got[1], want[0], want[1])
		}
	}

	// Backlog correctness: every seeded LBA must also be byte-equal.
	// streamBacklog ships these as WALKindBacklog; receiver applies.
	for lba := uint32(0); lba < backlogN; lba++ {
		pd, _ := primary.Read(lba)
		rd, _ := replica.Read(lba)
		if !bytes.Equal(pd, rd) {
			t.Errorf("backlog lba=%d mismatch", lba)
		}
	}

	// Coordinator returned to Idle (session ended cleanly via defer).
	if got := coord.Phase("r1"); got != recovery.PhaseIdle {
		t.Errorf("post-session phase=%s want Idle", got)
	}

	t.Logf("dual-lane live-write E2E: %d/%d pushes accepted, all byte-equal on replica; backlog %d entries also byte-equal",
		len(acceptedSet), writers*perWriter, backlogN)
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
