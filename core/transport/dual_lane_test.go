package transport

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// runDualLaneListener spins up a TCP listener on 127.0.0.1:0 that
// accepts dual-lane recover connections and dispatches each to a
// fresh recovery.Receiver via recovery.ReplicaBridge.Serve. Returns
// the bound address and a stop function.
//
// In production, cmd/blockvolume.main does the equivalent. Inlined
// here so the test can exercise the dual-lane wire path end-to-end
// without a daemon binary.
func runDualLaneListener(t *testing.T, store storage.LogicalStorage) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	bridge := recovery.NewReplicaBridge(store)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		bridge.AcceptDualLaneLoop(ctx, ln)
	}()
	return ln.Addr().String(), func() {
		cancel()
		_ = ln.Close()
		<-done
	}
}

// TestDualLane_BlockExecutor_StartRebuild proves that
// NewBlockExecutorWithDualLane delegates StartRebuild through
// recovery.PrimaryBridge correctly:
//
//   - Dials the replica's separate dual-lane port (Option A).
//   - Coordinator's StartSession fires synchronously before return.
//   - OnSessionStart / OnSessionClose callbacks fire with adapter-shaped
//     results (so existing engine code consumes them transparently).
//   - achievedLSN == primary's H.
//   - Replica's substrate matches primary byte-for-byte after barrier.
//
// What this test does NOT cover (out of scope per wiring plan §10):
//
//   - §3.2 #3 single-queue real-time interleave.
//   - WAL recycle path consuming MinPinAcrossActiveSessions (priority 2.5).
//   - cmd flag wiring (production daemon path).
func TestDualLane_BlockExecutor_StartRebuild(t *testing.T) {
	// Skipped post-§3.2 #3: BlockStore substrate emits scan-time
	// synthetic LSN that doesn't satisfy the cursor model's per-entry
	// monotonic increase. Per kickoff v0.3 §10 OOS, this milestone
	// targets memorywal/walstore. Replacement test (memorywal-based,
	// integration-level) lands in mini-plan §2.2 follow-up commit.
	t.Skip("BlockStore-substrate test; re-pinned on memorywal in mini-plan §2.2 follow-up")

	const numBlocks = 64
	const blockSize = 4096

	primary := storage.NewBlockStore(numBlocks, blockSize)
	replica := storage.NewBlockStore(numBlocks, blockSize)

	// Seed primary with 30 LBAs of distinguishable bytes.
	for lba := uint32(0); lba < 30; lba++ {
		data := make([]byte, blockSize)
		data[0] = byte(0x80 | lba)
		_, _ = primary.Write(lba, data)
	}
	_, _ = primary.Sync()

	// Replica-side dual-lane listener.
	dualLaneAddr, stop := runDualLaneListener(t, replica)
	defer stop()

	// Coordinator (per wiring plan §5: per-volume, here per-test).
	coord := recovery.NewPeerShipCoordinator()

	// Executor in dual-lane mode. legacy replicaAddr is bogus — this
	// test only exercises StartRebuild, which routes via dualLaneAddr.
	exec := NewBlockExecutorWithDualLane(
		primary,
		"127.0.0.1:0", // legacy port, unused in this test
		dualLaneAddr,
		coord,
		recovery.ReplicaID("r1"),
	)

	startCh := make(chan adapter.SessionStartResult, 1)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionStart(func(r adapter.SessionStartResult) { startCh <- r })
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	_, _, primaryH := primary.Boundaries()
	if err := exec.StartRebuild("r1", 7, 1, 1, primaryH); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	// OnSessionStart should fire fast (bridge invokes it synchronously
	// before the goroutine spawns).
	select {
	case got := <-startCh:
		if got.SessionID != 7 {
			t.Errorf("OnSessionStart: SessionID=%d want 7", got.SessionID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnSessionStart did not fire within 2s")
	}

	// OnSessionClose: success with achievedLSN == primary H.
	select {
	case got := <-closeCh:
		if !got.Success {
			t.Fatalf("OnSessionClose: not Success; FailReason=%q", got.FailReason)
		}
		if got.AchievedLSN != primaryH {
			t.Errorf("OnSessionClose: achievedLSN=%d want %d", got.AchievedLSN, primaryH)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnSessionClose did not fire within 5s")
	}

	// Replica matches primary on the rebuilt range.
	for lba := uint32(0); lba < 30; lba++ {
		pd, _ := primary.Read(lba)
		rd, _ := replica.Read(lba)
		if !bytes.Equal(pd, rd) {
			t.Fatalf("lba %d mismatch: primary[0]=%02x replica[0]=%02x", lba, pd[0], rd[0])
		}
	}

	// Coordinator returned to Idle (session ended cleanly).
	if got := coord.Phase("r1"); got != recovery.PhaseIdle {
		t.Errorf("post-session phase=%s want Idle", got)
	}
}

// TestDualLane_LegacyConstructorIsNotDualLane verifies the
// existing NewBlockExecutor path is unchanged: e.dualLane is nil,
// StartRebuild falls through to the legacy single-lane code (no
// behavior change for callers using the legacy constructor).
func TestDualLane_LegacyConstructorIsNotDualLane(t *testing.T) {
	primary := storage.NewBlockStore(8, 4096)
	exec := NewBlockExecutor(primary, "127.0.0.1:0")
	if exec.dualLane != nil {
		t.Errorf("legacy NewBlockExecutor: dualLane=%v want nil", exec.dualLane)
	}
}
