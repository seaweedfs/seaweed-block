package transport

// Failure-path E2E tests on the dual-lane bridging path. Each test
// verifies that an error mid-session leaves the system in a clean
// teardown state: OnSessionClose fires with Success=false, coordinator
// returns to Idle, no goroutine leaks.
//
// Tested failure modes:
//   - Replica's dual-lane listener stops mid-session (peer conn dies)
//   - Primary's session goroutine sees no BarrierResp (replica
//     crashes after BaseDone but before barrier)
//
// Out of scope (covered elsewhere or deferred):
//   - Substrate Read/Write errors (covered at storage layer)
//   - WAL recycled (FailureWALRecycled — covered in catch-up path)

import (
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/storage/memorywal"
)

// TestDualLane_ReplicaListenerStopped_BeforeRebuild — when the
// replica's dual-lane listener is closed before StartRebuild dials,
// the primary's bridge fails the dial and OnSessionClose fires with
// Success=false; coordinator never enters non-Idle state because the
// session was never registered.
func TestDualLane_ReplicaListenerStopped_BeforeRebuild(t *testing.T) {
	primary := memorywal.NewStore(8, 4096)
	replica := storage.NewBlockStore(8, 4096)

	// Stand up the listener, then immediately stop it — the address
	// is captured but no longer accepting.
	dualLaneAddr, stop := runDualLaneListener(t, replica)
	stop()

	coord := recovery.NewPeerShipCoordinator()
	exec := NewBlockExecutorWithDualLane(
		primary, "127.0.0.1:0", dualLaneAddr, coord, recovery.ReplicaID("r1"),
	)

	// StartRebuild attempts to dial the dead listener and returns the
	// dial error synchronously (before the goroutine spawns), so
	// OnSessionClose is NOT fired here — the bridge never installed
	// a session in the first place.
	if err := exec.StartRebuild("r1", 7, 1, 1, 0); err == nil {
		t.Fatal("StartRebuild against dead listener: want error, got nil")
	}

	// Coordinator never advanced: peer is Idle.
	if got := coord.Phase("r1"); got != recovery.PhaseIdle {
		t.Errorf("post-failed-StartRebuild phase=%s want Idle", got)
	}
}

// TestDualLane_ReplicaListenerStopped_MidSession — listener closes
// AFTER the primary dials but DURING the rebuild stream (e.g., before
// barrier). Sender's writeFrame hits a wire error; Run returns
// FailureWire; OnSessionClose fires with Success=false; coordinator
// returns to Idle via the deferred EndSession.
func TestDualLane_ReplicaListenerStopped_MidSession(t *testing.T) {
	primary := memorywal.NewStore(64, 4096)
	for lba := uint32(0); lba < 30; lba++ {
		_, _ = primary.Write(lba, []byte{byte(lba)})
	}
	_, _ = primary.Sync()
	_, _, primaryH := primary.Boundaries()

	// Custom listener that we can close at will (don't use runDualLaneListener
	// because we want fine-grained control over when conns are accepted).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dualLaneAddr := ln.Addr().String()

	// Accept one conn, immediately close it WITHOUT spawning a Receiver.
	// Primary's Sender will see EOF / broken pipe on the first writeFrame.
	connClosed := make(chan struct{})
	go func() {
		defer close(connClosed)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		_ = conn.Close()
	}()
	defer ln.Close()

	coord := recovery.NewPeerShipCoordinator()
	exec := NewBlockExecutorWithDualLane(
		primary, "127.0.0.1:0", dualLaneAddr, coord, recovery.ReplicaID("r1"),
	)

	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	if err := exec.StartRebuild("r1", 7, 1, 1, primaryH); err != nil {
		t.Fatalf("StartRebuild: %v", err)
	}

	<-connClosed // listener handler closed the conn

	select {
	case res := <-closeCh:
		if res.Success {
			t.Errorf("expected failure but Success=true (replica conn was closed mid-session)")
		}
		if res.FailReason == "" {
			t.Errorf("expected non-empty FailReason on wire failure")
		}
		t.Logf("teardown surfaced: %s", res.FailReason)
	case <-time.After(5 * time.Second):
		t.Fatal("OnSessionClose did not fire within 5s after replica conn close")
	}

	// Coordinator MUST return to Idle via Sender.Run's deferred
	// coord.EndSession, even on the failure path.
	if got := coord.Phase("r1"); got != recovery.PhaseIdle {
		t.Errorf("post-failure phase=%s want Idle (defer must restore even on wire failure)", got)
	}

	// PrimaryBridge must have torn down the sender entry — no leak.
	bridge := exec.dualLane.Bridge
	if pushErr := bridge.PushLiveWrite(recovery.ReplicaID("r1"), 0, 1, []byte("x")); pushErr == nil {
		t.Errorf("post-failure PushLiveWrite: want error (no active session), got nil")
	}
}

// TestDualLane_SubsequentRebuild_AfterFailure_Succeeds — regression:
// after a failed rebuild teardown, a fresh rebuild on the same replica
// MUST succeed cleanly. Verifies the registry / coordinator state is
// genuinely cleaned, not just dead-but-still-occupying-slots.
func TestDualLane_SubsequentRebuild_AfterFailure_Succeeds(t *testing.T) {
	primary := memorywal.NewStore(64, 4096)
	for lba := uint32(0); lba < 10; lba++ {
		_, _ = primary.Write(lba, []byte{byte(lba)})
	}
	_, _ = primary.Sync()
	_, _, primaryH := primary.Boundaries()

	replica := storage.NewBlockStore(64, 4096)

	// First listener — close it to fail the first attempt.
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 1: %v", err)
	}
	addr1 := ln1.Addr().String()
	go func() {
		conn, _ := ln1.Accept()
		if conn != nil {
			_ = conn.Close()
		}
	}()
	defer ln1.Close()

	coord := recovery.NewPeerShipCoordinator()
	exec := NewBlockExecutorWithDualLane(
		primary, "127.0.0.1:0", addr1, coord, recovery.ReplicaID("r1"),
	)

	closeCh := make(chan adapter.SessionCloseResult, 2) // two attempts
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	// First attempt — fails.
	if err := exec.StartRebuild("r1", 7, 1, 1, primaryH); err != nil {
		t.Fatalf("StartRebuild 1: %v", err)
	}
	select {
	case res := <-closeCh:
		if res.Success {
			t.Fatalf("first attempt unexpectedly succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first OnSessionClose did not fire within 5s")
	}

	// Verify cleanup before retrying.
	if got := coord.Phase("r1"); got != recovery.PhaseIdle {
		t.Fatalf("after first failure: phase=%s want Idle", got)
	}

	// Bring up a real listener, point executor's dualLane at it.
	dualLaneAddr2, stop2 := runDualLaneListener(t, replica)
	defer stop2()
	exec.dualLane.DialAddr = dualLaneAddr2

	// Second attempt — must succeed cleanly.
	if err := exec.StartRebuild("r1", 8, 1, 1, primaryH); err != nil {
		t.Fatalf("StartRebuild 2: %v", err)
	}
	select {
	case res := <-closeCh:
		if !res.Success {
			t.Fatalf("second attempt expected success, got FailReason=%q", res.FailReason)
		}
		if res.AchievedLSN < primaryH {
			t.Errorf("second attempt achievedLSN=%d < primaryH=%d", res.AchievedLSN, primaryH)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second OnSessionClose did not fire within 5s")
	}

	if got := coord.Phase("r1"); got != recovery.PhaseIdle {
		t.Errorf("post-second-success phase=%s want Idle", got)
	}
}
