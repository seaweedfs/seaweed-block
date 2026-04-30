package recovery

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// TestIntegrationStub_LifecycleCallbacks exercises the bridge as a
// CommandExecutor-shaped surface: StartRebuildSession spawns a
// goroutine, callback OnStart fires synchronously before return,
// PushLiveWrite during the session works, OnClose reports the
// achieved LSN at the end. After P2c-slice B-2 the session terminates
// autonomously when sink.DrainBacklog returns — there is no explicit
// FinishLiveWrites / sender.Close signal anymore. Live writes pushed
// before sender.Run barriers are buffered and shipped via flushAndSeal.
//
// This is the lifecycle/callback shape the integration PR will plug
// into core/transport/BlockExecutor.
func TestIntegrationStub_LifecycleCallbacks(t *testing.T) {
	const numBlocks = 32
	const blockSize = 4096

	primary := storage.NewBlockStore(numBlocks, blockSize)
	replica := storage.NewBlockStore(numBlocks, blockSize)

	// Seed primary with some data.
	for lba := uint32(0); lba < 10; lba++ {
		_, _ = primary.Write(lba, formulaPayload(lba, 0xD0, blockSize))
	}
	_, _ = primary.Sync()

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	coord := NewPeerShipCoordinator()

	// Capture callback firing order + values.
	var (
		startCalled    bool
		startReplicaID ReplicaID
		startSessionID uint64
		closeCalled    bool
		closeAchieved  uint64
		closeErr       error
		mu             sync.Mutex
		closeWG        sync.WaitGroup
	)
	closeWG.Add(1)

	bridge := NewPrimaryBridge(
		primary,
		coord,
		func(replicaID ReplicaID, sessionID uint64) {
			mu.Lock()
			startCalled = true
			startReplicaID = replicaID
			startSessionID = sessionID
			mu.Unlock()
		},
		func(replicaID ReplicaID, sessionID uint64, achieved uint64, err error) {
			mu.Lock()
			closeCalled = true
			closeAchieved = achieved
			closeErr = err
			mu.Unlock()
			closeWG.Done()
		},
	)

	// Replica side.
	replicaBridge := NewReplicaBridge(replica)
	var replicaErr error
	replicaWG := sync.WaitGroup{}
	replicaWG.Add(1)
	go func() {
		defer replicaWG.Done()
		_, replicaErr = replicaBridge.Serve(context.Background(), replicaConn)
	}()

	// Drive primary side.
	ctx := context.Background()
	_, _, primaryH := primary.Boundaries()
	if err := bridge.StartRebuildSession(ctx, primaryConn, "r1", 42, 0, primaryH); err != nil {
		t.Fatalf("StartRebuildSession: %v", err)
	}

	// OnStart should have fired before StartRebuildSession returned.
	mu.Lock()
	if !startCalled {
		t.Fatal("OnStart should have fired before StartRebuildSession returned")
	}
	if startReplicaID != "r1" || startSessionID != 42 {
		t.Errorf("OnStart values: got (%q, %d), want (r1, 42)", startReplicaID, startSessionID)
	}
	mu.Unlock()

	// Inject one live write while session is active.
	liveLBA := uint32(20)
	liveData := formulaPayload(liveLBA, 0xD0, blockSize)
	liveLSN, _ := primary.Write(liveLBA, liveData)
	if route := coord.RouteLocalWrite("r1", liveLSN); route != RouteSessionLane {
		t.Fatalf("RouteLocalWrite during session: got %v want SessionLane", route)
	}
	if err := bridge.PushLiveWrite("r1", liveLBA, liveLSN, liveData); err != nil {
		t.Fatalf("PushLiveWrite: %v", err)
	}

	// Single-flight: a second StartRebuildSession on same replica errors.
	if err := bridge.StartRebuildSession(ctx, primaryConn, "r1", 99, 0, primaryH); err == nil {
		t.Error("second StartRebuildSession on same peer: want error (single-flight)")
	}

	// P2c-slice B-2: sender.Run barriers autonomously after DrainBacklog
	// returns; OnClose fires when Run goroutine exits. No explicit
	// FinishLiveWrites needed.
	closeWG.Wait()
	replicaWG.Wait()

	if replicaErr != nil {
		t.Fatalf("replica bridge: %v", replicaErr)
	}

	mu.Lock()
	defer mu.Unlock()
	if !closeCalled {
		t.Fatal("OnClose should have fired")
	}
	if closeErr != nil {
		t.Fatalf("OnClose err: %v", closeErr)
	}
	if closeAchieved < liveLSN {
		t.Errorf("OnClose achievedLSN=%d, expected >= liveLSN=%d", closeAchieved, liveLSN)
	}

	// Verify replica got the live write.
	got, _ := replica.Read(liveLBA)
	if !bytes.Equal(got, liveData) {
		t.Errorf("live write at lba=%d not on replica", liveLBA)
	}

	// After OnClose, coord should be Idle.
	if got := coord.Phase("r1"); got != PhaseIdle {
		t.Errorf("post-close phase=%s want Idle", got)
	}
}

// TestIntegrationStub_ReceiverFailsEarly_SenderUnblocks — architect's
// lifecycle question: if receiver errors before sender's drain,
// does sender's defer still seal + EndSession?
//
// Setup: receiver returns immediately on first frame (simulated by
// closing its conn). Primary's sender, blocked on closeCh waiting
// for FinishLiveWrites, is woken up by ctx cancellation. After Run
// returns, sealed must be true so subsequent PushLiveWrite errors,
// and the coordinator must be back to Idle.
func TestIntegrationStub_ReceiverFailsEarly_SenderUnblocks(t *testing.T) {
	primary := storage.NewBlockStore(32, 4096)
	for lba := uint32(0); lba < 5; lba++ {
		_, _ = primary.Write(lba, formulaPayload(lba, 0xE0, 4096))
	}
	_, _ = primary.Sync()

	primaryConn, replicaConn := net.Pipe()
	// Close replica side before sender even tries to write — sender
	// will get a wire error during base lane.
	_ = replicaConn.Close()
	defer primaryConn.Close()

	coord := NewPeerShipCoordinator()

	var closeCalled bool
	var closeErr error
	var closeWG sync.WaitGroup
	closeWG.Add(1)

	bridge := NewPrimaryBridge(
		primary,
		coord,
		nil,
		func(replicaID ReplicaID, sessionID uint64, achieved uint64, err error) {
			closeCalled = true
			closeErr = err
			closeWG.Done()
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, primaryH := primary.Boundaries()
	if err := bridge.StartRebuildSession(ctx, primaryConn, "r1", 7, 0, primaryH); err != nil {
		t.Fatalf("StartRebuildSession: %v", err)
	}

	// Wait for OnClose.
	select {
	case <-waitClosed(&closeWG):
	case <-time.After(2 * time.Second):
		t.Fatal("OnClose did not fire within 2s")
	}

	if !closeCalled {
		t.Fatal("OnClose should have fired")
	}
	if closeErr == nil {
		t.Fatal("OnClose: expected non-nil err (receiver was closed before session start)")
	}

	// PushLiveWrite after Run returns should error (sealed by defer).
	pushErr := bridge.PushLiveWrite("r1", 0, 1, []byte{})
	if pushErr == nil {
		t.Error("PushLiveWrite after Run returns: want error (no active sender)")
	}

	// Coord should be back to Idle (EndSession ran in defer).
	if got := coord.Phase("r1"); got != PhaseIdle {
		t.Errorf("post-error phase=%s want Idle", got)
	}
}

// TestIntegrationStub_CtxCancelUnblocksSender — obsolete in
// P2c-slice B-2. Previously sender.Run blocked on closeCh waiting
// for FinishLiveWrites; ctx cancel was the escape hatch. After B-2,
// sender.Run never enters that idle wait — it barriers as soon as
// DrainBacklog returns. Coordinator returning to Idle on session
// completion is covered by the lifecycle test.
func TestIntegrationStub_CtxCancelUnblocksSender(t *testing.T) {
	t.Skip("obsolete after P2c-slice B-2: no closeCh wait — Run barriers as soon as DrainBacklog returns")
}

// waitClosed turns a sync.WaitGroup into a channel for select.
func waitClosed(wg *sync.WaitGroup) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	return ch
}

// TestIntegrationStub_FailureTypedOnReceiverDown — Layer 3 surface:
// when the receiver-side conn is closed before the sender even sends
// the SessionStart frame, sender's Run returns a wrapped Failure with
// Kind=Wire and a Phase that nails the failure point. Caller can
// errors.As / AsFailure to drive retry decisions.
func TestIntegrationStub_FailureTypedOnReceiverDown(t *testing.T) {
	primary := storage.NewBlockStore(16, 4096)
	_, _ = primary.Sync()

	primaryConn, replicaConn := net.Pipe()
	_ = replicaConn.Close()
	defer primaryConn.Close()

	coord := NewPeerShipCoordinator()

	var (
		closeWG  sync.WaitGroup
		closeErr error
	)
	closeWG.Add(1)
	bridge := NewPrimaryBridge(primary, coord, nil,
		func(_ ReplicaID, _ uint64, _ uint64, err error) {
			closeErr = err
			closeWG.Done()
		})

	ctx := context.Background()
	_, _, primaryH := primary.Boundaries()
	if err := bridge.StartRebuildSession(ctx, primaryConn, "r1", 99, 0, primaryH); err != nil {
		t.Fatalf("StartRebuildSession: %v", err)
	}
	closeWG.Wait()

	if closeErr == nil {
		t.Fatal("expected non-nil err from receiver-down session")
	}
	f := AsFailure(closeErr)
	if f == nil {
		t.Fatalf("expected typed *Failure, got %T: %v", closeErr, closeErr)
	}
	if f.Kind != FailureWire {
		t.Errorf("Kind=%s want Wire (peer closed before any frame went out)", f.Kind)
	}
	// Phase should be one of the early write paths.
	switch f.Phase {
	case PhaseSendStart, PhaseBaseLane:
		// OK
	default:
		t.Errorf("Phase=%q want SendStart or BaseLane", f.Phase)
	}
	if !f.Retryable() {
		t.Error("Wire failure should be Retryable=true")
	}
	t.Logf("typed failure: %v (retryable=%v)", f, f.Retryable())
}

// TestIntegrationStub_FailureTypedOnCancellation — obsolete in
// P2c-slice B-2. The previous design had sender.Run block on closeCh
// after DrainBacklog and tested ctx cancel during that wait
// (PhaseAwaitClose). After B-2, sender.Run barriers autonomously
// when DrainBacklog returns — there is no idle wait window to
// cancel into. Cancellation paths during DrainBacklog itself
// (substrate scan returning ctx.Err()) are covered by the sink's
// own DrainBacklog tests.
func TestIntegrationStub_FailureTypedOnCancellation(t *testing.T) {
	t.Skip("obsolete after P2c-slice B-2: no closeCh wait to cancel into; ctx-cancel during DrainBacklog is covered by sink-level tests")
}
