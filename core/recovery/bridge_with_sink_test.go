package recovery

// PrimaryBridge.StartRebuildSessionWithSink — pre-decision parallel
// wiring (P2d-blocked for production enable). The method exists so
// production callers can plug a real `transport.RecoverySink` adapter
// today without changing the bridge's public surface later. This file
// pins:
//   - sink == nil rejected with explicit error
//   - injected sink receives StartSession + DrainBacklog + EndSession
//     calls during the session lifecycle
//   - existing StartRebuildSession (legacy bridging path) unchanged

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// TestPrimaryBridge_StartRebuildSessionWithSink_NilRejected —
// fail-closed: nil sink is a programmer error; method MUST refuse
// rather than silently fall through to the bridging path. Callers
// that want the bridging path call StartRebuildSession explicitly.
func TestPrimaryBridge_StartRebuildSessionWithSink_NilRejected(t *testing.T) {
	primary := storage.NewBlockStore(4, 4096)
	coord := NewPeerShipCoordinator()
	bridge := NewPrimaryBridge(primary, coord, nil, nil)

	prim, replicaEnd := net.Pipe()
	defer prim.Close()
	defer replicaEnd.Close()

	err := bridge.StartRebuildSessionWithSink(
		context.Background(), prim, "r1", 7, 0, 0, nil,
	)
	if err == nil {
		t.Fatal("StartRebuildSessionWithSink with nil sink: want error, got nil")
	}
}

// TestPrimaryBridge_StartRebuildSessionWithSink_InvokesSinkLifecycle —
// the injected sink receives StartSession / DrainBacklog / EndSession
// in order. Mirrors TestSender_WithSink_DelegationOrder but exercised
// through the bridge's public surface — proves that production callers
// using StartRebuildSessionWithSink get the same lifecycle contract as
// direct NewSenderWithSink users.
//
// Uses a direct Receiver (not ReplicaBridge) + manual replicaConn.Close
// in the receiver goroutine so the sender's readerLoop unblocks when
// receiver exits — recordingSink doesn't pump WAL, so barrier won't
// converge and the receiver returns on its own with a wire/protocol
// error. The CONTRACT being tested is sink lifecycle ordering — that's
// independent of barrier outcome (same convention as
// TestSender_WithSink_DelegationOrder).
func TestPrimaryBridge_StartRebuildSessionWithSink_InvokesSinkLifecycle(t *testing.T) {
	const numBlocks = 32
	const blockSize = 4096

	primary := storage.NewBlockStore(numBlocks, blockSize)
	for lba := uint32(0); lba < 5; lba++ {
		_, _ = primary.Write(lba, bytes.Repeat([]byte{byte(lba)}, blockSize))
	}
	_, _ = primary.Sync()

	replica := storage.NewBlockStore(numBlocks, blockSize)
	coord := NewPeerShipCoordinator()

	var closeWG sync.WaitGroup
	closeWG.Add(1)
	bridge := NewPrimaryBridge(primary, coord, nil,
		func(_ ReplicaID, _ uint64, _ uint64, _ error) {
			closeWG.Done()
		})

	primaryConn, replicaConn := net.Pipe()
	defer primaryConn.Close()
	defer replicaConn.Close()

	receiver := NewReceiver(replica, replicaConn)
	var recvWG sync.WaitGroup
	recvWG.Add(1)
	go func() {
		defer recvWG.Done()
		_, _ = receiver.Run()
		_ = replicaConn.Close()
	}()

	// Drain DrainBacklog quickly via the recordingSink (it returns
	// immediately when drainBlocksUntil is nil).
	sink := newRecordingSink()

	_, _, primaryH := primary.Boundaries()
	if err := bridge.StartRebuildSessionWithSink(
		context.Background(), primaryConn, "r1", 42, 0, primaryH, sink,
	); err != nil {
		t.Fatalf("StartRebuildSessionWithSink: %v", err)
	}

	closeWG.Wait()
	recvWG.Wait()

	startCalls, drainCalls, endCalls, order := sink.snapshot()
	if startCalls != 1 {
		t.Errorf("StartSession calls=%d want 1", startCalls)
	}
	if drainCalls != 1 {
		t.Errorf("DrainBacklog calls=%d want 1", drainCalls)
	}
	if endCalls != 1 {
		t.Errorf("EndSession calls=%d want 1 (always-cleanup defer)", endCalls)
	}
	if len(order) < 3 {
		t.Fatalf("call log too short: %v", order)
	}
	want := []string{"StartSession", "DrainBacklog", "EndSession"}
	for i, w := range want {
		if order[i] != w {
			t.Errorf("call[%d]=%q want %q (full order=%v)", i, order[i], w, order)
		}
	}
}

// TestPrimaryBridge_StartRebuildSession_StillUsesBridgingSink —
// regression: the legacy entry point continues to construct a
// senderBacklogSink internally. Verifies P2c-B-1's NewSenderWithBacklogRelay
// path is still the default for callers that don't inject a sink.
func TestPrimaryBridge_StartRebuildSession_StillUsesBridgingSink(t *testing.T) {
	primary := storage.NewBlockStore(4, 4096)
	coord := NewPeerShipCoordinator()
	bridge := NewPrimaryBridge(primary, coord, nil, nil)

	prim, replicaEnd := net.Pipe()
	defer prim.Close()
	defer replicaEnd.Close()

	if err := bridge.StartRebuildSession(
		context.Background(), prim, "r1", 7, 0, 0,
	); err != nil {
		t.Fatalf("StartRebuildSession: %v", err)
	}

	bridge.mu.Lock()
	sender := bridge.senders["r1"]
	bridge.mu.Unlock()

	if sender == nil {
		t.Fatal("expected sender installed by StartRebuildSession")
	}
	if _, ok := sender.sink.(*senderBacklogSink); !ok {
		t.Errorf("StartRebuildSession installed sink type=%T, want *senderBacklogSink (bridging path)", sender.sink)
	}
}
