package transport

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The engine publishes a recovered replica healthy only after a durable
// ack observed after its session closed. The legacy catch-up and
// rebuild sessions must send that ack, as the dual-lane close does;
// otherwise a legacy-mode replica stays recovering after every
// successful recovery.

type closeOrAck struct {
	close *adapter.SessionCloseResult
	ack   *adapter.DurableAckResult
}

func recordCloseAndAck(exec *BlockExecutor) (<-chan adapter.SessionStartResult, <-chan closeOrAck) {
	startCh := make(chan adapter.SessionStartResult, 1)
	evCh := make(chan closeOrAck, 4)
	exec.SetOnSessionStart(func(r adapter.SessionStartResult) { startCh <- r })
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { evCh <- closeOrAck{close: &r} })
	exec.SetOnDurableAck(func(r adapter.DurableAckResult) { evCh <- closeOrAck{ack: &r} })
	return startCh, evCh
}

func nextEvent(t *testing.T, evCh <-chan closeOrAck, what string) closeOrAck {
	t.Helper()
	select {
	case ev := <-evCh:
		return ev
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
	}
	return closeOrAck{}
}

// checkCloseThenAck: a successful close is followed by exactly one
// durable ack carrying the session's lineage, the barrier-achieved LSN
// and the primary's boundaries.
func checkCloseThenAck(t *testing.T, evCh <-chan closeOrAck, sessionID uint64, primary *storage.BlockStore) {
	t.Helper()
	first := nextEvent(t, evCh, "session close")
	if first.close == nil {
		t.Fatalf("first event is not the session close: %+v", first.ack)
	}
	if !first.close.Success {
		t.Fatalf("session failed: %s", first.close.FailReason)
	}
	second := nextEvent(t, evCh, "post-close durable ack")
	if second.ack == nil {
		t.Fatalf("second event is not a durable ack: %+v", second.close)
	}
	_, pS, pH := primary.Boundaries()
	want := adapter.DurableAckResult{
		ReplicaID:       "r1",
		SessionID:       sessionID,
		EndpointVersion: 1, // the session's lineage (started at epoch 1, EV 1)
		TransportEpoch:  1,
		DurableLSN:      first.close.AchievedLSN,
		PrimaryTailLSN:  pS,
		PrimaryHeadLSN:  pH,
	}
	if *second.ack != want {
		t.Fatalf("post-close ack = %+v, want %+v", *second.ack, want)
	}
	select {
	case ev := <-evCh:
		t.Fatalf("unexpected extra event: close=%+v ack=%+v", ev.close, ev.ack)
	case <-time.After(200 * time.Millisecond):
	}
}

func waitStart(t *testing.T, startCh <-chan adapter.SessionStartResult) {
	t.Helper()
	select {
	case <-startCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for start")
	}
}

func TestLegacyCatchUpClose_EmitsPostCloseDurableAck(t *testing.T) {
	exec, primary, _ := newCatchUpExecutor(t)
	writeTestBlocks(primary, 5)
	_, _, pH := primary.Boundaries()
	startCh, evCh := recordCloseAndAck(exec)
	if err := exec.StartCatchUp("r1", 1, 1, 1, 1, pH); err != nil {
		t.Fatal(err)
	}
	waitStart(t, startCh)
	checkCloseThenAck(t, evCh, 1, primary)
}

func TestLegacyRebuildClose_EmitsPostCloseDurableAck(t *testing.T) {
	exec, primary, _ := newCatchUpExecutor(t)
	writeTestBlocks(primary, 5)
	_, _, pH := primary.Boundaries()
	startCh, evCh := recordCloseAndAck(exec)
	if err := exec.StartRebuild("r1", 7, 1, 1, pH); err != nil {
		t.Fatal(err)
	}
	waitStart(t, startCh)
	checkCloseThenAck(t, evCh, 7, primary)
}

// A failed session proves nothing durable: no ack.
func TestLegacyFailedClose_EmitsNoDurableAck(t *testing.T) {
	primary := &recycledStubStore{BlockStore: storage.NewBlockStore(64, 4096)}
	primary.Write(0, make([]byte, 4096))
	_, _, listener := setupPrimaryReplica(t)
	exec := NewBlockExecutor(primary, listener.Addr())
	startCh, evCh := recordCloseAndAck(exec)
	if err := exec.StartCatchUp("r1", 1, 1, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	waitStart(t, startCh)
	first := nextEvent(t, evCh, "session close")
	if first.close == nil || first.close.Success {
		t.Fatalf("want a failed close first, got close=%+v ack=%+v", first.close, first.ack)
	}
	select {
	case ev := <-evCh:
		t.Fatalf("failed session sent an event after its close: close=%+v ack=%+v", ev.close, ev.ack)
	case <-time.After(300 * time.Millisecond):
	}
}
