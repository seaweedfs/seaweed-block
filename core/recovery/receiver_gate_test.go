package recovery

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// recordingGate admits the first `admit` entries and refuses the rest. At each
// release it records the store's H boundary, so a test can check what a
// counted section covered.
type recordingGate struct {
	mu      sync.Mutex
	store   storage.LogicalStorage
	admit   int
	entered int
	atRel   []uint64
}

func (g *recordingGate) EnterDualLane(epoch uint64) (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.entered >= g.admit {
		return nil, errors.New("fenced")
	}
	g.entered++
	return func() {
		_, _, h := g.store.Boundaries()
		g.mu.Lock()
		g.atRel = append(g.atRel, h)
		g.mu.Unlock()
	}, nil
}

func gatedReceiver(t *testing.T, gate *recordingGate) (net.Conn, chan error) {
	t.Helper()
	primaryConn, replicaConn := net.Pipe()
	t.Cleanup(func() { _ = primaryConn.Close(); _ = replicaConn.Close() })
	receiver := NewReceiver(gate.store, replicaConn)
	receiver.gate = gate
	done := make(chan error, 1)
	go func() {
		_, err := receiver.Run()
		_ = replicaConn.Close()
		done <- err
	}()
	return primaryConn, done
}

// The frontier advance of BaseDone is a mutation: it must happen inside the
// counted section, so a fence cannot return while it is still pending.
func TestReceiverGate_BaseDoneFrontierInsideCountedSection(t *testing.T) {
	const basePinLSN = 99
	store := &basePreparingStore{LogicalStorage: storage.NewBlockStore(0, 4096)}
	gate := &recordingGate{store: store, admit: 10}
	conn, _ := gatedReceiver(t, gate)
	if err := writeFrame(conn, frameSessionStart, encodeSessionStart(sessionStartPayload{SessionID: 1, FromLSN: basePinLSN, Epoch: 5})); err != nil {
		t.Fatalf("write SessionStart: %v", err)
	}
	if err := writeFrame(conn, frameBaseDone, nil); err != nil {
		t.Fatalf("write BaseDone: %v", err)
	}
	if ft, _, err := readFrame(conn); err != nil || ft != frameBaseBatchAck {
		t.Fatalf("read BaseBatchAck: type=%v err=%v", ft, err)
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if n := len(gate.atRel); n != 2 || gate.atRel[1] < basePinLSN {
		t.Fatalf("BaseDone released its section before the frontier advance: H at each release %v, want the last >= %d", gate.atRel, basePinLSN)
	}
}

// A barrier completes the rebuild: a fenced replica must not answer it.
func TestReceiverGate_BarrierRefusedAfterFence(t *testing.T) {
	store := &basePreparingStore{LogicalStorage: storage.NewBlockStore(0, 4096)}
	gate := &recordingGate{store: store, admit: 2} // SessionStart and BaseDone, then fenced
	conn, done := gatedReceiver(t, gate)
	if err := writeFrame(conn, frameSessionStart, encodeSessionStart(sessionStartPayload{SessionID: 1, FromLSN: 7, Epoch: 5})); err != nil {
		t.Fatalf("write SessionStart: %v", err)
	}
	if err := writeFrame(conn, frameBaseDone, nil); err != nil {
		t.Fatalf("write BaseDone: %v", err)
	}
	if ft, _, err := readFrame(conn); err != nil || ft != frameBaseBatchAck {
		t.Fatalf("read BaseBatchAck: type=%v err=%v", ft, err)
	}
	if err := writeFrame(conn, frameBarrierReq, nil); err != nil {
		t.Fatalf("write BarrierReq: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if ft, _, err := readFrame(conn); err == nil && ft == frameBarrierResp {
		t.Fatalf("a fenced replica answered BarrierReq")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("receiver ended without an error after a refused barrier")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("receiver did not end after a refused barrier")
	}
}

// The barrier's section holds until its response is written, so a fence
// cannot slip between the check and the reply. net.Pipe writes block until
// read, so the release cannot happen before the test reads the response.
func TestReceiverGate_BarrierSectionHoldsUntilResponse(t *testing.T) {
	store := &basePreparingStore{LogicalStorage: storage.NewBlockStore(0, 4096)}
	gate := &recordingGate{store: store, admit: 10}
	conn, _ := gatedReceiver(t, gate)
	if err := writeFrame(conn, frameSessionStart, encodeSessionStart(sessionStartPayload{SessionID: 1, FromLSN: 7, Epoch: 5})); err != nil {
		t.Fatalf("write SessionStart: %v", err)
	}
	if err := writeFrame(conn, frameBaseDone, nil); err != nil {
		t.Fatalf("write BaseDone: %v", err)
	}
	if ft, _, err := readFrame(conn); err != nil || ft != frameBaseBatchAck {
		t.Fatalf("read BaseBatchAck: type=%v err=%v", ft, err)
	}
	if err := writeFrame(conn, frameBarrierReq, nil); err != nil {
		t.Fatalf("write BarrierReq: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	gate.mu.Lock()
	early := len(gate.atRel)
	gate.mu.Unlock()
	if early != 2 {
		t.Fatalf("barrier released its section before its response was read (%d releases, want 2)", early)
	}
	if ft, _, err := readFrame(conn); err != nil || ft != frameBarrierResp {
		t.Fatalf("read BarrierResp: type=%v err=%v", ft, err)
	}
}
