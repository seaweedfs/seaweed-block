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
	// onRelease, if set, runs at each release with mu held.
	onRelease func()
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
		if g.onRelease != nil {
			g.onRelease()
		}
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

// syncCounting counts Sync calls, so a gate release can see whether the
// barrier's effects ran inside its section.
type syncCounting struct {
	storage.LogicalStorage
	mu    sync.Mutex
	syncs int
	fail  bool
}

func (s *syncCounting) Sync() (uint64, error) {
	s.mu.Lock()
	s.syncs++
	fail := s.fail
	s.mu.Unlock()
	if fail {
		return 0, errors.New("sync failed")
	}
	return s.LogicalStorage.Sync()
}

func (s *syncCounting) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncs
}

// The barrier's section covers its effects (the sync and the completion check)
// and ends before the reply, so a fence never waits on a peer that does not
// read. net.Pipe writes block until read; the test does not read the reply.
func TestReceiverGate_BarrierSectionEndsBeforeUnreadReply(t *testing.T) {
	inner := &basePreparingStore{LogicalStorage: storage.NewBlockStore(0, 4096)}
	store := &syncCounting{LogicalStorage: inner}
	gate := &recordingGate{store: store, admit: 10}
	var syncsAtRelease []int
	gate.onRelease = func() { syncsAtRelease = append(syncsAtRelease, store.count()) }
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
	before := store.count() // syncs so far, e.g. for the BaseDone ack
	if err := writeFrame(conn, frameBarrierReq, nil); err != nil {
		t.Fatalf("write BarrierReq: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		gate.mu.Lock()
		n := len(gate.atRel)
		gate.mu.Unlock()
		if n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("barrier still holds its section while its reply is unread (%d releases, want 3)", n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if last := syncsAtRelease[len(syncsAtRelease)-1]; last <= before {
		t.Fatalf("barrier released its section before its own sync ran (%d syncs before the barrier, %d at its release)", before, last)
	}
}

// A failed barrier sync still ends its section, so a fence is not left waiting.
func TestReceiverGate_BarrierSyncErrorReleases(t *testing.T) {
	inner := &basePreparingStore{LogicalStorage: storage.NewBlockStore(0, 4096)}
	store := &syncCounting{LogicalStorage: inner}
	gate := &recordingGate{store: store, admit: 10}
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
	store.mu.Lock()
	store.fail = true
	store.mu.Unlock()
	if err := writeFrame(conn, frameBarrierReq, nil); err != nil {
		t.Fatalf("write BarrierReq: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("receiver ended without an error after a failed sync")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("receiver did not end after a failed sync")
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.entered != len(gate.atRel) {
		t.Fatalf("a failed barrier left its section open: %d entered, %d released", gate.entered, len(gate.atRel))
	}
}
