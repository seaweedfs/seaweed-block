package transport

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// G2 listener contract (fencing-gaps-todo.md): while the host is primary
// every replication mutation is refused, probes are still answered, and the
// epoch floor it set never goes down when it later supports another primary.

func g2Dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// g2Refused sends one mutating frame and reports whether the listener closed
// the connection (refusal) rather than leaving it open (accepted).
func g2Refused(t *testing.T, addr string, msgType byte, payload []byte) bool {
	t.Helper()
	conn := g2Dial(t, addr)
	if err := WriteMsg(conn, msgType, payload); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err := conn.Read(make([]byte, 1))
	return err != nil && !errors.Is(err, os.ErrDeadlineExceeded)
}

func g2ShipPayload(l RecoveryLineage, lsn uint64) []byte {
	return EncodeShipEntry(ShipEntry{Lineage: l, LBA: 0, LSN: lsn, Data: make([]byte, 4096)})
}

func TestG2_PrimaryListenerRefusesMutationsButAnswersProbes(t *testing.T) {
	_, _, listener := setupPrimaryReplica(t)
	listener.FenceAsPrimary(2)

	newer := RecoveryLineage{SessionID: 9, Epoch: 3, EndpointVersion: 1, TargetLSN: 1}
	if !g2Refused(t, listener.Addr(), MsgShipEntry, g2ShipPayload(newer, 1)) {
		t.Fatal("a primary applied an inbound ship (even at a higher epoch)")
	}
	if !g2Refused(t, listener.Addr(), MsgRecoveryLaneStart, EncodeLineage(newer)) {
		t.Fatal("a primary accepted a recovery lane start")
	}
	if !g2Refused(t, listener.Addr(), MsgRebuildBlock, EncodeRebuildBlock(newer, 0, make([]byte, 4096))) {
		t.Fatal("a primary accepted a rebuild block")
	}

	conn := g2Dial(t, listener.Addr())
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	probe := RecoveryLineage{SessionID: 4, Epoch: 2, EndpointVersion: 1, TargetLSN: 1}
	if err := WriteMsg(conn, MsgProbeReq, EncodeProbeReq(ProbeRequest{Lineage: probe})); err != nil {
		t.Fatal(err)
	}
	if msgType, _, err := ReadMsg(conn); err != nil || msgType != MsgProbeResp {
		t.Fatalf("a primary must still answer probes: type=0x%02x err=%v", msgType, err)
	}
}

func TestG2_FloorStaysWhenThePrimaryLaterSupportsAnother(t *testing.T) {
	_, _, listener := setupPrimaryReplica(t)
	listener.FenceAsPrimary(2)
	listener.AdmitAsReplica(3) // superseded, now supporting the epoch-3 primary

	if !g2Refused(t, listener.Addr(), MsgShipEntry, g2ShipPayload(RecoveryLineage{SessionID: 1, Epoch: 2, EndpointVersion: 1, TargetLSN: 1}, 1)) {
		t.Fatal("a ship below the floor (epoch 2 < 3) was accepted")
	}
	if g2Refused(t, listener.Addr(), MsgShipEntry, g2ShipPayload(RecoveryLineage{SessionID: 2, Epoch: 3, EndpointVersion: 1, TargetLSN: 1}, 1)) {
		t.Fatal("the epoch-3 primary's ship was refused by its supporting replica")
	}

	listener.AdmitAsReplica(1) // a stale, lower admission never lowers the floor
	if !g2Refused(t, listener.Addr(), MsgShipEntry, g2ShipPayload(RecoveryLineage{SessionID: 3, Epoch: 2, EndpointVersion: 1, TargetLSN: 1}, 2)) {
		t.Fatal("the floor went down after a lower admission")
	}
}

// pausingStore holds ApplyEntry inside the store until released, to put a
// ship frame "past the gate but not yet applied" at the moment of promotion
// (claude03's review control for #82).
type pausingStore struct {
	storage.LogicalStorage
	entered chan struct{}
	release chan struct{}
	applied chan time.Time
}

func (s *pausingStore) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	s.entered <- struct{}{}
	<-s.release
	err := s.LogicalStorage.ApplyEntry(lba, data, lsn)
	s.applied <- time.Now()
	return err
}

func g2PausedShip(t *testing.T) (*ReplicaListener, *pausingStore) {
	t.Helper()
	store := &pausingStore{
		LogicalStorage: storage.NewBlockStore(64, 4096),
		entered:        make(chan struct{}, 1),
		release:        make(chan struct{}),
		applied:        make(chan time.Time, 1),
	}
	listener, err := NewReplicaListener("127.0.0.1:0", store) // production no-hook path
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.Stop)
	conn := g2Dial(t, listener.Addr())
	old := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}
	if err := WriteMsg(conn, MsgShipEntry, g2ShipPayload(old, 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture: the epoch-1 ship never reached the store")
	}
	return listener, store
}

// An epoch-1 ship admitted just before promotion is inside ApplyEntry when
// FenceAsPrimary(2) is called. The fence must not return until that apply has
// finished: nothing from the deposed primary is applied after the host serves
// as primary (claude03 P2-1 on #82; before the fix it was applied ~18 µs after
// the fence returned).
func TestG2_FenceWaitsForAShipAlreadyPastTheGate(t *testing.T) {
	listener, store := g2PausedShip(t)
	type result struct {
		at  time.Time
		err error
	}
	fenced := make(chan result, 1)
	go func() {
		err := listener.FenceAsPrimary(2)
		fenced <- result{time.Now(), err}
	}()
	select {
	case r := <-fenced:
		close(store.release)
		t.Fatalf("FenceAsPrimary returned (err=%v) while an admitted epoch-1 ship was still applying", r.err)
	case <-time.After(300 * time.Millisecond):
	}
	close(store.release)
	appliedAt := <-store.applied
	select {
	case r := <-fenced:
		if r.err != nil {
			t.Fatalf("fence: %v", r.err)
		}
		if r.at.Before(appliedAt) {
			t.Fatalf("FenceAsPrimary returned %v before the in-flight apply finished", appliedAt.Sub(r.at))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("FenceAsPrimary never returned after the in-flight apply finished")
	}
}

// If an admitted mutation does not finish within the bound, FenceAsPrimary
// reports it (the host then refuses the primary role) instead of returning as
// if fenced; the listener stays fenced for new frames.
func TestG2_FenceReportsAMutationStuckPastTheBound(t *testing.T) {
	listener, store := g2PausedShip(t)
	defer close(store.release)
	listener.mu.Lock()
	listener.fenceDrainTimeout = 200 * time.Millisecond
	listener.mu.Unlock()
	if err := listener.FenceAsPrimary(2); err == nil {
		t.Fatal("FenceAsPrimary returned nil with a mutation still applying past the bound")
	}
	newer := RecoveryLineage{SessionID: 9, Epoch: 3, EndpointVersion: 1, TargetLSN: 1}
	if !g2Refused(t, listener.Addr(), MsgShipEntry, g2ShipPayload(newer, 2)) {
		t.Fatal("after a timed-out fence the listener accepted a new ship")
	}
}

// pausingExtentStore holds a rebuild base write (WriteExtentDirect) inside the
// store until released.
type pausingExtentStore struct {
	storage.LogicalStorage
	entered chan struct{}
	release chan struct{}
	written chan time.Time
}

func (s *pausingExtentStore) WriteExtentDirect(lba uint32, data []byte) error {
	s.entered <- struct{}{}
	<-s.release
	err := s.LogicalStorage.WriteExtentDirect(lba, data)
	s.written <- time.Now()
	return err
}

// The rebuild path has the same boundary as ship: a rebuild block admitted
// before promotion is inside its base write when FenceAsPrimary(2) is called.
// The fence must not return until that write has finished.
func TestG2_FenceWaitsForARebuildBlockAlreadyPastTheGate(t *testing.T) {
	store := &pausingExtentStore{
		LogicalStorage: storage.NewBlockStore(64, 4096),
		entered:        make(chan struct{}, 1),
		release:        make(chan struct{}),
		written:        make(chan time.Time, 1),
	}
	listener, err := NewReplicaListener("127.0.0.1:0", store)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.Stop)
	conn := g2Dial(t, listener.Addr())
	old := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}
	if err := WriteMsg(conn, MsgRebuildBlock, EncodeRebuildBlock(old, 0, make([]byte, 4096))); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture: the rebuild block never reached the store")
	}
	fenced := make(chan time.Time, 1)
	go func() {
		if err := listener.FenceAsPrimary(2); err != nil {
			t.Errorf("fence: %v", err)
		}
		fenced <- time.Now()
	}()
	select {
	case <-fenced:
		close(store.release)
		t.Fatal("FenceAsPrimary returned while an admitted rebuild block was still being written")
	case <-time.After(300 * time.Millisecond):
	}
	close(store.release)
	writtenAt := <-store.written
	select {
	case at := <-fenced:
		if at.Before(writtenAt) {
			t.Fatalf("FenceAsPrimary returned %v before the in-flight rebuild write finished", writtenAt.Sub(at))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("FenceAsPrimary never returned after the rebuild write finished")
	}
}

// pausingHook holds a live-lane apply inside the apply gate until released
// (the ship path used when an ApplyHook is installed).
type pausingHook struct {
	store   storage.LogicalStorage
	entered chan struct{}
	release chan struct{}
	applied chan time.Time
}

func (h *pausingHook) ApplyRecovery(l RecoveryLineage, lba uint32, data []byte, lsn uint64) error {
	return h.store.ApplyEntry(lba, data, lsn)
}

func (h *pausingHook) ApplyLive(l RecoveryLineage, lba uint32, data []byte, lsn uint64) error {
	h.entered <- struct{}{}
	<-h.release
	err := h.store.ApplyEntry(lba, data, lsn)
	h.applied <- time.Now()
	return err
}

// The ship path through an installed ApplyHook: the fence waits for a live
// apply already past the gate, as on the direct path.
func TestG2_FenceWaitsForAHookedShipAlreadyPastTheGate(t *testing.T) {
	hook := &pausingHook{store: storage.NewBlockStore(64, 4096), entered: make(chan struct{}, 1), release: make(chan struct{}), applied: make(chan time.Time, 1)}
	listener, err := NewReplicaListenerWithApplyHook("127.0.0.1:0", hook.store, hook)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.Stop)
	conn := g2Dial(t, listener.Addr())
	if err := WriteMsg(conn, MsgShipEntry, g2ShipPayload(RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}, 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-hook.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("fixture: the ship never reached the apply hook")
	}
	fenced := make(chan time.Time, 1)
	go func() {
		if err := listener.FenceAsPrimary(2); err != nil {
			t.Errorf("fence: %v", err)
		}
		fenced <- time.Now()
	}()
	select {
	case <-fenced:
		close(hook.release)
		t.Fatal("FenceAsPrimary returned while a hooked epoch-1 apply was still running")
	case <-time.After(300 * time.Millisecond):
	}
	close(hook.release)
	appliedAt := <-hook.applied
	select {
	case at := <-fenced:
		if at.Before(appliedAt) {
			t.Fatalf("FenceAsPrimary returned %v before the hooked apply finished", appliedAt.Sub(at))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("FenceAsPrimary never returned after the hooked apply finished")
	}
}
