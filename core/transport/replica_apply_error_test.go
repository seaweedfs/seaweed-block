package transport

import (
	"errors"
	"io"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// failingApplyStore refuses ApplyEntry for one LSN, the way walstore does when
// its WAL is full, applies every other entry, and records every call.
type failingApplyStore struct {
	storage.LogicalStorage
	failLSN uint64
	failed  chan uint64 // receives failLSN once its ApplyEntry has failed

	mu    sync.Mutex
	calls []uint64 // every LSN ApplyEntry was called with, in order
}

func (s *failingApplyStore) ApplyEntry(lba uint32, data []byte, lsn uint64) error {
	s.mu.Lock()
	s.calls = append(s.calls, lsn)
	s.mu.Unlock()
	if lsn == s.failLSN {
		s.failed <- lsn
		return errors.New("test: WAL full")
	}
	return s.LogicalStorage.ApplyEntry(lba, data, lsn)
}

func (s *failingApplyStore) applyCalls() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.calls...)
}

// closedByPeer reports whether err is the peer closing or resetting the
// connection, not a timeout or a local error.
func closedByPeer(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
}

// A replica whose ApplyEntry fails for LSN 2 must close that connection: the
// failure is observed in the store, LSN 3 sent after it never reaches
// ApplyEntry, the connection ends with EOF or a reset (not a timeout), and a
// barrier on a fresh connection reports exactly 1. Before the fix, the
// production (no-hook) listener only logged the failure, applied LSN 3 and
// acked the barrier on the same connection with AchievedLSN 3.
func TestReplicaApplyFailureStopsBarrierAtTheGap(t *testing.T) {
	store := &failingApplyStore{LogicalStorage: storage.NewBlockStore(64, 4096), failLSN: 2, failed: make(chan uint64, 1)}
	listener, err := NewReplicaListener("127.0.0.1:0", store) // production no-hook path
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.Stop)
	lineage := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}

	conn := g2Dial(t, listener.Addr())
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	for lsn := uint64(1); lsn <= 2; lsn++ {
		if err := WriteMsg(conn, MsgShipEntry, g2ShipPayload(lineage, lsn)); err != nil {
			t.Fatalf("ship LSN %d: %v", lsn, err)
		}
	}
	select {
	case lsn := <-store.failed:
		if lsn != 2 {
			t.Fatalf("injected failure at LSN %d, want 2", lsn)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ApplyEntry(2) was never called: the injected failure did not run")
	}

	// LSN 3 and a barrier after the failure. Either write may already fail
	// because the replica closed the connection; what must come back is the
	// close itself, never a barrier ack and never a timeout.
	_ = WriteMsg(conn, MsgShipEntry, g2ShipPayload(lineage, 3))
	_ = WriteMsg(conn, MsgBarrierReq, EncodeLineage(lineage))
	msgType, payload, err := ReadMsg(conn)
	switch {
	case err == nil && msgType == MsgBarrierResp:
		resp, derr := DecodeBarrierResp(payload)
		t.Fatalf("barrier on the failed connection was acked (AchievedLSN %d, decode err %v); the connection must close after the failed LSN 2", resp.AchievedLSN, derr)
	case err == nil:
		t.Fatalf("unexpected reply type 0x%02x on the failed connection", msgType)
	case errors.Is(err, os.ErrDeadlineExceeded):
		t.Fatal("the failed connection stayed open until the deadline; it must be closed by the replica")
	case !closedByPeer(err):
		t.Fatalf("the failed connection ended with %v, want EOF or a reset from the replica", err)
	}

	// A fresh connection, as the primary's next probe or barrier would use,
	// must stay below the gap: nothing after the failed entry was applied.
	fresh := g2Dial(t, listener.Addr())
	_ = fresh.SetDeadline(time.Now().Add(5 * time.Second))
	if err := WriteMsg(fresh, MsgBarrierReq, EncodeLineage(lineage)); err != nil {
		t.Fatal(err)
	}
	msgType, payload, err = ReadMsg(fresh)
	if err != nil || msgType != MsgBarrierResp {
		t.Fatalf("barrier on a fresh connection: type=0x%02x err=%v", msgType, err)
	}
	resp, err := DecodeBarrierResp(payload)
	if err != nil {
		t.Fatal(err)
	}
	if resp.AchievedLSN != 1 {
		t.Fatalf("barrier on a fresh connection acked AchievedLSN %d, want 1 (the last LSN before the gap)", resp.AchievedLSN)
	}
	if got := store.applyCalls(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("ApplyEntry calls %v, want [1 2]: LSN 3 must never reach the store after the failed LSN 2", got)
	}
}

// Control: with no failing apply, the same sequence acks every shipped LSN on
// the same connection, so the test above fails only because of the gap.
func TestReplicaApplyNoFailureAcksAllShipped(t *testing.T) {
	store := &failingApplyStore{LogicalStorage: storage.NewBlockStore(64, 4096), failed: make(chan uint64, 1)} // failLSN 0: never fails
	listener, err := NewReplicaListener("127.0.0.1:0", store)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.Stop)
	lineage := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}

	conn := g2Dial(t, listener.Addr())
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	for lsn := uint64(1); lsn <= 3; lsn++ {
		if err := WriteMsg(conn, MsgShipEntry, g2ShipPayload(lineage, lsn)); err != nil {
			t.Fatal(err)
		}
	}
	if err := WriteMsg(conn, MsgBarrierReq, EncodeLineage(lineage)); err != nil {
		t.Fatal(err)
	}
	msgType, payload, err := ReadMsg(conn)
	if err != nil || msgType != MsgBarrierResp {
		t.Fatalf("barrier: type=0x%02x err=%v", msgType, err)
	}
	resp, err := DecodeBarrierResp(payload)
	if err != nil {
		t.Fatal(err)
	}
	if resp.AchievedLSN != 3 {
		t.Fatalf("barrier acked AchievedLSN %d, want 3", resp.AchievedLSN)
	}
	if got := store.applyCalls(); len(got) != 3 {
		t.Fatalf("ApplyEntry calls %v, want [1 2 3]", got)
	}
}
