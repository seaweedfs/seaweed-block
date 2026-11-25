package replication

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

type heldAdmittedRecovery struct {
	*ReplicaApplyGate
	entered, release chan struct{}
	done             chan error
	closed           chan uint64
}

func (h *heldAdmittedRecovery) CloseSession(id uint64) {
	h.ReplicaApplyGate.CloseSession(id)
	if h.closed != nil {
		h.closed <- id
	}
}

func (h *heldAdmittedRecovery) ApplyRecovery(l transport.RecoveryLineage, lba uint32, data []byte, lsn uint64) error {
	if l.SessionID == 1 && lsn == 95 {
		close(h.entered)
		<-h.release
		err := h.ReplicaApplyGate.ApplyRecovery(l, lba, data, lsn)
		h.done <- err
		return err
	}
	return h.ReplicaApplyGate.ApplyRecovery(l, lba, data, lsn)
}

// An old frame can pass the listener's lineage check before a newer session
// advances that check, then reach the apply gate after the newer write.
func TestApplyGate_AlreadyAdmittedWireSessionNeverRegressesBytes(t *testing.T) {
	store := storage.NewBlockStore(64, blockSize)
	h := &heldAdmittedRecovery{ReplicaApplyGate: NewReplicaApplyGate(store), entered: make(chan struct{}), release: make(chan struct{}), done: make(chan error, 1), closed: make(chan uint64, 8)}
	listener, err := transport.NewReplicaListenerWithApplyHook("127.0.0.1:0", store, h)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	var once sync.Once
	release := func() { once.Do(func() { close(h.release) }) }
	t.Cleanup(func() { release(); listener.Stop() })
	dial := func() net.Conn {
		c, err := net.DialTimeout("tcp", listener.Addr(), 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		if err := c.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		return c
	}
	send := func(c net.Conn, kind byte, payload []byte) {
		if err := transport.WriteMsg(c, kind, payload); err != nil {
			t.Fatal(err)
		}
	}
	barrier := func(c net.Conn, l transport.RecoveryLineage) {
		send(c, transport.MsgBarrierReq, transport.EncodeLineage(l))
		kind, _, err := transport.ReadMsg(c)
		if err != nil || kind != transport.MsgBarrierResp {
			t.Fatalf("barrier: kind=%d err=%v", kind, err)
		}
	}
	old, newer := recoveryLineage(1, 120), recoveryLineage(2, 120)
	c1, c2 := dial(), dial()
	send(c1, transport.MsgRecoveryLaneStart, transport.EncodeRecoveryLaneStart(old, 0))
	send(c1, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: old, LBA: 0, Data: makeData(1), LSN: 1}))
	barrier(c1, old) // initialize the old session before the competing apply
	send(c1, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: old, LBA: 5, Data: makeData(0xAA), LSN: 95}))
	select {
	case <-h.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("old frame never reached post-admission hook")
	}
	send(c2, transport.MsgRecoveryLaneStart, transport.EncodeRecoveryLaneStart(newer, 0))
	want := bytes.Repeat([]byte{0xBB}, blockSize)
	send(c2, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: newer, LBA: 5, Data: want, LSN: 120}))
	barrier(c2, newer)
	if _, _, _, found := h.SessionState(old.SessionID); !found {
		t.Fatal("admitted old frame lost coverage before settlement")
	}
	release()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("admitted old frame did not settle")
	}
	select {
	case id := <-h.closed:
		if id != old.SessionID {
			t.Fatalf("retired session=%d want old%d", id, old.SessionID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("settled admitted old session was not retired")
	}
	if _, _, _, found := h.SessionState(old.SessionID); found {
		t.Fatal("settled old coverage survived retirement")
	}
	got, err := store.Read(5)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("admitted old wire session regressed bytes: err=%v", err)
	}
}
