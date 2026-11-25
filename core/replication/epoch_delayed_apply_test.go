package replication

import (
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// heldOldApply pauses recovery A's entry 7 inside the real apply gate's
// recovery path, after the listener admitted it.
type heldOldApply struct {
	*ReplicaApplyGate
	entered, release chan struct{}
	done             chan error
}

func (h *heldOldApply) ApplyRecovery(l transport.RecoveryLineage, lba uint32, data []byte, lsn uint64) error {
	if l.SessionID == 1 && lsn == 7 {
		close(h.entered)
		<-h.release
		err := h.ReplicaApplyGate.ApplyRecovery(l, lba, data, lsn)
		h.done <- err
		return err
	}
	return h.ReplicaApplyGate.ApplyRecovery(l, lba, data, lsn)
}

// Recovery A's epoch-2 entry 7 is admitted and held; recovery B (epoch 3)
// starts, ships entry 6 and asks to complete. B's start waits for A's apply
// to settle (the apply fence), A's entry lands under A's own mark, and B's
// completion then finds a synced end (7) beyond its own target (6): refused,
// the table stays invalid. It is never valid with epoch-2 bytes labelled 3.
func TestDelayedOldRecoveryApplyNeverLandsUnderANewValidTable(t *testing.T) {
	st := storage.NewBlockStore(16, 4096)
	for i := 0; i < 5; i++ {
		_, _ = st.Write(uint32(i), makeData(byte(i)))
	}
	_, _ = st.Sync()
	h := &heldOldApply{ReplicaApplyGate: NewReplicaApplyGate(st), entered: make(chan struct{}), release: make(chan struct{}), done: make(chan error, 1)}
	li, err := transport.NewReplicaListenerWithApplyHook("127.0.0.1:0", st, h)
	if err != nil {
		t.Fatal(err)
	}
	li.Serve()
	defer li.StopHard()
	tb, _ := transport.LoadEpochTable("", "v")
	_ = tb.Replace([]transport.EpochRow{{Epoch: 1, FirstLSN: 1}})
	li.SetEpochTable(tb)

	dial := func() net.Conn {
		c, err := net.Dial("tcp", li.Addr())
		if err != nil {
			t.Fatal(err)
		}
		_ = c.SetDeadline(time.Now().Add(4 * time.Second))
		t.Cleanup(func() { c.Close() })
		return c
	}
	send := func(c net.Conn, k byte, p []byte) {
		if err := transport.WriteMsg(c, k, p); err != nil {
			t.Fatal(err)
		}
	}
	a, b := dial(), dial()
	la := transport.RecoveryLineage{SessionID: 1, Epoch: 2, EndpointVersion: 1, TargetLSN: 7}
	lb := transport.RecoveryLineage{SessionID: 2, Epoch: 3, EndpointVersion: 1, TargetLSN: 6}
	send(a, transport.MsgRecoveryLaneStart, transport.EncodeLineage(la))
	send(a, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: la, LBA: 7, LSN: 7, Data: makeData(77)}))
	select {
	case <-h.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery A's entry was not admitted")
	}
	// B's start waits for A's in-flight apply: release A shortly after.
	go func() { time.Sleep(100 * time.Millisecond); close(h.release) }()
	send(b, transport.MsgRecoveryLaneStart, transport.EncodeLineage(lb))
	send(b, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: lb, LBA: 6, LSN: 6, Data: makeData(66)}))
	rows := []transport.EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 3, FirstLSN: 6}}
	send(b, transport.MsgBarrierReq, append(transport.EncodeLineage(lb), transport.EncodeEpochRows(rows)...))
	_, _, _ = transport.ReadMsg(b) // B's completion is refused: the connection closes
	select {
	case <-h.done:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery A's apply hung")
	}

	end, _ := st.Sync()
	data, _ := st.Read(7)
	r := tb.LastDataRowAt(end)
	if !tb.Invalid() && data[0] == 77 && r.Epoch == 3 {
		t.Fatalf("epoch-2 entry 7 lies under a valid table labelled epoch 3: end %d rows %v", end, tb.Rows())
	}
	if !tb.Invalid() {
		t.Fatalf("the table is valid (rows %v) although the store holds data beyond the completing recovery's own end", tb.Rows())
	}
}
