package transport

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// G1 (fencing-gaps-todo.md): the replica's lineage floor lived only in memory.
// After a restart its floor was zero, so a deposed primary still able to reach
// it could ship at an older epoch and have the bytes applied (and count the
// ACK toward its quorum).

func g1Listener(t *testing.T, store storage.LogicalStorage, floorPath string) *ReplicaListener {
	t.Helper()
	ln, err := NewReplicaListenerWithFloor("127.0.0.1:0", store, floorPath)
	if err != nil {
		t.Fatalf("listener: %v", err)
	}
	ln.Serve()
	return ln
}

func g1Block(t *testing.T, store storage.LogicalStorage) []byte {
	t.Helper()
	b, err := store.Read(0)
	if err != nil {
		t.Fatalf("read LBA 0: %v", err)
	}
	return b
}

// TestG1_RestartedReplicaRefusesShipBelowItsAcknowledgedEpoch — the G1 red
// test as the doc describes it: a listener accepts a ship at epoch 2, is
// stopped and restarted on the same store (and the same floor file the
// production wiring uses), then a ship at epoch 1 arrives. It must be refused.
func TestG1_RestartedReplicaRefusesShipBelowItsAcknowledgedEpoch(t *testing.T) {
	store := storage.NewBlockStore(16, 4096)
	floorPath := filepath.Join(t.TempDir(), "vol1.replica-floor")

	first := g1Listener(t, store, floorPath)
	p2 := RecoveryLineage{SessionID: 1, Epoch: 2, EndpointVersion: 1, TargetLSN: 1}
	conn := g2Dial(t, first.Addr())
	if err := WriteMsg(conn, MsgShipEntry, EncodeShipEntry(ShipEntry{Lineage: p2, LBA: 0, LSN: 1, Data: bytes.Repeat([]byte{'2'}, 4096)})); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !bytes.Equal(g1Block(t, store), bytes.Repeat([]byte{'2'}, 4096)) {
		if time.Now().After(deadline) {
			t.Fatal("fixture: the epoch-2 ship was never applied")
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = conn.Close()
	first.Stop()

	second := g1Listener(t, store, floorPath) // the restarted replica
	t.Cleanup(second.Stop)
	p1 := RecoveryLineage{SessionID: 1, Epoch: 1, EndpointVersion: 1, TargetLSN: 1}
	refused := g2Refused(t, second.Addr(), MsgShipEntry, EncodeShipEntry(ShipEntry{Lineage: p1, LBA: 0, LSN: 2, Data: bytes.Repeat([]byte{'1'}, 4096)}))
	if got := g1Block(t, store); bytes.Equal(got, bytes.Repeat([]byte{'1'}, 4096)) {
		t.Fatal("G1: after a restart the replica applied an epoch-1 ship below the epoch 2 it had acknowledged")
	}
	if !refused {
		t.Fatal("G1: after a restart the replica kept an epoch-1 primary's connection open (mutation not refused)")
	}
}
