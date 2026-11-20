package transport

import (
	"errors"
	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"strings"
	"testing"
	"time"
)

type failedRebuildSnapshot struct{ storage.LogicalStorage }

func (*failedRebuildSnapshot) AllBlocks() (map[uint32][]byte, error) {
	return map[uint32][]byte{0: makeData(0x77)}, errors.New("snapshot read fault")
}
func TestRebuildSnapshotErrorNeverShipsPartialImage(t *testing.T) {
	replica := storage.NewBlockStore(64, 4096)
	listener, err := NewReplicaListener("127.0.0.1:0", replica)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	defer listener.Stop()
	primary := &failedRebuildSnapshot{storage.NewBlockStore(64, 4096)}
	exec := NewBlockExecutor(primary, listener.Addr())
	defer exec.Stop()
	closed := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closed <- r })
	before := replica.NextLSN()
	if err := exec.StartRebuild("r1", 1, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-closed:
		if got.Success || !strings.Contains(got.FailReason, "snapshot read fault") {
			t.Fatalf("snapshot error lost: %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot failure did not settle rebuild")
	}
	if replica.NextLSN() != before {
		t.Fatal("failed snapshot shipped a partial image")
	}
}
