package transport

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A replica recovered through the dual lane becomes ack-eligible: its session
// takes the table's invalid mark under a token at SessionStart, and the final
// barrier carries the primary's rows, which make the table valid under that
// token once the data is synced. Before the barrier it reconciles as diverged.
func TestDualLaneRecoveryMakesTheEpochTableValid(t *testing.T) {
	const numBlocks, blockSize = 64, 4096
	primary := storage.NewBlockStore(numBlocks, blockSize)
	replica := storage.NewBlockStore(numBlocks, blockSize)
	for lba := uint32(0); lba < 20; lba++ {
		data := make([]byte, blockSize)
		data[0] = byte(0x80 | lba)
		_, _ = primary.Write(lba, data)
	}
	_, _ = primary.Sync()

	gate, err := NewReplicaListener("127.0.0.1:0", replica)
	if err != nil {
		t.Fatal(err)
	}
	theirs, _ := LoadEpochTable(filepath.Join(t.TempDir(), "v1.epoch-table"), "v1")
	_ = theirs.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}})
	gate.SetEpochTable(theirs)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bridge := recovery.NewReplicaBridgeWithGate(replica, gate)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); bridge.AcceptDualLaneLoop(ctx, ln) }()
	t.Cleanup(func() { cancel(); _ = ln.Close(); <-done })

	own, _ := LoadEpochTable("", "v1")
	_ = own.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: 3}})
	coord := recovery.NewPeerShipCoordinator()
	exec := NewBlockExecutorWithDualLane(primary, "127.0.0.1:0", ln.Addr().String(), coord, recovery.ReplicaID("r1"))
	exec.SetEpochTable(own)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })

	_, _, h := primary.Boundaries()
	if err := exec.StartRebuild("r1", 7, 2, 1, h); err != nil {
		t.Fatal(err)
	}
	var end uint64
	select {
	case r := <-closeCh:
		if !r.Success {
			t.Fatalf("dual-lane rebuild failed: %+v", r)
		}
		end = r.AchievedLSN
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for the dual-lane rebuild")
	}
	if theirs.Invalid() {
		t.Fatal("after a completed dual-lane recovery the table is still invalid: the replica would be rebuilt for ever")
	}
	want := RowsThrough(own.Rows(), end)
	if got := theirs.Rows(); len(got) != len(want) || got[len(got)-1] != want[len(want)-1] {
		t.Fatalf("replica rows %v after a dual-lane recovery to %d; want the primary's cut there %v", got, end, want)
	}
	row := theirs.LastDataRowAt(end)
	if _, ok := own.SharedEnd(row.Epoch, row.FirstLSN, end); !ok {
		t.Fatalf("the dual-lane-recovered replica (epoch %d from %d, end %d) does not reconcile in sync", row.Epoch, row.FirstLSN, end)
	}
}
