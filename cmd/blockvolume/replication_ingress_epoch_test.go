package main

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// Role adoption's table step, on the real ingress: a durable table is
// reloaded (trimmed above the recovered next LSN), a primary role notes its
// epoch at the next LSN, and a promotion over a table a recovery left
// invalid makes it exactly [(epoch, next LSN)].
func TestIngressPrimaryAdoptionTableStep(t *testing.T) {
	root := t.TempDir()
	f := flags{dataAddr: "127.0.0.1:0", volumeID: "vol1", durableRoot: root}
	store := storage.NewBlockStore(16, 4096)
	for i := 0; i < 4; i++ {
		if _, err := store.Write(uint32(i), make([]byte, 4096)); err != nil {
			t.Fatal(err)
		}
	}
	tb, err := transport.LoadEpochTable(epochTablePath(f), f.volumeID)
	if err != nil {
		t.Fatal(err)
	}
	_ = tb.Replace([]transport.EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: 9}}) // 9: above the store's next LSN (5)

	r := newReplicationIngress(f, store)
	t.Cleanup(r.Stop)
	if rows := r.EpochTable().Rows(); len(rows) != 1 {
		t.Fatalf("rows after the load trim %v; want [(1,1)] (the row at 9 names a lost tail)", rows)
	}
	if err := r.FenceAsPrimaryWithPeers(2, false); err != nil {
		t.Fatal(err)
	}
	if rows := r.EpochTable().Rows(); len(rows) != 2 || rows[1] != (transport.EpochRow{Epoch: 2, FirstLSN: 5}) {
		t.Fatalf("rows after adoption at epoch 2 %v; want [(1,1) (2,5)]", rows)
	}

	if err := r.EpochTable().Invalidate(); err != nil { // a recovery left it without a history
		t.Fatal(err)
	}
	if err := r.FenceAsPrimaryWithPeers(3, false); err != nil {
		t.Fatalf("a forced promotion over an invalid table was refused: %v", err)
	}
	if rows := r.EpochTable().Rows(); len(rows) != 1 || rows[0] != (transport.EpochRow{Epoch: 3, FirstLSN: 5}) || r.EpochTable().Invalid() {
		t.Fatalf("rows after a promotion over an invalid table %v (invalid %t); want exactly [(3,5)]", rows, r.EpochTable().Invalid())
	}
}
