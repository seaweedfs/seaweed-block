package transport

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A rebuild makes the replica's image the primary's, so its epoch table must
// become the primary's rows cut at the rebuilt end, in the done step before
// the ack: then it reconciles in sync, and the rows of the history it had
// before are gone.
func TestRebuildDoneReplacesTheEpochTableWithThePrimarysRows(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)
	theirs, _ := LoadEpochTable("", "v1")
	_ = theirs.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}})
	listener.SetEpochTable(theirs)

	primary := storage.NewBlockStore(64, 4096)
	writeTestBlocks(primary, 3)
	own, _ := LoadEpochTable("", "v1")
	_ = own.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: 2}, {Epoch: 3, FirstLSN: 900}})

	exec := NewBlockExecutor(primary, listener.Addr())
	exec.SetEpochTable(own)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })
	if err := exec.StartRebuild("r1", 2, 3, 1, 200); err != nil {
		t.Fatal(err)
	}
	var end uint64
	select {
	case r := <-closeCh:
		if !r.Success {
			t.Fatalf("rebuild failed: %+v", r)
		}
		end = r.AchievedLSN
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for the rebuild")
	}
	assertDataMatch(t, "after rebuild", primary, replica, 3)
	want := RowsThrough(own.Rows(), end)
	got := theirs.Rows()
	if len(got) != len(want) {
		t.Fatalf("replica rows %v after a rebuild to %d; want the primary's rows cut there %v", got, end, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("replica rows %v; want %v", got, want)
		}
	}
	if row := theirs.LastDataRowAt(end); func() bool { _, ok := own.SharedEnd(row.Epoch, row.FirstLSN, end); return !ok }() {
		t.Fatalf("the rebuilt replica (epoch %d, end %d) does not reconcile in sync", theirs.LastDataEpochAt(end), end)
	}
}

func TestEpochRowsWire(t *testing.T) {
	rows := []EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 4, FirstLSN: 30}}
	got, err := DecodeEpochRows(EncodeEpochRows(rows))
	if err != nil || len(got) != 2 || got[1] != rows[1] {
		t.Fatalf("round trip %v %v", got, err)
	}
	if _, err := DecodeEpochRows(EncodeEpochRows(rows)[:19]); err == nil {
		t.Fatal("a truncated row list was accepted")
	}
	bad := EncodeEpochRows([]EpochRow{{Epoch: 4, FirstLSN: 30}, {Epoch: 1, FirstLSN: 1}})
	if _, err := DecodeEpochRows(bad); err == nil {
		t.Fatal("unordered rows were accepted")
	}
}

// A crash after the rebuilt store is synced and before the table is
// replaced (here: the replace fails): no ack, so the rebuild is not done and
// nothing counts the replica; the table it kept names its old history, which
// reconciles as diverged: rebuild again, never a false in-sync.
func TestRebuildDoneWithoutTheTableReplaceIsNotAcked(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)
	theirs, _ := LoadEpochTable(filepath.Join(t.TempDir(), "v1.epoch-table"), "v1")
	_ = theirs.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}})
	listener.SetEpochTable(theirs)

	primary := storage.NewBlockStore(64, 4096)
	writeTestBlocks(primary, 3)
	own, _ := LoadEpochTable("", "v1")
	_ = own.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: 2}})

	saved := floorFS
	t.Cleanup(func() { floorFS = saved })
	ops := osFloorOps
	ops.rename = func(string, string) error { return errors.New("injected: crash before the table replace") }
	floorFS = ops

	exec := NewBlockExecutor(primary, listener.Addr())
	exec.SetEpochTable(own)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })
	if err := exec.StartRebuild("r1", 2, 2, 1, 200); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-closeCh:
		if r.Success {
			t.Fatalf("a rebuild whose table replace failed was acknowledged: %+v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for the rebuild")
	}
	floorFS = saved
	if rows := theirs.Rows(); len(rows) != 1 || rows[0] != (EpochRow{Epoch: 1, FirstLSN: 1}) {
		t.Fatalf("replica rows %v; want the old rows kept", rows)
	}
	end, _ := replica.Sync()
	row := theirs.LastDataRowAt(end)
	if _, ok := own.SharedEnd(row.Epoch, row.FirstLSN, end); ok && end >= 2 {
		t.Fatalf("the stale table (epoch %d from %d, end %d) reconciles in sync; want diverged", row.Epoch, row.FirstLSN, end)
	}
}
