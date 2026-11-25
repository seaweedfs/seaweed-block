package transport

import (
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// promoteOver is the role adoption's table step: over an invalid table the
// table becomes exactly [(epoch, next)]; otherwise the epoch is noted.
func promoteOver(t *testing.T, tb *EpochTable, epoch, next uint64) {
	t.Helper()
	var err error
	if tb.Invalid() {
		err = tb.Replace([]EpochRow{{Epoch: epoch, FirstLSN: next}})
	} else {
		err = tb.Note(epoch, next)
	}
	if err != nil {
		t.Fatal(err)
	}
}

// codex03's schedule: a catch-up's data sync succeeds and its table replace
// fails. The data is durable; the table must not keep naming it epoch 1. After
// a reload and a forced promotion at epoch 3, an epoch-1 survivor whose 6..10
// may differ must reconcile as diverged.
func TestCatchUpWithAFailedTableReplaceLeavesItInvalid(t *testing.T) {
	primary, replica, listener := setupPrimaryReplica(t)
	path := filepath.Join(t.TempDir(), "v1.epoch-table")
	theirs, _ := LoadEpochTable(path, "v1")
	_ = theirs.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}})
	listener.SetEpochTable(theirs)
	for i := 0; i < 10; i++ {
		lsn, err := primary.Write(uint32(i), makeData(byte(i+1)))
		if err != nil {
			t.Fatal(err)
		}
		if i < 5 {
			if err := replica.ApplyEntry(uint32(i), makeData(byte(i+1)), lsn); err != nil {
				t.Fatal(err)
			}
		}
	}
	_, _ = primary.Sync()
	_, _ = replica.Sync()
	own, _ := LoadEpochTable("", "v1")
	_ = own.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: 6}})

	// The invalidation (first rename) succeeds; the final replace's fails.
	saved := floorFS
	t.Cleanup(func() { floorFS = saved })
	ops, renames := osFloorOps, 0
	ops.rename = func(from, to string) error {
		renames++
		if renames >= 2 {
			return errors.New("injected: the final table replace fails")
		}
		return osFloorOps.rename(from, to)
	}
	floorFS = ops

	exec := NewBlockExecutor(primary, listener.Addr())
	exec.SetEpochTable(own)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })
	if err := exec.StartCatchUp("r1", 5, 2, 1, 5, 10); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-closeCh:
		if r.Success {
			t.Fatalf("a catch-up whose table replace failed was acknowledged: %+v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for the catch-up")
	}
	floorFS = saved

	end, _ := replica.Sync()
	if end != 10 {
		t.Fatalf("replica end %d; want the catch-up's data durable at 10", end)
	}
	reloaded, err := LoadEpochTable(path, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if !reloaded.Invalid() || reloaded.LastDataRowAt(end) != (EpochRow{}) {
		t.Fatalf("reloaded table invalid=%t row=%v; want invalid with no last data row", reloaded.Invalid(), reloaded.LastDataRowAt(end))
	}
	if err := reloaded.TrimAbove(end + 1); err != nil {
		t.Fatal(err)
	}
	promoteOver(t, reloaded, 3, end+1)
	if rows := reloaded.Rows(); len(rows) != 1 || rows[0] != (EpochRow{Epoch: 3, FirstLSN: 11}) || reloaded.Invalid() {
		t.Fatalf("forced promotion over an invalid table: rows %v invalid %t; want exactly [(3, 11)]", rows, reloaded.Invalid())
	}
	if _, shared := reloaded.SharedEnd(1, 1, 10); shared {
		t.Fatal("an epoch-1 survivor ending at 10 reconciles as shared with the promoted node; want diverged")
	}
}

// A crash after a recovery lane started (its entries possibly synced) and
// before its final barrier: the table reloads invalid; the replica reports
// no data epoch, so it is diverged, and a live note is refused.
func TestRecoveryLaneCrashBeforeTheFinalBarrierLeavesTheTableInvalid(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)
	path := filepath.Join(t.TempDir(), "v1.epoch-table")
	theirs, _ := LoadEpochTable(path, "v1")
	_ = theirs.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}})
	listener.SetEpochTable(theirs)
	for i := 0; i < 5; i++ {
		_ = replica.ApplyEntry(uint32(i), makeData(byte(i+1)), uint64(i+1))
	}

	lineage := RecoveryLineage{SessionID: 9, Epoch: 2, EndpointVersion: 1, TargetLSN: 10}
	conn, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteMsg(conn, MsgRecoveryLaneStart, EncodeLineage(lineage)); err != nil {
		t.Fatal(err)
	}
	for lsn := uint64(6); lsn <= 8; lsn++ {
		if err := WriteMsg(conn, MsgShipEntry, EncodeShipEntry(ShipEntry{Lineage: lineage, LBA: uint32(lsn), LSN: lsn, Data: makeData(0xEE)})); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for !theirs.Invalid() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	_ = conn.Close() // the primary dies before the final barrier

	reloaded, err := LoadEpochTable(path, "v1")
	if err != nil {
		t.Fatal(err)
	}
	end, _ := replica.Sync()
	if !reloaded.Invalid() || reloaded.LastDataRowAt(end) != (EpochRow{}) {
		t.Fatalf("after a crash mid-recovery: invalid=%t row=%v; want invalid, no last data row", reloaded.Invalid(), reloaded.LastDataRowAt(end))
	}
	if err := reloaded.Note(2, end+1); err == nil {
		t.Fatal("a live note on an invalid table was accepted")
	}
	own, _ := LoadEpochTable("", "v1")
	_ = own.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: 6}})
	if _, shared := own.SharedEnd(0, 0, end); shared {
		t.Fatalf("an invalid replica with data to %d reconciles as shared; want diverged", end)
	}
}
