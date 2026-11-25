package transport

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
)

// A catch-up ships history under the session's epoch; the history's epochs
// are the primary's. Primary rows [(1,1),(2,11)], durable end 10, a replica
// holding the same first five entries: after a catch-up at epoch 2 the
// replica's rows are the primary's cut at 10, [(1,1)], not [(1,1),(2,6)],
// so after the next promotion it still reconciles with an identical survivor
// of epoch 1 ending at 10.
func TestCatchUpKeepsTheHistorysEpochs(t *testing.T) {
	primary, replica, listener := setupPrimaryReplica(t)
	theirs, _ := LoadEpochTable("", "v1")
	_ = theirs.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}})
	listener.SetEpochTable(theirs)
	for i := 0; i < 10; i++ {
		data := make([]byte, 4096)
		data[0] = byte(i + 1)
		lsn, err := primary.Write(uint32(i), data)
		if err != nil {
			t.Fatal(err)
		}
		if i < 5 {
			if err := replica.ApplyEntry(uint32(i), data, lsn); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := primary.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.Sync(); err != nil {
		t.Fatal(err)
	}
	own, _ := LoadEpochTable("", "v1")
	_ = own.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}, {Epoch: 2, FirstLSN: 11}})

	exec := NewBlockExecutor(primary, listener.Addr())
	exec.SetEpochTable(own)
	closeCh := make(chan adapter.SessionCloseResult, 1)
	exec.SetOnSessionClose(func(r adapter.SessionCloseResult) { closeCh <- r })
	if err := exec.StartCatchUp("r1", 5, 2, 1, 5, 10); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-closeCh:
		if !r.Success {
			t.Fatalf("catch-up failed: %+v", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for the catch-up")
	}
	assertDataMatch(t, "after catch-up", primary, replica, 10)
	if rows := theirs.Rows(); len(rows) != 1 || rows[0] != (EpochRow{Epoch: 1, FirstLSN: 1}) {
		t.Fatalf("replica rows after catch-up %v; want the primary's cut at 10: [{1 1}]", rows)
	}
	// The next promotion: epoch 3 from 11 on this node; the former primary is
	// byte-identical at epoch 1, end 10.
	_ = theirs.Note(3, 11)
	if _, shared := theirs.SharedEnd(1, 1, 10); !shared {
		t.Fatalf("an identical survivor (epoch 1, end 10) diverges from rows %v", theirs.Rows())
	}
}
