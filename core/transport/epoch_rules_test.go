package transport

import (
	"errors"
	"net"
	"testing"
	"time"

)

// Rule 3': a catch-up completes only if the table was valid when it took the
// mark; one that took over another recovery's mark cannot vouch for the store,
// and only a full base install (rebuild) makes the table valid again.
func TestEpochTable_ACatchUpOverAnInvalidTableNeedsARebuild(t *testing.T) {
	tb, _ := LoadEpochTable("", "v1")
	_ = tb.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}})
	clean, _ := tb.Invalidate() // over a valid table
	if err := tb.CompleteRecovery([]EpochRow{{Epoch: 1, FirstLSN: 1}}, clean, false); err != nil {
		t.Fatalf("a catch-up that took the mark over a valid table: %v", err)
	}
	_, _ = tb.Invalidate()            // recovery A
	b, _ := tb.Invalidate()           // B takes over A's mark
	if err := tb.CompleteRecovery([]EpochRow{{Epoch: 1, FirstLSN: 1}}, b, false); !errors.Is(err, ErrCatchUpOverInvalid) {
		t.Fatalf("a catch-up over an invalid table: %v; want ErrCatchUpOverInvalid", err)
	}
	if !tb.Invalid() {
		t.Fatal("the refused catch-up made the table valid")
	}
	if err := tb.CompleteRecovery([]EpochRow{{Epoch: 1, FirstLSN: 1}}, b, true); err != nil {
		t.Fatalf("a rebuild over an invalid table: %v", err)
	}
}

// Rule 4: the catch-up's lane start names the LSN it starts after; the replica
// takes the mark only if its durable end is exactly that LSN.
func TestCatchUpStartRefusedUnlessTheDurableEndMatches(t *testing.T) {
	for _, c := range []struct {
		name string
		from uint64
		ok   bool
	}{{"matches", 5, true}, {"replica is ahead (a tail)", 4, false}, {"replica is behind", 7, false}} {
		t.Run(c.name, func(t *testing.T) {
			_, replica, listener := setupPrimaryReplica(t)
			for i := 0; i < 5; i++ {
				_ = replica.ApplyEntry(uint32(i), makeData(byte(i+1)), uint64(i+1))
			}
			_, _ = replica.Sync()
			tb, _ := LoadEpochTable("", "v1")
			_ = tb.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}})
			listener.SetEpochTable(tb)
			conn, err := net.Dial("tcp", listener.Addr())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			var from [8]byte
			for i := 0; i < 8; i++ {
				from[7-i] = byte(c.from >> (8 * i))
			}
			start := append(EncodeLineage(RecoveryLineage{SessionID: 3, Epoch: 2, EndpointVersion: 1, TargetLSN: 9}), from[:]...)
			if err := WriteMsg(conn, MsgRecoveryLaneStart, start); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(time.Second)
			for c.ok && !tb.Invalid() && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if !c.ok {
				time.Sleep(100 * time.Millisecond)
			}
			if tb.Invalid() != c.ok {
				t.Fatalf("lane start after %d over a durable end of 5: took the mark %t; want %t", c.from, tb.Invalid(), c.ok)
			}
		})
	}
}

// The live-lane receiver check: a replica holding an old-epoch tail (end 6)
// gets a live entry of a new epoch at LSN 5, sent without a reconciliation.
// The frame is refused and the table unchanged; at the next LSN it is noted.
func TestLiveEntryOfANewEpochOnlyAtTheNextLSN(t *testing.T) {
	_, replica, listener := setupPrimaryReplica(t)
	for i := 0; i < 6; i++ {
		_ = replica.ApplyEntry(uint32(i), makeData(byte(i+1)), uint64(i+1))
	}
	tb, _ := LoadEpochTable("", "v1")
	_ = tb.Replace([]EpochRow{{Epoch: 1, FirstLSN: 1}})
	listener.SetEpochTable(tb)
	if err := listener.AdmitAsReplica(2); err != nil {
		t.Fatal(err)
	}
	live := func(session, lsn uint64) {
		conn, err := net.Dial("tcp", listener.Addr())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		l := RecoveryLineage{SessionID: session, Epoch: 2, EndpointVersion: 1, TargetLSN: lsn}
		if err := WriteMsg(conn, MsgShipEntry, EncodeShipEntry(ShipEntry{Lineage: l, LBA: 9, LSN: lsn, Data: makeData(0x22)})); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	live(4, 5)
	if rows := tb.Rows(); len(rows) != 1 {
		t.Fatalf("a live epoch-2 entry at 5 over an epoch-1 end of 6 was noted: rows %v", rows)
	}
	if got, _ := replica.Read(9); got[0] == 0x22 {
		t.Fatal("the refused live entry was applied")
	}
	live(5, 7) // the primary reconnects with a new session after reconciling
	if rows := tb.Rows(); len(rows) != 2 || rows[1] != (EpochRow{Epoch: 2, FirstLSN: 7}) {
		t.Fatalf("a live epoch-2 entry at the next LSN 7: rows %v; want [(1,1) (2,7)]", rows)
	}
}
