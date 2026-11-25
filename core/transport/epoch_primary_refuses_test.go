package transport

import (
	"net"
	"path/filepath"
	"testing"
	"time"
)

// After the node takes the primary role, inbound recovery frames are refused:
// a catch-up's lane start does not reach the epoch table (no invalidation),
// and a dual-lane session cannot enter. The table the adoption wrote stays.
func TestPrimaryRefusesInboundRecoveryFramesOverItsTable(t *testing.T) {
	_, _, listener := setupPrimaryReplica(t)
	tb, _ := LoadEpochTable(filepath.Join(t.TempDir(), "v1.epoch-table"), "v1")
	_ = tb.Replace([]EpochRow{{Epoch: 3, FirstLSN: 11}})
	listener.SetEpochTable(tb)
	if err := listener.FenceAsPrimary(3); err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", listener.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := WriteMsg(conn, MsgRecoveryLaneStart, EncodeLineage(RecoveryLineage{SessionID: 9, Epoch: 3, EndpointVersion: 1, TargetLSN: 20})); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, _ = ReadMsg(conn) // the replica closes the connection
	if tb.Invalid() || len(tb.Rows()) != 1 {
		t.Fatalf("a recovery frame reached a primary's table: invalid %t rows %v", tb.Invalid(), tb.Rows())
	}
	if _, err := listener.EnterDualLane(3); err == nil {
		t.Fatal("a dual-lane session entered a primary")
	}
}
