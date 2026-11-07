package transport

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// G2 listener contract (fencing-gaps-todo.md): while the host is primary
// every replication mutation is refused, probes are still answered, and the
// epoch floor it set never goes down when it later supports another primary.

func g2Dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// g2Refused sends one mutating frame and reports whether the listener closed
// the connection (refusal) rather than leaving it open (accepted).
func g2Refused(t *testing.T, addr string, msgType byte, payload []byte) bool {
	t.Helper()
	conn := g2Dial(t, addr)
	if err := WriteMsg(conn, msgType, payload); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err := conn.Read(make([]byte, 1))
	return err != nil && !errors.Is(err, os.ErrDeadlineExceeded)
}

func g2ShipPayload(l RecoveryLineage, lsn uint64) []byte {
	return EncodeShipEntry(ShipEntry{Lineage: l, LBA: 0, LSN: lsn, Data: make([]byte, 4096)})
}

func TestG2_PrimaryListenerRefusesMutationsButAnswersProbes(t *testing.T) {
	_, _, listener := setupPrimaryReplica(t)
	listener.FenceAsPrimary(2)

	newer := RecoveryLineage{SessionID: 9, Epoch: 3, EndpointVersion: 1, TargetLSN: 1}
	if !g2Refused(t, listener.Addr(), MsgShipEntry, g2ShipPayload(newer, 1)) {
		t.Fatal("a primary applied an inbound ship (even at a higher epoch)")
	}
	if !g2Refused(t, listener.Addr(), MsgRecoveryLaneStart, EncodeLineage(newer)) {
		t.Fatal("a primary accepted a recovery lane start")
	}
	if !g2Refused(t, listener.Addr(), MsgRebuildBlock, EncodeRebuildBlock(newer, 0, make([]byte, 4096))) {
		t.Fatal("a primary accepted a rebuild block")
	}

	conn := g2Dial(t, listener.Addr())
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	probe := RecoveryLineage{SessionID: 4, Epoch: 2, EndpointVersion: 1, TargetLSN: 1}
	if err := WriteMsg(conn, MsgProbeReq, EncodeProbeReq(ProbeRequest{Lineage: probe})); err != nil {
		t.Fatal(err)
	}
	if msgType, _, err := ReadMsg(conn); err != nil || msgType != MsgProbeResp {
		t.Fatalf("a primary must still answer probes: type=0x%02x err=%v", msgType, err)
	}
}

func TestG2_FloorStaysWhenThePrimaryLaterSupportsAnother(t *testing.T) {
	_, _, listener := setupPrimaryReplica(t)
	listener.FenceAsPrimary(2)
	listener.AdmitAsReplica(3) // superseded, now supporting the epoch-3 primary

	if !g2Refused(t, listener.Addr(), MsgShipEntry, g2ShipPayload(RecoveryLineage{SessionID: 1, Epoch: 2, EndpointVersion: 1, TargetLSN: 1}, 1)) {
		t.Fatal("a ship below the floor (epoch 2 < 3) was accepted")
	}
	if g2Refused(t, listener.Addr(), MsgShipEntry, g2ShipPayload(RecoveryLineage{SessionID: 2, Epoch: 3, EndpointVersion: 1, TargetLSN: 1}, 1)) {
		t.Fatal("the epoch-3 primary's ship was refused by its supporting replica")
	}

	listener.AdmitAsReplica(1) // a stale, lower admission never lowers the floor
	if !g2Refused(t, listener.Addr(), MsgShipEntry, g2ShipPayload(RecoveryLineage{SessionID: 3, Epoch: 2, EndpointVersion: 1, TargetLSN: 1}, 2)) {
		t.Fatal("the floor went down after a lower admission")
	}
}
