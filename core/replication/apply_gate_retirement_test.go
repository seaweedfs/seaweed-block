package replication

import (
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/transport"
)

func TestApplyGate_WireSessionTurnoverRetiresSettledCoverage(t *testing.T) {
	gate, store := newGate(t)
	listener, err := transport.NewReplicaListenerWithFloorAndApplyHook("127.0.0.1:0", store, "v1", "", gate)
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.AdmitAsReplica(1); err != nil {
		listener.StopHard()
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(listener.StopHard)
	for session := uint64(1); session <= 128; session++ {
		c, err := net.DialTimeout("tcp", listener.Addr(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			c.Close()
			t.Fatal(err)
		}
		l := liveLineage(session)
		err = transport.WriteMsg(c, transport.MsgShipEntry, transport.EncodeShipEntry(transport.ShipEntry{Lineage: l, LBA: 5, LSN: session, Data: makeData(byte(session))}))
		if err == nil {
			err = transport.WriteMsg(c, transport.MsgBarrierReq, transport.EncodeLineage(l))
		}
		if err == nil {
			_, _, err = transport.ReadMsg(c)
		}
		c.Close()
		if err != nil {
			t.Fatal(err)
		}
		if session > 1 {
			if _, _, _, found := gate.SessionState(session - 1); found {
				t.Fatalf("settled session coverage survived turnover: session=%d", session-1)
			}
		}
	}
	// Retirement must leave the shared suffix version intact.
	if err := gate.ApplyLive(liveLineage(129), 5, makeData(0), 127); err == nil {
		t.Fatal("coverage retirement erased shared version")
	}
}
