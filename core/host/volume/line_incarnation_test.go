package volume

import (
	"testing"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// A restarted primary process holds the dead process's line until the
// master re-issues it: that line's sessions (endpoint version, session id)
// are the ones each replica still holds, and a session opened on it could
// equal one of them. So the process adopts nothing on it (no replica
// session, no role) and adopts the re-issued line for itself.
func TestHost_ApplyFact_LineIssuedForAnotherProcessIsNotAdopted(t *testing.T) {
	rec := &orderingRecorder{}
	h, a, r := newStubbedHost(t, "vol1", "r1", rec)
	h.incarnation = "p2"
	fact := func(ev uint64, incarnation string) *control.AssignmentFact {
		return &control.AssignmentFact{
			VolumeId: "vol1", ReplicaId: "r1", Epoch: 5, EndpointVersion: ev,
			DataAddr: "127.0.0.1:1000", CtrlAddr: "127.0.0.1:1001",
			Peers:             []*control.ReplicaDescriptor{{ReplicaId: "r2", Epoch: 5, EndpointVersion: ev, DataAddr: "127.0.0.1:2000", CtrlAddr: "127.0.0.1:2001"}},
			PeerSetGeneration: 5<<32 | ev,
			LineIncarnation:   incarnation,
		}
	}
	h.applyFixtureFact(fact(1, "p1"))
	if r.callCount.Load() != 0 || a.callCount.Load() != 0 {
		t.Fatalf("the dead process's line was adopted: replica sets %d, assignments %d", r.callCount.Load(), a.callCount.Load())
	}
	h.applyFixtureFact(fact(2, "p2"))
	if r.callCount.Load() != 1 || a.callCount.Load() != 1 || a.lastInfo.EndpointVersion != 2 {
		t.Fatalf("the line re-issued for this process: replica sets %d, assignments %d (EV %d); want both at EV 2", r.callCount.Load(), a.callCount.Load(), a.lastInfo.EndpointVersion)
	}
}
