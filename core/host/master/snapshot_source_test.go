package master

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

func TestPhase175SnapshotSourceRequiresMatchingCurrentPrimaryFacts(t *testing.T) {
	line := authority.AuthorityBasis{
		Assigned: true, ReplicaID: "r2", Epoch: 7, EndpointVersion: 3,
		DataAddr: "10.0.0.2:19101", CtrlAddr: "10.0.0.2:19102",
	}
	slot := authority.SlotFact{
		VolumeID: "vol-a", ReplicaID: "r2",
		DataAddr: "10.0.0.2:19101", CtrlAddr: "10.0.0.2:19102",
		SnapshotRuntimeEndpoint: "https://10.0.0.2:24443",
		Reachable:               true, ReadyForPrimary: true, Eligible: true,
		ReportingServerID: "node-b",
	}
	got, ok := snapshotSourceFromFacts("vol-a", "node-b", 1<<20, line, true, slot)
	if !ok || got.VolumeID != "vol-a" || got.ReplicaID != "r2" || got.Epoch != 7 || got.EndpointVersion != 3 || got.RuntimeEndpoint != slot.SnapshotRuntimeEndpoint || got.SizeBytes != 1<<20 {
		t.Fatalf("resolved=%+v ok=%v", got, ok)
	}
}

func TestPhase175SnapshotSourceFailsClosedOnUnreadyOrDriftedObservation(t *testing.T) {
	line := authority.AuthorityBasis{
		Assigned: true, ReplicaID: "r1", Epoch: 4, EndpointVersion: 2,
		DataAddr: "10.0.0.1:19101", CtrlAddr: "10.0.0.1:19102",
	}
	valid := authority.SlotFact{
		VolumeID: "vol-a", ReplicaID: "r1",
		DataAddr: line.DataAddr, CtrlAddr: line.CtrlAddr,
		SnapshotRuntimeEndpoint: "https://10.0.0.1:24443",
		Reachable:               true, ReadyForPrimary: true, Eligible: true,
		ReportingServerID: "node-a",
	}
	tests := []struct {
		name    string
		hasSlot bool
		mutate  func(*authority.SlotFact)
	}{
		{name: "missing slot"},
		{name: "not ready", hasSlot: true, mutate: func(s *authority.SlotFact) { s.ReadyForPrimary = false }},
		{name: "withdrawn", hasSlot: true, mutate: func(s *authority.SlotFact) { s.Withdrawn = true }},
		{name: "address drift", hasSlot: true, mutate: func(s *authority.SlotFact) { s.DataAddr = "10.0.0.9:19101" }},
		{name: "reporting server drift", hasSlot: true, mutate: func(s *authority.SlotFact) { s.ReportingServerID = "node-z" }},
		{name: "endpoint host drift", hasSlot: true, mutate: func(s *authority.SlotFact) { s.SnapshotRuntimeEndpoint = "https://10.0.0.9:24443" }},
		{name: "missing endpoint", hasSlot: true, mutate: func(s *authority.SlotFact) { s.SnapshotRuntimeEndpoint = "" }},
		{name: "insecure endpoint", hasSlot: true, mutate: func(s *authority.SlotFact) { s.SnapshotRuntimeEndpoint = "http://10.0.0.1:24443" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			slot := valid
			if tc.mutate != nil {
				tc.mutate(&slot)
			}
			if got, ok := snapshotSourceFromFacts("vol-a", "node-a", 1<<20, line, tc.hasSlot, slot); ok {
				t.Fatalf("unexpected resolution=%+v", got)
			}
		})
	}
}
