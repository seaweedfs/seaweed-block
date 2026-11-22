package authority

import "testing"

// INV-OBS-SLOT-FROM-EXPECTED-SERVER-001: a replica's slot in the assembled
// topology takes its addresses and health only from a fresh fact reported
// by the server its expected slot names. Another server's fact for the
// same replica (whatever it claims, and whichever comes first) is never
// used: with no fact from the expected server the slot is a placeholder.
func TestAssembleVolumeTopology_SlotFactOnlyFromTheExpectedServer(t *testing.T) {
	expected := VolumeExpected{VolumeID: "v1", Slots: []ExpectedSlot{{ReplicaID: "r1", ServerID: "node-a"}, {ReplicaID: "r2", ServerID: "node-b"}}}
	foreign := slotWithServer{serverID: "node-x", slot: SlotFact{VolumeID: "v1", ReplicaID: "r1", DataAddr: "10.9.9.9:1", CtrlAddr: "10.9.9.9:2", Reachable: true, ReadyForPrimary: true, Eligible: true, EvidenceScore: 99}}
	own := slotWithServer{serverID: "node-a", slot: SlotFact{VolumeID: "v1", ReplicaID: "r1", DataAddr: "10.0.0.1:1", CtrlAddr: "10.0.0.1:2", Reachable: true, Eligible: true}}
	r2 := slotWithServer{serverID: "node-b", slot: SlotFact{VolumeID: "v1", ReplicaID: "r2", DataAddr: "10.0.0.2:1", CtrlAddr: "10.0.0.2:2", Reachable: true, Eligible: true}}

	slot := func(topo VolumeTopologySnapshot, replica string) ReplicaCandidate {
		t.Helper()
		for _, s := range topo.Slots {
			if s.ReplicaID == replica {
				return s
			}
		}
		t.Fatalf("no slot %s in %+v", replica, topo.Slots)
		return ReplicaCandidate{}
	}

	// The foreign fact comes first: the expected server's fact still wins.
	got := slot(assembleVolumeTopology("v1", expected, []slotWithServer{foreign, own, r2}, AuthorityBasis{}, false), "r1")
	if got.DataAddr != own.slot.DataAddr || got.CtrlAddr != own.slot.CtrlAddr || got.ServerID != "node-a" || got.EvidenceScore != 0 {
		t.Fatalf("r1 = %+v, want node-a's fact (addresses %s/%s, score 0)", got, own.slot.DataAddr, own.slot.CtrlAddr)
	}

	// Only the foreign server reported r1: the slot is a placeholder, never
	// the foreign addresses or health.
	got = slot(assembleVolumeTopology("v1", expected, []slotWithServer{foreign, r2}, AuthorityBasis{}, false), "r1")
	if got.DataAddr != "" || got.CtrlAddr != "" || got.Reachable || got.Eligible || got.ReadyForPrimary {
		t.Fatalf("r1 = %+v, want a placeholder (no foreign addresses or health)", got)
	}
}
