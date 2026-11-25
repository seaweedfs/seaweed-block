package master

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
)

// Every primary change with a live current primary is a planned move and
// passes one guard: a failback (plannedMoveEvidence) and a rebalance (the
// gate's PlannedMoveAllowed on the slot, which the controller requires) are
// allowed exactly when plannedMoveAllowed holds for the volume's ack profile
// and replica count. One row per profile x RF; each row checks both paths.
func TestPlannedMoveOneGuardForFailbackAndRebalance(t *testing.T) {
	for _, profile := range []string{"sync_all", "sync_quorum", "best_effort"} {
		for _, rf := range []int{2, 3} {
			t.Run(fmt.Sprintf("%s/RF%d", profile, rf), func(t *testing.T) {
				want := plannedMoveAllowed(profile, rf) == nil
				h := newTestMasterWithControllerConfig(t, t.TempDir(), authority.TopologyControllerConfig{ExpectedSlotsPerVolume: rf})
				defer closeTestMaster(t, h)
				vol := "vol-rf2"
				if rf == 3 {
					vol = "vol-rf3"
					seedRF3Placement(t, h)
				} else {
					seedRF2Placement(t, h)
				}
				candidates := []PromotionCandidateEvidence{{ReplicaID: "r2", Ready: true, DurableLSN: 1}}
				if rf == 3 {
					candidates = append(candidates, PromotionCandidateEvidence{ReplicaID: "r3", Ready: true, DurableLSN: 1})
				}
				// The current primary is alive and ready: a move now is planned.
				h.SetPromotionEvidenceProvider(staticPromotionProbe{result: PromotionProbeResult{
					AckProfile:   profile,
					SyncAckKnown: true,
					CurrentKnown: true,
					Current:      PromotionCandidateEvidence{ReplicaID: "r1", Ready: true, DurableLSN: 1},
					Candidates:   candidates,
				}})

				// Failback.
				_, err := h.plannedMoveEvidence(vol, "r2")
				if (err == nil) != want {
					t.Fatalf("failback to r2: err=%v; want allowed=%t", err, want)
				}

				// Rebalance: the gate's annotation of the snapshot the
				// controller reads.
				slot := func(r, s string) authority.ReplicaCandidate {
					return authority.ReplicaCandidate{ReplicaID: r, ServerID: s, DataAddr: "d-" + r, CtrlAddr: "c-" + r, Reachable: true, ReadyForPrimary: true, Eligible: true}
				}
				snap := authority.ClusterSnapshot{
					Servers: []authority.ServerObservation{{ServerID: "s1", Reachable: true, Eligible: true}, {ServerID: "s2", Reachable: true, Eligible: true}, {ServerID: "s3", Reachable: true, Eligible: true}},
					Volumes: []authority.VolumeTopologySnapshot{{
						VolumeID:  vol,
						Authority: authority.AuthorityBasis{Assigned: true, ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, DataAddr: "d-r1", CtrlAddr: "c-r1"},
						Slots:     []authority.ReplicaCandidate{slot("r1", "s1"), slot("r2", "s2")},
					}},
				}
				if rf == 3 {
					snap.Volumes[0].Slots = append(snap.Volumes[0].Slots, slot("r3", "s3"))
				}
				h.applyPromotionEvidenceGate(&snap)
				for _, s := range snap.Volumes[0].Slots[1:] {
					if !s.ReadyForPrimary {
						t.Fatalf("%s: the gate did not allow a ready candidate: %+v", s.ReplicaID, s)
					}
					if s.PlannedMoveAllowed != want {
						t.Fatalf("rebalance to %s: PlannedMoveAllowed=%t; want %t", s.ReplicaID, s.PlannedMoveAllowed, want)
					}
				}
			})
		}
	}
}
