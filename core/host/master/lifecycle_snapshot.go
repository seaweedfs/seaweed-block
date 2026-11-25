package master

import (
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

// LifecycleSnapshot is a read-only product registration view. It is
// deliberately not authority-shaped.
type LifecycleSnapshot struct {
	Volumes            []lifecycle.VolumeRecord
	Nodes              []lifecycle.NodeRegistration
	Placements         []lifecycle.PlacementIntent
	VerifiedPlacements []lifecycle.VerifiedPlacement
}

// LifecycleSnapshot returns copies of lifecycle facts when configured.
func (h *Host) LifecycleSnapshot() (LifecycleSnapshot, bool) {
	stores := h.Lifecycle()
	if stores == nil {
		return LifecycleSnapshot{}, false
	}
	nodes := stores.Nodes.ListNodes()
	placements := stores.Placements.ListPlacements()
	verified := h.verifyPlacements(placements, nodes)
	return LifecycleSnapshot{
		Volumes:            stores.Volumes.ListVolumes(),
		Nodes:              nodes,
		Placements:         placements,
		VerifiedPlacements: verified,
	}, true
}

func (h *Host) verifyPlacements(placements []lifecycle.PlacementIntent, nodes []lifecycle.NodeRegistration) []lifecycle.VerifiedPlacement {
	placements = h.readyPlacements(placements)
	verified := make([]lifecycle.VerifiedPlacement, 0, len(placements))
	var obsStore *authority.ObservationStore
	if h.obs != nil {
		obsStore = h.obs.Store()
	}
	if obsStore != nil {
		for _, placement := range placements {
			bindings, bound, err := h.lifecycle.WorkspaceBindings.Get(placement.VolumeID)
			if bound || err != nil {
				out := lifecycle.VerifiedPlacement{VolumeID: placement.VolumeID, Reason: lifecycle.VerifyReasonMissingObservation}
				if err == nil && len(placement.Slots) == 1 {
					slot := placement.Slots[0]
					for _, b := range bindings {
						if b.Retired || b.ServerID != slot.ServerID || b.DataAddr != slot.DataAddr || b.CtrlAddr != slot.CtrlAddr {
							continue
						}
						fact, ok := obsStore.SlotFactOnServer(b.ServerID, placement.VolumeID, slot.ReplicaID)
						if ok && fact.DataAddr == b.DataAddr && fact.CtrlAddr == b.CtrlAddr {
							out.Verified = true
							out.Reason = lifecycle.VerifyReasonOK
							out.Slots = []lifecycle.VerifiedPlacementSlot{{ServerID: b.ServerID, ReplicaID: slot.ReplicaID, Source: slot.Source, DataAddr: b.DataAddr, CtrlAddr: b.CtrlAddr, VerifiedBy: "bound_server_observation"}}
						}
					}
				}
				verified = append(verified, out)
				continue
			}
			verified = append(verified, verifyPlacementWithObservationStore(placement, obsStore))
		}
		return verified
	}
	cfg := lifecycle.VerificationConfig{
		Now:             time.Now().UTC(),
		FreshnessWindow: h.cfg.Freshness.FreshnessWindow,
	}
	for _, placement := range placements {
		verified = append(verified, lifecycle.VerifyPlacementIntent(placement, nodes, cfg))
	}
	return verified
}

func verifyPlacementWithObservationStore(intent lifecycle.PlacementIntent, obsStore *authority.ObservationStore) lifecycle.VerifiedPlacement {
	out := lifecycle.VerifiedPlacement{
		VolumeID: intent.VolumeID,
		Reason:   lifecycle.VerifyReasonOK,
	}
	if obsStore == nil {
		out.Reason = lifecycle.VerifyReasonMissingObservation
		return out
	}
	for _, slot := range intent.Slots {
		if slot.Source == lifecycle.PlacementSourceBlankPool {
			// Blank pool candidates have no replica slot yet. They can
			// only be verified by node inventory in this slice.
			out.Reason = lifecycle.VerifyReasonMissingObservation
			return out
		}
		fact, ok := obsStore.SlotFact(intent.VolumeID, slot.ReplicaID)
		if !ok {
			out.Reason = lifecycle.VerifyReasonMissingObservation
			return out
		}
		if fact.DataAddr == "" || fact.CtrlAddr == "" {
			out.Reason = lifecycle.VerifyReasonMissingAddress
			return out
		}
		out.Slots = append(out.Slots, lifecycle.VerifiedPlacementSlot{
			ServerID:   slot.ServerID,
			ReplicaID:  slot.ReplicaID,
			Source:     slot.Source,
			DataAddr:   fact.DataAddr,
			CtrlAddr:   fact.CtrlAddr,
			VerifiedBy: "observation_store",
		})
	}
	out.Verified = true
	return out
}
