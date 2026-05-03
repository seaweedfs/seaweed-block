package lifecycle

import (
	"time"
)

const (
	VerifiedByNodeRegistration = "node_registration"

	VerifyReasonOK                 = "ok"
	VerifyReasonMissingObservation = "missing_observation"
	VerifyReasonStaleObservation   = "stale_observation"
	VerifyReasonMissingAddress     = "missing_address"
	VerifyReasonReplicaMismatch    = "replica_mismatch"
	VerifyReasonInvalidIntent      = "invalid_intent"
)

// VerificationConfig controls freshness evaluation for placement intent
// verification.
type VerificationConfig struct {
	Now             time.Time
	FreshnessWindow time.Duration
}

// VerifiedPlacement is a non-authority controller bridge output. It proves
// that placement intent intersects with fresh observation; it is still not an
// assignment and not readiness.
type VerifiedPlacement struct {
	VolumeID string
	Slots    []VerifiedPlacementSlot
	Verified bool
	Reason   string
}

// VerifiedPlacementSlot is a fresh, address-bearing placement slot.
type VerifiedPlacementSlot struct {
	ServerID   string
	PoolID     string
	ReplicaID  string
	Source     string
	DataAddr   string
	CtrlAddr   string
	VerifiedBy string
}

// VerifyPlacementIntent intersects placement intent with fresh node
// registration facts. Missing or stale observation fails closed.
func VerifyPlacementIntent(intent PlacementIntent, nodes []NodeRegistration, cfg VerificationConfig) VerifiedPlacement {
	out := VerifiedPlacement{
		VolumeID: intent.VolumeID,
		Reason:   VerifyReasonOK,
	}
	if err := validatePlacementIntent(intent); err != nil {
		out.Reason = VerifyReasonInvalidIntent
		return out
	}
	if cfg.Now.IsZero() {
		cfg.Now = time.Now().UTC()
	}
	nodeByID := make(map[string]NodeRegistration, len(nodes))
	for _, node := range nodes {
		nodeByID[node.ServerID] = node
	}
	for _, slot := range intent.Slots {
		node, ok := nodeByID[slot.ServerID]
		if !ok {
			out.Reason = VerifyReasonMissingObservation
			return out
		}
		if !node.SeenAt.IsZero() && cfg.FreshnessWindow > 0 && cfg.Now.Sub(node.SeenAt) > cfg.FreshnessWindow {
			out.Reason = VerifyReasonStaleObservation
			return out
		}
		dataAddr, ctrlAddr := nodePlacementAddrs(node)
		if dataAddr == "" || ctrlAddr == "" {
			out.Reason = VerifyReasonMissingAddress
			return out
		}
		verified := VerifiedPlacementSlot{
			ServerID:   slot.ServerID,
			PoolID:     slot.PoolID,
			ReplicaID:  slot.ReplicaID,
			Source:     slot.Source,
			DataAddr:   dataAddr,
			CtrlAddr:   ctrlAddr,
			VerifiedBy: VerifiedByNodeRegistration,
		}
		if slot.Source == PlacementSourceExistingReplica {
			replica, ok := matchingReplica(intent.VolumeID, node.Replicas)
			if !ok || replica.ReplicaID != slot.ReplicaID {
				out.Reason = VerifyReasonReplicaMismatch
				return out
			}
		}
		out.Slots = append(out.Slots, verified)
	}
	out.Verified = true
	return out
}

func nodePlacementAddrs(node NodeRegistration) (dataAddr, ctrlAddr string) {
	if node.DataAddr != "" || node.CtrlAddr != "" {
		return node.DataAddr, node.CtrlAddr
	}
	return node.Addr, node.Addr
}
