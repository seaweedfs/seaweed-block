package master

import (
	"errors"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
)

// ErrPlannedMoveNeedsSeal: under this ack profile and replica count a write
// can be acknowledged without the target holding it (RF3 sync_quorum: the
// other replica acks alone), so a planned move is safe only after the old
// primary stops acknowledging (a seal), which v1 does not have.
var ErrPlannedMoveNeedsSeal = errors.New("planned move: the target is not in every acknowledgement set; it needs the old primary sealed first (not available), or use a forced move with its lost range")

// ErrPlannedMoveBestEffort: best_effort acknowledges a write without any
// replica, so a planned move can drop acknowledged writes by definition.
var ErrPlannedMoveBestEffort = errors.New("planned move: best_effort acknowledges writes without a replica; only a forced move (its lost range reported) can change the primary")

// plannedMoveAllowed is the guard of a planned move (an operator moving the
// primary to a live, caught-up replica without a seal): the target must be
// in every acknowledgement set, so every write acknowledged before it adopts
// is already durable on it (and after it adopts, its epoch floor refuses the
// old primary's frames). That holds for sync_all at any replica count, and
// for sync_quorum when the target is the only replica (RF2). replicas counts
// every slot of the volume, the primary included.
func plannedMoveAllowed(ackProfile string, replicas int) error {
	switch strings.ToLower(strings.ReplaceAll(ackProfile, "-", "_")) {
	case "sync_all":
		return nil
	case "sync_quorum":
		if replicas <= 2 {
			return nil
		}
		return ErrPlannedMoveNeedsSeal
	case "best_effort":
		return ErrPlannedMoveBestEffort
	}
	return fmt.Errorf("planned move: unknown ack profile %q", ackProfile)
}

// ackProfiled is a promotion evidence provider that knows the ack profile.
type ackProfiled interface{ AckProfile() string }

func (p *roundEvidenceProvider) AckProfile() string { return p.ackProfile }

// plannedMoveEvidence is the promotion gate's decision for an operator's
// planned move to target: the guard, then a probe round of the target, which
// must answer ready; its evidence goes on the line (kind PROMOTION).
func (h *Host) plannedMoveEvidence(volumeID, target string) (adapter.PrimaryEvidence, error) {
	h.promotionMu.RLock()
	prober := h.promotionProber
	h.promotionMu.RUnlock()
	if prober == nil {
		return adapter.PrimaryEvidence{}, fmt.Errorf("planned move: no promotion gate is configured")
	}
	profile := ""
	if ap, ok := prober.(ackProfiled); ok {
		profile = ap.AckProfile()
	}
	// The volume's replicas and the target's node, from its placement.
	placement, ok := h.lifecycle.Placements.GetPlacement(volumeID)
	if !ok || len(placement.Slots) == 0 {
		return adapter.PrimaryEvidence{}, fmt.Errorf("planned move: volume %s has no placement", volumeID)
	}
	targetServer := ""
	for _, s := range placement.Slots {
		if s.ReplicaID == target {
			targetServer = s.ServerID
		}
	}
	if err := plannedMoveAllowed(profile, len(placement.Slots)); err != nil {
		return adapter.PrimaryEvidence{}, err
	}
	result, err := prober.ProbePromotionCandidates(volumeID, authority.AuthorityBasis{}, []authority.ReplicaCandidate{{ReplicaID: target, ServerID: targetServer}})
	if err != nil {
		return adapter.PrimaryEvidence{}, fmt.Errorf("planned move: probe round: %w", err)
	}
	for _, c := range result.Candidates {
		if c.ReplicaID == target && c.Ready {
			return promotionEvidenceFor(result, target), nil
		}
	}
	return adapter.PrimaryEvidence{}, fmt.Errorf("planned move: replica %s did not answer the probe round ready (caught up, epoch table valid)", target)
}
