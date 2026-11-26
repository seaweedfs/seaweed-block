package master

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
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
		if errors.Is(err, ErrPlannedMoveNeedsSeal) {
			return h.fencedMoveEvidence(volumeID, target, profile, placement.Slots)
		}
		return adapter.PrimaryEvidence{}, err
	}
	result, err := prober.ProbePromotionCandidates(volumeID, authority.AuthorityBasis{}, []authority.ReplicaCandidate{{ReplicaID: target, ServerID: targetServer}})
	if err != nil {
		return adapter.PrimaryEvidence{}, fmt.Errorf("planned move: probe round: %w", err)
	}
	for _, c := range result.Candidates {
		if c.ReplicaID == target && c.Ready {
			ev := promotionEvidenceFor(result, target)
			ev.FencedEpoch, ev.Basis = 0, adapter.BasisPlanned
			return ev, nil
		}
	}
	return adapter.PrimaryEvidence{}, fmt.Errorf("planned move: replica %s did not answer the probe round ready (caught up, epoch table valid)", target)
}

// fencedMoveEvidence is the operator's move where a planned move needs a seal
// (the target is not in every acknowledgement set: RF3 sync_quorum). The
// fencing round is that seal: every other replica is fenced at the line's
// next epoch, so the old primary, dead or alive, can no longer collect an
// acknowledgement, and the decision is read from the round alone
// (fencedDecision: enough counted answers, the target holds the highest
// fenced end and is ready). A round that does not allow the target publishes
// nothing and leaves the fence pending, so a primary that answers again is
// continued at e+1 (healAfterFailedFence) by the product loop.
func (h *Host) fencedMoveEvidence(volumeID, target, profile string, slots []lifecycle.PlacementSlotIntent) (adapter.PrimaryEvidence, error) {
	current, ok := h.Publisher().VolumeAuthorityLine(volumeID)
	if !ok {
		return adapter.PrimaryEvidence{}, fmt.Errorf("fenced move: volume %s has no authority line", volumeID)
	}
	h.promotionMu.RLock()
	prober := h.promotionProber
	h.promotionMu.RUnlock()
	var candidates []authority.ReplicaCandidate
	for _, s := range slots {
		if s.ReplicaID != current.ReplicaID {
			candidates = append(candidates, authority.ReplicaCandidate{ReplicaID: s.ReplicaID, ServerID: s.ServerID})
		}
	}
	plain, err := prober.ProbePromotionCandidates(volumeID, current, candidates)
	if err != nil {
		return adapter.PrimaryEvidence{}, fmt.Errorf("fenced move: probe round: %w", err)
	}
	if plain.AckProfile == "" {
		plain.AckProfile = profile
	}
	fenced, allowed := h.fencedDecision(volumeID, current, plain, len(candidates))
	if allowed[target] {
		return promotionEvidenceFor(fenced, target), nil
	}
	h.noteFencePending(volumeID, current.Epoch+1)
	others := make([]string, 0, len(allowed))
	for r := range allowed {
		others = append(others, r)
	}
	sort.Strings(others)
	return adapter.PrimaryEvidence{}, fmt.Errorf("fenced move: the fencing round at epoch %d does not allow %s (allowed: %v): it must hold the highest fenced end and be ready, with enough fenced survivors", current.Epoch+1, target, others)
}
