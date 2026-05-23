package master

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/ops"
)

// LifecycleProductTickResult summarizes one explicit product-loop tick.
type LifecycleProductTickResult struct {
	ReconciledVolumes  int
	VerifiedPlacements int
	PublishedAsks      int
	SkippedCurrent     int
	SkippedUnverified  int
	PromotionProbes    int
	PromotionBlocked   int
}

type PromotionCandidateEvidence struct {
	ReplicaID  string
	Ready      bool
	DurableLSN uint64
	ProbeAddr  string
}

type PromotionProbeResult struct {
	AckProfile   string
	SyncAckLSN   uint64
	CurrentKnown bool
	Current      PromotionCandidateEvidence
	Candidates   []PromotionCandidateEvidence
}

type PromotionEvidenceProvider interface {
	ProbePromotionCandidates(volumeID string, current authority.AuthorityBasis, candidates []authority.ReplicaCandidate) (PromotionProbeResult, error)
}

func (h *Host) SetPromotionEvidenceProvider(p PromotionEvidenceProvider) {
	h.promotionMu.Lock()
	defer h.promotionMu.Unlock()
	h.promotionProber = p
}

// RunLifecycleProductTick drives the first G9G product loop:
// lifecycle facts -> verified placement -> AssignmentAsk -> controller
// directive queue. It never calls Publisher.apply; Publisher remains the only
// authority minter through its normal Run loop.
func (h *Host) RunLifecycleProductTick() (LifecycleProductTickResult, error) {
	stores := h.Lifecycle()
	if stores == nil {
		return LifecycleProductTickResult{}, nil
	}
	nodes := stores.Nodes.ListNodes()
	reconciled := lifecycle.ReconcilePlacement(stores.Volumes.ListVolumes(), nodes, stores.Placements)
	placements := stores.Placements.ListPlacements()
	verified := h.verifyPlacements(placements, nodes)
	result := LifecycleProductTickResult{
		ReconciledVolumes:  len(reconciled),
		VerifiedPlacements: len(verified),
	}
	h.recordPlacementVerifiedEvents(verified)
	submitted, probeCount, blockedCount, directAsks, err := h.submitPlacementSnapshots(placements)
	if err != nil {
		return result, err
	}
	result.PromotionProbes += probeCount
	result.PromotionBlocked += blockedCount
	if submitted {
		result.PublishedAsks++
	}
	for _, ask := range directAsks {
		if h.assignmentAskAlreadyCurrent(ask.VolumeID, ask.ReplicaID, ask.DataAddr, ask.CtrlAddr) {
			result.SkippedCurrent++
			continue
		}
		if err := h.ctrl.SubmitAssignmentAsk(ask); err != nil {
			return result, err
		}
		result.PublishedAsks++
	}
	for _, placement := range verified {
		if len(placement.Slots) > 1 {
			continue
		}
		if !placement.Verified {
			result.SkippedUnverified++
			continue
		}
		asks, err := assignmentRequestsFromVerifiedPlacement(placement)
		if err != nil {
			return result, err
		}
		for _, ask := range asks {
			if h.assignmentAskAlreadyCurrent(ask.VolumeID, ask.ReplicaID, ask.DataAddr, ask.CtrlAddr) {
				result.SkippedCurrent++
				continue
			}
			if err := h.ctrl.SubmitAssignmentAsk(ask); err != nil {
				return result, err
			}
			result.PublishedAsks++
		}
	}
	return result, nil
}

func (h *Host) submitPlacementSnapshots(placements []lifecycle.PlacementIntent) (bool, int, int, []authority.AssignmentAsk, error) {
	if h.obs == nil || h.ctrl == nil {
		return false, 0, 0, nil, nil
	}
	topology, ok := acceptedTopologyFromPlacementIntents(placements)
	if !ok {
		return false, 0, 0, nil, nil
	}
	build := authority.BuildSnapshot(h.obs.Store().Snapshot(), topology, h.Publisher())
	probeCount, blockedCount, directAsks := h.applyPromotionEvidenceGate(&build.Snapshot)
	supported := supportedSnapshotVolumes(build.Snapshot)
	unsupportedProbeCount, unsupportedBlockedCount, unsupportedDirectAsks := h.applyUnsupportedPromotionEvidenceGate(placements, build.Report, supported)
	probeCount += unsupportedProbeCount
	blockedCount += unsupportedBlockedCount
	directAsks = append(directAsks, unsupportedDirectAsks...)
	return true, probeCount, blockedCount, directAsks, h.ctrl.SubmitObservedState(build.Snapshot, build.Report)
}

func (h *Host) applyPromotionEvidenceGate(snap *authority.ClusterSnapshot) (int, int, []authority.AssignmentAsk) {
	if snap == nil || len(snap.Volumes) == 0 {
		return 0, 0, nil
	}
	probeCount, blockedCount := 0, 0
	var directAsks []authority.AssignmentAsk
	serverIndex := map[string]authority.ServerObservation{}
	for _, server := range snap.Servers {
		serverIndex[server.ServerID] = server
	}
	for vi := range snap.Volumes {
		vol := &snap.Volumes[vi]
		if len(vol.Slots) < 3 || !vol.Authority.Assigned {
			continue
		}
		currentIdx := replicaIndex(vol.Slots, vol.Authority.ReplicaID)
		if currentIdx < 0 {
			continue
		}
		currentAcceptable := candidateAcceptableForPromotionGate(vol.Slots[currentIdx], serverIndex)
		candidates := make([]authority.ReplicaCandidate, 0, len(vol.Slots)-1)
		readyCandidate := false
		for _, slot := range vol.Slots {
			if slot.ReplicaID != vol.Authority.ReplicaID {
				candidates = append(candidates, slot)
				if candidateAcceptableForPromotionGate(slot, serverIndex) {
					readyCandidate = true
				}
			}
		}
		if currentAcceptable && !readyCandidate {
			continue
		}
		probeCount++
		result, ok := h.probePromotionCandidates(vol.VolumeID, vol.Authority, candidates)
		allowed := promotionAllowedReplicas(result, ok)
		if ok {
			h.log.Printf("blockmaster: promotion gate volume=%s current=%s current_known=%t current_ready=%t ack_profile=%s required_lsn=%d candidates=%v allowed=%v",
				vol.VolumeID, vol.Authority.ReplicaID, result.CurrentKnown, result.Current.Ready, result.AckProfile, result.SyncAckLSN, result.Candidates, allowed)
			h.recordPromotionEvaluationEvents(vol.VolumeID, result, allowed)
		} else {
			h.log.Printf("blockmaster: promotion gate volume=%s current=%s probe_unavailable", vol.VolumeID, vol.Authority.ReplicaID)
			h.recordPromotionProbeUnavailableEvent(vol.VolumeID, vol.Authority.ReplicaID)
		}
		enabledCandidate := false
		for si := range vol.Slots {
			slot := &vol.Slots[si]
			if slot.ReplicaID == vol.Authority.ReplicaID {
				continue
			}
			if allowed[slot.ReplicaID] {
				slot.ReadyForPrimary = true
				enabledCandidate = true
			} else {
				slot.ReadyForPrimary = false
			}
		}
		if enabledCandidate && result.CurrentKnown && !result.Current.Ready {
			current := &vol.Slots[currentIdx]
			current.Reachable = false
			current.ReadyForPrimary = false
			if target, ok := chooseAllowedPromotionTarget(*vol, result, allowed); ok {
				h.log.Printf("blockmaster: promotion gate volume=%s direct_reassign=%s current=%s", vol.VolumeID, target.ReplicaID, vol.Authority.ReplicaID)
				directAsks = append(directAsks, authority.AssignmentAsk{
					VolumeID:  vol.VolumeID,
					ReplicaID: target.ReplicaID,
					DataAddr:  target.DataAddr,
					CtrlAddr:  target.CtrlAddr,
					Intent:    authority.IntentReassign,
				})
			}
		}
		if !enabledCandidate {
			blockedCount++
			h.recordVolumeBlockedEvent(vol.VolumeID, ops.ReasonNoPromotionReadyCandidate)
		}
	}
	return probeCount, blockedCount, directAsks
}

func (h *Host) applyUnsupportedPromotionEvidenceGate(placements []lifecycle.PlacementIntent, report authority.SupportabilityReport, supported map[string]bool) (int, int, []authority.AssignmentAsk) {
	if len(report.Unsupported) == 0 {
		return 0, 0, nil
	}
	probeCount, blockedCount := 0, 0
	var directAsks []authority.AssignmentAsk
	for _, placement := range placements {
		if supported[placement.VolumeID] || len(placement.Slots) < 3 || !placementIsConcreteExistingReplica(placement) {
			continue
		}
		evidence, ok := report.Unsupported[placement.VolumeID]
		if !ok || !unsupportedReasonsAllowPromotionProbe(evidence.Reasons) {
			continue
		}
		current, ok := h.Publisher().VolumeAuthorityLine(placement.VolumeID)
		if !ok || !current.Assigned {
			continue
		}
		vol, ok := topologyFromUnsupportedPlacement(placement, current)
		if !ok {
			continue
		}
		currentIdx := replicaIndex(vol.Slots, current.ReplicaID)
		if currentIdx < 0 {
			continue
		}
		candidates := make([]authority.ReplicaCandidate, 0, len(vol.Slots)-1)
		for _, slot := range vol.Slots {
			if slot.ReplicaID != current.ReplicaID {
				candidates = append(candidates, slot)
			}
		}
		probeCount++
		result, probeOK := h.probePromotionCandidates(vol.VolumeID, current, candidates)
		allowed := promotionAllowedReplicas(result, probeOK)
		if probeOK {
			h.log.Printf("blockmaster: promotion gate unsupported volume=%s current=%s reasons=%v current_known=%t current_ready=%t ack_profile=%s required_lsn=%d candidates=%v allowed=%v",
				vol.VolumeID, current.ReplicaID, evidence.Reasons, result.CurrentKnown, result.Current.Ready, result.AckProfile, result.SyncAckLSN, result.Candidates, allowed)
			h.recordPromotionEvaluationEvents(vol.VolumeID, result, allowed)
		} else {
			h.log.Printf("blockmaster: promotion gate unsupported volume=%s current=%s probe_unavailable reasons=%v", vol.VolumeID, current.ReplicaID, evidence.Reasons)
			h.recordPromotionProbeUnavailableEvent(vol.VolumeID, current.ReplicaID)
		}
		enabledCandidate := false
		for _, slot := range vol.Slots {
			if slot.ReplicaID != current.ReplicaID && allowed[slot.ReplicaID] {
				enabledCandidate = true
				break
			}
		}
		if enabledCandidate && (!result.CurrentKnown || !result.Current.Ready) {
			if target, ok := chooseAllowedPromotionTarget(vol, result, allowed); ok {
				h.log.Printf("blockmaster: promotion gate unsupported volume=%s direct_reassign=%s current=%s", vol.VolumeID, target.ReplicaID, current.ReplicaID)
				directAsks = append(directAsks, authority.AssignmentAsk{
					VolumeID:  vol.VolumeID,
					ReplicaID: target.ReplicaID,
					DataAddr:  target.DataAddr,
					CtrlAddr:  target.CtrlAddr,
					Intent:    authority.IntentReassign,
				})
			}
			continue
		}
		if !enabledCandidate {
			blockedCount++
			h.recordVolumeBlockedEvent(vol.VolumeID, ops.ReasonNoPromotionReadyCandidate)
		}
	}
	return probeCount, blockedCount, directAsks
}

func supportedSnapshotVolumes(snap authority.ClusterSnapshot) map[string]bool {
	out := make(map[string]bool, len(snap.Volumes))
	for _, volume := range snap.Volumes {
		out[volume.VolumeID] = true
	}
	return out
}

func unsupportedReasonsAllowPromotionProbe(reasons []string) bool {
	if len(reasons) == 0 {
		return false
	}
	for _, reason := range reasons {
		switch reason {
		case authority.ReasonPartialInventory, authority.ReasonMissingServerObservation, authority.ReasonStaleObservation:
			continue
		default:
			return false
		}
	}
	return true
}

func topologyFromUnsupportedPlacement(placement lifecycle.PlacementIntent, current authority.AuthorityBasis) (authority.VolumeTopologySnapshot, bool) {
	vol := authority.VolumeTopologySnapshot{
		VolumeID:  placement.VolumeID,
		Authority: current,
		Slots:     make([]authority.ReplicaCandidate, 0, len(placement.Slots)),
	}
	for _, slot := range placement.Slots {
		dataAddr := slot.DataAddr
		ctrlAddr := slot.CtrlAddr
		if slot.ReplicaID == current.ReplicaID {
			if dataAddr == "" {
				dataAddr = current.DataAddr
			}
			if ctrlAddr == "" {
				ctrlAddr = current.CtrlAddr
			}
		}
		if dataAddr == "" || ctrlAddr == "" {
			return authority.VolumeTopologySnapshot{}, false
		}
		vol.Slots = append(vol.Slots, authority.ReplicaCandidate{
			ReplicaID: slot.ReplicaID,
			ServerID:  slot.ServerID,
			DataAddr:  dataAddr,
			CtrlAddr:  ctrlAddr,
			Reachable: true,
			Eligible:  true,
		})
	}
	return vol, true
}

func (h *Host) recordPlacementVerifiedEvents(placements []lifecycle.VerifiedPlacement) {
	for _, placement := range placements {
		if !placement.Verified {
			continue
		}
		h.events.append(ops.ClusterEvent{
			VolumeID: placement.VolumeID,
			Type:     "placement_verified",
			Severity: "info",
			Message:  fmt.Sprintf("placement verified with %d replica slots", len(placement.Slots)),
			Reason:   "placement_verified",
		})
	}
}

func (h *Host) recordPromotionEvaluationEvents(volumeID string, result PromotionProbeResult, allowed map[string]bool) {
	for _, candidate := range result.Candidates {
		reason := ops.ReasonNoPromotionReadyCandidate
		severity := "warning"
		message := fmt.Sprintf("candidate %s is not promotion-ready", candidate.ReplicaID)
		if allowed[candidate.ReplicaID] {
			reason = ops.ReasonCandidateCoversRequiredFrontier
			severity = "info"
			message = fmt.Sprintf("candidate %s covers required frontier lsn=%d", candidate.ReplicaID, result.SyncAckLSN)
		} else if candidate.Ready {
			reason = ops.ReasonCandidateFrontierBehind
			message = fmt.Sprintf("candidate %s ready=%t durable_lsn=%d required_lsn=%d", candidate.ReplicaID, candidate.Ready, candidate.DurableLSN, result.SyncAckLSN)
		}
		h.events.append(ops.ClusterEvent{
			VolumeID:  volumeID,
			ReplicaID: candidate.ReplicaID,
			Type:      "promotion_candidate_evaluated",
			Severity:  severity,
			Reason:    reason,
			Message:   message,
		})
	}
}

func (h *Host) recordPromotionProbeUnavailableEvent(volumeID, currentReplica string) {
	h.events.append(ops.ClusterEvent{
		VolumeID:  volumeID,
		ReplicaID: currentReplica,
		Type:      "promotion_candidate_evaluated",
		Severity:  "warning",
		Reason:    ops.ReasonNoPromotionReadyCandidate,
		Message:   "promotion evidence probe unavailable",
	})
}

func (h *Host) recordVolumeBlockedEvent(volumeID, reason string) {
	h.events.append(ops.ClusterEvent{
		VolumeID: volumeID,
		Type:     "volume_blocked",
		Severity: "warning",
		Reason:   reason,
		Message:  "no candidate passed promotion readiness gate",
	})
}

func (h *Host) recordAuthorityPublishedEvent(event authority.PublishEvent) {
	if h == nil || h.events == nil {
		return
	}
	message := fmt.Sprintf("authority published replica %s epoch=%d endpoint_version=%d", event.Info.ReplicaID, event.Info.Epoch, event.Info.EndpointVersion)
	if event.Ask.Intent == authority.IntentReassign {
		message = fmt.Sprintf("authority reassigned to replica %s epoch=%d endpoint_version=%d", event.Info.ReplicaID, event.Info.Epoch, event.Info.EndpointVersion)
	}
	h.events.append(ops.ClusterEvent{
		VolumeID:        event.Info.VolumeID,
		ReplicaID:       event.Info.ReplicaID,
		Type:            "authority_published",
		Severity:        "info",
		Reason:          ops.ReasonCandidateCoversRequiredFrontier,
		Message:         message,
		NewValue:        event.Info.ReplicaID,
		Epoch:           event.Info.Epoch,
		EndpointVersion: event.Info.EndpointVersion,
	})
}

func replicaIndex(slots []authority.ReplicaCandidate, replicaID string) int {
	for i, slot := range slots {
		if slot.ReplicaID == replicaID {
			return i
		}
	}
	return -1
}

func candidateAcceptableForPromotionGate(slot authority.ReplicaCandidate, servers map[string]authority.ServerObservation) bool {
	server, ok := servers[slot.ServerID]
	return ok &&
		slot.Reachable &&
		slot.ReadyForPrimary &&
		slot.Eligible &&
		!slot.Withdrawn &&
		server.Reachable &&
		server.Eligible
}

func (h *Host) probePromotionCandidates(volumeID string, current authority.AuthorityBasis, candidates []authority.ReplicaCandidate) (PromotionProbeResult, bool) {
	h.promotionMu.RLock()
	prober := h.promotionProber
	h.promotionMu.RUnlock()
	if prober == nil {
		return PromotionProbeResult{}, false
	}
	result, err := prober.ProbePromotionCandidates(volumeID, current, candidates)
	if err != nil {
		h.log.Printf("blockmaster: promotion probe volume=%s failed: %v", volumeID, err)
		return PromotionProbeResult{}, false
	}
	return result, true
}

func promotionAllowedReplicas(result PromotionProbeResult, ok bool) map[string]bool {
	allowed := map[string]bool{}
	if !ok || !isKnownAckProfile(result.AckProfile) {
		return allowed
	}
	if result.SyncAckLSN > 0 && !isQuorumAckProfile(result.AckProfile) {
		return allowed
	}
	var bestLSN uint64
	for _, candidate := range result.Candidates {
		if candidate.ReplicaID == "" || !candidate.Ready {
			continue
		}
		if result.SyncAckLSN > 0 && candidate.DurableLSN < result.SyncAckLSN {
			continue
		}
		if result.SyncAckLSN == 0 && candidate.DurableLSN == 0 {
			continue
		}
		if candidate.DurableLSN > bestLSN {
			bestLSN = candidate.DurableLSN
			allowed = map[string]bool{candidate.ReplicaID: true}
			continue
		}
		if candidate.DurableLSN == bestLSN {
			allowed[candidate.ReplicaID] = true
		}
	}
	return allowed
}

func chooseAllowedPromotionTarget(vol authority.VolumeTopologySnapshot, result PromotionProbeResult, allowed map[string]bool) (authority.ReplicaCandidate, bool) {
	durable := map[string]uint64{}
	for _, candidate := range result.Candidates {
		durable[candidate.ReplicaID] = candidate.DurableLSN
	}
	var best authority.ReplicaCandidate
	var bestLSN uint64
	ok := false
	for _, slot := range vol.Slots {
		if !allowed[slot.ReplicaID] {
			continue
		}
		lsn := durable[slot.ReplicaID]
		if !ok || lsn > bestLSN || (lsn == bestLSN && slot.ReplicaID < best.ReplicaID) {
			best = slot
			bestLSN = lsn
			ok = true
		}
	}
	return best, ok
}

func isKnownAckProfile(profile string) bool {
	switch strings.ToLower(strings.ReplaceAll(profile, "-", "_")) {
	case "best_effort", "sync_quorum", "sync_all":
		return true
	default:
		return false
	}
}

func isQuorumAckProfile(profile string) bool {
	switch strings.ToLower(strings.ReplaceAll(profile, "-", "_")) {
	case "sync_quorum", "sync_all":
		return true
	default:
		return false
	}
}

func acceptedTopologyFromPlacementIntents(placements []lifecycle.PlacementIntent) (authority.AcceptedTopology, bool) {
	topology := authority.AcceptedTopology{Volumes: make([]authority.VolumeExpected, 0, len(placements))}
	for _, placement := range placements {
		if len(placement.Slots) <= 1 {
			continue
		}
		if !placementIsConcreteExistingReplica(placement) {
			continue
		}
		expected := authority.VolumeExpected{
			VolumeID: placement.VolumeID,
			Slots:    make([]authority.ExpectedSlot, 0, len(placement.Slots)),
		}
		for _, slot := range placement.Slots {
			expected.Slots = append(expected.Slots, authority.ExpectedSlot{
				ReplicaID: slot.ReplicaID,
				ServerID:  slot.ServerID,
			})
		}
		topology.Volumes = append(topology.Volumes, expected)
	}
	return topology, len(topology.Volumes) > 0
}

func placementIsConcreteExistingReplica(placement lifecycle.PlacementIntent) bool {
	for _, slot := range placement.Slots {
		if slot.Source != lifecycle.PlacementSourceExistingReplica || slot.ReplicaID == "" || slot.ServerID == "" {
			return false
		}
	}
	return true
}

func (h *Host) assignmentAskAlreadyCurrent(volumeID, replicaID, dataAddr, ctrlAddr string) bool {
	line, ok := h.Publisher().VolumeAuthorityLine(volumeID)
	if !ok {
		return false
	}
	return line.Assigned &&
		line.ReplicaID == replicaID &&
		line.DataAddr == dataAddr &&
		line.CtrlAddr == ctrlAddr
}
