package master

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/storage"

	"github.com/seaweedfs/seaweed-block/core/adapter"
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
	ReplicaID string
	Ready     bool
	// AwaitingReplicaStoresEpoch: the primary answered not ready only because
	// its assignment at this epoch still awaits replica stores (its own
	// report); 0: not awaiting.
	AwaitingReplicaStoresEpoch uint64
	DurableLSN                 uint64
	ProbeAddr                  string
	// LastDataEpoch and LastDataFirstLSN are the candidate's epoch-table row
	// at DurableLSN (zero: no table, or no row): the promotion line carries
	// them, and the target adopts only if its own row at that LSN is the same.
	LastDataEpoch    uint64
	LastDataFirstLSN uint64
	// TableInvalid: a recovery left the candidate's table without a known
	// history; such a candidate is never Ready (never a promotion survivor).
	TableInvalid bool
	// TableValid: the candidate's evidence was read, on a present and valid
	// epoch table. Only such an answer is evidence: one whose table is
	// absent or could not be read says nothing about its end.
	TableValid bool
	// StoreID is the candidate's probed store (its superblock UUID). A
	// candidate is Ready only when it equals the store the volume's primary
	// last attested for it (Host.AttestedStore): a replaced store, or one
	// never attested, is not a survivor.
	StoreID storage.StoreID
	// Floor is the candidate's durable epoch floor when it answered; in a
	// fencing round it is counted only at the round's FencedEpoch.
	Floor uint64
}

type PromotionProbeResult struct {
	// FencedEpoch: this is a fencing round's result at that epoch (0: a plain
	// probe).
	FencedEpoch uint64
	AckProfile  string
	SyncAckLSN  uint64
	// SyncAckKnown reports that SyncAckLSN is a known required
	// acknowledgement frontier, which may be zero. A nonzero SyncAckLSN
	// is always a known frontier; a zero SyncAckLSN without SyncAckKnown
	// is unknown and is never read as "nothing was acknowledged".
	SyncAckKnown bool
	CurrentKnown bool
	Current      PromotionCandidateEvidence
	Candidates   []PromotionCandidateEvidence
}

type PromotionEvidenceProvider interface {
	ProbePromotionCandidates(volumeID string, current authority.AuthorityBasis, candidates []authority.ReplicaCandidate) (PromotionProbeResult, error)
	// FencePromotionCandidates runs the fencing round of a decision to
	// replace the primary of line current: survivors (ready in the plain
	// round, with their probed store) make current.Epoch+1 their durable
	// floor, then answer. The result's FencedEpoch is current.Epoch+1.
	FencePromotionCandidates(volumeID string, current authority.AuthorityBasis, survivors []PromotionCandidateEvidence) (PromotionProbeResult, error)
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
	h.workspaceMu.Lock()
	stores := h.Lifecycle()
	if stores == nil {
		h.workspaceMu.Unlock()
		return LifecycleProductTickResult{}, nil
	}
	nodes := stores.Nodes.ListNodes()
	// New placements never choose a co-located node under strict journal
	// isolation; existing placements still verify against every node.
	reconciled := h.reconcilePlacements(stores, nodes)
	placements := h.readyPlacements(stores.Placements.ListPlacements())
	verified := h.verifyPlacements(placements, nodes)
	result := LifecycleProductTickResult{
		ReconciledVolumes:  len(reconciled),
		VerifiedPlacements: len(verified),
	}
	h.recordPlacementVerifiedEvents(verified)
	h.workspaceMu.Unlock()
	submitted, probeCount, blockedCount, directAsks, err := h.submitPlacementSnapshots(placements)
	h.workspaceMu.Lock()
	defer h.workspaceMu.Unlock()
	if err != nil {
		return result, err
	}
	// A remote probe may have overlapped a placement/move/delete. Never use
	// its captured intent after that local intent has changed.
	if !reflect.DeepEqual(placements, h.readyPlacements(stores.Placements.ListPlacements())) {
		return result, nil
	}
	verified = h.verifyPlacements(placements, stores.Nodes.ListNodes())
	result.PromotionProbes += probeCount
	result.PromotionBlocked += blockedCount
	if submitted {
		result.PublishedAsks++
	}
	for _, ask := range directAsks {
		if err := stores.WorkspaceBindings.CheckAvailable(lifecycle.WorkspaceBinding{VolumeID: ask.VolumeID, DataAddr: ask.DataAddr, CtrlAddr: ask.CtrlAddr}); err != nil {
			result.SkippedUnverified++
			continue
		}
		// A fenced reassign is minted only at its fenced epoch (a second
		// application is refused there), so it is never "already current":
		// the heal of a failed fencing round names the current replica.
		if ask.Evidence.FencedEpoch == 0 && h.assignmentAskAlreadyCurrent(ask.VolumeID, ask.ReplicaID, ask.DataAddr, ask.CtrlAddr) {
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
			if err := stores.WorkspaceBindings.CheckAvailable(lifecycle.WorkspaceBinding{VolumeID: ask.VolumeID, ServerID: placement.Slots[0].ServerID, DataAddr: ask.DataAddr, CtrlAddr: ask.CtrlAddr}); err != nil {
				result.SkippedUnverified++
				continue
			}
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
	placements = h.readyPlacements(placements)
	captured := placements

	if h.obs == nil || h.ctrl == nil {
		return false, 0, 0, nil, nil
	}
	// Explicit RF1 placements use only the server-and-pair verified direct
	// path below; generic snapshots may select a different same-replica claim.
	legacy := make([]lifecycle.PlacementIntent, 0, len(placements))
	for _, p := range placements {
		_, bound, err := h.lifecycle.WorkspaceBindings.Get(p.VolumeID)
		if err != nil {
			return false, 0, 0, nil, err
		}
		if !bound {
			legacy = append(legacy, p)
		}
	}
	placements = legacy
	if len(placements) == 0 {
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
	// Probes above are remote. Only the final local validation and queueing
	// serialize with workspace mutations, so unrelated CLEAN ACKs can proceed.
	h.workspaceMu.Lock()
	defer h.workspaceMu.Unlock()
	if !reflect.DeepEqual(captured, h.lifecycle.Placements.ListPlacements()) {
		return false, probeCount, blockedCount, nil, nil
	}
	for _, p := range placements {
		if _, bound, err := h.lifecycle.WorkspaceBindings.Get(p.VolumeID); err != nil {
			return false, probeCount, blockedCount, nil, err
		} else if bound {
			return false, probeCount, blockedCount, nil, nil
		}
	}
	for _, v := range build.Snapshot.Volumes {
		for _, slot := range v.Slots {
			if err := h.lifecycle.WorkspaceBindings.CheckAvailable(lifecycle.WorkspaceBinding{VolumeID: v.VolumeID, DataAddr: slot.DataAddr, CtrlAddr: slot.CtrlAddr}); err != nil {
				return false, probeCount, blockedCount, nil, nil
			}
		}
	}
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
		h.noteSlotServers(vol.VolumeID, vol.Slots)
		// Every replicated volume (RF2 and RF3) decides a promotion here.
		if len(vol.Slots) < 2 || !vol.Authority.Assigned {
			continue
		}
		currentIdx := replicaIndex(vol.Slots, vol.Authority.ReplicaID)
		if currentIdx < 0 {
			continue
		}
		// Creation is its own phase: no replacement decision of any kind
		// (no reassign, no fencing round, no heal at a higher epoch) for a
		// volume whose master record still awaits its replica stores.
		if why := h.inCreationPhase(vol.VolumeID, vol.Authority, nil); why != "" {
			h.holdCreating(vol, why)
			continue
		}
		currentAcceptable := candidateAcceptableForPromotionGate(vol.Slots[currentIdx], serverIndex)
		// A fencing round fenced survivors above this line and nothing was
		// published: they refuse the primary at its epoch (and may no longer
		// look ready), so heal before anything else while it is acceptable.
		if pend := h.pendingFence(vol.VolumeID, vol.Authority.Epoch); pend && currentAcceptable {
			if heal, ok := h.healAfterFailedFence(vol.VolumeID, vol.Authority); ok {
				directAsks = append(directAsks, heal)
				continue
			}
		}
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
		result, ok := h.probePromotionCandidatesCurrent(vol.VolumeID, vol.Authority, candidates)
		// The same phase by the primary's own answer: its assignment at
		// this line's epoch still awaits the stores (the master's wait may
		// be over while the re-sent assignment has not arrived).
		if why := h.inCreationPhase(vol.VolumeID, vol.Authority, answerOf(result, ok)); why != "" {
			h.holdCreating(vol, why)
			continue
		}
		allowed := promotionAllowedReplicas(result, ok, len(candidates), h.freshPromotionPeers(vol.VolumeID, candidates))
		// Replacing the primary (it is not acceptable, or answered not
		// ready): the decision is read from a fencing round, never from the
		// plain answers, so no write the old primary gets acknowledged after
		// a survivor answered can be missing from the target.
		// A primary in its boot grace is not replaced, so no fencing round
		// cuts it off from its replicas while it starts.
		replacing := (!currentAcceptable || (result.CurrentKnown && !result.Current.Ready)) &&
			!h.primaryBootGraceHoldsNow(vol.VolumeID, vol.Authority.ReplicaID)
		// A survivor answering above the line was fenced by a round that
		// published nothing (e.g. before a master restart): heal.
		if ok && !replacing && floorAboveLine(result, vol.Authority.Epoch) {
			if heal, healed := h.healAfterFailedFence(vol.VolumeID, vol.Authority); healed {
				directAsks = append(directAsks, heal)
			}
		}
		if replacing && ok && isBestEffortProfile(result.AckProfile) {
			// The same refusal as the operator's move (plannedMoveAllowed):
			// a best_effort write may exist on the primary alone, so no
			// replica takes its place automatically.
			allowed = map[string]bool{}
			h.log.Printf("blockmaster: promotion gate volume=%s current=%s ack_profile=best_effort: no automatic failover", vol.VolumeID, vol.Authority.ReplicaID)
			h.recordVolumeBlockedEvent(vol.VolumeID, ops.ReasonBestEffortNoAutomaticFailover)
		} else if (len(allowed) > 0 || emptyVolumeSurvivors(result, ok)) && replacing {
			result, allowed = h.fencedDecision(vol.VolumeID, vol.Authority, result, len(candidates))
			if len(allowed) == 0 {
				h.noteFencePending(vol.VolumeID, vol.Authority.Epoch+1)
				// The fencing round ended without a publication: survivors may
				// now refuse the primary at e. Continue that primary at e+1 if
				// it answers ready, else the volume waits (visibly).
				if heal, ok := h.healAfterFailedFence(vol.VolumeID, vol.Authority); ok {
					directAsks = append(directAsks, heal)
				} else {
					h.recordVolumeBlockedEvent(vol.VolumeID, ops.ReasonFenceRoundIncomplete)
				}
			}
		}
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
				slot.PromotionEvidence = promotionEvidenceFor(result, slot.ReplicaID)
				slot.PlannedMoveAllowed = plannedMoveAllowed(result.AckProfile, len(vol.Slots)) == nil
				enabledCandidate = true
			} else {
				slot.ReadyForPrimary = false
				slot.PromotionEvidence = adapter.PrimaryEvidence{}
				slot.PlannedMoveAllowed = false
			}
		}
		// After the (blocking) probe, judge the current primary from its
		// own fresh heartbeat slot as it is now, not from the snapshot
		// taken before the probe.
		if enabledCandidate && result.CurrentKnown && !result.Current.Ready && h.primaryBootGraceHoldsNow(vol.VolumeID, vol.Authority.ReplicaID) {
			h.log.Printf("blockmaster: promotion gate volume=%s current=%s not ready but in boot grace; no direct reassign", vol.VolumeID, vol.Authority.ReplicaID)
		} else if enabledCandidate && result.CurrentKnown && !result.Current.Ready {
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
					Evidence:  promotionEvidenceFor(result, target.ReplicaID),
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

// fencedDecision runs the fencing round for every replica that answered the
// plain round, ready or not, and decides on its answers alone. An answer
// counts when it made the line's next epoch its durable floor (a higher
// floor was fenced by a newer decision), from the reference store, on a
// valid epoch table: a lagging replica counts, since it may be the only
// holder of an acknowledged write. At least unknownFrontierSurvivorsNeeded
// must count. The target holds the highest counted end and is ready; when
// only a replica that is not ready holds it, nothing is promoted (a lower
// ready one would drop what only that replica has) and the volume waits.
func (h *Host) fencedDecision(volumeID string, current authority.AuthorityBasis, plain PromotionProbeResult, peerCount int) (PromotionProbeResult, map[string]bool) {
	h.promotionMu.RLock()
	prober := h.promotionProber
	h.promotionMu.RUnlock()
	var survivors []PromotionCandidateEvidence
	for _, c := range plain.Candidates {
		if c.ReplicaID != "" && c.ProbeAddr != "no answer" {
			survivors = append(survivors, c)
		}
	}
	fenced, err := prober.FencePromotionCandidates(volumeID, current, survivors)
	if err != nil || fenced.FencedEpoch != current.Epoch+1 {
		h.log.Printf("blockmaster: promotion gate volume=%s fencing round at %d failed: %v", volumeID, current.Epoch+1, err)
		return plain, map[string]bool{}
	}
	fenced.AckProfile, fenced.SyncAckLSN, fenced.SyncAckKnown = plain.AckProfile, plain.SyncAckLSN, plain.SyncAckKnown
	fenced.CurrentKnown, fenced.Current = plain.CurrentKnown, plain.Current
	counted := map[string]bool{}
	var highest uint64
	for i := range fenced.Candidates {
		c := &fenced.Candidates[i]
		if c.ReplicaID != "" && c.Floor == fenced.FencedEpoch && c.TableValid {
			counted[c.ReplicaID] = true
			if c.DurableLSN > highest {
				highest = c.DurableLSN
			}
		} else {
			c.Ready = false
		}
	}
	if need := unknownFrontierSurvivorsNeeded(plain.AckProfile, peerCount); len(counted) < need {
		h.log.Printf("blockmaster: promotion gate volume=%s fencing round at %d: %d fenced survivors, need %d; nothing published", volumeID, fenced.FencedEpoch, len(counted), need)
		return fenced, map[string]bool{}
	}
	allowed := map[string]bool{}
	var holders []string
	for _, c := range fenced.Candidates {
		if !counted[c.ReplicaID] || c.DurableLSN != highest {
			continue
		}
		holders = append(holders, c.ReplicaID)
		// highest == 0: N-K+1 fenced answers intersect every acknowledgement
		// set and none holds an entry, so nothing was ever acknowledged (an
		// empty volume whose primary was lost): a ready survivor may take it.
		if c.Ready && (fenced.SyncAckLSN == 0 || c.DurableLSN >= fenced.SyncAckLSN) && (c.DurableLSN > 0 || highest == 0) {
			allowed[c.ReplicaID] = true
		}
	}
	if len(allowed) == 0 {
		h.log.Printf("blockmaster: promotion gate volume=%s fencing round at %d: the highest fenced end %d is held by %v, none ready; waiting for it", volumeID, fenced.FencedEpoch, highest, holders)
		h.recordVolumeWaitingEvent(volumeID, ops.ReasonHighestEndNotReady, fmt.Sprintf("the highest fenced end %d is held by %v, not ready for primary", highest, holders))
	}
	return fenced, allowed
}

// isBestEffortProfile: the ack profile acknowledges a write without any
// replica (planned_move.go's ErrPlannedMoveBestEffort).
func isBestEffortProfile(profile string) bool {
	return strings.ToLower(strings.ReplaceAll(profile, "-", "_")) == "best_effort"
}

// emptyVolumeSurvivors: the plain round saw a ready replica and every answer
// at end 0 with no required frontier. promotionAllowedReplicas never allows
// an end-0 candidate (no data is no evidence); only a fencing round can show
// that nothing was acknowledged, so this case goes to the fence.
func emptyVolumeSurvivors(result PromotionProbeResult, ok bool) bool {
	if !ok || !isKnownAckProfile(result.AckProfile) || result.SyncAckLSN != 0 {
		return false
	}
	ready := false
	for _, c := range result.Candidates {
		if c.DurableLSN != 0 {
			return false
		}
		ready = ready || c.Ready
	}
	return ready
}

func floorAboveLine(result PromotionProbeResult, lineEpoch uint64) bool {
	for _, c := range result.Candidates {
		if c.Floor > lineEpoch {
			return true
		}
	}
	return false
}

func (h *Host) noteFencePending(volumeID string, epoch uint64) {
	h.promotionMu.Lock()
	defer h.promotionMu.Unlock()
	if h.fencePending == nil {
		h.fencePending = map[string]uint64{}
	}
	if epoch > h.fencePending[volumeID] {
		h.fencePending[volumeID] = epoch
	}
}

// pendingFence reports a fencing round above the line at lineEpoch that has
// not been followed by a line; one the line has reached is forgotten.
func (h *Host) pendingFence(volumeID string, lineEpoch uint64) bool {
	h.promotionMu.Lock()
	defer h.promotionMu.Unlock()
	e, ok := h.fencePending[volumeID]
	if ok && lineEpoch >= e {
		delete(h.fencePending, volumeID)
		return false
	}
	return ok
}

func (h *Host) recordVolumeWaitingEvent(volumeID, reason, message string) {
	h.events.append(ops.ClusterEvent{VolumeID: volumeID, Type: "volume_blocked", Severity: "warning", Reason: reason, Message: message})
}

// healAfterFailedFence continues the line's own primary at e+1 after a
// fencing round that published nothing, on that primary's answer to a round
// opened for the heal (a plain one: it needs no fence against itself). The
// current primary holds every write it acknowledged, so continuing it at e+1
// loses nothing; the replicas, at floor e+1 already or later, rejoin by
// reconciliation against its table. Its line is a PROMOTION whose evidence
// is that primary's own answer; the target check is the usual one.
func (h *Host) healAfterFailedFence(volumeID string, current authority.AuthorityBasis) (authority.AssignmentAsk, bool) {
	h.promotionMu.RLock()
	prober := h.promotionProber
	h.promotionMu.RUnlock()
	res, err := prober.ProbePromotionCandidates(volumeID, current, nil)
	c := res.Current
	if err != nil || !res.CurrentKnown || c.ReplicaID != current.ReplicaID || !c.Ready || !c.TableValid || c.StoreID.IsZero() {
		h.log.Printf("blockmaster: promotion gate volume=%s heal: primary %s did not answer ready (%v); waiting", volumeID, current.ReplicaID, err)
		return authority.AssignmentAsk{}, false
	}
	h.log.Printf("blockmaster: promotion gate volume=%s heal: continuing %s at epoch %d", volumeID, current.ReplicaID, current.Epoch+1)
	return authority.AssignmentAsk{
		VolumeID:  volumeID,
		ReplicaID: current.ReplicaID,
		DataAddr:  current.DataAddr,
		CtrlAddr:  current.CtrlAddr,
		Intent:    authority.IntentReassign,
		Evidence: adapter.PrimaryEvidence{Kind: adapter.PrimaryKindPromotion, DurableLSN: c.DurableLSN, DataEpoch: c.LastDataEpoch, DataFirstLSN: c.LastDataFirstLSN,
			StoreID: c.StoreID, FencedEpoch: current.Epoch + 1, Basis: adapter.BasisHeal},
	}, true
}

func (h *Host) applyUnsupportedPromotionEvidenceGate(placements []lifecycle.PlacementIntent, report authority.SupportabilityReport, supported map[string]bool) (int, int, []authority.AssignmentAsk) {
	if len(report.Unsupported) == 0 {
		return 0, 0, nil
	}
	probeCount, blockedCount := 0, 0
	var directAsks []authority.AssignmentAsk
	for _, placement := range placements {
		if supported[placement.VolumeID] {
			continue
		}
		evidence, ok := report.Unsupported[placement.VolumeID]
		if !ok {
			continue
		}
		// Record the current primary's own fresh slot fact for every
		// unsupported volume, whatever the reason and whether or not a
		// promotion is considered, so a ready report ends its boot grace
		// and every start counts toward flap history (#128). The
		// promotion rules below are unchanged.
		h.recordCurrentBootFact(placement.VolumeID)
		if line, ok := h.Publisher().VolumeAuthorityLine(placement.VolumeID); ok {
			if why := h.inCreationPhase(placement.VolumeID, line, nil); why != "" {
				h.noteCreating(placement.VolumeID, line.ReplicaID, why)
				continue
			}
		}
		// A fencing round fenced survivors above this line and nothing was
		// published (e.g. the operator's refused failover): continue the
		// line's own primary at e+1 when it is back and answers ready,
		// before any candidate check, as the supported gate does.
		if line, ok := h.Publisher().VolumeAuthorityLine(placement.VolumeID); ok && line.Assigned &&
			h.pendingFence(placement.VolumeID, line.Epoch) && h.freshPrimaryAcceptable(placement.VolumeID, line.ReplicaID) {
			if heal, healed := h.healAfterFailedFence(placement.VolumeID, line); healed {
				directAsks = append(directAsks, heal)
				continue
			}
		}
		if len(placement.Slots) < 2 || !placementIsConcreteExistingReplica(placement) || !unsupportedReasonsAllowPromotionProbe(evidence.Reasons) {
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
		result, probeOK := h.probePromotionCandidatesCurrent(vol.VolumeID, current, candidates)
		if why := h.inCreationPhase(vol.VolumeID, current, answerOf(result, probeOK)); why != "" {
			h.noteCreating(vol.VolumeID, current.ReplicaID, why)
			continue
		}
		allowed := promotionAllowedReplicas(result, probeOK, len(candidates), h.freshPromotionPeers(vol.VolumeID, candidates))
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
		// Only a positive not-ready report from the current primary moves the
		// line. A primary that did not answer is unknown, never failed.
		if enabledCandidate && result.CurrentKnown && !result.Current.Ready {
			// Unsupported does not mean the current primary is unhealthy:
			// a different slot may be stale. Judge the current primary
			// from its own fresh heartbeat slot, never from placement.
			// After the (blocking) probe: the authority must still be the
			// one probed, and the current primary is judged from its own
			// fresh heartbeat slot as it is now.
			if now, ok := h.Publisher().VolumeAuthorityLine(vol.VolumeID); !ok || now.ReplicaID != current.ReplicaID || now.Epoch != current.Epoch || now.EndpointVersion != current.EndpointVersion {
				h.log.Printf("blockmaster: promotion gate unsupported volume=%s authority moved during the probe; no direct reassign", vol.VolumeID)
				continue
			}
			if h.primaryBootGraceHoldsNow(vol.VolumeID, current.ReplicaID) {
				h.log.Printf("blockmaster: promotion gate unsupported volume=%s current=%s not ready but in boot grace; no direct reassign", vol.VolumeID, current.ReplicaID)
				continue
			}
			if isBestEffortProfile(result.AckProfile) {
				h.log.Printf("blockmaster: promotion gate unsupported volume=%s current=%s ack_profile=best_effort: no automatic failover", vol.VolumeID, current.ReplicaID)
				h.recordVolumeBlockedEvent(vol.VolumeID, ops.ReasonBestEffortNoAutomaticFailover)
				continue
			}
			// One decision function: the failover is read from a fencing
			// round here too, never from the plain answers.
			result, allowed = h.fencedDecision(vol.VolumeID, current, result, len(candidates))
			if len(allowed) == 0 {
				h.noteFencePending(vol.VolumeID, current.Epoch+1)
				if heal, healed := h.healAfterFailedFence(vol.VolumeID, current); healed {
					directAsks = append(directAsks, heal)
				} else {
					h.recordVolumeBlockedEvent(vol.VolumeID, ops.ReasonFenceRoundIncomplete)
				}
				continue
			}
			if target, ok := chooseAllowedPromotionTarget(vol, result, allowed); ok {
				h.log.Printf("blockmaster: promotion gate unsupported volume=%s direct_reassign=%s current=%s", vol.VolumeID, target.ReplicaID, current.ReplicaID)
				directAsks = append(directAsks, authority.AssignmentAsk{
					VolumeID:  vol.VolumeID,
					ReplicaID: target.ReplicaID,
					DataAddr:  target.DataAddr,
					CtrlAddr:  target.CtrlAddr,
					Intent:    authority.IntentReassign,
					Evidence:  promotionEvidenceFor(result, target.ReplicaID),
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

// probePromotionCandidatesCurrent probes like probePromotionCandidates,
// but does not let an outdated verdict on the current primary stand
// (#128). The provider probes the current primary before the peers; if
// the current primary's own fresh heartbeat advanced to ready (a newer
// fact, or a new incarnation) while the probe ran, its "not ready"
// verdict may predate a finished recovery, so the probe is run once
// more and that fresh result is used. The trigger is the whole fresh
// verdict, never the single ReadyForPrimary bit: the newer fact must be
// ready AND placeable (reachable, eligible, not withdrawn), so frontend
// readiness can never preserve a primary the newest fact excludes. A
// heartbeat alone is never taken as frontend readiness either: the fresh
// probe still decides, so a frontend that is still unready keeps a
// legitimate failover.
func (h *Host) probePromotionCandidatesCurrent(volumeID string, current authority.AuthorityBasis, candidates []authority.ReplicaCandidate) (PromotionProbeResult, bool) {
	before, beforeOK := h.freshCurrentSlot(volumeID, current.ReplicaID)
	result, ok := h.probePromotionCandidates(volumeID, current, candidates)
	if !ok || (result.CurrentKnown && result.Current.Ready) {
		return result, ok
	}
	after, afterOK := h.freshCurrentSlot(volumeID, current.ReplicaID)
	// A primary that answers it awaits replica stores is in the creation
	// phase: the gates hold the volume on that answer (inCreationPhase),
	// so it is returned as it is.
	if result.CurrentKnown && current.Epoch != 0 && result.Current.AwaitingReplicaStoresEpoch == current.Epoch {
		return result, ok
	}
	if !afterOK || !after.ReadyForPrimary || !after.Reachable || !after.Eligible || after.Withdrawn {
		return result, ok
	}
	if beforeOK && !after.ObservedAt.After(before.ObservedAt) && after.Incarnation == before.Incarnation {
		return result, ok
	}
	h.log.Printf("blockmaster: promotion gate volume=%s current=%s reported ready during the probe; probing again", volumeID, current.ReplicaID)
	return h.probePromotionCandidates(volumeID, current, candidates)
}

// freshPromotionPeers returns the peers that have a fresh heartbeat slot
// now, by the observation store's own freshness rule (FreshSlotCandidate),
// and report themselves reachable. Only these may count toward the
// unknown-frontier survivor rule in promotionAllowedReplicas.
func (h *Host) freshPromotionPeers(volumeID string, peers []authority.ReplicaCandidate) map[string]bool {
	fresh := make(map[string]bool, len(peers))
	for _, peer := range peers {
		if slot, ok := h.freshCurrentSlot(volumeID, peer.ReplicaID); ok && slot.Reachable {
			fresh[peer.ReplicaID] = true
		}
	}
	return fresh
}

// unknownFrontierSurvivorsNeeded is how many of peerCount peers must be
// probed ready, with a known durable frontier, before the highest durable
// candidate may be promoted while the required acknowledgement frontier is
// unknown. It mirrors replication.waitForSyncAcks
// (core/replication/async_ack.go): a write is acknowledged only after the
// primary's local sync and K peer acks, K = peerCount for sync_all and
// K = (peerCount+1)/2 for sync_quorum. An acknowledged write therefore lives
// on at least K of the peerCount peers, so any peerCount-K+1 peers include at
// least one of them, and the highest durable frontier among those peers
// covers every acknowledged write. With fewer, an acknowledged write may
// live only on the peers that were not seen. RF3 sync_quorum: 2 of 2;
// RF3 sync_all: 1 of 2.
func unknownFrontierSurvivorsNeeded(ackProfile string, peerCount int) int {
	required := peerCount
	if strings.ToLower(strings.ReplaceAll(ackProfile, "-", "_")) == "sync_quorum" {
		required = (peerCount + 1) / 2
	}
	return peerCount - required + 1
}

// promotionAllowedReplicas returns the candidates that may be promoted.
// peerCount is the number of replicas other than the current primary, and
// freshPeers holds those of them with a fresh heartbeat now.
//
// With a quorum ack profile and a known required frontier (SyncAckKnown or
// a nonzero SyncAckLSN), a candidate must cover it: durable >= required.
// With a quorum ack profile and an unknown frontier, coverage must be
// proven from the survivors instead: at least
// unknownFrontierSurvivorsNeeded peers must be probed ready in this probe
// (ready implies a known durable frontier) and be fresh; otherwise nothing
// is allowed. An unknown frontier is never read as zero. best_effort keeps
// its rule: no required frontier can be covered, so a nonzero one refuses,
// and otherwise the highest durable ready candidate is allowed.
func promotionAllowedReplicas(result PromotionProbeResult, ok bool, peerCount int, freshPeers map[string]bool) map[string]bool {
	allowed := map[string]bool{}
	if !ok || !isKnownAckProfile(result.AckProfile) {
		return allowed
	}
	if result.SyncAckLSN > 0 && !isQuorumAckProfile(result.AckProfile) {
		return allowed
	}
	frontierKnown := result.SyncAckKnown || result.SyncAckLSN > 0
	if isQuorumAckProfile(result.AckProfile) && !frontierKnown {
		// Survivors are counted only from result, the probe run for this
		// tick, and freshPeers, read for this tick. Nothing from an
		// earlier tick is kept: a survivor seen ready last tick and not
		// seen now does not count.
		seen := map[string]bool{}
		for _, candidate := range result.Candidates {
			if candidate.ReplicaID != "" && candidate.Ready && freshPeers[candidate.ReplicaID] {
				seen[candidate.ReplicaID] = true
			}
		}
		if len(seen) < unknownFrontierSurvivorsNeeded(result.AckProfile, peerCount) {
			return allowed
		}
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

// promotionEvidenceFor is the line's evidence for promoting replicaID: what
// this tick's probe saw on it (its durable LSN and its epoch-table row there).
func promotionEvidenceFor(result PromotionProbeResult, replicaID string) adapter.PrimaryEvidence {
	for _, c := range result.Candidates {
		if c.ReplicaID == replicaID {
			ev := adapter.PrimaryEvidence{Kind: adapter.PrimaryKindPromotion, DurableLSN: c.DurableLSN, DataEpoch: c.LastDataEpoch, DataFirstLSN: c.LastDataFirstLSN, StoreID: c.StoreID, FencedEpoch: result.FencedEpoch}
			if result.FencedEpoch != 0 {
				ev.Basis = adapter.BasisFenced
			}
			return ev
		}
	}
	return adapter.PrimaryEvidence{Kind: adapter.PrimaryKindPromotion}
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

// primaryBootGraceHoldsNow reports whether the current primary is a
// restarted primary that is merely not ready yet, judged from its own
// fresh heartbeat slot read now: placeable by that report and inside its
// boot grace (#128). No fresh slot, or an explicit unhealthy report
// (unreachable, ineligible, withdrawn), never holds.
func (h *Host) primaryBootGraceHoldsNow(volumeID, replicaID string) bool {
	if h.ctrl == nil {
		return false
	}
	cur, ok := h.freshCurrentSlot(volumeID, replicaID)
	if !ok {
		return false
	}
	return h.ctrl.PrimaryBootGraceHolds(volumeID, cur)
}

// recordCurrentBootFact records the current primary's own fresh slot
// fact for the volume, if it has an authority line and a fresh slot.
func (h *Host) recordCurrentBootFact(volumeID string) {
	if h.ctrl == nil {
		return
	}
	line, ok := h.Publisher().VolumeAuthorityLine(volumeID)
	if !ok || !line.Assigned {
		return
	}
	if cur, ok := h.freshCurrentSlot(volumeID, line.ReplicaID); ok {
		h.ctrl.RecordPrimaryBootFact(volumeID, cur)
	}
}

// freshCurrentSlot returns the replica's own fresh heartbeat slot from
// the observation store (server facts folded in).
// freshPrimaryAcceptable: the replica has a fresh heartbeat slot that is
// reachable, eligible and not withdrawn.
func (h *Host) freshPrimaryAcceptable(volumeID, replicaID string) bool {
	s, ok := h.freshCurrentSlot(volumeID, replicaID)
	return ok && s.Reachable && s.Eligible && !s.Withdrawn
}

func (h *Host) freshCurrentSlot(volumeID, replicaID string) (authority.ReplicaCandidate, bool) {
	if h.obs == nil {
		return authority.ReplicaCandidate{}, false
	}
	return h.obs.Store().Snapshot().FreshSlotCandidate(volumeID, replicaID)
}

// inCreationPhase is the one decision of the creation phase, which every
// replacement entry (both promotion gates, the operator's move and planned
// move, the rebalance through the gate) calls first: it returns why the
// volume is creating, or "" once creation ended. A replacement acts only on
// ended: one durable, one-way fact in the master's record, written by the
// first of
//   - the line's primary answering ready (its first Healthy), or
//   - a survivor answering DurableLSN > 0 at the current line (under the
//     quorum ack every acknowledged write is on a survivor, so a volume
//     that served and lost its primary before the master read a ready
//     answer still ends creation and fails over),
// once every initial store is attested (stage 1 holds whatever is
// answered). Nothing clears it. Besides, the current
// primary's own answer, when one was read, holds the volume while its
// assignment at this line's epoch still awaits the stores (the master's
// wait may be over before the re-sent assignment arrives); an answer can
// only hold a volume, never release one.
func (h *Host) inCreationPhase(volumeID string, current authority.AuthorityBasis, answer *PromotionProbeResult) string {
	if h.attest != nil {
		if waiting, _ := h.attest.awaited(volumeID, h.replicaSlotsFor(volumeID)); waiting > 0 {
			return "the master's record awaits replica stores"
		}
		if h.attest.awaitingFirstReady(volumeID) {
			by := ""
			if h.firstReadyNow(volumeID, current, answer) {
				by = "primary " + current.ReplicaID + " answered ready"
			} else if holder, ok := h.survivorHoldingAWrite(volumeID, current, answer); ok {
				by = "survivor " + holder + " holds a write"
			} else {
				return "neither a ready answer of the primary nor a write on a survivor yet"
			}
			ended, err := h.attest.noteFirstReady(volumeID)
			if err != nil {
				return "the end of creation could not be recorded: " + err.Error()
			}
			if !ended {
				return "creation may not end now"
			}
			h.log.Printf("blockmaster: volume=%s creation complete: %s at %s@%d", volumeID, by, current.ReplicaID, current.Epoch)
		}
	}
	if answer != nil && answer.CurrentKnown && current.Epoch != 0 && answer.Current.AwaitingReplicaStoresEpoch == current.Epoch {
		return "the primary's answer awaits replica stores"
	}
	return ""
}

// firstReadyNow: the line's own primary answers ready now (from the answer
// given, or a round of the primary alone). A primary that awaits the stores,
// or one that does not answer, is not ready.
func (h *Host) firstReadyNow(volumeID string, current authority.AuthorityBasis, answer *PromotionProbeResult) bool {
	if !current.Assigned || current.ReplicaID == "" {
		return false
	}
	res := answer
	if res == nil {
		h.promotionMu.RLock()
		prober := h.promotionProber
		h.promotionMu.RUnlock()
		if prober == nil {
			return false
		}
		r, err := prober.ProbePromotionCandidates(volumeID, current, nil)
		if err != nil {
			return false
		}
		res = &r
	}
	return res.CurrentKnown && res.Current.ReplicaID == current.ReplicaID && res.Current.Ready && res.Current.AwaitingReplicaStoresEpoch == 0
}

// survivorHoldingAWrite: a survivor (any replica but the line's own) answers,
// in a probe of the current line, DurableLSN > 0 with its epoch-table row at
// the current epoch (or no row). It reads the answer given, else a round of
// the survivors alone. It names the survivor.
func (h *Host) survivorHoldingAWrite(volumeID string, current authority.AuthorityBasis, answer *PromotionProbeResult) (string, bool) {
	if !current.Assigned || current.ReplicaID == "" {
		return "", false
	}
	res := answer
	if res == nil {
		h.promotionMu.RLock()
		prober := h.promotionProber
		h.promotionMu.RUnlock()
		if prober == nil {
			return "", false
		}
		r, err := prober.ProbePromotionCandidates(volumeID, current, h.survivorCandidates(volumeID, current.ReplicaID))
		if err != nil {
			return "", false
		}
		res = &r
	}
	for _, c := range res.Candidates {
		if c.ReplicaID == "" || c.ReplicaID == current.ReplicaID {
			continue
		}
		if c.DurableLSN > 0 && (c.LastDataEpoch == 0 || c.LastDataEpoch == current.Epoch) {
			return c.ReplicaID, true
		}
	}
	return "", false
}

// survivorCandidates are the volume's replicas other than the line's own,
// each from its fresh heartbeat slot when it has one.
func (h *Host) survivorCandidates(volumeID, currentReplica string) []authority.ReplicaCandidate {
	var out []authority.ReplicaCandidate
	for _, id := range h.replicaSlotsFor(volumeID) {
		if id == currentReplica {
			continue
		}
		c, ok := h.freshCurrentSlot(volumeID, id)
		if !ok {
			c = authority.ReplicaCandidate{ReplicaID: id}
		}
		out = append(out, c)
	}
	return out
}

// answerOf is the probe's answer, or nil when the probe gave none.
func answerOf(result PromotionProbeResult, ok bool) *PromotionProbeResult {
	if !ok {
		return nil
	}
	return &result
}

// holdCreating keeps a creating volume out of every replacement decision:
// no candidate is enabled (the controller reassigns only to an enabled
// slot), and the wait is visible.
func (h *Host) holdCreating(vol *authority.VolumeTopologySnapshot, why string) {
	for si := range vol.Slots {
		slot := &vol.Slots[si]
		if slot.ReplicaID == vol.Authority.ReplicaID {
			continue
		}
		slot.ReadyForPrimary = false
		slot.PromotionEvidence = adapter.PrimaryEvidence{}
		slot.PlannedMoveAllowed = false
	}
	h.noteCreating(vol.VolumeID, vol.Authority.ReplicaID, why)
}

// noteCreating makes the hold of a creating volume visible.
func (h *Host) noteCreating(volumeID, current, why string) {
	h.log.Printf("blockmaster: promotion gate volume=%s current=%s creating (%s): no replacement decision", volumeID, current, why)
	h.recordVolumeWaitingEvent(volumeID, ops.ReasonAwaitingReplicaAttestations, "creating: "+why+"; no replacement decision")
}
