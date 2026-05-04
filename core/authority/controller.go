package authority

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// TopologyController is the multi-volume, cluster-scoped directive
// for the full P14 close target. It converts cluster snapshots into
// bounded desired AssignmentAsk values, then feeds those asks to the
// publisher through the existing Directive seam.
//
// The controller owns:
//   - accepted topology validation for the richer P14 set
//   - cluster-scoped placement / failover / rebalance decisions
//   - convergence state ("desired per volume" until observed current)
//
// It does NOT mint Epoch / EndpointVersion and does not call the
// adapter directly. Publisher remains the sole minter and the
// adapter remains the only consumer ingress.
type TopologyController struct {
	config TopologyControllerConfig
	reader AuthorityBasisReader
	queue  *assignmentQueue

	mu               sync.Mutex
	desired          map[string]DesiredAssignment
	unsupported      map[string]UnsupportedEvidence
	convergenceStuck map[string]*ConvergenceStuckEvidence
	now              func() time.Time
}

func NewTopologyController(config TopologyControllerConfig, reader AuthorityBasisReader) *TopologyController {
	return &TopologyController{
		config:           config.withDefaults(),
		reader:           reader,
		queue:            newAssignmentQueue(),
		desired:          make(map[string]DesiredAssignment),
		unsupported:      make(map[string]UnsupportedEvidence),
		convergenceStuck: make(map[string]*ConvergenceStuckEvidence),
		now:              time.Now,
	}
}

// SetNowForTest overrides the controller's clock source. Tests
// use this to drive RetryWindow / Stuck transitions deterministically.
// Production code MUST NOT call this.
func (c *TopologyController) SetNowForTest(now func() time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// SubmitClusterSnapshot is the cluster-scoped entry point for
// topology, health, and convergence observations. Rejects empty
// snapshots to preserve the "supported truth only" invariant on
// this path.
//
// For the observation-fed intake that also carries pending /
// unsupported transitions, see SubmitObservedState.
func (c *TopologyController) SubmitClusterSnapshot(snap ClusterSnapshot) error {
	if len(snap.Volumes) == 0 {
		return fmt.Errorf("topology: cluster snapshot has no volumes")
	}
	serverIndex := indexServers(snap.Servers)
	volumes := make([]VolumeTopologySnapshot, len(snap.Volumes))
	copy(volumes, snap.Volumes)
	sortVolumeSnapshots(volumes)

	seenVolumes := make(map[string]struct{}, len(volumes))
	for _, v := range volumes {
		if _, ok := seenVolumes[v.VolumeID]; ok {
			return fmt.Errorf("topology: duplicate volume snapshot %s", v.VolumeID)
		}
		seenVolumes[v.VolumeID] = struct{}{}
		if err := validateVolumeTopology(v, c.config.ExpectedSlotsPerVolume); err != nil {
			return err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	currentLoad := c.currentServerLoad(volumes)
	projectedLoad := cloneLoad(currentLoad)

	for _, vol := range volumes {
		currentLine, hasLine := c.reader.VolumeAuthorityLine(vol.VolumeID)
		c.processSupportedVolumeLocked(vol, snap, serverIndex, currentLine, hasLine, currentLoad, projectedLoad)
	}
	return nil
}

// SubmitObservedState is the observation-institution intake. It
// accepts one synthesized ClusterSnapshot together with the
// per-volume supportability report for the same build. This is
// the intake the observation host calls every rebuild, including
// when snap.Volumes is empty but report carries transitions.
//
// Contract:
//  1. snap.Volumes carries ONLY supported volumes. They drive the
//     decision table the same way SubmitClusterSnapshot does.
//  2. For every volume in report.Pending or report.Unsupported,
//     any previously-desired assignment is cleared, and the
//     per-volume unsupported evidence is updated (Unsupported
//     only — Pending does NOT produce evidence; it records that
//     action is paused).
//  3. An entirely-empty intake (no Volumes, no Pending,
//     no Unsupported) is a no-op rather than an error — a single
//     tick with nothing new is not a contract violation.
//
// This is the intake fix for the architect finding that
// supportability transitions could not propagate to the
// controller. Suppressing submission on empty Volumes left old
// desired state live; SubmitObservedState always processes the
// report.
func (c *TopologyController) SubmitObservedState(snap ClusterSnapshot, report SupportabilityReport) error {
	// Validate non-empty supported snapshots the same way
	// SubmitClusterSnapshot does. Empty supported + empty report
	// is allowed (no-op).
	if len(snap.Volumes) == 0 && len(report.Pending) == 0 && len(report.Unsupported) == 0 {
		return nil
	}

	// Pre-validate supported volumes (if any).
	serverIndex := indexServers(snap.Servers)
	volumes := make([]VolumeTopologySnapshot, len(snap.Volumes))
	copy(volumes, snap.Volumes)
	sortVolumeSnapshots(volumes)

	seen := make(map[string]struct{}, len(volumes))
	for _, v := range volumes {
		if _, ok := seen[v.VolumeID]; ok {
			return fmt.Errorf("topology: duplicate volume snapshot %s", v.VolumeID)
		}
		seen[v.VolumeID] = struct{}{}
		if err := validateVolumeTopology(v, c.config.ExpectedSlotsPerVolume); err != nil {
			return err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Process pending / unsupported transitions first. Clearing
	// desired state for these volumes before decision logic runs
	// means any NEWLY-supported volume in snap.Volumes gets fresh
	// decision treatment, while any transitioned-away volume is
	// no longer held as "desired" by the controller.
	for vid := range report.Pending {
		c.clearDesiredLocked(vid)
		// Pending does not produce unsupported evidence. Any
		// stale unsupported record from a prior build is cleared,
		// because pending is a transitional state, not a fault.
		delete(c.unsupported, vid)
	}
	for vid, ev := range report.Unsupported {
		c.clearDesiredLocked(vid)
		// Record per-volume unsupported evidence. The joined
		// reasons string preserves the observation-layer reason
		// vocabulary in the controller's existing per-volume
		// evidence shape. The Basis is left at zero — the
		// observation layer already captured the offending basis
		// in its own VolumeUnsupportedEvidence; the controller's
		// evidence exists to surface "this volume is not acted
		// on" at the cluster-scope level.
		c.unsupported[vid] = UnsupportedEvidence{
			SnapshotRevision: ev.SnapshotRevision,
			CollectedAt:      ev.EvaluatedAt,
			Basis:            AuthorityBasis{},
			Reason:           joinReasons(ev.Reasons),
		}
	}

	// Process supported volumes via the existing decision path.
	// A volume appearing here means observation cleared all the
	// supportability rules; any prior unsupported evidence for
	// this volume is dropped.
	for _, v := range volumes {
		delete(c.unsupported, v.VolumeID)
	}

	currentLoad := c.currentServerLoad(volumes)
	projectedLoad := cloneLoad(currentLoad)
	for _, vol := range volumes {
		currentLine, hasLine := c.reader.VolumeAuthorityLine(vol.VolumeID)
		c.processSupportedVolumeLocked(vol, snap, serverIndex, currentLine, hasLine, currentLoad, projectedLoad)
	}
	return nil
}

// processSupportedVolumeLocked is the shared supported-volume
// convergence loop used by both SubmitClusterSnapshot and
// SubmitObservedState. Per-iteration order (§5.2):
//
//  1. confirmDesiredLocked — if confirmation contract is
//     satisfied, clear desired + stuck evidence (Active→None).
//  2. supersedeDesiredLocked — §9 case 1/2: drop desired if
//     publisher itself has moved past the proposed basis to a
//     different replica, or observation and publisher agree on a
//     different outcome. Case 3 ("decision produces different
//     ask") is covered by the same-ask dedupe further below.
//  3. Topology-honesty checks (clear desired on out-of-topology
//     authority from either side).
//  4. evaluateStuckLocked — if desired still outstanding, mark
//     Stuck when DesiredAt + RetryWindow has elapsed. Runs
//     before the stale-basis continue so publish-but-not-
//     observed scenarios can surface Stuck evidence.
//  5. Stale-basis continue — keep desired, no decide this tick.
//  6. decideVolume + same-ask dedupe + enqueue. On enqueue,
//     DesiredAt is anchored (or re-anchored) to c.now().
func (c *TopologyController) processSupportedVolumeLocked(
	vol VolumeTopologySnapshot,
	snap ClusterSnapshot,
	serverIndex map[string]ServerObservation,
	currentLine AuthorityBasis,
	hasLine bool,
	currentLoad map[string]int,
	projectedLoad map[string]int,
) {
	// A volume appearing in the supported set means observation
	// cleared every supportability rule for it. Any prior
	// unsupported evidence for this volume must drop — otherwise
	// LastUnsupported() would keep reporting a stale fault after
	// the volume is healthy. (Regression from the pre-refactor
	// S4 behavior; stuck evidence has its own lifecycle and is
	// NOT cleared here — §10 clearing rule 4.)
	delete(c.unsupported, vol.VolumeID)

	c.confirmDesiredLocked(vol, currentLine, hasLine)

	if c.supersedeDesiredLocked(vol, currentLine, hasLine) {
		// fall through — decideVolume may emit a new ask this tick.
	}

	if vol.Authority.Assigned && !volumeIncludesReplica(vol, vol.Authority.ReplicaID) {
		c.recordUnsupportedLocked(vol.VolumeID, snap, vol.Authority, "snapshot authority outside accepted topology: "+vol.Authority.ReplicaID)
		c.clearDesiredLocked(vol.VolumeID)
		return
	}
	if hasLine && !volumeIncludesReplica(vol, currentLine.ReplicaID) {
		c.recordUnsupportedLocked(vol.VolumeID, snap, currentLine, "publisher authority on replica outside accepted topology: "+currentLine.ReplicaID)
		c.clearDesiredLocked(vol.VolumeID)
		return
	}

	// Evaluate stuck state for any still-outstanding desired. This
	// must run before the stale-basis continue so "publish-but-
	// not-observed" — which shows up here as a stale basis — can
	// surface ConvergenceStuckEvidence after the retry window.
	c.evaluateStuckLocked(vol.VolumeID)

	// Stale snapshot: do not emit from a stale basis, but keep any
	// existing desired (publish-until-observed convergence record).
	if !basisMatchesLine(vol.Authority, currentLine, hasLine) {
		return
	}

	ask, reason, emit := c.decideVolume(vol, serverIndex, currentLine, hasLine, currentLoad, projectedLoad)
	if !emit {
		c.clearDesiredLocked(vol.VolumeID)
		return
	}
	// Same-ask dedupe: if the identical ask is already the current
	// desired, do not re-enqueue. Without this guard, repeated
	// identical supported feeds would mint a new Reassign epoch
	// each iteration — authority churn.
	if prev, ok := c.desired[vol.VolumeID]; ok && sameAsk(prev.Ask, ask) {
		return
	}

	// A different ask (or a new desired from None) replaces any
	// prior stuck evidence — the old stuck record no longer
	// describes the currently-outstanding desired (§10 clearing
	// rule 2).
	c.clearConvergenceStuckLocked(vol.VolumeID)

	desired := DesiredAssignment{
		Ask:           ask,
		Reason:        reason,
		Revision:      snap.CollectedRevision,
		ProposedBasis: currentLine,
		DesiredAt:     c.now(),
	}
	if !hasLine {
		desired.ProposedBasis = AuthorityBasis{}
	}
	c.desired[vol.VolumeID] = desired
	c.queue.enqueue(ask)
	applyProjectedMove(projectedLoad, vol, currentLine, hasLine, ask)
}

// joinReasons returns a stable comma-joined reason string from
// the observation-layer vocabulary. Used to carry the observation
// reason set into the controller's per-volume evidence slot.
func joinReasons(reasons []string) string {
	if len(reasons) == 0 {
		return ""
	}
	out := reasons[0]
	for i := 1; i < len(reasons); i++ {
		out += "," + reasons[i]
	}
	return out
}

// Next satisfies Directive by draining the per-volume latest-wins
// assignment queue.
func (c *TopologyController) Next(ctx context.Context) (AssignmentAsk, error) {
	return c.queue.next(ctx)
}

// SubmitAssignmentAsk appends a controller-verified ask to the publisher
// directive queue. It is intentionally still an ask-level seam: Publisher
// remains the only epoch / endpoint-version author.
func (c *TopologyController) SubmitAssignmentAsk(ask AssignmentAsk) error {
	if err := validateAsk(ask); err != nil {
		return err
	}
	c.queue.enqueue(ask)
	return nil
}

// LastUnsupported returns the most recent unsupported evidence for
// one volume, if any.
func (c *TopologyController) LastUnsupported(volumeID string) (UnsupportedEvidence, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ev, ok := c.unsupported[volumeID]
	return ev, ok
}

// LastConvergenceStuck returns the per-volume
// ConvergenceStuckEvidence if the desired assignment for this
// volume is currently Stuck. Returns (_, false) when no desired
// is outstanding, when a desired is outstanding but still within
// the retry window, or when the desired was confirmed /
// superseded / cleared.
//
// Returned value is a copy: mutating it does not affect
// controller state.
func (c *TopologyController) LastConvergenceStuck(volumeID string) (ConvergenceStuckEvidence, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ev, ok := c.convergenceStuck[volumeID]
	if !ok {
		return ConvergenceStuckEvidence{}, false
	}
	return *ev, true
}

// DesiredFor returns a copy of the current DesiredAssignment for
// one volume, if any. Test-surface accessor; production code
// should observe convergence outcomes through the publisher
// stream and snapshot feeds, not by reading desired state.
func (c *TopologyController) DesiredFor(volumeID string) (DesiredAssignment, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	d, ok := c.desired[volumeID]
	return d, ok
}

func (c *TopologyController) clearDesiredLocked(volumeID string) {
	_, hadDesired := c.desired[volumeID]
	delete(c.desired, volumeID)
	if hadDesired {
		c.queue.discard(volumeID)
	}
	// §10 clearing rule 1 / 2 / 3: whenever the desired clears
	// (confirmed, superseded, supportability-driven), the stuck
	// evidence clears with it. Supportability regaining does NOT
	// invoke this path — that is handled by the supported-volume
	// loop (§10 clearing rule 4).
	c.clearConvergenceStuckLocked(volumeID)
}

func (c *TopologyController) recordUnsupportedLocked(volumeID string, snap ClusterSnapshot, basis AuthorityBasis, reason string) {
	c.unsupported[volumeID] = UnsupportedEvidence{
		SnapshotRevision: snap.CollectedRevision,
		CollectedAt:      snap.CollectedAt,
		Basis:            basis,
		Reason:           reason,
	}
}

func (c *TopologyController) confirmDesiredLocked(vol VolumeTopologySnapshot, currentLine AuthorityBasis, hasLine bool) {
	desired, ok := c.desired[vol.VolumeID]
	if !ok {
		return
	}
	if desiredObserved(desired, vol, currentLine, hasLine) {
		// Route through clearDesiredLocked so §10 clearing rule 1
		// (stuck evidence clears with the desired) applies to
		// late confirmations that happen AFTER the volume had
		// already transitioned into Stuck. Deleting the desired
		// entry directly here would leave c.convergenceStuck[vid]
		// populated, reporting a volume as still stuck after
		// convergence had actually completed.
		c.clearDesiredLocked(vol.VolumeID)
	}
}

func desiredObserved(desired DesiredAssignment, vol VolumeTopologySnapshot, currentLine AuthorityBasis, hasLine bool) bool {
	if !hasLine || !vol.Authority.Assigned {
		return false
	}
	if vol.Authority.ReplicaID != currentLine.ReplicaID ||
		vol.Authority.Epoch != currentLine.Epoch ||
		vol.Authority.EndpointVersion != currentLine.EndpointVersion {
		return false
	}
	if currentLine.ReplicaID != desired.Ask.ReplicaID {
		return false
	}
	switch desired.Ask.Intent {
	case IntentRefreshEndpoint:
		return vol.Authority.DataAddr == desired.Ask.DataAddr &&
			vol.Authority.CtrlAddr == desired.Ask.CtrlAddr
	case IntentBind, IntentReassign:
		return true
	default:
		return false
	}
}

func (c *TopologyController) currentServerLoad(volumes []VolumeTopologySnapshot) map[string]int {
	load := make(map[string]int)
	for _, vol := range volumes {
		line, ok := c.reader.VolumeAuthorityLine(vol.VolumeID)
		if !ok {
			continue
		}
		slot, ok := candidateByReplica(vol, line.ReplicaID)
		if !ok {
			continue
		}
		load[slot.ServerID]++
	}
	return load
}

func (c *TopologyController) decideVolume(
	vol VolumeTopologySnapshot,
	serverIndex map[string]ServerObservation,
	currentLine AuthorityBasis,
	hasLine bool,
	currentLoad map[string]int,
	projectedLoad map[string]int,
) (AssignmentAsk, string, bool) {
	if !hasLine {
		cands := acceptableCandidates(vol, serverIndex)
		if len(cands) == 0 {
			return AssignmentAsk{}, "", false
		}
		best := choosePlacementCandidate(cands, projectedLoad)
		return AssignmentAsk{
			VolumeID:  vol.VolumeID,
			ReplicaID: best.ReplicaID,
			DataAddr:  best.DataAddr,
			CtrlAddr:  best.CtrlAddr,
			Intent:    IntentBind,
		}, "initial placement", true
	}

	current, ok := candidateByReplica(vol, currentLine.ReplicaID)
	if !ok {
		return AssignmentAsk{}, "", false
	}

	if candidateAcceptable(current, serverIndex) {
		if current.DataAddr != currentLine.DataAddr || current.CtrlAddr != currentLine.CtrlAddr {
			return AssignmentAsk{
				VolumeID:  vol.VolumeID,
				ReplicaID: current.ReplicaID,
				DataAddr:  current.DataAddr,
				CtrlAddr:  current.CtrlAddr,
				Intent:    IntentRefreshEndpoint,
			}, "endpoint refresh", true
		}
		if target, ok := chooseRebalanceCandidate(vol, current, serverIndex, currentLoad, projectedLoad, c.config.RebalanceSkew); ok {
			return AssignmentAsk{
				VolumeID:  vol.VolumeID,
				ReplicaID: target.ReplicaID,
				DataAddr:  target.DataAddr,
				CtrlAddr:  target.CtrlAddr,
				Intent:    IntentReassign,
			}, "rebalance", true
		}
		return AssignmentAsk{}, "", false
	}

	other := acceptableCandidatesExcept(vol, serverIndex, current.ReplicaID)
	if len(other) == 0 {
		return AssignmentAsk{}, "", false
	}
	best := chooseFailoverCandidate(other, projectedLoad)
	return AssignmentAsk{
		VolumeID:  vol.VolumeID,
		ReplicaID: best.ReplicaID,
		DataAddr:  best.DataAddr,
		CtrlAddr:  best.CtrlAddr,
		Intent:    IntentReassign,
	}, "failover", true
}

func volumeIncludesReplica(vol VolumeTopologySnapshot, replicaID string) bool {
	for _, slot := range vol.Slots {
		if slot.ReplicaID == replicaID {
			return true
		}
	}
	return false
}

func candidateByReplica(vol VolumeTopologySnapshot, replicaID string) (ReplicaCandidate, bool) {
	for _, slot := range vol.Slots {
		if slot.ReplicaID == replicaID {
			return slot, true
		}
	}
	return ReplicaCandidate{}, false
}

func candidateAcceptable(slot ReplicaCandidate, servers map[string]ServerObservation) bool {
	server, ok := servers[slot.ServerID]
	if !ok {
		return false
	}
	return slot.Reachable &&
		slot.ReadyForPrimary &&
		slot.Eligible &&
		!slot.Withdrawn &&
		server.Reachable &&
		server.Eligible
}

func acceptableCandidates(vol VolumeTopologySnapshot, servers map[string]ServerObservation) []ReplicaCandidate {
	var out []ReplicaCandidate
	for _, slot := range vol.Slots {
		if candidateAcceptable(slot, servers) {
			out = append(out, slot)
		}
	}
	return out
}

func acceptableCandidatesExcept(vol VolumeTopologySnapshot, servers map[string]ServerObservation, excludeReplica string) []ReplicaCandidate {
	var out []ReplicaCandidate
	for _, slot := range vol.Slots {
		if slot.ReplicaID == excludeReplica {
			continue
		}
		if candidateAcceptable(slot, servers) {
			out = append(out, slot)
		}
	}
	return out
}

func choosePlacementCandidate(cands []ReplicaCandidate, load map[string]int) ReplicaCandidate {
	best := cands[0]
	for _, cand := range cands[1:] {
		if load[cand.ServerID] < load[best.ServerID] {
			best = cand
			continue
		}
		if load[cand.ServerID] == load[best.ServerID] && cand.EvidenceScore > best.EvidenceScore {
			best = cand
			continue
		}
		if load[cand.ServerID] == load[best.ServerID] && cand.EvidenceScore == best.EvidenceScore && cand.ReplicaID < best.ReplicaID {
			best = cand
		}
	}
	return best
}

func chooseFailoverCandidate(cands []ReplicaCandidate, load map[string]int) ReplicaCandidate {
	best := cands[0]
	for _, cand := range cands[1:] {
		if cand.EvidenceScore > best.EvidenceScore {
			best = cand
			continue
		}
		if cand.EvidenceScore == best.EvidenceScore && load[cand.ServerID] < load[best.ServerID] {
			best = cand
			continue
		}
		if cand.EvidenceScore == best.EvidenceScore && load[cand.ServerID] == load[best.ServerID] && cand.ReplicaID < best.ReplicaID {
			best = cand
		}
	}
	return best
}

func chooseRebalanceCandidate(
	vol VolumeTopologySnapshot,
	current ReplicaCandidate,
	servers map[string]ServerObservation,
	currentLoad map[string]int,
	projectedLoad map[string]int,
	skew int,
) (ReplicaCandidate, bool) {
	lightest := currentLoad[current.ServerID]
	for _, slot := range vol.Slots {
		if !candidateAcceptable(slot, servers) {
			continue
		}
		if currentLoad[slot.ServerID] < lightest {
			lightest = currentLoad[slot.ServerID]
		}
	}
	if currentLoad[current.ServerID]-lightest <= skew {
		return ReplicaCandidate{}, false
	}
	best := ReplicaCandidate{}
	ok := false
	for _, slot := range vol.Slots {
		if slot.ReplicaID == current.ReplicaID || !candidateAcceptable(slot, servers) {
			continue
		}
		if !ok ||
			projectedLoad[slot.ServerID] < projectedLoad[best.ServerID] ||
			(projectedLoad[slot.ServerID] == projectedLoad[best.ServerID] && slot.EvidenceScore > best.EvidenceScore) ||
			(projectedLoad[slot.ServerID] == projectedLoad[best.ServerID] && slot.EvidenceScore == best.EvidenceScore && slot.ReplicaID < best.ReplicaID) {
			best = slot
			ok = true
		}
	}
	return best, ok
}

func cloneLoad(src map[string]int) map[string]int {
	dst := make(map[string]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func applyProjectedMove(projected map[string]int, vol VolumeTopologySnapshot, currentLine AuthorityBasis, hasLine bool, ask AssignmentAsk) {
	target, ok := candidateByReplica(vol, ask.ReplicaID)
	if !ok {
		return
	}
	switch ask.Intent {
	case IntentBind:
		projected[target.ServerID]++
	case IntentReassign:
		if hasLine {
			if current, ok := candidateByReplica(vol, currentLine.ReplicaID); ok && projected[current.ServerID] > 0 {
				projected[current.ServerID]--
			}
		}
		projected[target.ServerID]++
	}
}

func sameAsk(a, b AssignmentAsk) bool {
	return a.VolumeID == b.VolumeID &&
		a.ReplicaID == b.ReplicaID &&
		a.DataAddr == b.DataAddr &&
		a.CtrlAddr == b.CtrlAddr &&
		a.Intent == b.Intent
}
