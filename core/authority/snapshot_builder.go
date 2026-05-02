package authority

import (
	"fmt"
	"sort"
	"time"
)

// ============================================================
// P14 S4 — ClusterSnapshot Synthesizer
//
// Pure function: (storeSnapshot, AcceptedTopology) -> BuildResult.
//
// Supportability decisions read ONLY from the storeSnapshot input
// and the AcceptedTopology config. They never read from the
// synthesized output (sketch §12 one-way pipeline).
//
// Pending and Unsupported are VISIBLY DISTINCT in the result:
//   - Supported volumes appear in BuildResult.Snapshot.Volumes
//   - Pending volumes appear in BuildResult.Report.Pending only
//   - Unsupported volumes appear in BuildResult.Report.Unsupported only
// No volume appears in more than one of these three places.
// ============================================================

// BuildSnapshot synthesizes one ClusterSnapshot + per-volume
// supportability report from the given store view and accepted
// topology.
//
// reader is a read-only authority view. It is consulted ONLY to
// stamp the per-volume current authority basis into
// VolumeTopologySnapshot.Authority for supported volumes, so the
// controller's stale-resistance check (basisMatchesLine) can
// succeed under the widened intake contract. Observation
// institution does not MINT authority here — it reads what the
// publisher currently holds (sketch §3 / §16a). reader may be
// nil; in that case Authority.Assigned=false, which is the
// honest "observation has no view" default.
//
// Pure function. Safe to call as often as needed. Callers
// typically invoke it from the host's reactive rebuild path,
// once per store mutation.
func BuildSnapshot(store storeSnapshot, topology AcceptedTopology, reader AuthorityBasisReader) BuildResult {
	result := BuildResult{
		Report:   *newSupportabilityReport(),
		BuiltAt:  store.evaluatedAt,
		InputRev: store.revision,
	}

	// Server list is computed from FRESH observations only.
	// Expired observations stay in the store (visible for
	// diagnosis, sketch §6) but must not contribute to the
	// supported server inventory.
	var servers []ServerObservation
	for id, obs := range store.observations {
		if store.serverFreshness[id] != ServerFresh {
			continue
		}
		servers = append(servers, ServerObservation{
			ServerID:  obs.ServerID,
			Reachable: obs.Server.Reachable,
			Eligible:  obs.Server.Eligible,
		})
	}
	sort.Slice(servers, func(i, j int) bool {
		return servers[i].ServerID < servers[j].ServerID
	})

	// Collect per-volume fresh slot facts (keyed by VolumeID).
	// Expired-server facts are excluded here; they are
	// semantically ineligible for supported synthesis (§6).
	freshSlotsByVolume := map[string][]slotWithServer{}
	for id, obs := range store.observations {
		if store.serverFreshness[id] != ServerFresh {
			continue
		}
		for _, s := range obs.Slots {
			freshSlotsByVolume[s.VolumeID] = append(freshSlotsByVolume[s.VolumeID],
				slotWithServer{slot: s, serverID: obs.ServerID, observedAt: obs.ObservedAt})
		}
	}

	// Walk configured volumes in order for deterministic output.
	configuredIDs := topology.allVolumeIDs()
	sort.Strings(configuredIDs)
	var volumes []VolumeTopologySnapshot
	configured := map[string]bool{}
	for _, vid := range configuredIDs {
		configured[vid] = true
	}

	for _, vid := range configuredIDs {
		expected, _ := topology.lookupVolume(vid)
		var basis AuthorityBasis
		hasLine := false
		if reader != nil {
			basis, hasLine = reader.VolumeAuthorityLine(vid)
		}
		verdict := evaluateVolume(vid, expected, freshSlotsByVolume[vid], store, basis, hasLine)

		switch verdict.state {
		case SupportabilitySupported:
			// Stamp the per-volume current authority basis from
			// the reader (read-only). This lets the controller's
			// stale-resistance check match on subsequent feeds.
			if hasLine {
				verdict.topology.Authority = basis
			}
			volumes = append(volumes, verdict.topology)
		case SupportabilityPending:
			result.Report.Pending[vid] = VolumePendingStatus{
				VolumeID:       vid,
				EvaluatedAt:    store.evaluatedAt,
				PendingReasons: verdict.reasons,
				Note:           verdict.note,
			}
		case SupportabilityUnsupported:
			result.Report.Unsupported[vid] = VolumeUnsupportedEvidence{
				VolumeID:         vid,
				SnapshotRevision: store.revision,
				EvaluatedAt:      store.evaluatedAt,
				Reasons:          verdict.reasons,
				OffendingFacts:   verdict.offendingFacts,
				Note:             verdict.note,
			}
		}
	}

	// Unknown observed volumes — any VolumeID that appears in
	// freshSlotsByVolume but is NOT in the configured topology.
	// These must surface as explicit unsupported evidence, not
	// silent drop, so operators and 14A review can see that
	// observation reported extra volumes the system does not
	// accept.
	var unknownIDs []string
	for vid := range freshSlotsByVolume {
		if !configured[vid] {
			unknownIDs = append(unknownIDs, vid)
		}
	}
	sort.Strings(unknownIDs)
	for _, vid := range unknownIDs {
		offenders := make([]Observation, 0)
		seen := map[string]bool{}
		for _, f := range freshSlotsByVolume[vid] {
			if seen[f.serverID] {
				continue
			}
			seen[f.serverID] = true
			if obs, ok := store.observations[f.serverID]; ok {
				offenders = append(offenders, obs)
			}
		}
		result.Report.Unsupported[vid] = VolumeUnsupportedEvidence{
			VolumeID:         vid,
			SnapshotRevision: store.revision,
			EvaluatedAt:      store.evaluatedAt,
			Reasons:          []string{ReasonUnknownReplicaClaim},
			OffendingFacts:   offenders,
			Note:             "observed volume not in accepted topology",
		}
	}

	result.Snapshot = ClusterSnapshot{
		CollectedRevision: store.revision,
		CollectedAt:       store.evaluatedAt,
		Servers:           servers,
		Volumes:           volumes,
	}
	return result
}

// slotWithServer is a per-volume fresh slot fact together with
// its reporting ServerID and ObservedAt, so the builder can tell
// which server contributed a given slot claim.
type slotWithServer struct {
	slot       SlotFact
	serverID   string
	observedAt time.Time
}

// volumeVerdict is the internal per-volume decision from
// evaluateVolume. Unsupported / pending states carry reasons and
// offending facts; supported carries the assembled
// VolumeTopologySnapshot for inclusion in ClusterSnapshot.
type volumeVerdict struct {
	state          Supportability
	reasons        []string
	offendingFacts []Observation
	note           string
	topology       VolumeTopologySnapshot
}

// evaluateVolume implements the pinned supportability rules for
// one volume. Rules run in an explicit priority order; structural
// violations emit unsupported regardless of pending grace, while
// coverage gaps emit pending within grace and unsupported past
// it.
func evaluateVolume(
	volumeID string,
	expected VolumeExpected,
	freshSlots []slotWithServer,
	store storeSnapshot,
	authorityLine AuthorityBasis,
	hasAuthorityLine bool,
) volumeVerdict {
	// If the volume is not in the accepted topology at all, it is
	// unsupported — we cannot honestly build a VolumeTopologySnapshot
	// for slot-expectations we do not have.
	if expected.VolumeID == "" {
		return volumeVerdict{
			state:   SupportabilityUnsupported,
			reasons: []string{ReasonUnknownReplicaClaim},
			note:    "volume not in accepted topology",
		}
	}

	expectedReplicas := map[string]ExpectedSlot{}
	for _, e := range expected.Slots {
		expectedReplicas[e.ReplicaID] = e
	}

	var reasons []string
	var offenders []Observation
	addReason := func(r string) {
		for _, existing := range reasons {
			if existing == r {
				return
			}
		}
		reasons = append(reasons, r)
	}
	addOffender := func(serverID string) {
		if obs, ok := store.observations[serverID]; ok {
			for _, prev := range offenders {
				if prev.ServerID == serverID {
					return
				}
			}
			offenders = append(offenders, obs)
		}
	}

	// Rule §12a — duplicate-server topology across fresh slots of
	// the same volume. Two fresh slots with different ReplicaIDs
	// but the same ServerID violate the distinct-servers
	// invariant.
	seenServers := map[string]string{} // ServerID -> ReplicaID
	for _, f := range freshSlots {
		if prev, ok := seenServers[f.serverID]; ok && prev != f.slot.ReplicaID {
			addReason(ReasonDuplicateServerTopology)
			addOffender(f.serverID)
			// Also flag the earlier contributor.
			for _, g := range freshSlots {
				if g.serverID == f.serverID && g.slot.ReplicaID == prev {
					addOffender(g.serverID)
				}
			}
		} else if !ok {
			seenServers[f.serverID] = f.slot.ReplicaID
		}
	}

	// Rule §8 — conflicting primary claim. Two or more fresh
	// slots claiming LocalRolePrimary for the same volume. The
	// observation layer never picks a winner, regardless of
	// epoch / timestamp / evidence score.
	primaryClaimants := 0
	for _, f := range freshSlots {
		if f.slot.LocalRoleClaim == LocalRolePrimary {
			primaryClaimants++
			if primaryClaimants >= 2 {
				addReason(ReasonConflictingPrimaryClaim)
				addOffender(f.serverID)
			}
		}
	}
	if primaryClaimants >= 2 {
		for _, f := range freshSlots {
			if f.slot.LocalRoleClaim == LocalRolePrimary {
				addOffender(f.serverID)
			}
		}
	}

	// Rule §9 — unknown replica claim. A fresh slot fact refers
	// to a ReplicaID not in the accepted topology for this
	// volume.
	for _, f := range freshSlots {
		if _, ok := expectedReplicas[f.slot.ReplicaID]; !ok {
			addReason(ReasonUnknownReplicaClaim)
			addOffender(f.serverID)
		}
	}

	// Rule §12 — missing / stale per-slot server coverage,
	// evaluated against the PRE-SNAPSHOT fresh server inventory
	// (via store.freshnessFor), not against any synthesized
	// ClusterSnapshot.Servers. Per expected slot, check if the
	// assigned server is fresh, bootstrapping, missing, or
	// expired.
	coveredReplicas := map[string]bool{}
	pending := false
	degradedByMissingOrExpiredCurrent := false
	for _, expectedSlot := range expected.Slots {
		serverFresh := store.freshnessFor(expectedSlot.ServerID)
		switch serverFresh {
		case ServerFresh:
			// Server is fresh. Does its observation include the
			// expected ReplicaID?
			found := false
			for _, f := range freshSlots {
				if f.serverID == expectedSlot.ServerID && f.slot.ReplicaID == expectedSlot.ReplicaID {
					found = true
					break
				}
			}
			if found {
				coveredReplicas[expectedSlot.ReplicaID] = true
			}
			// If not found: partial inventory on that server.
			// Handled by the "coverage gap" pass below.
		case ServerBootstrapping:
			// Still within bootstrap grace; contributes to
			// pending, not unsupported.
			pending = true
		case ServerMissing:
			if hasAuthorityLine && expectedSlot.ReplicaID == authorityLine.ReplicaID {
				degradedByMissingOrExpiredCurrent = true
			} else {
				addReason(ReasonMissingServerObservation)
			}
		case ServerExpired:
			if hasAuthorityLine && expectedSlot.ReplicaID == authorityLine.ReplicaID {
				degradedByMissingOrExpiredCurrent = true
			} else {
				addReason(ReasonStaleObservation)
				addOffender(expectedSlot.ServerID)
			}
		}
	}

	// Coverage gap: some expected ReplicaIDs are not covered by
	// fresh slot facts. This is PartialInventory. Within bootstrap
	// grace it is pending; past it, unsupported.
	if len(coveredReplicas) < len(expected.Slots) {
		// Check whether all uncovered expected slots are explained
		// by a missing/stale/bootstrapping server reason we already
		// recorded above. If some are covered by a fresh server
		// whose observation just does not mention the expected
		// ReplicaID, that is the "partial inventory on a fresh
		// server" case.
		for _, expectedSlot := range expected.Slots {
			if coveredReplicas[expectedSlot.ReplicaID] {
				continue
			}
			serverFresh := store.freshnessFor(expectedSlot.ServerID)
			if serverFresh == ServerFresh {
				if store.evaluatedAt.Sub(store.startedAt) < store.pendingGrace {
					pending = true
				} else {
					addReason(ReasonPartialInventory)
				}
			}
		}
	}
	if degradedByMissingOrExpiredCurrent && len(coveredReplicas) == 0 {
		addReason(ReasonStaleObservation)
		for _, expectedSlot := range expected.Slots {
			if store.freshnessFor(expectedSlot.ServerID) == ServerExpired {
				addOffender(expectedSlot.ServerID)
			}
		}
	}

	// Collapse: unsupported wins over pending.
	if len(reasons) > 0 {
		sort.Strings(reasons)
		return volumeVerdict{
			state:          SupportabilityUnsupported,
			reasons:        reasons,
			offendingFacts: offenders,
			note:           fmt.Sprintf("%d reason(s); %d offending fact(s)", len(reasons), len(offenders)),
		}
	}

	// If no unsupported reason but some slot needed grace, the
	// volume is pending.
	if pending {
		return volumeVerdict{
			state:   SupportabilityPending,
			reasons: []string{ReasonPartialInventory},
			note:    "within bootstrap pending grace",
		}
	}

	// Otherwise, supported — assemble the VolumeTopologySnapshot.
	return volumeVerdict{
		state:    SupportabilitySupported,
		topology: assembleVolumeTopology(volumeID, expected, freshSlots, authorityLine, hasAuthorityLine),
	}
}

// assembleVolumeTopology builds the VolumeTopologySnapshot for a
// supported volume. Slot order follows expected.Slots so the
// controller sees deterministic input. Each expected slot maps to
// a fresh SlotFact produced by the expected ServerID.
func assembleVolumeTopology(
	volumeID string,
	expected VolumeExpected,
	freshSlots []slotWithServer,
	authorityLine AuthorityBasis,
	hasAuthorityLine bool,
) VolumeTopologySnapshot {
	slots := make([]ReplicaCandidate, 0, len(expected.Slots))

	byReplicaID := map[string]slotWithServer{}
	for _, f := range freshSlots {
		if _, ok := byReplicaID[f.slot.ReplicaID]; !ok {
			byReplicaID[f.slot.ReplicaID] = f
		}
	}

	for _, e := range expected.Slots {
		f, ok := byReplicaID[e.ReplicaID]
		if !ok {
			placeholder := ReplicaCandidate{
				ReplicaID: e.ReplicaID,
				ServerID:  e.ServerID,
			}
			if hasAuthorityLine && authorityLine.ReplicaID == e.ReplicaID {
				placeholder.DataAddr = authorityLine.DataAddr
				placeholder.CtrlAddr = authorityLine.CtrlAddr
			}
			slots = append(slots, placeholder)
			continue
		}
		slots = append(slots, ReplicaCandidate{
			ReplicaID:       f.slot.ReplicaID,
			ServerID:        e.ServerID,
			DataAddr:        f.slot.DataAddr,
			CtrlAddr:        f.slot.CtrlAddr,
			Reachable:       f.slot.Reachable,
			ReadyForPrimary: f.slot.ReadyForPrimary,
			Eligible:        f.slot.Eligible,
			Withdrawn:       f.slot.Withdrawn,
			EvidenceScore:   f.slot.EvidenceScore,
		})
	}

	// Authority is stamped by BuildSnapshot AFTER this function
	// returns, using the read-only AuthorityBasisReader (publisher
	// current line). Observation institution never mints authority
	// here. The default zero value Authority.Assigned=false is
	// correct for the initial-bind case (no publisher line yet).
	return VolumeTopologySnapshot{
		VolumeID: volumeID,
		Slots:    slots,
	}
}
