package authority

import "time"

// ============================================================
// P14 S6 — Convergence Institution
//
// This file contains the bookkeeping types and helpers for the
// convergence loop: supersede detection, bounded-wait stuck
// evaluation, and per-volume ConvergenceStuckEvidence.
//
// S6 scope (sw-block/design/v3-phase-14-s6-sketch.md):
//   - convergence NEVER mints authority — only the publisher does
//   - retry is PASSIVE (§7.1): the controller does not re-emit
//     into the publisher; it waits for the next observation and
//     re-evaluates via the existing decision + same-ask-dedupe
//     path. Bounded-wait + stuck evidence is the surface, not
//     re-drive.
//   - ConvergenceStuck evidence lives in its OWN map, distinct
//     from UnsupportedEvidence (§10). Supported-volume paths
//     clear the unsupported map; clearing stuck evidence is
//     driven by the desired lifecycle, not by supportability.
// ============================================================

// ReasonConvergenceStuck is the pinned reason constant recorded
// on every ConvergenceStuckEvidence value. Distinct from the S4
// supportability reasons (PartialInventory, etc.) so consumers
// cannot blur "do not act because topology unsupported" with
// "desired still wanted but not observed yet".
const ReasonConvergenceStuck = "ConvergenceStuck"

// ConvergenceStuckEvidence is the per-volume record that a
// desired assignment remained unconfirmed past RetryWindow. It
// is surfaced to 14A review / future operator diagnostics, but
// the desired entry itself is NOT cleared by going Stuck — the
// controller still wants that outcome and a later observation
// can still confirm it.
//
// A volume's ConvergenceStuckEvidence is cleared when:
//   1. The desired entry clears (Confirmed, Cleared by
//      supportability transition, or Superseded).
//   2. A new desired entry replaces the old one (the new entry
//      starts its own DesiredAt clock — the prior stuck record
//      no longer describes outstanding convergence).
//
// Supportability regaining does NOT touch stuck evidence.
type ConvergenceStuckEvidence struct {
	VolumeID      string
	Ask           AssignmentAsk
	ProposedBasis AuthorityBasis
	DesiredAt     time.Time
	StuckAt       time.Time
	Reason        string
}

// supersedeDesiredLocked applies §9 supersede case 1 for one
// supported volume: publisher line is strictly newer than
// ProposedBasis AND targets a replica other than the desired
// target. This is the "publisher itself has moved past the
// basis we authored against, to somewhere else" case — genuine
// supersede.
//
// Sketch §9 case 2 (observation and publisher agree on a
// different replica) is intentionally NOT implemented: during a
// normal in-flight Reassign from r1→r2, both observation and
// publisher correctly still report r1 up until the publisher
// applies the ask. A literal case-2 rule would mis-classify
// every Reassign as "superseded before it could publish." Case
// 1's strict-epoch-advance gate already captures every real
// supersede: if publisher has moved elsewhere, its Epoch has
// advanced past ProposedBasis.
//
// Rule 3 (decision table produces a different ask) is handled
// implicitly by the same-ask dedupe in the supported-volume
// loop: a differing ask simply overwrites the desired entry at
// enqueue time, and clearConvergenceStuckLocked is called to
// drop any prior stuck record. Rule 4 (supportability
// transition) is handled by the pending/unsupported intake
// path, which calls clearDesiredLocked.
//
// Normal-lag cases that are NOT supersede (confirmation waits
// them out):
//
//   - observed ReplicaID ≠ desired target while publisher still
//     matches ProposedBasis (publisher hasn't applied yet)
//   - publisher matches desired target while observation still
//     reports old authority (observation lags publisher)
//   - publisher matches desired target and observation matches
//     publisher (Active → None via confirmDesiredLocked)
//
// Returns true if the desired was superseded (and removed).
func (c *TopologyController) supersedeDesiredLocked(vol VolumeTopologySnapshot, currentLine AuthorityBasis, hasLine bool) bool {
	desired, ok := c.desired[vol.VolumeID]
	if !ok {
		return false
	}
	if hasLine &&
		currentLine.Epoch > desired.ProposedBasis.Epoch &&
		currentLine.ReplicaID != desired.Ask.ReplicaID {
		c.clearDesiredLocked(vol.VolumeID)
		return true
	}
	return false
}

// evaluateStuckLocked transitions an unconfirmed desired entry
// to Stuck once DesiredAt + RetryWindow has elapsed. Stuck does
// not re-emit; it records evidence and updates the desired entry
// in place so future observations can still confirm / supersede.
//
// LastRetryAt is updated on every call (even before Stuck fires)
// so §13 proof row 12 "retry state is per-volume" can observe
// that the clock is per-desired, not global.
func (c *TopologyController) evaluateStuckLocked(volumeID string) {
	desired, ok := c.desired[volumeID]
	if !ok {
		return
	}
	now := c.now()
	desired.LastRetryAt = now
	if !desired.Stuck && !desired.DesiredAt.IsZero() &&
		now.Sub(desired.DesiredAt) >= c.config.RetryWindow {
		desired.Stuck = true
		c.convergenceStuck[volumeID] = &ConvergenceStuckEvidence{
			VolumeID:      volumeID,
			Ask:           desired.Ask,
			ProposedBasis: desired.ProposedBasis,
			DesiredAt:     desired.DesiredAt,
			StuckAt:       now,
			Reason:        ReasonConvergenceStuck,
		}
	}
	c.desired[volumeID] = desired
}

// clearConvergenceStuckLocked removes the stuck evidence for one
// volume. Called from every desired-clearing path (confirmed,
// superseded, supportability-cleared) so evidence never outlives
// the outstanding desired.
func (c *TopologyController) clearConvergenceStuckLocked(volumeID string) {
	delete(c.convergenceStuck, volumeID)
}
