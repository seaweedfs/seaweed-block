package authority

import "github.com/seaweedfs/seaweed-block/core/adapter"

// testPromotion is a reassign's kind in tests of the publisher's epoch
// mechanics: a promotion decided by the gate.
// testPromotion: the tests here stand in for the master as a planned move.
var testPromotion = adapter.PrimaryEvidence{Kind: adapter.PrimaryKindPromotion, Basis: adapter.BasisPlanned}

// gatedSink stands in, in these tests, for the master's promotion gate: it
// gives every slot the gate would allow (ready, reachable, eligible, not
// withdrawn) the gate's evidence before the controller sees the snapshot. A
// primary change is published only with that evidence, so a route fed by
// heartbeats alone (no gate) promotes nothing.
type gatedSink struct{ ctrl ControllerSink }

func (g gatedSink) SubmitObservedState(snap ClusterSnapshot, report SupportabilityReport) error {
	for vi := range snap.Volumes {
		for si := range snap.Volumes[vi].Slots {
			s := &snap.Volumes[vi].Slots[si]
			if s.ReadyForPrimary && s.Reachable && s.Eligible && !s.Withdrawn {
				s.PromotionEvidence = adapter.PrimaryEvidence{Kind: adapter.PrimaryKindPromotion, DurableLSN: s.EvidenceScore}
			}
		}
	}
	return g.ctrl.SubmitObservedState(snap, report)
}
