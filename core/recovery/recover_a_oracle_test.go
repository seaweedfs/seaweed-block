package recovery

// Pure-function dual-oracle table test for the recover() semantics
// migration. Source: sw-block/design/recover-semantics-adjustment-plan.md
// §8.2 (QA-canonical handoff).
//
// This file is INTENTIONALLY decoupled from production types — no
// imports from core/transport, core/replication, or any other
// production-package. The state struct, oracle functions, and
// counterexample table all live test-local. Per §8.2.8
// anti-discipline check #3: pure-function oracle test must NOT
// import production types.
//
// What this file proves:
//
//   BandOracle (legacy recover(a,b)): walApplied ≥ frozen target Y ∧
//   baseDone. This is the existing CI gate; stays GREEN as the
//   "this slice closed at band Y" semantic.
//
//   RecoverAOracle (§IV.2.1 conjunct): baseDone ∧ PrimaryWalLegOk ∧
//   BarrierWitness with matching engine cut. AchievedLSN reported
//   as witness, NOT compared against TargetLSN. Y is enumerator
//   only.
//
// The 8-case counterexample table (§8.2.3) demonstrates the two
// oracles are mutually independent — Band-true does not imply
// RecoverA-true (cases #1/#6/#7), and Band-false does not imply
// RecoverA-false (case #8). This is the dual-oracle minimal
// expression: neither is implied by the other.
//
// Pinned by §IV.2 / §IV.2.1 / §I P8 (consensus); plan §8.1 + §8.2.

import "testing"

// RecoverState — state vector for dual-oracle table tests. Test-local
// data shape; not coupled to production types per §8.2.1.
type RecoverState struct {
	Cursor           uint64          // shipper cursor (last emitted LSN)
	Head             uint64          // primary's WAL head observed at oracle eval time
	BaseDone         bool            // bitmap MarkBaseComplete fired
	LiveClassEmitted bool            // ever emitted SessionLive frame (PrimaryLiveTail seed)
	WalApplied       uint64          // replica's high-water (legacy semantic)
	TargetLSN        uint64          // session contract Y (audit-band only)
	Barrier          *BarrierWitness // nil if no handshake yet
}

// BarrierWitness — handshake outcome at a specific engine cut.
// AchievedLSN is reported AS-OBSERVED at the cut (witness), not as
// a target-crossing verdict.
type BarrierWitness struct {
	CutID    uint64 // engine-authored CheckpointCutSeq
	Achieved uint64 // replica-reported witness LSN at the cut
	CutMatch bool   // engine cut on req == replica's witness cut
}

// BandOracle — legacy recover(a,b) closure: walApplied ≥ frozen target ∧
// baseDone. Stays in CI as today's GREEN gate; explained as "this slice
// closed at band Y", NOT "recover(a) closure".
//
// Source: §8.2.2.
func BandOracle(s RecoverState) bool {
	return s.BaseDone && s.WalApplied >= s.TargetLSN
}

// RecoverAOracle — §IV.2.1 conjunct: baseDone ∧ PrimaryWalLegOk ∧
// BarrierWitness. PrimaryWalLegOk = PrimaryDebtZero ∨ PrimaryLiveTail.
// Barrier witness must echo a matching engine cut; AchievedLSN reported
// as witness, NOT compared against TargetLSN.
//
// §IV.2(2) guard simplified at the unit layer to LiveClassEmitted;
// component-layer exercises the real guard.
//
// Source: §8.2.2.
func RecoverAOracle(s RecoverState) bool {
	if !s.BaseDone {
		return false
	}
	primaryDebtZero := s.Cursor == s.Head
	primaryLiveTail := s.LiveClassEmitted // §IV.2(2) guard simplified for unit
	walLegOk := primaryDebtZero || primaryLiveTail
	if !walLegOk {
		return false
	}
	if s.Barrier == nil {
		return false
	}
	if !s.Barrier.CutMatch {
		return false
	}
	return true
}

// TestRecoverA_DualOracle_Counterexamples — §8.2.3 8-case table.
//
// Mutual independence demonstration:
//
//   Band-true does NOT imply RecoverA-true:
//     case #1 (FS-1): cursor<head, walApplied≥Y, no barrier
//     case #6 (FS-2): same as #1, barrier explicitly nil
//     case #7 (FS-3): cursor==head, walApplied=Y, but cut mismatch
//
//   Band-false does NOT imply RecoverA-false:
//     case #8 (FS-4 reverse): walApplied<Y, but LiveTail+barrier OK
//
// Sanity rows (#2/#3/#4/#5) confirm both oracles agree where they
// must.
func TestRecoverA_DualOracle_Counterexamples(t *testing.T) {
	cases := []struct {
		name      string
		state     RecoverState
		wantBand  bool
		wantRecA  bool
		labelTag  string
	}{
		{
			// CORE FS-1: walApplied≥Y but shipper still in debt + no barrier witness.
			name: "FS-1_BandTrue_RecoverAFalse_DebtNoBarrier",
			state: RecoverState{
				Cursor: 100, Head: 200, BaseDone: true,
				LiveClassEmitted: false, WalApplied: 200, TargetLSN: 200,
				Barrier: nil,
			},
			wantBand: true, wantRecA: false,
			labelTag: "CORE FS-1",
		},
		{
			// Sanity: fully legitimate close — both agree GREEN.
			name: "Sanity_BothTrue_DebtZero_BarrierMatch",
			state: RecoverState{
				Cursor: 200, Head: 200, BaseDone: true,
				LiveClassEmitted: false, WalApplied: 200, TargetLSN: 200,
				Barrier: &BarrierWitness{CutID: 1, Achieved: 200, CutMatch: true},
			},
			wantBand: true, wantRecA: true,
			labelTag: "sanity",
		},
		{
			// Sanity: head moved past target during session; LiveTail+barrier covers.
			name: "Sanity_BothTrue_HeadAdvancedPostTarget_LiveTail",
			state: RecoverState{
				Cursor: 200, Head: 300, BaseDone: true,
				LiveClassEmitted: true, WalApplied: 200, TargetLSN: 200,
				Barrier: &BarrierWitness{CutID: 1, Achieved: 200, CutMatch: true},
			},
			wantBand: true, wantRecA: true,
			labelTag: "sanity",
		},
		{
			// Sanity: baseDone=false; both oracles reject.
			name: "Sanity_BothFalse_BaseNotDone",
			state: RecoverState{
				Cursor: 200, Head: 200, BaseDone: false,
				WalApplied: 200, TargetLSN: 200,
			},
			wantBand: false, wantRecA: false,
			labelTag: "sanity",
		},
		{
			// Sanity: neither WAL nor BASE complete; both reject.
			name: "Sanity_BothFalse_NeitherReached",
			state: RecoverState{
				Cursor: 100, Head: 200, BaseDone: false,
				WalApplied: 150, TargetLSN: 200,
			},
			wantBand: false, wantRecA: false,
			labelTag: "sanity",
		},
		{
			// CORE FS-2: pair with #1 — emphasize barrier=nil alone disqualifies RecoverA.
			name: "FS-2_BandTrue_RecoverAFalse_BarrierNil",
			state: RecoverState{
				Cursor: 100, Head: 200, BaseDone: true,
				LiveClassEmitted: false, WalApplied: 200, TargetLSN: 200,
				Barrier: nil,
			},
			wantBand: true, wantRecA: false,
			labelTag: "CORE FS-2",
		},
		{
			// CORE FS-3: cut mismatch — Band can't see cut id, RecoverA must reject.
			name: "FS-3_BandTrue_RecoverAFalse_CutMismatch",
			state: RecoverState{
				Cursor: 200, Head: 200, BaseDone: true,
				WalApplied: 200, TargetLSN: 200,
				Barrier: &BarrierWitness{CutID: 1, Achieved: 200, CutMatch: false},
			},
			wantBand: true, wantRecA: false,
			labelTag: "CORE FS-3",
		},
		{
			// CORE FS-4 (reverse): walApplied<Y but LiveTail+barrier hold —
			// RecoverA done, Band misses. Proves Y is enumerator only.
			name: "FS-4_BandFalse_RecoverATrue_LiveTailBelowTarget",
			state: RecoverState{
				Cursor: 100, Head: 200, BaseDone: true,
				LiveClassEmitted: true, WalApplied: 150, TargetLSN: 200,
				Barrier: &BarrierWitness{CutID: 1, Achieved: 150, CutMatch: true},
			},
			wantBand: false, wantRecA: true,
			labelTag: "CORE FS-4 (reverse)",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotBand := BandOracle(tc.state)
			gotRecA := RecoverAOracle(tc.state)
			if gotBand != tc.wantBand {
				t.Errorf("[%s] BandOracle = %v, want %v", tc.labelTag, gotBand, tc.wantBand)
			}
			if gotRecA != tc.wantRecA {
				t.Errorf("[%s] RecoverAOracle = %v, want %v", tc.labelTag, gotRecA, tc.wantRecA)
			}
		})
	}
}

// TestRecoverA_DualOracle_MutualIndependence — explicit pin of the
// dual-oracle minimal claim: neither implies the other across the
// counterexample set.
func TestRecoverA_DualOracle_MutualIndependence(t *testing.T) {
	// Band-true ∧ RecoverA-false witnesses (FS-1, FS-2, FS-3 from §8.2.3):
	bandTrueRecAFalse := []RecoverState{
		// FS-1
		{Cursor: 100, Head: 200, BaseDone: true,
			LiveClassEmitted: false, WalApplied: 200, TargetLSN: 200,
			Barrier: nil},
		// FS-3
		{Cursor: 200, Head: 200, BaseDone: true,
			WalApplied: 200, TargetLSN: 200,
			Barrier: &BarrierWitness{CutID: 1, Achieved: 200, CutMatch: false}},
	}
	for i, s := range bandTrueRecAFalse {
		if !BandOracle(s) {
			t.Errorf("witness[%d]: expected Band=true, got false", i)
		}
		if RecoverAOracle(s) {
			t.Errorf("witness[%d]: expected RecoverA=false, got true (Band ⇒ RecoverA would be a false implication)", i)
		}
	}

	// Band-false ∧ RecoverA-true witnesses (FS-4 from §8.2.3):
	bandFalseRecATrue := []RecoverState{
		// FS-4
		{Cursor: 100, Head: 200, BaseDone: true,
			LiveClassEmitted: true, WalApplied: 150, TargetLSN: 200,
			Barrier: &BarrierWitness{CutID: 1, Achieved: 150, CutMatch: true}},
	}
	for i, s := range bandFalseRecATrue {
		if BandOracle(s) {
			t.Errorf("witness[%d]: expected Band=false, got true", i)
		}
		if !RecoverAOracle(s) {
			t.Errorf("witness[%d]: expected RecoverA=true, got false (¬Band ⇒ ¬RecoverA would be a false implication; Y is enumerator only, not terminal)", i)
		}
	}
}
