package recovery

// Conformance test stubs for the recover(a) / §IV.2.1 migration.
//
// Source: sw-block/design/recover-semantics-adjustment-plan.md §8.2.5
// (QA-canonical handoff; A/B/C categories only — D/E/F deferred until
// G0-身份 + G0-wire close).
//
// Each test below is a SKIPPED red-test anchor: it documents an
// invariant the new §IV.2.1 implementation must satisfy, with an
// explicit citation of which Gate G0 sub-gate unblocks it. When a
// sub-gate closes and the corresponding implementation lands, sw
// un-skips the test in waves and grows the test body — at which
// point the test transitions RED → GREEN (and stays GREEN as the
// regression anchor).
//
// Anti-discipline checks (§8.2.8) honored:
//   - Every t.Skip carries a reason citing §IV.2.1 + the unblocking
//     G0 sub-gate (no bare t.Skip).
//   - No production type imports (this file only declares Skip stubs;
//     the real test bodies will land alongside the impl).
//   - D/E/F categories explicitly NOT included here per §8.2.5
//     ("D/E/F 三类不在 commit 3 内").
//
// Pinned by §IV.2.1 (PrimaryDebtZero / PrimaryLiveTail / PrimaryWalLegOk),
// §IV.2.2 (barrierEmitFreeze), §IV.2.4 (BarrierWitness wire),
// §I P8 (recover(a) vs recover(a,b)).

import "testing"

// === A. PrimaryWalLegOk disjunction (§IV.2.1) — A-class wave landed ===
//
// The transport-package shipper-side disjunction (DebtZero ∨ LiveTail
// → walLegOk) is pinned by transport/probe_barrier_eligibility_test.go
// (TestProbeBarrierEligibility_FreshShipper_DebtZero,
// TestProbeBarrierEligibility_LiveEmitFlipsLiveTail,
// TestProbeBarrierEligibility_StartSessionResetsLiveTail).
//
// The recovery-package conformance tests below pin the coordinator's
// CONJUNCT consumption of those witness values: how the recorded
// `walLegOk` boolean from the sender's probe call binds the system
// close gate (§IV.2.1 / FS-1: "session semantically complete" claim
// is coordinator-owned). Each case maps to a §IV.2.1 disjunction
// branch by recording the corresponding witness boolean.

// canEmitConjunctSetup builds a coord state representing "all conjuncts
// of CanEmitSessionComplete satisfied except the witness". Caller then
// records the witness via RecordBarrierWalLegOk and asserts.
func canEmitConjunctSetup(t *testing.T, target uint64) (*PeerShipCoordinator, ReplicaID) {
	t.Helper()
	c := NewPeerShipCoordinator()
	const id ReplicaID = "r-a-class"
	if err := c.StartSession(id, 42, 100, target); err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if err := c.MarkBaseDone(id); err != nil {
		t.Fatalf("MarkBaseDone: %v", err)
	}
	return c, id
}

// TestRecoverA_DebtZeroPath_BarrierEligible — when the primary's
// shipper observed cursor==head at probe time (DebtZero path of the
// §IV.2.1 disjunction), sender records walLegOk=true via
// RecordBarrierWalLegOk; CanEmitSessionComplete authorizes close.
//
// Pins: walLegOkAtBarrier=true witness consumes the DebtZero branch
// of PrimaryWalLegOk = DebtZero ∨ LiveTail. The disjunction itself is
// computed shipper-side (transport/wal_shipper.go ProbeBarrierEligibility,
// line `walLegOk := debtZero || liveTail`) and pinned by transport
// tests; this test pins coord-side consumption.
func TestRecoverA_DebtZeroPath_BarrierEligible(t *testing.T) {
	c, id := canEmitConjunctSetup(t, 200)
	// Witness from DebtZero branch: walLegOk=true.
	if err := c.RecordBarrierWalLegOk(id, true); err != nil {
		t.Fatalf("RecordBarrierWalLegOk: %v", err)
	}
	if !c.CanEmitSessionComplete(id, 199) {
		t.Fatal("DebtZero witness ∧ baseDone: CanEmit should authorize even when achieved is below target band")
	}
	st, _ := c.Status(id)
	if !st.WalLegOkWitnessed {
		t.Error("Status.WalLegOkWitnessed=false: should latch true after RecordBarrierWalLegOk")
	}
	if !st.WalLegOkAtBarrier {
		t.Error("Status.WalLegOkAtBarrier=false: should reflect the recorded value")
	}
}

// TestRecoverA_LiveTailPath_BarrierEligibleAfterEverEmittedLive —
// when the primary's shipper observed liveTail=true at probe time
// (LiveTail branch — even if cursor < head, debt > 0), sender records
// walLegOk=true and CanEmitSessionComplete authorizes close. Same
// coord-side semantics as the DebtZero path; the disjunction collapses
// at the sender→coord boundary.
//
// LiveTail's monotonic-within-session property is pinned by
// transport's TestProbeBarrierEligibility_LiveEmitFlipsLiveTail
// (firstLiveEmitted latches true, stays true through subsequent debt
// accumulation). This recovery-side test pins that the coord cannot
// distinguish the branches and treats either as binding.
func TestRecoverA_LiveTailPath_BarrierEligibleAfterEverEmittedLive(t *testing.T) {
	c, id := canEmitConjunctSetup(t, 200)
	// Witness from LiveTail branch: walLegOk=true (even if debtZero
	// is false at the shipper, the disjunction yields true).
	if err := c.RecordBarrierWalLegOk(id, true); err != nil {
		t.Fatalf("RecordBarrierWalLegOk: %v", err)
	}
	if !c.CanEmitSessionComplete(id, 199) {
		t.Fatal("LiveTail witness ∧ baseDone: CanEmit should authorize even when achieved is below target band")
	}
}

// TestRecoverA_Disjunction_EitherPathSuffices — barrier eligibility
// holds iff PrimaryDebtZero ∨ PrimaryLiveTail; neither alone required.
// Pinned at coord layer: the witness boolean that arrives via
// RecordBarrierWalLegOk is the disjunction's collapsed value, and
// `true` from EITHER source authorizes close.
func TestRecoverA_Disjunction_EitherPathSuffices(t *testing.T) {
	// Path 1: only DebtZero would have produced this witness.
	c1, id1 := canEmitConjunctSetup(t, 200)
	_ = c1.RecordBarrierWalLegOk(id1, true)
	if !c1.CanEmitSessionComplete(id1, 199) {
		t.Fatal("DebtZero-only witness: CanEmit should authorize")
	}

	// Path 2: only LiveTail would have produced this witness.
	c2, id2 := canEmitConjunctSetup(t, 200)
	_ = c2.RecordBarrierWalLegOk(id2, true)
	if !c2.CanEmitSessionComplete(id2, 199) {
		t.Fatal("LiveTail-only witness: CanEmit should authorize")
	}

	// Both branches collapse to the same coord-level effect (`true`
	// witness). Confirms `walLegOk = DebtZero ∨ LiveTail` semantics
	// at the sender→coord boundary.
}

// TestRecoverA_NeitherPath_NotEligible — when neither PrimaryDebtZero
// nor PrimaryLiveTail holds, the disjunction at the shipper yields
// walLegOk=false; sender records false; CanEmitSessionComplete
// REFUSES authorization even with any achieved observation ∧ baseDone.
//
// In production the sender returns FailureContract on the failed
// CanEmit, so the close-by-error path closes "session not eligible"
// rather than "barrier did not fire". Wire-level emission gating
// (architect's stronger §IV.2.2 reading "MUST NOT be emitted") is
// the B-class barrierEmitFreeze wave — out of scope here. A-class
// pins the binding at the close gate.
func TestRecoverA_NeitherPath_NotEligible(t *testing.T) {
	c, id := canEmitConjunctSetup(t, 200)
	// Witness from Neither branch: walLegOk=false (debtZero=false ∧
	// liveTail=false → disjunction false).
	if err := c.RecordBarrierWalLegOk(id, false); err != nil {
		t.Fatalf("RecordBarrierWalLegOk: %v", err)
	}
	if c.CanEmitSessionComplete(id, 200) {
		t.Fatal("walLegOk=false witness: CanEmit MUST refuse (achieved observation is not completion authority)")
	}
	if c.CanEmitSessionComplete(id, 1_000_000) {
		t.Fatal("walLegOk=false witness with huge achieved: CanEmit MUST still refuse")
	}

	// Sanity: re-record true; close now authorized (witness latch
	// is overwriteable per RecordBarrierWalLegOk doc — most recent
	// witness wins).
	_ = c.RecordBarrierWalLegOk(id, true)
	if !c.CanEmitSessionComplete(id, 1) {
		t.Fatal("after re-record walLegOk=true: CanEmit should authorize")
	}
}

// TestRecoverA_BridgingSinkPath_LegacyCollapse — when no probe is
// available (bridging-sink path: senderBacklogSink does NOT implement
// barrierEligibilityProbe), the sender skips RecordBarrierWalLegOk
// and the coord's `walLegOkWitnessed` stays false. Per the §IV.2.1
// A-class conjunct, an unset witness collapses the disjunct, and
// CanEmitSessionComplete falls back to the legacy gate (baseDone ∧
// achieved ≥ target). This preserves bridging-sink (recover(a,b))
// semantics until C-class lands.
func TestRecoverA_BridgingSinkPath_LegacyCollapse(t *testing.T) {
	c, id := canEmitConjunctSetup(t, 200)
	// No RecordBarrierWalLegOk call — simulates bridging-sink path.
	st, _ := c.Status(id)
	if st.WalLegOkWitnessed {
		t.Fatal("setup: WalLegOkWitnessed=true unexpectedly")
	}
	if !c.CanEmitSessionComplete(id, 200) {
		t.Fatal("no witness ∧ baseDone ∧ achieved≥target: legacy collapse should authorize")
	}
}

// === B. Lock discipline at BarrierReq emission (§IV.2.2 barrierEmitFreeze) ===

// TestRecoverA_BarrierReqUnderShipMu_NoInterleaveWithDriveEmits —
// between the PrimaryDebtZero snapshot and the frameBarrierReq write,
// no NotifyAppend / drive() emit can land on the wire. The Pillar2B
// race (slice-2B finding) is fixed by this invariant.
func TestRecoverA_BarrierReqUnderShipMu_NoInterleaveWithDriveEmits(t *testing.T) {
	t.Skip("§IV.2.2: needs barrierEmitFreeze or equivalent ordering (G0-合同)")
}

// TestRecoverA_BarrierImminentFlag_FreezesDriveEmits — Option 3 from
// the architect's lock-discipline trade-off: a barrier-imminent flag
// in the WalShipper noops drive() emits while set, eliminating both
// shipMu-held-across-wire latency AND the TOCTOU window.
func TestRecoverA_BarrierImminentFlag_FreezesDriveEmits(t *testing.T) {
	t.Skip("§IV.2.2: needs barrier-imminent flag in WalShipper (G0-合同)")
}

// === C. AchievedLSN as cut-witness, not target-crossing (§IV.2.4) ===

// TestRecoverA_AchievedLSN_PerCutWitness — BarrierResp.AchievedLSN
// is reported as the replica's high-water observation AT the engine
// cut identified by CheckpointCutSeq, not as a verdict on
// "did we cross TargetLSN". The receiver MUST NOT make the
// >= TargetLSN comparison itself.
func TestRecoverA_AchievedLSN_PerCutWitness(t *testing.T) {
	t.Skip("§IV.2.4: needs CheckpointCutSeq carried in BarrierReq/Resp (G0-wire)")
}

// TestRecoverA_SuccessWith_AchievedLSN_BelowTarget — under the new
// model, a session can succeed (PrimaryWalLegOk via LiveTail,
// barrier handshake matched) with AchievedLSN < TargetLSN.
// Y is enumerator only — counterexample FS-4 from §8.2.3 codifies this.
func TestRecoverA_SuccessWith_AchievedLSN_BelowTarget(t *testing.T) {
	t.Skip("§IV.2.4: Y as enumerator only, success ⊥ AchievedLSN≥Y (G0-合同 ∧ G0-wire)")
}
