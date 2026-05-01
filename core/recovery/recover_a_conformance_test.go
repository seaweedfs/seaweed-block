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

// === A. PrimaryWalLegOk disjunction (§IV.2.1) ===

// TestRecoverA_DebtZeroPath_BarrierEligible — at the moment of
// frameBarrierReq emission, a snapshot taken under the WalShipper
// serializer mutex MUST observe cursor == head. The observation IS
// the eligibility signal — not a post-hoc AchievedLSN comparison.
func TestRecoverA_DebtZeroPath_BarrierEligible(t *testing.T) {
	t.Skip("§IV.2.1: needs PrimaryDebtZero observation hook under shipMu (G0-合同)")
}

// TestRecoverA_LiveTailPath_BarrierEligibleAfterEverEmittedLive —
// once the shipper has emitted at least one SessionLive frame in
// this session, PrimaryLiveTail is set monotonically and stays set
// for the remainder of the session, even if subsequent appends
// extend head past cursor.
func TestRecoverA_LiveTailPath_BarrierEligibleAfterEverEmittedLive(t *testing.T) {
	t.Skip("§IV.2.1: needs PrimaryLiveTail flag + §IV.2(2) guard (G0-合同)")
}

// TestRecoverA_Disjunction_EitherPathSuffices — barrier eligibility
// holds iff PrimaryDebtZero ∨ PrimaryLiveTail; neither alone is
// required. Both sanity rows of §8.2.3 (#2, #3) demonstrate.
func TestRecoverA_Disjunction_EitherPathSuffices(t *testing.T) {
	t.Skip("§IV.2.1: needs PrimaryWalLegOk = DebtZero ∨ LiveTail (G0-合同)")
}

// TestRecoverA_NeitherPath_NotEligible — when neither
// PrimaryDebtZero nor PrimaryLiveTail holds, frameBarrierReq MUST
// NOT be emitted. The shipper waits or escalates, but does not
// silently fire the handshake.
func TestRecoverA_NeitherPath_NotEligible(t *testing.T) {
	t.Skip("§IV.2.1: needs PrimaryWalLegOk=false → barrier MUST NOT fire (G0-合同)")
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
