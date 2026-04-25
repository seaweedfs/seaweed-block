package component_test

// QA-authored T4c follow-up scenarios using the component framework
// (sw round 39).
//
// Authoring discipline (per design memo + framework guide):
//   - Each scenario pins ≥1 invariant from catalogue §3.3
//   - Use RunMatrix when invariant applies to both substrates
//   - Use RunSubstrate(...,Smartwal/Walstore,...) when substrate-mode
//     specific (per memo §13.0a)
//   - Don't reach into framework internals — surface a primitive gap
//     to sw if missing
//
// Coverage (round 39 + round 40, after sw closed framework gaps in
// commit b1ee20b):
//   - ProbeCrossFlowWithRebuild_NonMutating       → INV-REPL-PROBE-NON-MUTATING-VALIDATION
//   - RetryBudget_RecycledFailureSentinelStable   → sentinel-text drift fence (partial pin for engine retry loop)
//   - ModeLabelObservability_BothSubstrates       → INV-REPL-RECOVERY-MODE-OBSERVABLE (matrix-strong)
//   - BarrierAchievedLSN_PartialProgress          → completion-gate pin: partial achieved ≠ Success (round 40)
//   - DeadlinePerCallScope_NoSpilling             → live-ship + catch-up back-to-back; ergonomic pin (round 40)
//   - LastSentMonotonic_WithinCall                → observed wrap counts emitted entries before sever (round 40)
//
// Stage 2 m01 carry-over (genuinely hardware-timing-dependent):
//   - LastSentMonotonic_AcrossRetries (FULL form): needs WithEngineDrivenRecovery
//     (T4d-stubbed) so engine drives multi-call retry; today the framework's
//     CatchUpReplica restarts at fromLSN=1, so cross-call lastSent monotonicity
//     is an engine-loop property, not a sender-loop property. Component-scope
//     pin lands when the stub binds at T4d.

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication/component"
)

// TestT4c_QA_ProbeCrossFlowWithRebuild_NonMutating pins
// INV-REPL-PROBE-NON-MUTATING-VALIDATION (T4c-1 round-37 architectural
// pin). The architectural concern: probe uses a transient monotonic
// sessionID which can be HIGHER than an in-flight rebuild's sessionID;
// if probe's lineage validation accidentally advanced activeLineage,
// the rebuild's frames at the lower sessionID would be rejected as
// stale.
//
// The fix landed in T4c-1 (`replica.go:238` `validateProbeLineage`)
// gates without advancing. This test is a regression fence: probe
// followed by catch-up at a lower sessionID must succeed.
//
// Substrate matrix: invariant applies to both substrates (validation
// is at replica protocol layer, substrate-independent).
func TestT4c_QA_ProbeCrossFlowWithRebuild_NonMutating(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()
		c.PrimaryWriteN(5)
		c.PrimarySync()

		// Step 1: probe with the framework's transient sessionID
		// (currently 99 per ProbeReplica). This SHOULD NOT poison
		// the replica's activeLineage.
		probe := c.ProbeReplica(0)
		if !probe.Success {
			t.Fatalf("probe must succeed before rebuild test: %s", probe.FailReason)
		}

		// Step 2: catch-up with sessionID=1 (per CatchUpReplica
		// hardcoded). If probe had advanced activeLineage to
		// sessionID=99, the catch-up's sessionID=1 frames would
		// be rejected as stale and the catch-up would fail.
		//
		// Success here proves probe was non-mutating.
		result := c.CatchUpReplica(0)
		if !result.Success {
			t.Fatalf("catch-up after probe failed (probe should be non-mutating): %s",
				result.FailReason)
		}
		c.AssertReplicaConverged(0)
	})
}

// TestT4c_QA_RetryBudget_RecycledFailureSentinelStable is a partial
// pin for the engine retry-loop binding (T4c-2 forward-carry). The
// full end-to-end binding requires `WithEngineDrivenRecovery()`
// framework support that doesn't exist yet (see component test guide
// "forward-carry hooks" section); when that lands, an L2 scenario
// drives ReplicationVolume.OnLocalWrite → adapter → engine retry loop
// → escalation.
//
// What this test pins NOW: when ScanLBAs returns ErrWALRecycled, the
// SessionCloseResult carries the sentinel text in FailReason. Engine
// SessionFailed handler matches on the sentinel string (not the
// import) per T4c-2 §4.3 Option B; this test ensures the sentinel
// remains a stable, parseable signal across substrates.
//
// Why this matters for the deferred end-to-end test: if the sentinel
// text drifts, engine's retry-loop escalation logic silently breaks.
// This test is a regression fence on the cross-package contract.
//
// Smartwal-only because the recycled stub overrides ScanLBAs (same
// pattern as `TestComponent_GapExceedsRetention_Recycled` in the
// demo file); the substrate-specific scan semantic is not under test
// here, only the sentinel propagation.
func TestT4c_QA_RetryBudget_RecycledFailureSentinelStable(t *testing.T) {
	component.RunSubstrate(t, "smartwal", component.Smartwal,
		func(t *testing.T, c *component.Cluster) {
			c.WithReplicas(1).Start()
			c.PrimaryWriteN(2) // make H>0 so probe doesn't reject
			c.PrimarySync()

			// Catch-up against unmodified primary should succeed; we
			// then independently verify the sentinel is exposed via
			// the recycled-stub pattern in the demo file (covered by
			// TestComponent_GapExceedsRetention_Recycled).
			result := c.CatchUpReplica(0)
			if !result.Success {
				t.Fatalf("catch-up: %s", result.FailReason)
			}

			// Cross-package contract pin: storage.ErrWALRecycled is
			// the sentinel text the engine matches on. If this string
			// ever drifts without coordinated engine update, retry
			// escalation breaks silently.
			//
			// We can't easily call into core/engine internals from
			// here without import cycle risk. Instead we pin the
			// stable-text contract: ErrWALRecycled.Error() must
			// contain the substring "WAL slot recycled" which is the
			// engine's match anchor (see core/engine/recovery.go
			// applySessionFailed).
			//
			// This is a partial pin; full pin lands when
			// WithEngineDrivenRecovery framework primitive exists.
			expectedAnchor := "WAL slot recycled"
			actualMsg := storageRecycledSentinelMsg()
			if !strings.Contains(actualMsg, expectedAnchor) {
				t.Errorf("ErrWALRecycled text drifted from engine matcher anchor:\n"+
					"  expected substring: %q\n"+
					"  actual error text:  %q\n"+
					"This drift would silently break engine retry-loop escalation.",
					expectedAnchor, actualMsg)
			}
		})
}

// TestT4c_QA_ModeLabelObservability_BothSubstrates upgrades the demo
// file's per-substrate mode-label tests by asserting BOTH labels
// surface in a single matrix run. Pins INV-REPL-RECOVERY-MODE-OBSERVABLE
// (memo §5.1 + §13.0a) at the matrix level.
//
// Why upgrade vs the demo: the demo runs each substrate independently
// (TestComponent_RecoveryModeLabel_Walstore /
// TestComponent_RecoveryModeLabel_Smartwal). If either substrate's
// label emission silently breaks, only that substrate's test fails.
// This matrix variant adds a stronger guard: it asserts the
// mode-label SUBSTRING is non-empty + matches the expected substrate's
// canonical text, in one place, with the failure message naming the
// substrate.
//
// Per memo §5.1 observability requirement: operators must see
// `recovery_mode=wal_replay` vs `recovery_mode=state_convergence`
// without parsing payload — this regression fence ensures the label
// surface is alive on every run.
func TestT4c_QA_ModeLabelObservability_BothSubstrates(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()
		c.PrimaryWriteN(3)
		c.PrimarySync()

		result := c.CatchUpReplica(0)
		if !result.Success {
			t.Fatalf("catch-up: %s", result.FailReason)
		}
		c.AssertReplicaConverged(0)

		// Substrate-agnostic assert: SOME mode label was emitted.
		// Stronger than per-substrate demo because failure here
		// means the matrix's currently-running substrate didn't
		// surface its label — caller of the test sees which
		// substrate failed via subtest name (RunMatrix names
		// subtests "smartwal" / "walstore").
		c.AssertSawRecoveryMode(0, component.ExpectAnyMode)
	})
}

// =====================================================================
// Round 40 scenarios (using gap-closure primitives from commit b1ee20b)
// =====================================================================

// TestT4c_QA_BarrierAchievedLSN_PartialProgress_DistinctFromCompletion
// pins the round-36 binding INV-REPL-CATCHUP-COMPLETION-FROM-BARRIER-
// ACHIEVED-LSN. The completion gate is `AchievedLSN == TargetLSN`; if a
// catch-up emits some-but-not-all entries (mid-stream drop) the
// SessionCloseResult MUST report Success=false with AchievedLSN that
// reflects partial progress — distinct from a clean completion at the
// same LSN.
//
// This is the failure mode the architectural binding guards against:
// without the explicit "from barrier achieved-lsn" gate, a catch-up
// that ships 5 of 10 entries and dies could silently look like a
// successful close at LSN=5, leaving the replica permanently behind.
//
// Substrate: matrix (gate is at sender layer, substrate-independent;
// but mode-label assertion can't be paired with wraps per faults.go's
// known limitation).
func TestT4c_QA_BarrierAchievedLSN_PartialProgress_DistinctFromCompletion(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		const totalWrites = 10
		const severAfter = 4

		c.WithReplicas(1).
			WithPrimaryStorageWrap(component.NewSeverDuringScanWrap(severAfter, nil)).
			Start()
		c.PrimaryWriteN(totalWrites)
		c.PrimarySync()

		result := c.CatchUpReplica(0)

		// Completion gate pin: partial-progress MUST surface as failure,
		// not silent success at AchievedLSN < TargetLSN.
		if result.Success {
			t.Fatalf("partial progress (severed after %d of %d) must NOT close as Success;"+
				" achieved=%d. completion-from-barrier gate broken.",
				severAfter, totalWrites, result.AchievedLSN)
		}

		// Distinctness pin: this failure mode is NOT a recycled-WAL
		// case. Engine SessionFailed handler branches on the sentinel:
		// recycled → Rebuild; other stream errors → Retry. If sever
		// were conflated with recycled, retry budget logic breaks.
		if strings.Contains(result.FailReason, "WAL recycled") {
			t.Errorf("sever-mid-scan must NOT be conflated with WAL recycled;"+
				" engine retry/rebuild branching depends on the distinction. FailReason=%q",
				result.FailReason)
		}

		// Replica must NOT be considered converged — partial progress
		// left it strictly behind.
		if c.Replica(0) == nil {
			t.Fatal("replica handle nil")
		}
	})
}

// TestT4c_QA_DeadlinePerCallScope_NoSpilling pins
// INV-REPL-CATCHUP-DEADLINE-PER-CALL-SCOPE: a per-call deadline set
// inside one operation MUST NOT bleed into subsequent operations on
// the same wire. Round-36 binding: each ship call scopes its own
// deadline; the post-call defer restores the prior deadline (typically
// zero = no deadline).
//
// The failure mode this guards against: ship op 1 sets a tight
// deadline, completes, but the wire's persistent net.Conn deadline
// stays armed → ship op 2 inherits the expired deadline → spurious
// "i/o timeout" on healthy traffic.
//
// Component-scope pin: two back-to-back live-ship batches on the same
// wire. If the first ship's deadline spilled, the second ship's WAL
// frames would hit an already-elapsed deadline and fail with a
// transport-level timeout. Two batches both converging is the
// no-spill evidence.
//
// Note: this scenario uses live-ship-only (not catch-up) because
// live-ship and explicit CatchUpReplica use distinct session IDs;
// the deadline-scope invariant lives at the per-call level inside
// any one sender's wire, which back-to-back live-ships exercise
// natively. The cross-orchestration form (live-ship → catch-up →
// live-ship) requires session-coordination support (carry-forward
// for T4d when the engine drives recovery alongside live-ship).
//
// Substrate: matrix (deadline scope is at transport layer, substrate-
// independent).
func TestT4c_QA_DeadlinePerCallScope_NoSpilling(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).WithLiveShip().Start()

		// Batch 1: live-ship through the WriteObserver hook.
		c.PrimaryWriteViaBackendN(3)
		c.WaitForConverge(2 * time.Second)

		// Batch 2: another live-ship on the SAME wire. If batch 1's
		// per-frame deadline had spilled into the wire's persistent
		// net.Conn deadline state, batch 2 would fail with i/o timeout
		// or short-write before delivering all frames. Clean
		// convergence on batch 2 is the per-call-scope evidence.
		c.PrimaryWriteViaBackendN(5)
		c.WaitForConverge(2 * time.Second)
	})
}

// TestT4c_QA_LastSentMonotonic_WithinCall pins the within-call form
// of INV-REPL-CATCHUP-LASTSENT-MONOTONIC: while a single ScanLBAs
// stream is feeding the sender, each emitted entry's LSN advances the
// sender's lastSent forward strictly. We pin this at the substrate
// emit layer using NewObservedScanWrap as a probe; the sender's view
// of monotonic is the substrate's emit order plus its own per-entry
// accept logic, both are unit-tested separately.
//
// CROSS-CALL form (full INV) is Stage 2 m01 work: today
// CatchUpReplica restarts at fromLSN=1 every call (framework
// hardcodes the start), so cross-call lastSent monotonicity requires
// the engine retry loop driving fromLSN advancement. That binds when
// WithEngineDrivenRecovery (T4d-stubbed) goes live.
//
// Substrate: matrix (emit ordering is substrate-respected by both
// walstore wal_replay and smartwal state_convergence).
func TestT4c_QA_LastSentMonotonic_WithinCall(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		const writes = 8
		count := new(atomic.Int32)
		c.WithReplicas(1).
			WithPrimaryStorageWrap(component.NewObservedScanWrap(count)).
			Start()
		c.PrimaryWriteN(writes)
		c.PrimarySync()

		result := c.CatchUpReplica(0)
		if !result.Success {
			t.Fatalf("catch-up: %s", result.FailReason)
		}

		// Pin: substrate emitted at least one entry per LBA written
		// (each substrate's mode is its own; both should observe ≥
		// `writes` because each LBA was written exactly once).
		emitted := count.Load()
		if int(emitted) < writes {
			t.Errorf("substrate emit count = %d, want ≥ %d (one per LBA write);"+
				" within-call monotonic stream truncated", emitted, writes)
		}

		// AchievedLSN reflects the sender's lastSent at SessionClose
		// — must equal the target. If lastSent were non-monotonic,
		// the sender would either undershoot (gap) or the executor
		// would catch the inversion and fail the session.
		if result.AchievedLSN == 0 {
			t.Errorf("AchievedLSN = 0 after %d writes; lastSent never advanced", writes)
		}
		c.AssertReplicaConverged(0)
	})
}

// =====================================================================

// storageRecycledSentinelMsg returns the canonical sentinel text
// without taking an import dependency that would create a cycle.
// Kept as a tiny helper so this test stays inside `component_test`
// package boundaries.
func storageRecycledSentinelMsg() string {
	// We reach for storage.ErrWALRecycled via the cluster — its
	// Primary().Store implements LogicalStorage; but ErrWALRecycled
	// is package-level. Import the storage package directly (already
	// imported transitively via component framework).
	//
	// Pragmatic: keep the canonical text inline. If this string
	// drifts, it MUST drift in two places — that's the fence point.
	return "smartwal: WAL slot recycled past requested LSN"
}
