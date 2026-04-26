package component_test

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/replication/component"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// T4d-2 component-scope scenarios for the apply gate. Pins
// round-43/44 invariants at the integration boundary: real wire,
// real substrate, real apply path through the gate.
//
// Mini-plan §2.2 test list — adversarial set + lane-discrimination
// + Option C hybrid + restart-safety.

// --- Round-44 #1: COVERAGE-ADVANCES-ON-SKIP (integration) ---

// TestComponent_RecoveryStaleSkip_CoverageStillAdvances pins
// INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP at the integration
// boundary. Recovery applies LSN=50 first; then a stale LSN=30 over
// the same LBA. Data must NOT regress; recoveryCovered MUST still
// include the LBA after the stale-skip.
func TestComponent_RecoveryStaleSkip_CoverageStillAdvances(t *testing.T) {
	component.RunSubstrate(t, "walstore", component.Walstore, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).WithApplyGate().Start()

		gate := c.ApplyGate(0)
		if gate == nil {
			t.Fatal("WithApplyGate did not install gate")
		}

		// Recovery-lane lineage (TargetLSN > 1).
		lin := transport.RecoveryLineage{
			SessionID: 7, Epoch: 1, EndpointVersion: 1, TargetLSN: 100,
		}

		dataB := makeData(0xBB)
		if err := gate.Apply(lin, 5, dataB, 50); err != nil {
			t.Fatalf("first apply: %v", err)
		}

		dataA := makeData(0xAA)
		if err := gate.Apply(lin, 5, dataA, 30); err != nil {
			t.Errorf("stale recovery apply must NOT error; got %v", err)
		}

		// Coverage advances even though data was skipped.
		if !gate.SessionRecoveryCoverage(7, 5) {
			t.Fatal("FAIL: recoveryCovered MUST advance on stale-skip (round-44 #1)")
		}
		// Data not regressed.
		got, _ := c.Replica(0).Store.Read(5)
		if got[0] != 0xBB {
			t.Errorf("FAIL: stale recovery apply regressed data: %02x, want B", got[0])
		}
	})
}

// --- Round-44 #2: LIVE-LANE-STALE-FAILS-LOUD (integration) ---

// TestComponent_LiveLaneStaleEntry_FailsLoud pins
// INV-REPL-LIVE-LANE-STALE-FAILS-LOUD at the integration boundary.
// Live-lane stale entry MUST return error from gate.Apply (NOT
// silent skip); replica handler logs + drops conn (in real wire).
func TestComponent_LiveLaneStaleEntry_FailsLoud(t *testing.T) {
	component.RunSubstrate(t, "walstore", component.Walstore, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).WithApplyGate().Start()

		gate := c.ApplyGate(0)
		// Live-lane lineage (TargetLSN == liveShipTargetLSN == 1).
		lin := transport.RecoveryLineage{
			SessionID: 99, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
		}

		// Newer apply.
		if err := gate.Apply(lin, 5, makeData(0xBB), 50); err != nil {
			t.Fatalf("first live apply: %v", err)
		}

		// Stale: must fail loud.
		err := gate.Apply(lin, 5, makeData(0xAA), 30)
		if err == nil {
			t.Fatal("FAIL: round-44 INV-REPL-LIVE-LANE-STALE-FAILS-LOUD — live-lane stale MUST return error")
		}
		if !strings.Contains(err.Error(), "INV-REPL-LIVE-LANE-STALE-FAILS-LOUD") {
			t.Errorf("error should reference invariant for traceability: %v", err)
		}

		// recoveryCovered MUST NOT have advanced for live-lane apply.
		if gate.SessionRecoveryCoverage(99, 5) {
			t.Fatal("FAIL: live lane MUST NOT advance recoveryCovered")
		}
	})
}

// --- Lane discrimination (INV-REPL-LANE-DERIVED-FROM-HANDLER-CONTEXT) ---

// TestComponent_LaneDerivedFromTargetLSN pins
// INV-REPL-LANE-DERIVED-FROM-HANDLER-CONTEXT (Q2): the gate reads
// lineage.TargetLSN to discriminate lane. No wire byte; existing
// signal.
func TestComponent_LaneDerivedFromTargetLSN(t *testing.T) {
	component.RunSubstrate(t, "walstore", component.Walstore, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).WithApplyGate().Start()

		gate := c.ApplyGate(0)

		// Same SessionID across two synthetic lineages — the only
		// difference is TargetLSN. Recovery (TargetLSN=100) skips
		// stale; live (TargetLSN=1) fail-louds on stale. Apply newer
		// to recovery first, then attempt stale via live with the
		// SAME sessionID — gate keys appliedLSN by sessionID, so
		// the live apply sees recovery's appliedLSN[5]=50 too. (In
		// production, live + recovery use distinct sessionIDs; this
		// test is structural — pinning the lane discriminator only.)
		recov := transport.RecoveryLineage{
			SessionID: 7, Epoch: 1, EndpointVersion: 1, TargetLSN: 100,
		}
		live := transport.RecoveryLineage{
			SessionID: 7, Epoch: 1, EndpointVersion: 1, TargetLSN: 1,
		}

		// Recovery sets appliedLSN[5]=50 + recoveryCovered.
		gate.Apply(recov, 5, makeData(0xBB), 50)
		if !gate.SessionRecoveryCoverage(7, 5) {
			t.Fatal("recovery lane should have advanced coverage")
		}

		// Live attempts STALE LSN=30 → must fail-loud (lane decided
		// by TargetLSN=1).
		err := gate.Apply(live, 5, makeData(0xAA), 30)
		if err == nil {
			t.Fatal("live lane (TargetLSN=1) should fail-loud on stale")
		}
	})
}

// --- Option C hybrid: substrate seed honored at session start ---

// TestComponent_OptionCHybrid_WalstoreSeed pins that walstore's
// AppliedLSNs() output seeds the gate's session map at init.
func TestComponent_OptionCHybrid_WalstoreSeed(t *testing.T) {
	component.RunSubstrate(t, "walstore", component.Walstore, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).WithApplyGate().Start()

		// Pre-apply on the replica's substrate directly to populate
		// dirty map (which walstore's AppliedLSNs() reads).
		dataPrev := makeData(0xCC)
		if err := c.Replica(0).Store.ApplyEntry(5, dataPrev, 50); err != nil {
			t.Fatal(err)
		}

		gate := c.ApplyGate(0)
		recov := transport.RecoveryLineage{
			SessionID: 7, Epoch: 1, EndpointVersion: 1, TargetLSN: 100,
		}

		// Trigger session init (any apply does it). Use unrelated LBA.
		gate.Apply(recov, 99, makeData(0xCC), 60)

		// Now LBA=5's seed should be visible.
		seedLSN, ok := gate.SessionAppliedLSN(7, 5)
		if !ok {
			t.Fatal("walstore AppliedLSNs seed missing — Option C hybrid broken")
		}
		if seedLSN != 50 {
			t.Errorf("seed appliedLSN[5] = %d, want 50", seedLSN)
		}

		// Stale recovery for LBA=5 at LSN=30 must skip (data unchanged).
		gate.Apply(recov, 5, makeData(0xAA), 30)
		got, _ := c.Replica(0).Store.Read(5)
		if got[0] != 0xCC {
			t.Errorf("FAIL: stale recovery apply regressed substrate; got %02x, want CC", got[0])
		}
	})
}

// helpers
func makeData(marker byte) []byte {
	d := make([]byte, component.DefaultBlockSize)
	d[0] = marker
	return d
}
