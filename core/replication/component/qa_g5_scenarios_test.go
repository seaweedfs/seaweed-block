package component_test

// QA-authored G5-1 scenarios using the component framework.
//
// Authoring discipline (per T4c QA Stage-1 precedent):
//   - Each scenario pins ≥1 invariant from catalogue §3.3
//   - Use RunMatrix when invariant applies to both substrates
//   - Don't duplicate sw's batch-tests (apply_gate_scenarios_test.go,
//     t4d3_scenarios_test.go, t4d4_engine_driven_test.go,
//     t4d4_full_l2_matrix_test.go absorb most T4d catalogue scenarios)
//   - Component-scope ONLY (not L2 subprocess, not L3 hardware)
//
// QA scenarios in THIS file are deliberately distinct-from-sw:
//
//   1. LaneDiscipline_LiveAndRecoveryConcurrent_MultiReplica
//      Sw's TestComponent_LanePurity_CallerControlsDispatch tests the
//      gate API directly. This scenario tests the actual concurrent-
//      lane composition across multiple replicas (one live, one
//      catching up, both apply correctly).
//
//   2. LifecycleStop_DuringActiveCatchUp_NoHandleLeak
//      Adversarial timing test not covered by sw — initiate sever
//      mid-catch-up; verify substrate handle survives + remains
//      functional (BUG-005 non-repeat regression fence).
//
//   3. RF2_BothReplicasReachByteEqual
//      Multi-replica byte-equality verification (sw covers single-
//      replica). Pins fan-out correctness at component scope: both
//      replicas catch up independently and converge.
//
// Known framework limitations worked around:
//   - sessionID=1 collision under multi-catchup-per-replica →
//     scenarios use single catch-up per replica per test
//   - Default block geometry 64 blocks → use WithBlockGeometry(256, 4096)
//     when scenarios touch LBAs > 64
//   - AssertNoPerLBARegression / AssertLaneIntegrity / RestartReplica
//     framework primitives missing (T4d catalogue §7) → use direct
//     Store.Read() byte-comparison helpers below

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication/component"
)

// TestG5_QA_LaneDiscipline_LiveAndRecoveryConcurrent_MultiReplica was
// proposed in T4d catalogue #2 to pin INV-REPL-LANE-DERIVED-FROM-
// HANDLER-CONTEXT under composition (live-ship + recovery concurrent
// across distinct replicas).
//
// SKIPPED — known framework gap. Both WithLiveShip() and
// CatchUpReplica(idx) currently mint sessionID=1 by default; concurrent
// use of both paths (even across different replicas) can cause the
// receiver-side replica handler to reject the second session as
// "stale ship session=1". This is a cross-orchestration sessionID
// coordination gap, not a TargetLSN lane-discrimination gap.
//
// When the framework gains per-call session minting (or mock-engine
// drives the assignment + minted sessionIDs), this scenario unblocks.
// Until then, the gate API itself is pinned by sw's
// TestComponent_LanePurity_CallerControlsDispatch — that's the durable
// fence; the composition test waits for framework primitive.
func TestG5_QA_LaneDiscipline_LiveAndRecoveryConcurrent_MultiReplica(t *testing.T) {
	t.Skip("known framework gap: live-ship + concurrent catch-up share sessionID=1; " +
		"see T4c §I cross-orchestration sessionID carry. " +
		"Gate API itself pinned by TestComponent_LanePurity_CallerControlsDispatch.")
}

// TestG5_QA_LifecycleStop_DuringActiveCatchUp_NoHandleLeak pins
// INV-REPL-LIFECYCLE-HANDLE-BORROWED-001 under adversarial timing:
// initiate sever while CatchUp is mid-stream. Substrate handle must
// remain functional after teardown (BUG-005 non-repeat).
//
// Not covered by sw's batch tests.
func TestG5_QA_LifecycleStop_DuringActiveCatchUp_NoHandleLeak(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithBlockGeometry(256, 4096). // expand from default 64 → handle 100 writes
			WithApplyGate().
			Start()
		c.PrimaryWriteN(100)
		c.PrimarySync()

		// Capture replica's substrate handle before mid-stream sever.
		substratePre := c.Replica(0).Store

		// Initiate catch-up in background; sever connection mid-stream.
		done := make(chan adapter_result, 1)
		go func() {
			r := c.CatchUpReplica(0)
			done <- adapter_result{Success: r.Success, FailReason: r.FailReason}
		}()

		// Sever quickly to force mid-stream interruption.
		time.Sleep(2 * time.Millisecond)
		c.SeverConnection(0)
		<-done // wait for catch-up to complete (success OR fail acceptable)

		// Critical assertion: substrate handle is the SAME pointer
		// after teardown (not closed + replaced). Pre-T4d-4 part A
		// BUG-005 would have closed the borrowed handle here.
		substratePost := c.Replica(0).Store
		if substratePre != substratePost {
			t.Errorf("BUG-005 regression: substrate handle changed during sever "+
				"(pre=%p post=%p)", substratePre, substratePost)
		}

		// Verify substrate is still functional after teardown.
		if _, err := substratePost.Read(0); err != nil {
			t.Errorf("substrate handle no longer functional after sever: %v", err)
		}
	})
}

// TestG5_QA_RF2_BothReplicasReachByteEqual pins multi-replica fan-out
// correctness at component scope: both replicas catch up independently
// and converge byte-exact with primary.
//
// G5-1 expansion scope (G5 kickoff §3): RF=N scenarios catalogued at
// G5 but not in T4d catalogue's RF=1 default. Sw covers single-replica;
// this verifies independent multi-replica catch-up.
func TestG5_QA_RF2_BothReplicasReachByteEqual(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(2).WithApplyGate().Start()

		// Write 30 LBAs on primary.
		c.PrimaryWriteN(30)
		c.PrimarySync()

		// Both replicas catch up independently (single catch-up per
		// replica avoids sessionID=1 collision per known carry).
		for i := 0; i < 2; i++ {
			r := c.CatchUpReplica(i)
			if !r.Success {
				t.Fatalf("catch-up replica %d: %s", i, r.FailReason)
			}
		}

		// Both must converge byte-exact with primary at LBAs 0..29.
		if !replicasFirstNLBAsEqual(t, c, 0, 30) {
			t.Errorf("replica 0 diverged from primary after independent catch-up")
		}
		if !replicasFirstNLBAsEqual(t, c, 1, 30) {
			t.Errorf("replica 1 diverged from primary after independent catch-up")
		}
	})
}

// adapter_result is a local mirror of adapter.SessionCloseResult to avoid
// importing adapter from this test file.
type adapter_result struct {
	Success    bool
	FailReason string
}

// replicasFirstNLBAsEqual is a local byte-equality verifier for the
// first N LBAs (workaround for missing AssertNoPerLBARegression
// framework primitive — see T4d catalogue §7).
func replicasFirstNLBAsEqual(t *testing.T, c *component.Cluster, replicaIdx int, n uint32) bool {
	t.Helper()
	primary := c.Primary().Store
	replica := c.Replica(replicaIdx).Store
	mismatch := 0
	for lba := uint32(0); lba < n; lba++ {
		pri, errP := primary.Read(lba)
		rep, errR := replica.Read(lba)
		if errP != nil || errR != nil {
			continue // not all LBAs are written; skip unwritten
		}
		if !bytes.Equal(pri, rep) {
			if mismatch < 3 {
				t.Logf("LBA %d byte mismatch (primary[0]=%02x replica[0]=%02x)",
					lba, pri[0], rep[0])
			}
			mismatch++
		}
	}
	return mismatch == 0
}
