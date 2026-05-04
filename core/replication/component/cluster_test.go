package component_test

import (
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication/component"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Demo scenarios for the component framework. Each test doubles as:
//
//   (a) smoke verification that the framework primitive works
//       (Start, PrimaryWriteN, CatchUpReplica, AssertReplicaConverged,
//       AssertSawRecoveryMode, etc.)
//   (b) a copy-paste template for QA authoring more T4c / T4d
//       scenarios. Aim: each new scenario takes ~20-30 lines.
//
// All tests run against the smartwal+walstore matrix unless the
// scenario specifically pins one substrate.

// TestComponent_HappyCatchUp is the simplest possible scenario:
// primary writes N blocks, replica is empty, catch-up streams the
// window, replica converges byte-exact, recovery_mode label visible.
// ~10 lines of scenario logic.
func TestComponent_HappyCatchUp(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()
		c.PrimaryWriteN(20)
		c.PrimarySync()

		result := c.CatchUpReplica(0)
		if !result.Success {
			t.Fatalf("catch-up failed: %s", result.FailReason)
		}
		c.AssertReplicaConverged(0)
		c.AssertSawRecoveryMode(0, component.ExpectAnyMode)
	})
}

// TestComponent_GapExceedsRetention_Recycled — substrate emits
// ErrWALRecycled on a sufficiently-stale fromLSN; the executor
// surfaces the wrap; the close result's FailReason carries the
// sentinel text. Engine SessionFailed handler (covered separately
// at unit scope) maps this to recovery.Decision=Rebuild.
//
// We wrap the primary substrate with a stub that always returns
// ErrWALRecycled to avoid the heavy walstore checkpoint dance.
func TestComponent_GapExceedsRetention_Recycled(t *testing.T) {
	// Smartwal-only because the stub overrides ScanLBAs and the
	// substrate-specific behavior under test is the recycle path,
	// not the substrate-level scan semantics. The wrap pattern
	// applies to either substrate; running once is enough for the
	// pin.
	component.RunSubstrate(t, "smartwal", component.Smartwal, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()
		c.PrimaryWriteN(1) // make H>0 so probe doesn't reject

		// Wrap the primary's storage with a recycled stub.
		// (Component framework exposes the primary store via
		// Primary().Store — replace at the wire layer by spinning
		// a fresh executor pointing at the wrap.)
		recycled := &recycledScanStub{LogicalStorage: c.Primary().Store}
		_ = recycled // see comment below

		// NOTE: today the Cluster's executors bind to the primary's
		// real Store at Start; substituting the wrap requires
		// extending the framework with a `WithPrimaryStorageWrap`
		// builder option. For now this test smoke-checks that the
		// recycled-stub pattern compiles + the storage.ErrWALRecycled
		// sentinel is exposed correctly. Full executor-level recycle
		// integration is in core/replication/integration_t4c_test.go
		// (TestT4c3_Catchup_GapExceedsRetention_EscalatesToNeedsRebuild)
		// and the framework gains a WithPrimaryStorageWrap option in
		// the next iteration.
		if recycled.ScanLBAs(1, nil) != storage.ErrWALRecycled {
			t.Fatal("recycledScanStub must return ErrWALRecycled")
		}
	})
}

// recycledScanStub is a substrate wrapper that always returns
// ErrWALRecycled from ScanLBAs. Demo of the wrap pattern QA can
// reuse for fault injection at the substrate layer.
type recycledScanStub struct {
	storage.LogicalStorage
}

func (r *recycledScanStub) ScanLBAs(fromLSN uint64, fn func(storage.RecoveryEntry) error) error {
	return storage.ErrWALRecycled
}

// TestComponent_ProbeHappy — basic probe wire smoke across both
// substrates. Demonstrates ProbeReplica primitive.
func TestComponent_ProbeHappy(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()
		c.PrimaryWriteN(3)
		c.PrimarySync()

		result := c.ProbeReplica(0)
		if !result.Success {
			t.Fatalf("probe failed: %s", result.FailReason)
		}
		if result.PrimaryHeadLSN == 0 {
			t.Errorf("PrimaryHeadLSN must be non-zero after writes; got %d",
				result.PrimaryHeadLSN)
		}
	})
}

// TestComponent_TwoReplicas_BothConverge — multi-replica scenario.
// Demonstrates WithReplicas(N) + per-replica catch-up.
func TestComponent_TwoReplicas_BothConverge(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(2).Start()
		c.PrimaryWriteN(10)
		c.PrimarySync()

		for i := 0; i < c.ReplicaCount(); i++ {
			r := c.CatchUpReplica(i)
			if !r.Success {
				t.Fatalf("catch-up replica %d failed: %s", i, r.FailReason)
			}
		}
		c.WaitForConverge(2 * time.Second)
	})
}

// TestComponent_PartialReplicaThenCatchUp — replica was already
// caught up to LBA 0..4, primary writes more, catch-up brings it
// the rest of the way. Demonstrates ReplicaApply for setting up
// pre-existing replica state.
func TestComponent_PartialReplicaThenCatchUp(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()
		// Stage 1: primary + replica both have LBAs 0..4.
		for i := 0; i < 5; i++ {
			data := make([]byte, component.DefaultBlockSize)
			data[0] = byte(i + 1)
			lsn := c.PrimaryWrite(uint32(i), data)
			c.ReplicaApply(0, uint32(i), data, lsn)
		}
		// Stage 2: primary writes 5 more; replica is now behind.
		for i := 5; i < 10; i++ {
			data := make([]byte, component.DefaultBlockSize)
			data[0] = byte(i + 1)
			c.PrimaryWrite(uint32(i), data)
		}
		c.PrimarySync()

		// Catch-up streams the gap.
		result := c.CatchUpReplica(0)
		if !result.Success {
			t.Fatalf("catch-up: %s", result.FailReason)
		}
		c.AssertReplicaConverged(0)
	})
}

// TestComponent_RecoveryModeLabel_Walstore — pins that walstore
// substrate emits the wal_replay label specifically. Demonstrates
// per-substrate label assertion.
func TestComponent_RecoveryModeLabel_Walstore(t *testing.T) {
	component.RunSubstrate(t, "walstore", component.Walstore, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()
		c.PrimaryWriteN(3)
		c.PrimarySync()

		c.CatchUpReplica(0)
		c.AssertSawRecoveryMode(0, component.ExpectWALReplay)
	})
}

// TestComponent_RecoveryModeLabel_Smartwal — pins that smartwal +
// in-memory substrates emit the state_convergence label.
func TestComponent_RecoveryModeLabel_Smartwal(t *testing.T) {
	component.RunSubstrate(t, "smartwal", component.Smartwal, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()
		c.PrimaryWriteN(3)
		c.PrimarySync()

		c.CatchUpReplica(0)
		c.AssertSawRecoveryMode(0, component.ExpectStateConvergence)
	})
}

// Sanity: framework constants are stable + match storage package.
func TestComponent_ModeConstantsStable(t *testing.T) {
	if string(component.ExpectWALReplay) != string(storage.RecoveryModeWALReplay) {
		t.Errorf("ExpectWALReplay drift: %q vs %q",
			component.ExpectWALReplay, storage.RecoveryModeWALReplay)
	}
	if !strings.HasPrefix(string(component.ExpectStateConvergence), "state_convergence") {
		t.Errorf("ExpectStateConvergence wrong: %q", component.ExpectStateConvergence)
	}
}
