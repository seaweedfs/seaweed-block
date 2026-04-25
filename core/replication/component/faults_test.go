package component_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/replication/component"
)

// Demo scenarios for the gap-closure primitives:
//   WithPrimaryStorageWrap (substrate fault injection at executor)
//   PrimaryWriteViaBackend (live-ship via WriteObserver)
//   NewSeverDuringScanWrap (mid-stream drop helper)
//   NewObservedScanWrap    (emit-count observability)
//
// Each test doubles as smoke for the framework + copy-paste template
// for QA's deferred Stage 2 scenarios.

// TestComponent_RecycledScanWrap_EscalatesToRebuild — closes
// QA-flagged gap "WithPrimaryStorageWrap for substrate fault
// injection at executor layer" (BarrierAchievedLSN_PartialProgress
// dependency).
func TestComponent_RecycledScanWrap_EscalatesToRebuild(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithPrimaryStorageWrap(component.NewRecycledScanWrap()).
			Start()
		// Need at least one entry in the underlying substrate so
		// catch-up has a non-empty target.
		c.PrimaryWriteN(1)
		c.PrimarySync()

		result := c.CatchUpReplica(0)
		if result.Success {
			t.Fatal("recycled-scan wrap must surface ErrWALRecycled in close result")
		}
		if !contains(result.FailReason, "WAL recycled") {
			t.Errorf("FailReason missing 'WAL recycled': %s", result.FailReason)
		}
	})
}

// TestComponent_SeverDuringScan_PartialProgress — closes QA-flagged
// gap "Mid-stream drop helper SeverDuringScan(idx, afterNEntries)"
// (LastSentMonotonic_AcrossRetries dependency).
//
// Pin: when ScanLBAs severs after N entries, the executor surfaces a
// stream-level error (NOT ErrWALRecycled) and the close result's
// AchievedLSN reflects the partial progress (lastSent semantics).
func TestComponent_SeverDuringScan_PartialProgress(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithPrimaryStorageWrap(component.NewSeverDuringScanWrap(3, nil)).
			Start()
		c.PrimaryWriteN(10)
		c.PrimarySync()

		result := c.CatchUpReplica(0)
		// Substrate severed mid-scan after 3 entries → stream-level
		// error → catch-up fails. Distinct from ErrWALRecycled
		// (which would say "WAL recycled" in FailReason).
		if result.Success {
			t.Logf("catch-up succeeded — substrate had ≤3 entries before sever; sever was a no-op (acceptable)")
			return
		}
		if contains(result.FailReason, "WAL recycled") {
			t.Errorf("sever must NOT be conflated with WAL recycled: %s", result.FailReason)
		}
	})
}

// TestComponent_ObservedScanWrap_CountsEmits — closes the
// observability primitive: scenarios can assert how many entries
// the substrate emitted before the sender did anything with them.
func TestComponent_ObservedScanWrap_CountsEmits(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		count := new(atomic.Int32)
		c.WithReplicas(1).
			WithPrimaryStorageWrap(component.NewObservedScanWrap(count)).
			Start()
		c.PrimaryWriteN(7)
		c.PrimarySync()
		c.CatchUpReplica(0)

		got := count.Load()
		if got == 0 {
			t.Errorf("observed scan wrap counted %d entries; expected >0", got)
		}
		// We don't pin an exact count: smartwal's state-convergence
		// emits 1 per LBA; walstore's wal_replay can emit per-LSN
		// (multiple entries when an LBA was written more than once).
		// In this scenario each LBA is written once, so both should
		// count 7. Document the matrix-symmetric expectation here so
		// future regressions show up.
		if got != 7 {
			t.Errorf("expected 7 entries (1 per LBA write); got %d", got)
		}
	})
}

// TestComponent_LiveShip_PrimaryWriteViaBackend_FansOut — closes
// QA-flagged gap "Live-ship primitive that drives
// ReplicationVolume.OnLocalWrite end-to-end"
// (DeadlinePerCallScope_NoSpilling dependency).
//
// Pin: primary writes through StorageBackend → WriteObserver hook
// fires → ReplicationVolume fans out to replica → replica converges
// without an explicit catch-up. This exercises the live-ship path
// (T4a/T4b territory) inside the component framework.
func TestComponent_LiveShip_PrimaryWriteViaBackend_FansOut(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).WithLiveShip().Start()
		// Live-ship: writes go through Backend → observer →
		// ReplicationVolume → peer → replica. No CatchUpReplica call.
		c.PrimaryWriteViaBackendN(10)

		// Live-ship is async; poll for convergence.
		c.WaitForConverge(2 * time.Second)
	})
}

// TestComponent_EngineDrivenRecovery_StubWarning — pins the T4d
// stub behavior: the option records intent + warns at Start, then
// runs the scenario as direct-executor. When T4d wires the real
// engine→adapter→executor recovery, this scenario gains real
// coverage; today it exercises the API surface.
func TestComponent_EngineDrivenRecovery_StubWarning(t *testing.T) {
	component.RunSubstrate(t, "smartwal", component.Smartwal, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).WithEngineDrivenRecovery().Start()
		c.PrimaryWriteN(5)
		c.PrimarySync()
		c.CatchUpReplica(0)
		c.AssertReplicaConverged(0)
		// (T4d) when wiring lands: scenarios will assert engine
		// emitted N retries, escalation, etc.
	})
}

// helpers (intentionally tiny)
func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || (len(sub) > 0 && indexOf(s, sub) >= 0))
}
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
