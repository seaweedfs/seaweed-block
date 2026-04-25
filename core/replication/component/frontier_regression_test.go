package component_test

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/replication/component"
)

// PARTIAL PIN — round-41 architect note:
//
// The frontier-monotonicity tests below cover the BENIGN FULL-REPLAY
// shape only. They do NOT prove "no data regression on partial-
// failure interrupted replay" (the real adversarial case). For that,
// see stale_entry_adversarial_test.go which is the load-bearing
// pin for INV-REPL-RECOVERY-STALE-ENTRY-SKIP-PER-LBA.
//
// Don't cite these tests as proof that pinLSN < replicaLSN is
// universally safe. They prove a narrower thing: full-replay
// over-scan stabilizes the frontier.

// TestComponent_CatchupFromBelowReplicaLSN_NoFrontierRegression
// answers QA round 41 question: if primary starts catch-up at
// pinLSN < replicaLSN, does replica's R / walHead regress under
// FULL successful replay? Partial pin only — see file header.
//
// Setup: replica is "ahead" of where catch-up scans from. Primary
// re-ships entries the replica already has, then ships new entries.
//
// Pin (storage contract from logical_storage.go §3): "Stable
// frontier (synced LSN) never goes backward across Sync calls
// within one process lifetime." Re-applying an older LSN MUST NOT
// regress walHead / syncedLSN.
//
// Catalogue invariant (named here for round-41 inscription):
// INV-REPL-CATCHUP-FRONTIER-MONOTONIC-UNDER-OVER-SCAN.
func TestComponent_CatchupFromBelowReplicaLSN_NoFrontierRegression(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()

		// Stage 1: prime replica with LBAs 0..4 at LSNs 1..5.
		// (Use ReplicaApply to install state directly — bypasses
		// catch-up so we control the replica's starting R.)
		for i := 0; i < 5; i++ {
			data := make([]byte, component.DefaultBlockSize)
			data[0] = byte(i + 1)
			lsn := c.PrimaryWrite(uint32(i), data)
			c.ReplicaApply(0, uint32(i), data, lsn)
		}
		// Sync replica to advance its R.
		c.Replica(0).Store.Sync()

		_, _, replicaH_before := c.Replica(0).Store.Boundaries()
		if replicaH_before == 0 {
			t.Fatal("test premise: replica's H must be >0 before catch-up")
		}

		// Stage 2: primary writes LBAs 5..9 at LSNs 6..10.
		// Replica is now behind on those.
		for i := 5; i < 10; i++ {
			data := make([]byte, component.DefaultBlockSize)
			data[0] = byte(i + 1)
			c.PrimaryWrite(uint32(i), data)
		}
		c.PrimarySync()

		// Stage 3: catch-up. Today's executor scans from LSN=1
		// (over-scan acknowledged in catchup_sender.go TODO).
		// Re-ships LSNs 1..5 (replica already has) + 6..10.
		result := c.CatchUpReplica(0)
		if !result.Success {
			t.Fatalf("catch-up: %s", result.FailReason)
		}

		// Convergence check.
		c.AssertReplicaConverged(0)

		// Frontier-monotonicity check (the actual round-41 pin):
		// replica's H AFTER catch-up MUST be >= replica's H BEFORE
		// catch-up. Re-shipping LSNs 1..5 must NOT regress walHead.
		_, _, replicaH_after := c.Replica(0).Store.Boundaries()
		if replicaH_after < replicaH_before {
			t.Fatalf("FRONTIER REGRESSION: replica H went backwards across catch-up: before=%d after=%d",
				replicaH_before, replicaH_after)
		}
		// And it must reach primary's H.
		_, _, primaryH := c.Primary().Store.Boundaries()
		if replicaH_after < primaryH {
			t.Errorf("replica H after catch-up = %d, want >= primary H = %d",
				replicaH_after, primaryH)
		}
	})
}

// TestComponent_RepeatedReshipping_FrontierStable variation: ship
// the same gap multiple times. Idempotence at the frontier-tracking
// level should mean walHead stabilizes, never regresses.
func TestComponent_RepeatedReshipping_FrontierStable(t *testing.T) {
	component.RunMatrix(t, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()
		c.PrimaryWriteN(5)
		c.PrimarySync()

		// First catch-up.
		c.CatchUpReplica(0)
		_, _, h1 := c.Replica(0).Store.Boundaries()

		// Second catch-up — re-ships everything.
		c.CatchUpReplica(0)
		_, _, h2 := c.Replica(0).Store.Boundaries()

		// Third for good measure.
		c.CatchUpReplica(0)
		_, _, h3 := c.Replica(0).Store.Boundaries()

		if h2 < h1 || h3 < h2 {
			t.Fatalf("frontier non-monotonic across repeated catch-ups: h1=%d h2=%d h3=%d",
				h1, h2, h3)
		}
	})
}
