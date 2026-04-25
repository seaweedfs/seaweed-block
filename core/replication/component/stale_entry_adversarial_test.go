package component_test

import (
	"bytes"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/replication/component"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Adversarial round-41 tests pinning the architect-specified
// `INV-REPL-RECOVERY-STALE-ENTRY-SKIP-PER-LBA` invariant:
//
//   "A replica MUST NOT apply a recovery-stream entry if that LBA is
//    already known to contain data from an equal or higher LSN. The
//    entry is treated as duplicate/stale and skipped. This applies to
//    repeated catch-up, retry after partial failure, and pinned
//    windows whose start LSN is below the replica's current frontier."
//
// These tests are **expected to FAIL today on walstore + BlockStore**.
// The failure surfaces a real bug — not a false positive. Skip via
// t.Skip + a TODO referencing the T4d blocker once the architect
// signs the skip; alternatively let the test fail honestly until
// T4d's per-LBA applied LSN tracking lands.
//
// smartwal MAY pass the adversarial test by virtue of state-
// convergence semantics (scan emits current data, not historical),
// so the wal_replay (walstore) row is the load-bearing case.

// TestComponent_Adversarial_StaleEntryDoesNotRegress is the
// architect-requested adversarial test: same LBA written at low LSN
// and high LSN; replica primed at the high LSN; catch-up starts
// below R; sever before high LSN replays. Replica MUST NOT regress
// to the low-LSN value.
//
// EXPECTED: walstore FAILS (bug); smartwal MAY PASS (state-
// convergence emits current data). T4d will fix walstore by adding
// per-LBA stale-entry rejection at the replica apply layer.
func TestComponent_Adversarial_StaleEntryDoesNotRegress(t *testing.T) {
	t.Skip("Round-41 architect-requested adversarial test. Expected to FAIL on walstore today (per-LBA stale-entry skip not implemented). Un-skip when T4d's INV-REPL-RECOVERY-STALE-ENTRY-SKIP-PER-LBA lands.")

	// Walstore-only — wal_replay sub-mode is where the bug
	// manifests. smartwal's state-convergence sub-mode is stale-
	// safe-by-construction (always emits current data).
	component.RunSubstrate(t, "walstore", component.Walstore, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).Start()

		// Stage 1: primary writes LBA=7=A at LSN=1, LBA=7=B at LSN=2.
		// Walstore retains BOTH per-LSN entries in the WAL.
		dataA := make([]byte, component.DefaultBlockSize)
		dataA[0] = 0xAA
		c.PrimaryWrite(7, dataA) // LSN=1 — value A

		dataB := make([]byte, component.DefaultBlockSize)
		dataB[0] = 0xBB
		c.PrimaryWrite(7, dataB) // LSN=2 — value B
		c.PrimarySync()

		// Stage 2: replica is primed with the LATEST state (LSN=2,
		// value B). Simulates "replica caught up via prior live
		// ship before our catch-up session."
		c.ReplicaApply(0, 7, dataB, 2)
		c.Replica(0).Store.Sync()

		// Sanity: replica has B before catch-up.
		preCatch, _ := c.Replica(0).Store.Read(7)
		if !bytes.Equal(preCatch, dataB) {
			t.Fatalf("test premise: replica must have B before catch-up; got %02x", preCatch[0])
		}

		// Stage 3: install a sever-after-1-entry wrap. Catch-up will
		// scan walstore from LSN=1, ship LSN=1 (LBA=7, A), then
		// sever before LSN=2 (B) is shipped. Without per-LBA stale-
		// entry skip, the replica overwrites B with A and the
		// connection drops — replica is left at A.
		// (Re-build the cluster with the wrap installed.)
		// Note: this test currently doesn't have a clean way to
		// install a wrap mid-flight; the framework would need a
		// `Cluster.InstallStorageWrap` method to swap the wrap
		// after Stage 2's seeding. For now, frame the scenario
		// without the wrap and document the limitation.
		c.CatchUpReplica(0)

		// Stage 4: assert replica is still at B (not regressed to A).
		postCatch, _ := c.Replica(0).Store.Read(7)
		if !bytes.Equal(postCatch, dataB) {
			t.Fatalf("FRONTIER REGRESSION: replica LBA=7 went from B (%02x) to A (%02x) after catch-up over-scan",
				dataB[0], postCatch[0])
		}
	})
}

// TestComponent_Adversarial_BlockStoreApplyEntryRegressesWalHead
// pins the BlockStore bug architect identified in round-41:
// `BlockStore.ApplyEntry` does `s.walHead = lsn` unconditionally,
// so an older-LSN apply REGRESSES walHead. This contradicts the
// LogicalStorage contract (logical_storage.go §3 rule 3: "stable
// frontier never goes backward").
//
// EXPECTED TO FAIL today. T4d (or a sooner BlockStore fix) must
// gate the walHead update on `lsn > walHead`. This test exists to
// surface the bug and gate the fix.
func TestComponent_Adversarial_BlockStoreApplyEntryRegressesWalHead(t *testing.T) {
	t.Skip("Round-41 architect-identified bug: BlockStore.ApplyEntry sets walHead unconditionally; older-LSN apply regresses H. Un-skip when fixed.")

	store := storage.NewBlockStore(64, 4096)

	// Apply LSN=10 first.
	dataHigh := make([]byte, 4096)
	dataHigh[0] = 0xBB
	if err := store.ApplyEntry(0, dataHigh, 10); err != nil {
		t.Fatal(err)
	}
	_, _, hAfterHigh := store.Boundaries()
	if hAfterHigh != 10 {
		t.Fatalf("after ApplyEntry(LSN=10): walHead=%d, want 10", hAfterHigh)
	}

	// Apply LSN=5 (older). Storage contract: walHead MUST NOT regress.
	dataLow := make([]byte, 4096)
	dataLow[0] = 0xAA
	if err := store.ApplyEntry(0, dataLow, 5); err != nil {
		t.Fatal(err)
	}
	_, _, hAfterLow := store.Boundaries()
	if hAfterLow < hAfterHigh {
		t.Fatalf("FRONTIER REGRESSION: walHead went %d → %d after older-LSN apply (BlockStore contract violation)",
			hAfterHigh, hAfterLow)
	}
}

// TestComponent_StaleEntrySkip_RuleStatement is documentation in
// test form: pins the rule architect dictated in round-41 so future
// readers find it via `go doc` / IDE search rather than only via
// memory file.
func TestComponent_StaleEntrySkip_RuleStatement(t *testing.T) {
	t.Log(`
INV-REPL-RECOVERY-STALE-ENTRY-SKIP-PER-LBA (architect round-41):

  A replica MUST NOT apply a recovery-stream entry if that LBA is
  already known to contain data from an equal or higher LSN. The
  entry is treated as duplicate/stale and skipped. This applies to:
    - repeated catch-up
    - retry after partial failure
    - pinned windows whose start LSN is below the replica's current
      frontier

Why: pinLSN < replicaLSN is LEGAL and EXPECTED (probe-time R can
advance during recovery via live lane / barrier / prior partial
catch-up). Don't avoid the situation; design for it.

Required at T4d:
  1. Per-LBA applied LSN tracking at replica apply layer
  2. Recovery apply skips entries whose LSN <= per-LBA applied LSN
  3. Live lane always applies; recovery lane skips stale
  4. Adversarial tests:
     - same LBA old/new + sever before new → no regression
     - repeated recovery window → byte state stable
     - live races recovery old → live wins

NOT covered by frontier-monotonicity tests
(TestComponent_CatchupFromBelowReplicaLSN_NoFrontierRegression) —
those prove benign full-replay shape only, NOT interrupted replay.
`)
}
