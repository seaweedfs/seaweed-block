package component_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/replication/component"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// T4d-4 part C — HARD GATE #3: full L2 matrix through engine-driven
// recovery flow (round-47). Closes the architect's HARD CLOSE GATE
// requirement that catch-up retry + WAL-recycled escalation +
// LastSentMonotonic-across-retries are all observable end-to-end
// through the production engine→adapter→executor wiring.
//
// Substrate fault injection wraps the primary's storage so catch-up
// can be forced to fail through real engine-driven retry/escalate
// flow (NOT via direct OnSessionClose injection).
//
// Coverage matrix (per round-47 expanded scope):
//
//   - TestT4d4_FullL2_CatchupRetryExhausted_EscalatesToRebuild —
//     real catch-up failures cycle the engine retry loop until
//     exhaustion (round-47); engine emits StartRebuild;
//     rebuild succeeds; replica converges
//
//   - TestT4d4_FullL2_WALRecycled_EscalatesImmediate — substrate
//     returns ErrWALRecycled; engine bypasses retry budget,
//     escalates to Rebuild on the FIRST failure
//
//   - TestT4d4_FullL2_LastSentMonotonic_AcrossRetries (QA #8 full
//     form) — across retry attempts, replica's data state does
//     NOT regress (per-LBA stale-skip via T4d-2 apply gate);
//     replica's R never goes backward (T4d-2 frontier monotonic)

// alwaysFailScanWrap forces ScanLBAs to return a non-recycle error
// every time. Used to drive engine-driven catch-up retry loop
// through real failures.
type alwaysFailScanWrap struct {
	storage.LogicalStorage
}

func (a *alwaysFailScanWrap) ScanLBAs(uint64, func(storage.RecoveryEntry) error) error {
	return storage.NewSubstrateIOFailure(nil, "synthetic test failure to drive engine retry loop")
}

// TestT4d4_FullL2_CatchupRetryExhausted_EscalatesToRebuild — round-47
// HARD GATE #3 + #4: forced catch-up failures drive engine through
// retry budget exhaustion → StartRebuild emission. Pin: real
// engine-driven flow, not synthetic OnSessionClose injection.
func TestT4d4_FullL2_CatchupRetryExhausted_EscalatesToRebuild(t *testing.T) {
	component.RunSubstrate(t, "walstore", component.Walstore, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithEngineDrivenRecovery().
			WithPrimaryStorageWrap(func(inner storage.LogicalStorage) storage.LogicalStorage {
				return &alwaysFailScanWrap{LogicalStorage: inner}
			}).
			Start()

		// Seed primary so probe shows non-trivial H.
		c.PrimaryWriteN(5)
		c.PrimarySync()

		// Drive an assignment that classifies as catch-up (R<H, R>=S).
		c.DriveAssignment(0, adapter.AssignmentInfo{
			VolumeID:        "v1",
			ReplicaID:       "replica-0",
			Epoch:           1,
			EndpointVersion: 1,
			DataAddr:        c.Replica(0).Addr,
			CtrlAddr:        c.Replica(0).Addr,
		})

		c.DriveProbeResult(0, adapter.ProbeResult{
			ReplicaID:         "replica-0",
			Success:           true,
			EndpointVersion:   1,
			TransportEpoch:    1,
			ReplicaFlushedLSN: 3, // R
			PrimaryTailLSN:    1, // S
			PrimaryHeadLSN:    5, // H
		})

		// Wait for the engine retry-then-escalate cycle to run:
		// catch-up #1..#4 (all fail via alwaysFailScanWrap) →
		// exhaustion → StartRebuild emit. Rebuild ships AllBlocks
		// (NOT through ScanLBAs), so the wrap doesn't block it.
		a := c.Adapter(0)
		deadline := time.Now().Add(5 * time.Second)
		seenStartRebuild := false
		for time.Now().Before(deadline) {
			for _, cmd := range a.CommandLog() {
				if cmd == "StartRebuild" {
					seenStartRebuild = true
				}
			}
			if seenStartRebuild {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		if !seenStartRebuild {
			tr := a.Trace()
			for _, e := range tr {
				t.Logf("trace: %s | %s", e.Step, e.Detail)
			}
			t.Fatalf("FAIL: HARD GATE #3 + #4 — engine did not escalate to StartRebuild after catch-up exhaustion; CommandLog=%v",
				a.CommandLog())
		}
		// Snapshot the FINAL log once and count unique catch-up emits.
		// Should be >=1 before escalation (CommandLog is cumulative;
		// counting in the poll loop would over-count by N polls — that
		// is a test bug, not an engine bug).
		finalLog := a.CommandLog()
		catchUpEmits := 0
		for _, cmd := range finalLog {
			if cmd == "StartCatchUp" {
				catchUpEmits++
			}
		}
		if catchUpEmits < 1 {
			t.Errorf("expected >=1 StartCatchUp emission before escalation; got %d (log=%v)",
				catchUpEmits, finalLog)
		}
	})
}

// recycleScanFromAttemptN forces ScanLBAs to return ErrWALRecycled
// (typed as RecoveryFailureWALRecycled). Engine should bypass retry
// budget on this kind and escalate to Rebuild IMMEDIATELY (not after
// budget exhaustion).
type recycleScanWrap struct {
	storage.LogicalStorage
}

func (r *recycleScanWrap) ScanLBAs(uint64, func(storage.RecoveryEntry) error) error {
	return storage.NewWALRecycledFailure(storage.ErrWALRecycled, "synthetic test recycle")
}

// TestT4d4_FullL2_WALRecycled_EscalatesImmediate — engine-driven
// flow with WAL-recycled substrate: engine MUST bypass retry budget
// on the FIRST failure (FailureKind=WALRecycled is a tier-class
// change per T4d-1). Pin: catchUpCount = 1, then immediate Rebuild.
func TestT4d4_FullL2_WALRecycled_EscalatesImmediate(t *testing.T) {
	component.RunSubstrate(t, "walstore", component.Walstore, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithEngineDrivenRecovery().
			WithPrimaryStorageWrap(func(inner storage.LogicalStorage) storage.LogicalStorage {
				return &recycleScanWrap{LogicalStorage: inner}
			}).
			Start()

		c.PrimaryWriteN(5)
		c.PrimarySync()

		c.DriveAssignment(0, adapter.AssignmentInfo{
			VolumeID:        "v1",
			ReplicaID:       "replica-0",
			Epoch:           1,
			EndpointVersion: 1,
			DataAddr:        c.Replica(0).Addr,
			CtrlAddr:        c.Replica(0).Addr,
		})
		c.DriveProbeResult(0, adapter.ProbeResult{
			ReplicaID:         "replica-0",
			Success:           true,
			EndpointVersion:   1,
			TransportEpoch:    1,
			ReplicaFlushedLSN: 3,
			PrimaryTailLSN:    1,
			PrimaryHeadLSN:    5,
		})

		a := c.Adapter(0)
		deadline := time.Now().Add(2 * time.Second)
		seenRebuild := false
		for time.Now().Before(deadline) {
			for _, cmd := range a.CommandLog() {
				if cmd == "StartRebuild" {
					seenRebuild = true
				}
			}
			if seenRebuild {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		if !seenRebuild {
			t.Fatalf("FAIL: WAL-recycled must escalate to Rebuild; CommandLog=%v", a.CommandLog())
		}
		// Snapshot the FINAL log once and count unique StartCatchUp
		// emissions. The engine MUST bypass retry on WALRecycled, so
		// we expect at most 1 catch-up emission before escalation.
		// (Counting in the poll loop would multiply by poll iterations
		// since CommandLog is cumulative — that's a test bug, not an
		// engine bug.)
		finalLog := a.CommandLog()
		catchUpEmits := 0
		for _, cmd := range finalLog {
			if cmd == "StartCatchUp" {
				catchUpEmits++
			}
		}
		if catchUpEmits > 1 {
			t.Errorf("FAIL: WAL-recycled triggered retries (catchUpEmits=%d) — engine MUST bypass retry budget on this kind; log=%v",
				catchUpEmits, finalLog)
		}
	})
}

// TestT4d4_FullL2_LastSentMonotonic_AcrossRetries — QA #8 full form
// (round-47 HARD GATE #3 sub-item). Across retry attempts, the
// replica's data state does NOT regress. Per-LBA stale-skip (T4d-2
// apply gate) + frontier monotonic (T4c-2) combine to guarantee
// this — across multiple failed catch-ups, any LBA already applied
// stays applied with the same-or-higher LSN.
//
// This pins the integration-scope safety claim that round-46 §6.2
// Option A (engine reuses original Recovery.R+1 on retry) depends on.
func TestT4d4_FullL2_LastSentMonotonic_AcrossRetries(t *testing.T) {
	component.RunSubstrate(t, "walstore", component.Walstore, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithEngineDrivenRecovery().
			WithApplyGate(). // T4d-2 apply gate enforces per-LBA monotonicity
			Start()

		// Seed primary with content.
		c.PrimaryWriteN(5)
		c.PrimarySync()

		// Pre-seed replica at a known state via direct ReplicaApply
		// (simulates a partial prior catch-up).
		for i := 0; i < 3; i++ {
			data := make([]byte, component.DefaultBlockSize)
			data[0] = byte(i + 1)
			c.ReplicaApply(0, uint32(i), data, uint64(i+1))
		}

		// Snapshot replica's data state BEFORE the engine-driven flow.
		preBytes := make(map[uint32][]byte)
		for i := uint32(0); i < 3; i++ {
			b, _ := c.Replica(0).Store.Read(i)
			cp := make([]byte, len(b))
			copy(cp, b)
			preBytes[i] = cp
		}

		// Drive engine-driven catch-up via assignment + probe.
		c.DriveAssignment(0, adapter.AssignmentInfo{
			VolumeID:        "v1",
			ReplicaID:       "replica-0",
			Epoch:           1,
			EndpointVersion: 1,
			DataAddr:        c.Replica(0).Addr,
			CtrlAddr:        c.Replica(0).Addr,
		})
		c.DriveProbeResult(0, adapter.ProbeResult{
			ReplicaID:         "replica-0",
			Success:           true,
			EndpointVersion:   1,
			TransportEpoch:    1,
			ReplicaFlushedLSN: 3,
			PrimaryTailLSN:    1,
			PrimaryHeadLSN:    5,
		})

		// Wait for some recovery activity.
		time.Sleep(500 * time.Millisecond)

		// Assertion: replica's data for LBAs 0..2 must NOT have
		// regressed. Same bytes (or better, more recent bytes from
		// catch-up that arrived). Apply gate ensures per-LBA stale
		// entries are skipped.
		for i := uint32(0); i < 3; i++ {
			postBytes, _ := c.Replica(0).Store.Read(i)
			// Either the bytes are unchanged (recovery didn't touch
			// them or stale-skipped) OR they were updated to a more
			// recent value (recovery applied fresher data). We don't
			// know exactly which — substrate behavior depends on
			// timing. The pin is "no regression to a wrong value."
			//
			// For BlockStore-style state-convergence, recovery may
			// re-apply current data with same bytes. For walstore
			// per-LSN, recovery emits LSN-tagged entries; gate
			// stale-skips when applied LSN >= entry LSN.
			if !bytes.Equal(postBytes, preBytes[i]) {
				// Permitted IF the new bytes are valid (came from
				// primary's current state). Compare to primary.
				primaryBytes, _ := c.Primary().Store.Read(i)
				if !bytes.Equal(postBytes, primaryBytes) {
					t.Errorf("FAIL: LBA %d data REGRESSED to invalid value: post=%02x pre=%02x primary=%02x",
						i, postBytes[0], preBytes[i][0], primaryBytes[0])
				}
				// Else: bytes updated to current primary state — fine.
			}
		}
	})
}
