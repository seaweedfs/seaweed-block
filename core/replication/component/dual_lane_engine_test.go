package component_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/recovery"
	"github.com/seaweedfs/seaweed-block/core/replication/component"
)

// TestDualLane_EngineDrivenRebuild_HappyPath — closes the
// G7-redo wiring milestone: full chain from master fact through
// engine decide → adapter dispatch → BlockExecutor (dual-lane mode)
// → recovery.PrimaryBridge → wire (separate listener at replica's
// DualLaneAddr) → recovery.Receiver → barrier → SessionClosedCompleted
// flowing back to the adapter / engine.
//
// What this pins (per docs/recovery-wiring-plan.md §7):
//
//   - WithDualLaneRecovery + WithEngineDrivenRecovery compose: engine
//     emits StartRebuild via adapter; adapter dispatches to
//     dual-lane-mode BlockExecutor; rebuild succeeds.
//   - The cluster's per-volume PeerShipCoordinator is reachable via
//     Coord(); after session close it is back to PhaseIdle for this
//     replica (no leak).
//   - Replica's substrate matches primary byte-for-byte after the
//     session — the headline correctness invariant.
//
// What this does NOT cover (still out of scope):
//
//   - Hardware integration (separate phase).
//   - WAL recycle path consuming MinPin (g7-redo/recycle-pin-hookup).
//   - §3.2 #3 single-queue real-time interleave (architect priority #2).
//   - Multi-replica fanout under dual-lane (single replica here for
//     focus; coordinator already covered for N-replica MinPin in unit
//     tests).
func TestDualLane_EngineDrivenRebuild_HappyPath(t *testing.T) {
	component.RunSubstrate(t, "memorywal", component.MemoryWAL, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithEngineDrivenRecovery().
			WithDualLaneRecovery().
			Start()

		// Seed primary with N writes so probe returns non-trivial R/S/H.
		const seedN = 12
		c.PrimaryWriteN(seedN)
		c.PrimarySync()

		a := c.Adapter(0)

		// Drive the master assignment — engine ingests Identity, emits
		// the auto-probe.
		c.DriveAssignment(0, adapter.AssignmentInfo{
			VolumeID:        "v1",
			ReplicaID:       "replica-0",
			Epoch:           1,
			EndpointVersion: 1,
			DataAddr:        c.Replica(0).Addr,
			CtrlAddr:        c.Replica(0).Addr,
		})

		// Drive a probe result that classifies as REBUILD: R < S means
		// the replica has been recycled past — engine.decide() picks
		// rebuild. R=0 (empty replica), S=1 (primary's WAL retains
		// LSN 1+), H=12 (primary head).
		c.DriveProbeResult(0, adapter.ProbeResult{
			ReplicaID:         "replica-0",
			Success:           true,
			EndpointVersion:   1,
			TransportEpoch:    1,
			ReplicaFlushedLSN: 0,     // R
			PrimaryTailLSN:    1,     // S
			PrimaryHeadLSN:    seedN, // H = 12
		})

		// Wait for engine to dispatch StartRebuild (CommandLog) AND
		// for the dual-lane session to complete (Trace records the
		// SessionClosedCompleted event flowing back from
		// adapter.OnSessionClose into the engine).
		deadline := time.Now().Add(5 * time.Second)
		var sawStartRebuild bool
		var sawSessionClosed bool
		for time.Now().Before(deadline) {
			for _, cmd := range a.CommandLog() {
				if cmd == "StartRebuild" {
					sawStartRebuild = true
				}
			}
			for _, te := range a.Trace() {
				if te.Step == "event" && te.Detail == "SessionClosedCompleted" {
					sawSessionClosed = true
				}
			}
			if sawStartRebuild && sawSessionClosed {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !sawStartRebuild {
			t.Fatalf("engine did NOT emit StartRebuild; CommandLog=%v", a.CommandLog())
		}
		if !sawSessionClosed {
			t.Fatalf("dual-lane session did NOT close cleanly; CommandLog=%v Trace=%v",
				a.CommandLog(), a.Trace())
		}

		// Coordinator should be back to Idle for replica-0.
		// Phase-0-Fix-#1 INV-RID-UNIFIED-PER-SESSION: the engine arg
		// flowing through StartRebuild is "replica-0"; the bridge now
		// keys coord under that arg. (Pre-fix this was "r-0",
		// matching the cluster harness's stale construction RID.)
		if got := c.Coord().Phase(recovery.ReplicaID("replica-0")); got != recovery.PhaseIdle {
			t.Errorf("coord phase post-session: %s want Idle", got)
		}

		// Replica's substrate matches primary byte-for-byte across the
		// rebuilt range. PrimaryWriteN writes LBA 0..N-1 with a known
		// pattern (cluster's helper). Read each LBA from both sides
		// and assert equality.
		for lba := uint32(0); lba < seedN; lba++ {
			pd, err := c.Primary().Store.Read(lba)
			if err != nil {
				t.Fatalf("primary Read lba=%d: %v", lba, err)
			}
			rd, err := c.Replica(0).Store.Read(lba)
			if err != nil {
				t.Fatalf("replica Read lba=%d: %v", lba, err)
			}
			if !bytes.Equal(pd, rd) {
				t.Fatalf("dual-lane post-rebuild: lba %d primary[0]=%02x replica[0]=%02x not equal",
					lba, pd[0], rd[0])
			}
		}
	})
}

// TestDualLane_CoordReleasesPinFloorAfterSession — pins the
// post-session lifecycle invariant: when a rebuild session ends
// (success or failure), the coordinator's pin_floor for that
// replica is dropped (PinFloor returns 0 for an Idle peer per the
// existing coordinator contract). Without this, a long-lived
// daemon could leak pin floors and stall WAL recycle.
//
// Also implicitly pins INV-SESSION-TEARDOWN-IS-EXPLICIT.
func TestDualLane_CoordReleasesPinFloorAfterSession(t *testing.T) {
	component.RunSubstrate(t, "memorywal", component.MemoryWAL, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithEngineDrivenRecovery().
			WithDualLaneRecovery().
			Start()

		c.PrimaryWriteN(8)
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
			ReplicaFlushedLSN: 0,
			PrimaryTailLSN:    1,
			PrimaryHeadLSN:    8,
		})

		// Wait for session to complete via Trace (SessionClosedCompleted
		// is an EVENT, not a Command; CommandLog won't show it).
		a := c.Adapter(0)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			done := false
			for _, te := range a.Trace() {
				if te.Step == "event" && te.Detail == "SessionClosedCompleted" {
					done = true
				}
			}
			if done {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		// MinPinAcrossActiveSessions should report no active sessions
		// after teardown. Pin floor for the replica reads 0.
		if _, anyActive := c.Coord().MinPinAcrossActiveSessions(); anyActive {
			t.Errorf("post-session: MinPinAcrossActiveSessions still reports anyActive=true")
		}
		// Phase-0-Fix-#1: keyed under the engine arg "replica-0" (was "r-0").
		if got := c.Coord().PinFloor(recovery.ReplicaID("replica-0")); got != 0 {
			t.Errorf("post-session: PinFloor=%d want 0 (released)", got)
		}
	})
}

// TestDualLane_EngineDrivenRebuild_WithPushLiveDuringSession is the
// component-lane extension of TestDualLane_EngineDrivenRebuild_HappyPath:
// after the engine emits StartRebuild, tests inject one extra WAL entry
// via primary substrate Write + PrimaryBridge.PushLiveWrite (session
// lane), matching core/recovery/integration_stub_test.go semantics.
//
// Note (post-C2): with StrictRealtimeOrdering=false (default), the
// PushLiveWrite-during-session race against sink.StartSession logs
// a WALSHIPPER-OUT-OF-ORDER warning but still emits, so the test
// continues to pass. Production callers that opt into strict mode
// MUST synchronize on session-ready (WalShipper in Backlog) before
// PushLiveWrite. Replacement coverage for the strict variant is in
// TestC1_Realtime_OutOfOrder_StrictFailsClosed.
func TestDualLane_EngineDrivenRebuild_WithPushLiveDuringSession(t *testing.T) {
	component.RunSubstrate(t, "memorywal", component.MemoryWAL, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithEngineDrivenRecovery().
			WithDualLaneRecovery().
			Start()

		const seedN = 12
		c.PrimaryWriteN(seedN)
		c.PrimarySync()

		a := c.Adapter(0)

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
			ReplicaFlushedLSN: 0,
			PrimaryTailLSN:    1,
			PrimaryHeadLSN:    seedN,
		})

		br, coordRID, ok := c.BlockExecutor(0).DualLanePrimaryBridge()
		if !ok || br == nil {
			t.Fatal("BlockExecutor missing dual-lane PrimaryBridge")
		}

		// PeerShipCoordinator keys match NewBlockExecutorWithDualLane replica id
		// ("replica-0" post-Phase-0-Fix-#1). DualLanePrimaryBridge() returns the
		// construction RID, which now agrees with the engine arg per
		// INV-RID-UNIFIED-PER-SESSION.
		if c.Coord().Phase(coordRID) == recovery.PhaseIdle {
			t.Fatal("coord still Idle after rebuild dispatch — PushLiveWrite would fail")
		}

		liveLBA := uint32(55)
		liveData := make([]byte, component.DefaultBlockSize)
		liveData[0] = 0xEE
		liveData[component.DefaultBlockSize-1] = 0xDD
		liveLSN := c.PrimaryWrite(liveLBA, liveData)

		if rt := c.Coord().RouteLocalWrite(coordRID, liveLSN); rt != recovery.RouteSessionLane {
			t.Fatalf("RouteLocalWrite during session: got %v want RouteSessionLane", rt)
		}
		waitBridgeLiveReady(t, br, coordRID)
		// In Backlog mode, NotifyAppend is lag-only (no emit); the
		// substrate scan in drainOpportunity picks up the entry from
		// primary's WAL. So the live LSN reaches replica via the
		// drain path, not the direct-emit path.
		if err := br.PushLiveWrite(coordRID, liveLBA, liveLSN, liveData); err != nil {
			t.Fatalf("PushLiveWrite: %v", err)
		}

		deadline := time.Now().Add(8 * time.Second)
		var sawStartRebuild bool
		var sawSessionClosed bool
		for time.Now().Before(deadline) {
			for _, cmd := range a.CommandLog() {
				if cmd == "StartRebuild" {
					sawStartRebuild = true
				}
			}
			for _, te := range a.Trace() {
				if te.Step == "event" && te.Detail == "SessionClosedCompleted" {
					sawSessionClosed = true
				}
			}
			if sawStartRebuild && sawSessionClosed {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !sawStartRebuild || !sawSessionClosed {
			t.Fatalf("expected StartRebuild + SessionClosedCompleted got cmd=%v trace=%v",
				a.CommandLog(), a.Trace())
		}

		for lba := uint32(0); lba < seedN; lba++ {
			pd, err := c.Primary().Store.Read(lba)
			if err != nil {
				t.Fatalf("primary Read lba=%d: %v", lba, err)
			}
			rd, err := c.Replica(0).Store.Read(lba)
			if err != nil {
				t.Fatalf("replica Read lba=%d: %v", lba, err)
			}
			if !bytes.Equal(pd, rd) {
				t.Fatalf("mismatch seeded lba %d", lba)
			}
		}
		pl, err := c.Primary().Store.Read(liveLBA)
		if err != nil {
			t.Fatalf("primary Read live lba=%d: %v", liveLBA, err)
		}
		rl, err := c.Replica(0).Store.Read(liveLBA)
		if err != nil {
			t.Fatalf("replica Read live lba=%d: %v", liveLBA, err)
		}
		if !bytes.Equal(pl, rl) {
			t.Fatalf("live PushLiveWrite LBA mismatch primary[0]=%02x replica[0]=%02x",
				pl[0], rl[0])
		}
	})
}

// TestPillar3Slice2_EngineDriven_SameLBAArbitration — slice-2 of
// §11.7 pillar 3 receiver-convergence claim, lifted onto the
// engine-driven path (component.Cluster + DriveAssignment +
// DriveProbeResult). Slice-1 (transport-stack only,
// core/transport/pillar3_convergence_test.go, commit 291e652 + polish
// 3495a12) proved that the receiver arbitrates same-LBA conflict
// correctly when StartRebuild is invoked directly. Slice-2 proves the
// adapter/engine command path doesn't bypass or alter that arbitration.
//
// Same shape as slice-1:
//   - Seed N LBAs (component.PrimaryWriteN ⇒ data[0]=lba+1,
//     data[blockSize-1]=0xC4^lba — pattern "A").
//   - After engine emits StartRebuild, live-overwrite a SUBSET of those
//     SAME LBAs with a distinct pattern "B" (data[0]=0x80|lba,
//     data[1]=0xB0).
//   - Wait for SessionClosedCompleted in adapter.Trace.
//   - Assert: replica.Read(lba) == primary.Read(lba) for ALL seeded
//     LBAs; specifically untouched LBAs hold A, overwritten LBAs hold B.
//
// Discriminating assertion: same as slice-1 — replica == primary on
// the overwritten LBAs. If the engine path's command flow somehow
// short-circuited the SessionLive flush or applied frames in a way
// that lost LSN-ordering arbitration, an overwritten LBA would differ.
//
// What this DOES add over slice-1:
//   - Full adapter→engine→executor→bridge dispatch path is exercised
//     (slice-1 calls executor.StartRebuild directly).
//   - SessionStartResult / SessionCloseResult callbacks flow through
//     the adapter back to the engine and surface as Trace events
//     (SessionClosedCompleted) — proves the closure of the
//     command-event loop, not just the wire path.
//
// Out of scope (slice-2):
//   - RF > 1.
//   - §IV T2 closure (architect-open).
//   - Wire-level frame tap.
//   - Long-duration soak.
//
// Pinned by: §11.7 pillar 3. Companion to e354813 / e5a8763 / 7d051e2
// / 9f62ebe / 291e652 / 3495a12.
func TestPillar3Slice2_EngineDriven_SameLBAArbitration(t *testing.T) {
	component.RunSubstrate(t, "memorywal", component.MemoryWAL, func(t *testing.T, c *component.Cluster) {
		c.WithReplicas(1).
			WithEngineDrivenRecovery().
			WithDualLaneRecovery().
			Start()

		const seedN = 12
		overwriteLBAs := []uint32{3, 5, 7}

		c.PrimaryWriteN(seedN)
		c.PrimarySync()

		a := c.Adapter(0)

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
			ReplicaFlushedLSN: 0,
			PrimaryTailLSN:    1,
			PrimaryHeadLSN:    seedN,
		})

		// Wait for engine to emit StartRebuild AND for the bridge to be
		// reachable (DualLanePrimaryBridge returns once the executor has
		// the dual-lane config).
		br, coordRID, ok := c.BlockExecutor(0).DualLanePrimaryBridge()
		if !ok || br == nil {
			t.Fatal("BlockExecutor missing dual-lane PrimaryBridge")
		}
		waitBridgeLiveReady(t, br, coordRID)

		// Live-overwrite the SAME LBAs as the backlog. Each Write goes
		// onto primary's WAL with a fresh LSN > seedN; PushLiveWrite
		// routes it as SessionLive (RouteSessionLane during active
		// rebuild). flushAndSeal at DrainBacklog completion ships them.
		bRefs := make(map[uint32][]byte, len(overwriteLBAs))
		for _, lba := range overwriteLBAs {
			data := make([]byte, component.DefaultBlockSize)
			data[0] = byte(0x80 | lba)
			data[1] = 0xB0
			lsn := c.PrimaryWrite(lba, data)
			if lsn <= uint64(seedN) {
				t.Fatalf("live LSN %d should be > seedN %d", lsn, seedN)
			}
			if rt := c.Coord().RouteLocalWrite(coordRID, lsn); rt != recovery.RouteSessionLane {
				t.Fatalf("RouteLocalWrite during session lba=%d lsn=%d: got %v want RouteSessionLane",
					lba, lsn, rt)
			}
			if err := br.PushLiveWrite(coordRID, lba, lsn, data); err != nil {
				t.Fatalf("PushLiveWrite lba=%d lsn=%d: %v", lba, lsn, err)
			}
			bRefs[lba] = data
		}

		// Wait for engine's command loop to close: StartRebuild emitted
		// AND SessionClosedCompleted observed in the adapter's Trace.
		deadline := time.Now().Add(8 * time.Second)
		var sawStartRebuild bool
		var sawSessionClosed bool
		for time.Now().Before(deadline) {
			for _, cmd := range a.CommandLog() {
				if cmd == "StartRebuild" {
					sawStartRebuild = true
				}
			}
			for _, te := range a.Trace() {
				if te.Step == "event" && te.Detail == "SessionClosedCompleted" {
					sawSessionClosed = true
				}
			}
			if sawStartRebuild && sawSessionClosed {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !sawStartRebuild || !sawSessionClosed {
			t.Fatalf("expected StartRebuild + SessionClosedCompleted; got cmd=%v trace=%v",
				a.CommandLog(), a.Trace())
		}

		// Convergence: replica.Read(lba) == primary.Read(lba) for every
		// seeded LBA. The discriminating direction is on overwritten
		// LBAs — primary holds B (last write wins), replica must also.
		overwriteSet := make(map[uint32]struct{}, len(overwriteLBAs))
		for _, lba := range overwriteLBAs {
			overwriteSet[lba] = struct{}{}
		}
		for lba := uint32(0); lba < seedN; lba++ {
			pd, err := c.Primary().Store.Read(lba)
			if err != nil {
				t.Fatalf("primary Read lba=%d: %v", lba, err)
			}
			rd, err := c.Replica(0).Store.Read(lba)
			if err != nil {
				t.Fatalf("replica Read lba=%d: %v", lba, err)
			}
			if !bytes.Equal(pd, rd) {
				if _, ovw := overwriteSet[lba]; ovw {
					t.Errorf("INV-PILLAR3-SAMELBA-CONVERGENCE (engine path): overwritten lba=%d replica != primary (got_replica[0:2]=%02x %02x got_primary[0:2]=%02x %02x); engine command flow lost arbitration",
						lba, rd[0], rd[1], pd[0], pd[1])
				} else {
					t.Errorf("untouched lba=%d replica != primary (got_replica[0:2]=%02x %02x got_primary[0:2]=%02x %02x)",
						lba, rd[0], rd[1], pd[0], pd[1])
				}
			}
		}

		// Sanity: overwritten LBAs hold the live-write payload (caches
		// vs the actual replica state). Catches the case where replica
		// matches primary, but primary itself drifted from the cached
		// payload — would surface a primary-write contract bug.
		for _, lba := range overwriteLBAs {
			rd, _ := c.Replica(0).Store.Read(lba)
			if !bytes.Equal(rd, bRefs[lba]) {
				t.Errorf("overwritten lba=%d replica differs from cached live-write payload (test-internal sanity)",
					lba)
			}
		}
	})
}

func waitBridgeLiveReady(t *testing.T, bridge interface {
	HasActiveSession(recovery.ReplicaID) bool
}, replicaID recovery.ReplicaID) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if bridge.HasActiveSession(replicaID) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("bridge session for %s did not become live-ready", replicaID)
}
