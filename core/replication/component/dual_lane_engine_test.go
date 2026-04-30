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
			ReplicaFlushedLSN: 0,      // R
			PrimaryTailLSN:    1,      // S
			PrimaryHeadLSN:    seedN,  // H = 12
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
		if got := c.Coord().Phase(recovery.ReplicaID("r-0")); got != recovery.PhaseIdle {
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
		if got := c.Coord().PinFloor(recovery.ReplicaID("r-0")); got != 0 {
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
// This pins the coordinator/bridge path for "live tail during rebuild"
// without yet wiring ReplicationVolume steady-ship to PushLiveWrite
// (that remains product work). WalShipper remains separate from the dual-
// lane rebuild sender until P2d unifies EncodeShipEntry vs frameWALEntry.
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

		// PeerShipCoordinator keys match NewBlockExecutorWithDualLane replica id ("r-0").
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
