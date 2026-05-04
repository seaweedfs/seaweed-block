// L1 component-route tests for T0 (v3-phase-15-t0-sketch.md §8.2).
// The route composed here is:
//
//	volume-side heartbeat loop -> real gRPC ObservationService ->
//	  master-side ObservationHost.IngestHeartbeat ->
//	  TopologyController decision -> Publisher mint ->
//	  real gRPC AssignmentService stream ->
//	  volume-side decodeAssignmentFact ->
//	  adapter.OnAssignment -> adapter.Projection() Epoch advance
//
// The test binds the master to 127.0.0.1:0 and has the volume
// connect over real TCP — so the same wire path the L2
// subprocess test will use is exercised in-process here.
package host_test

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
)

// topoV1 is the single-volume three-slot accepted topology used
// by the L1 tests in this file.
func topoV1() authority.AcceptedTopology {
	return authority.AcceptedTopology{
		Volumes: []authority.VolumeExpected{{
			VolumeID: "v1",
			Slots: []authority.ExpectedSlot{
				{ReplicaID: "r1", ServerID: "s1"},
				{ReplicaID: "r2", ServerID: "s2"},
				{ReplicaID: "r3", ServerID: "s3"},
			},
		}},
	}
}

// newMasterHost spins a master bound to 127.0.0.1:0 with topoV1.
func newMasterHost(t *testing.T, retryWindow time.Duration) *master.Host {
	t.Helper()
	m, err := master.New(master.Config{
		AuthorityStoreDir: t.TempDir(),
		Listen:            "127.0.0.1:0",
		Topology:          topoV1(),
		Freshness:         authority.FreshnessConfig{FreshnessWindow: 30 * time.Second, PendingGrace: 50 * time.Millisecond},
		ControllerConfig:  authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3, RetryWindow: retryWindow},
	})
	if err != nil {
		t.Fatalf("master.New: %v", err)
	}
	m.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = m.Close(ctx)
	})
	return m
}

func TestT0Route_HeartbeatToAssignmentInProcess(t *testing.T) {
	m := newMasterHost(t, 5*time.Second)

	// --- three volume hosts, one per replica slot ---
	// We need facts for all three slots to reach supportability.
	ready := make(chan adapter.AssignmentInfo, 1)
	v1 := mustNewVolume(t, m.Addr(), "s1", "v1", "r1", "data-r1", "ctrl-r1", ready)
	v2 := mustNewVolume(t, m.Addr(), "s2", "v1", "r2", "data-r2", "ctrl-r2", nil)
	v3vh := mustNewVolume(t, m.Addr(), "s3", "v1", "r3", "data-r3", "ctrl-r3", nil)
	v1.Start()
	v2.Start()
	v3vh.Start()
	defer v1.Close()
	defer v2.Close()
	defer v3vh.Close()

	// Expectation: the master's controller sees all three slots
	// observed healthy, publisher mints Bind(r1) (tiebreak by
	// replica ID), bridge delivers to volume r1's subscription
	// stream, which decodes and calls OnAssignment.
	select {
	case info := <-ready:
		if info.VolumeID != "v1" || info.ReplicaID != "r1" || info.Epoch != 1 {
			t.Fatalf("first assignment: got %+v, want v1/r1@1", info)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("volume r1 did not receive its assignment within 5s")
	}

	// Projection must reflect the assigned lineage. Per T0 sketch
	// §2.1 downgrade — do NOT assert ModeHealthy.
	p := v1.Adapter().Projection()
	if p.Epoch != 1 || p.EndpointVersion != 1 {
		t.Fatalf("adapter projection: got Epoch=%d EV=%d, want 1/1", p.Epoch, p.EndpointVersion)
	}

	// Sanity: boundary — the volume host must NOT have called
	// Publisher.apply or stuffed any AssignmentAsk. All wire
	// traffic went through real gRPC. Nothing to assert
	// functionally beyond the static AST guard
	// (TestNoOtherAssignmentInfoConstruction).
}

// TestT0Route_RefreshEndpoint_DeliveredSameEpoch is the round-5
// regression for the lexicographic (Epoch, EndpointVersion)
// dedupe fix. RefreshEndpoint is same-epoch by design
// (publisher keeps Epoch, bumps EndpointVersion). An
// epoch-only dedupe at the master-side SubscribeAssignments
// fan-in would silently drop the refresh. This test drives the
// refresh through the full product RPC route — heartbeats →
// controller decision → publisher mint → subscription fan-in →
// volume adapter — and asserts that when a slot heartbeat
// reports NEW addrs for the current replica, the volume's
// adapter projection advances to the new EndpointVersion on
// the same Epoch.
func TestT0Route_RefreshEndpoint_DeliveredSameEpoch(t *testing.T) {
	m := newMasterHost(t, 5*time.Second)

	// Three volume processes with initial addrs *-v1. When all
	// three slots are reported healthy, controller decides Bind
	// and publisher mints r1@1,EV=1.
	ready := make(chan adapter.AssignmentInfo, 4)
	v1 := mustNewVolume(t, m.Addr(), "s1", "v1", "r1", "data-r1-v1", "ctrl-r1-v1", ready)
	v2 := mustNewVolume(t, m.Addr(), "s2", "v1", "r2", "data-r2-v1", "ctrl-r2-v1", nil)
	v3 := mustNewVolume(t, m.Addr(), "s3", "v1", "r3", "data-r3-v1", "ctrl-r3-v1", nil)
	v1.Start()
	v2.Start()
	v3.Start()
	defer v1.Close()
	defer v2.Close()
	defer v3.Close()

	// Initial bind must arrive first at EV=1.
	select {
	case info := <-ready:
		if info.Epoch != 1 || info.EndpointVersion != 1 {
			t.Fatalf("initial bind: got %+v, want Epoch=1 EV=1", info)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("initial bind never reached volume via product RPC")
	}

	// Now swap v1 (the bound replica) for a new process with
	// CHANGED addrs. The controller's decideVolume will see the
	// supported slot's DataAddr/CtrlAddr drift from the published
	// line and emit IntentRefreshEndpoint. Publisher keeps Epoch
	// and bumps EndpointVersion to 2.
	v1.Close()
	v1b := mustNewVolume(t, m.Addr(), "s1", "v1", "r1", "data-r1-v2", "ctrl-r1-v2", nil)
	v1b.Start()
	defer v1b.Close()

	// Poll the NEW v1b's adapter projection until the refresh
	// arrives (Epoch=1 unchanged, EndpointVersion=2). Under the
	// epoch-only dedupe bug this would never happen — the master
	// would drop r1@1,EV=2 as "stale" because Epoch didn't grow.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p := v1b.Adapter().Projection()
		if p.Epoch == 1 && p.EndpointVersion == 2 {
			return // pass — refresh delivered through product RPC
		}
		time.Sleep(20 * time.Millisecond)
	}
	p := v1b.Adapter().Projection()
	t.Fatalf("RefreshEndpoint never reached adapter via product RPC: final projection %+v (want Epoch=1 EV=2)", p)
}

func mustNewVolume(t *testing.T, masterAddr, serverID, vid, rid, dataAddr, ctrlAddr string, ready chan<- adapter.AssignmentInfo) *volume.Host {
	t.Helper()
	h, err := volume.New(volume.Config{
		MasterAddr:        masterAddr,
		ServerID:          serverID,
		VolumeID:          vid,
		ReplicaID:         rid,
		DataAddr:          dataAddr,
		CtrlAddr:          ctrlAddr,
		HeartbeatInterval: 200 * time.Millisecond,
		ReadyMarker:       ready,
	})
	if err != nil {
		t.Fatalf("volume.New(%s/%s): %v", vid, rid, err)
	}
	return h
}
