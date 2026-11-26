package master

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// supportedCreatingTick ingests r1 (as given) and two ready replicas, checks
// the volume takes the SUPPORTED gate, and runs one product tick.
func supportedCreatingTick(t *testing.T, h *Host, r1 rf3Slot) {
	t.Helper()
	ingestRF3Incarnations(t, h, r1, readySlot("r2-a"), readySlot("r3-a"))
	topo, ok := acceptedTopologyFromPlacementIntents(h.Lifecycle().Placements.ListPlacements())
	if !ok {
		t.Fatal("no accepted topology from placements")
	}
	build := authority.BuildSnapshot(h.obs.Store().Snapshot(), topo, h.Publisher())
	if _, unsupported := build.Report.Unsupported["vol-rf3"]; unsupported {
		t.Fatalf("vol-rf3 is unsupported (%+v); this test needs the supported gate", build.Report.Unsupported["vol-rf3"])
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
}

// Creation is its own phase: a volume whose first line has not reached
// Healthy is never subject to a replacement decision, so no fencing round
// runs and no heal moves the line, however ready the replicas are. The
// schedule from the NET fence-heal run on the SUPPORTED gate: an empty new
// volume, r1 awaiting the replica stores (reachable, not primary-ready),
// r2 and r3 turned ready first.
func TestCreatingVolumeRunsNoFencingRoundOnTheSupportedGate(t *testing.T) {
	for _, c := range []struct {
		name  string
		setup func(t *testing.T, h *Host)
	}{
		{"the master still awaits the stores", func(*testing.T, *Host) {}},
		{"the master's wait is over, the primary has not got the stores yet", func(t *testing.T, h *Host) { attestAllStores(t, h) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
			defer closeTestMaster(t, h)
			c.setup(t, h)
			p := awaitingProbe(t, h, true)
			p.mu.Lock()
			p.empty = true // a new volume: every end is zero
			p.mu.Unlock()
			time.Sleep(250 * time.Millisecond) // past the boot grace
			awaiting := rf3Slot{incarnation: "r1-a", reachable: true, eligible: true}
			supportedCreatingTick(t, h, awaiting)
			supportedCreatingTick(t, h, awaiting)
			p.mu.Lock()
			fences := p.fenceCalls
			p.mu.Unlock()
			if fences != 0 {
				t.Fatalf("%d fencing rounds against a creating volume", fences)
			}
			expectLineStaysR1(t, h, "a creating volume with ready replicas")
		})
	}
}

// While the master's record still awaits the stores, the volume is creating
// whatever the primary answers: a creating primary that stops answering is
// not replaced either (the volume waits, visibly).
func TestCreatingVolumeIsNotReplacedWhenItsPrimaryIsSilent(t *testing.T) {
	h := newBootGraceMasterCreating(t, Config{ControllerConfig: authority.TopologyControllerConfig{PrimaryBootGrace: 200 * time.Millisecond}})
	defer closeTestMaster(t, h)
	if !inCreationWait(h) {
		t.Fatal("fixture: the volume is not creating")
	}
	p := probeOf(t, h)
	p.mu.Lock()
	p.empty, p.currentUnknown = true, true
	p.mu.Unlock()
	time.Sleep(250 * time.Millisecond)
	for i := 0; i < 2; i++ {
		ingestRF3Incarnations(t, h, silent, readySlot("r2-a"), readySlot("r3-a"))
		if _, err := h.RunLifecycleProductTick(); err != nil {
			t.Fatal(err)
		}
	}
	p.mu.Lock()
	fences := p.fenceCalls
	p.mu.Unlock()
	if fences != 0 {
		t.Fatalf("%d fencing rounds against a creating volume with a silent primary", fences)
	}
	expectLineStaysR1(t, h, "a creating volume whose primary is silent")
}

// establishVolume records the current primary's attestation of every
// replica's store, as a primary's first heartbeat after its bind does, so
// the volume has left the creation phase (an established volume). Fixtures
// that exercise promotion on an established volume call it after the first
// line is published.
func establishVolume(t *testing.T, h *Host, volumeID string) {
	t.Helper()
	line := waitAuthorityLine(t, h.Publisher(), volumeID)
	p, ok := h.Lifecycle().Placements.GetPlacement(volumeID)
	if !ok {
		t.Fatalf("establish %s: no placement", volumeID)
	}
	stores := map[string]storage.StoreID{}
	for _, s := range p.Slots {
		stores[s.ReplicaID] = storeOf(s.ReplicaID)
	}
	published, _ := h.Publisher().PublishedAuthorityLine(volumeID)
	server := h.workspaceLineOwner(published, volumeID, line.ReplicaID, line.Epoch)
	if err := h.ingestStoreAttestation(server, attestation(volumeID, line.ReplicaID, line.Epoch, stores)); err != nil {
		t.Fatalf("establish %s: %v", volumeID, err)
	}
	if h.creatingByRecord(volumeID) {
		t.Fatalf("establish %s: still creating after the attestation (line %+v, server %q)", volumeID, line, server)
	}
}
