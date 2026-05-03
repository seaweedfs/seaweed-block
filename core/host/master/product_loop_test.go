package master

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func TestG9G_ProductLoopPublishesVerifiedExistingReplica(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	seedVerifiedExistingReplicaPlacement(t, h)

	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("product tick: %v", err)
	}
	if result.PublishedAsks != 1 {
		t.Fatalf("result=%+v want one published ask", result)
	}
	line := waitAuthorityLine(t, h.Publisher(), "vol-a")
	if line.ReplicaID != "r2" || line.Epoch != 1 || line.EndpointVersion != 1 {
		t.Fatalf("line=%+v want publisher-minted bind for r2", line)
	}
}

func TestG9G_ProductLoopDoesNotPublishUnverifiedPlacement(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	stores := h.Lifecycle()
	if _, err := stores.Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID:  "vol-a",
		DesiredRF: 1,
		Candidates: []lifecycle.PlacementCandidate{{
			VolumeID:  "vol-a",
			ServerID:  "node-a",
			ReplicaID: "r2",
			Source:    lifecycle.PlacementSourceExistingReplica,
		}},
	}); err != nil {
		t.Fatalf("apply placement: %v", err)
	}

	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("product tick: %v", err)
	}
	if result.PublishedAsks != 0 {
		t.Fatalf("result=%+v want no published ask", result)
	}
	if _, ok := h.Publisher().VolumeAuthorityLine("vol-a"); ok {
		t.Fatal("unverified placement must not publish authority")
	}
}

func TestG9G_ProductLoopIsIdempotentForSameAuthorityLine(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	seedVerifiedExistingReplicaPlacement(t, h)

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("first product tick: %v", err)
	}
	first := waitAuthorityLine(t, h.Publisher(), "vol-a")
	result, err := h.RunLifecycleProductTick()
	if err != nil {
		t.Fatalf("second product tick: %v", err)
	}
	if result.PublishedAsks != 0 {
		t.Fatalf("second tick result=%+v want no duplicate ask", result)
	}
	second, ok := h.Publisher().VolumeAuthorityLine("vol-a")
	if !ok {
		t.Fatal("authority line disappeared")
	}
	if second.ReplicaID != first.ReplicaID || second.Epoch != first.Epoch || second.EndpointVersion != first.EndpointVersion {
		t.Fatalf("line changed first=%+v second=%+v", first, second)
	}
}

func TestG9G_BlockvolumeSubscriptionReceivesProductLoopAssignment(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	seedVerifiedExistingReplicaPlacement(t, h)
	ch, cancel := h.Publisher().Subscribe("vol-a", "r2")
	defer cancel()

	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatalf("product tick: %v", err)
	}
	got := recvAssignmentInfo(t, ch)
	if got.VolumeID != "vol-a" || got.ReplicaID != "r2" || got.Epoch != 1 || got.EndpointVersion != 1 {
		t.Fatalf("assignment=%+v want product-loop publisher bind", got)
	}
}

func seedVerifiedExistingReplicaPlacement(t *testing.T, h *Host) {
	t.Helper()
	stores := h.Lifecycle()
	if _, err := stores.Placements.ApplyPlan(lifecycle.PlacementPlan{
		VolumeID:  "vol-a",
		DesiredRF: 1,
		Candidates: []lifecycle.PlacementCandidate{{
			VolumeID:  "vol-a",
			ServerID:  "node-a",
			ReplicaID: "r2",
			Source:    lifecycle.PlacementSourceExistingReplica,
		}},
	}); err != nil {
		t.Fatalf("apply placement: %v", err)
	}
	if err := h.ObservationHost().Ingest(authority.Observation{
		ServerID:   "node-a",
		ObservedAt: time.Now().UTC(),
		Slots: []authority.SlotFact{{
			VolumeID:  "vol-a",
			ReplicaID: "r2",
			DataAddr:  "127.0.0.1:9202",
			CtrlAddr:  "127.0.0.1:9102",
		}},
	}); err != nil {
		t.Fatalf("ingest observation: %v", err)
	}
}

func recvAssignmentInfo(t *testing.T, ch <-chan adapter.AssignmentInfo) adapter.AssignmentInfo {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for assignment")
		return adapter.AssignmentInfo{}
	}
}
