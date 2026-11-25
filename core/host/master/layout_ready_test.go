package master

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLayoutStoreNotReadyCannotBePlacedOrMoved(t *testing.T) {
	h := newTestMaster(t, t.TempDir())
	defer closeTestMaster(t, h)
	h.cfg.LayoutCreateVolume = func(context.Context, lifecycle.VolumeSpec) error { return nil }
	h.cfg.WorkspaceRPC = true
	stores := h.Lifecycle()
	rec, err := stores.Volumes.CreateVolume(lifecycle.VolumeSpec{VolumeID: "v", SizeBytes: 1 << 20, ReplicationFactor: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stores.Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: "m02", DataAddr: "10.0.0.2:9201", CtrlAddr: "10.0.0.2:9101", Pools: []lifecycle.StoragePool{{PoolID: "default", TotalBytes: 1 << 30, FreeBytes: 1 << 30, BlockSize: 4096}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	if _, ok := stores.Placements.GetPlacement("v"); ok {
		t.Fatal("record without VOLUME and HEAD was placed")
	}
	// A persisted intent can predate a failed filer create (or survive a
	// restart). Neither workload planning nor the product assignment loop
	// may treat that intent as authority while store_ready is false.
	if results := lifecycle.ReconcilePlacement([]lifecycle.VolumeRecord{rec}, stores.Nodes.ListNodes(), stores.Placements); len(results) != 1 || !results[0].Applied {
		t.Fatalf("fixture placement: %+v", results)
	}
	plans, err := h.RunLifecycleWorkloadPlanTick(lifecycle.WorkloadPlanConfig{})
	if err != nil || plans.PlannedVolumes != 0 {
		t.Fatalf("unready workload plans=%+v err=%v", plans, err)
	}
	tick, err := h.RunLifecycleProductTick()
	if err != nil || tick.PublishedAsks != 0 || tick.PromotionProbes != 0 {
		t.Fatalf("unready assignment/promotion tick=%+v err=%v", tick, err)
	}
	if err := newServices(h).workspaceVolume("v"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("move admission before store ready: %v", err)
	}
	resp, err := newServices(h).CreateVolume(context.Background(), &control.CreateVolumeRequest{VolumeId: "v", SizeBytes: 1 << 20, ReplicationFactor: 1})
	if err != nil || !resp.GetStoreReady() || resp.GetLayoutId() != rec.Spec.LayoutID {
		t.Fatalf("healing create: response=%+v err=%v", resp, err)
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	if _, ok := stores.Placements.GetPlacement("v"); !ok {
		t.Fatal("ready record was not placed")
	}
}
