package master

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// MarkVolumeDeleting is the master-side admission of a chunk volume delete
// (codex01's review of #152, P1 3): refused while the volume has live
// authority; once marked, a placement that already exists no longer leads
// to an assignment, and the id is never created, placed or bound again.

func TestMarkVolumeDeleting_RefusedWhileAssigned(t *testing.T) {
	h, c, ctx := reviewBindingHost(t)
	reviewLegacyBind(t, h, c, ctx, "live")
	_, err := control.NewLifecycleServiceClient(c).MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "live"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("mark of an assigned volume: %v; want FailedPrecondition", err)
	}
	if rec, _ := h.Lifecycle().Volumes.GetVolume("live"); rec.Deleting {
		t.Fatal("a refused mark marked the record")
	}
}

func TestMarkVolumeDeleting_StopsLaterAssignmentPlacementAndBinding(t *testing.T) {
	h, c, ctx := reviewBindingHost(t)
	lc := control.NewLifecycleServiceClient(c)
	// "placed" has a placement already (no authority yet); "fresh" has none.
	for _, vol := range []string{"placed", "fresh"} {
		reviewCreate(t, c, ctx, vol)
	}
	if _, e := h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: "placed", DesiredRF: 1, Candidates: []lifecycle.PlacementCandidate{{VolumeID: "placed", ServerID: "node-a", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}}); e != nil {
		t.Fatal(e)
	}
	for _, vol := range []string{"placed", "fresh"} {
		resp, err := lc.MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: vol})
		if err != nil || !resp.GetKnown() {
			t.Fatalf("mark %s: %+v %v", vol, resp, err)
		}
		if rec, _ := h.Lifecycle().Volumes.GetVolume(vol); !rec.Deleting {
			t.Fatalf("%s not marked", vol)
		}
	}
	// The placed volume's replica reports in; the product tick would bind
	// it. Neither volume may get authority, and "fresh" no placement.
	reviewHeartbeat(t, c, ctx, "placed", "node-a", "127.0.0.1:10000", "127.0.0.1:10001")
	reviewHeartbeat(t, c, ctx, "fresh", "node-a", "127.0.0.1:10002", "127.0.0.1:10003")
	for i := 0; i < 3; i++ {
		if _, e := h.RunLifecycleProductTick(); e != nil {
			t.Fatal(e)
		}
	}
	time.Sleep(200 * time.Millisecond) // let the publisher drain any queued ask
	for _, vol := range []string{"placed", "fresh"} {
		if line, ok := h.Publisher().VolumeAuthorityLine(vol); ok {
			t.Fatalf("%s got authority after its delete mark: %+v", vol, line)
		}
	}
	if _, ok := h.Lifecycle().Placements.GetPlacement("fresh"); ok {
		t.Fatal("the reconciler placed a volume marked deleting")
	}
	// Never created, bound or placed again.
	if _, err := lc.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "fresh", SizeBytes: 1 << 20, ReplicationFactor: 1}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("re-create of a deleting volume: %v; want FailedPrecondition", err)
	}
	if _, err := control.NewWorkspaceServiceClient(c).RegisterWorkspaceBinding(ctx, &control.WorkspaceBinding{VolumeId: "fresh", ServerId: "node-b", DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("binding of a deleting volume: %v; want FailedPrecondition", err)
	}
	// A volume the master never knew is admitted after the authority check.
	if resp, err := lc.MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "unknown"}); err != nil || resp.GetKnown() {
		t.Fatalf("mark of an unknown volume: %+v %v; want admitted, not known", resp, err)
	}
}
