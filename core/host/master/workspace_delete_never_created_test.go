package master

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// neverCreatedVolume builds a layout volume the production way (create,
// binding, placement, heartbeat, a product tick binds its line) whose
// primary holds a live lease, and, when established, records its stores.
func neverCreatedVolume(t *testing.T, established bool) (*Host, control.LifecycleServiceClient, control.WorkspaceServiceClient, *control.WorkspaceIdentity) {
	t.Helper()
	h, conn, ctx := reviewBindingHostBeforeStart(t, func(context.Context, string) (authority.WorkspaceHead, error) {
		return authority.WorkspaceHead{Gen: 2}, authority.ErrWorkspaceHeadUnclaimed
	}, nil)
	reviewCreate(t, conn, ctx, "v")
	ws := control.NewWorkspaceServiceClient(conn)
	b := &control.WorkspaceBinding{VolumeId: "v", ServerId: "node-a", DataAddr: "127.0.0.1:10000", CtrlAddr: "127.0.0.1:10001"}
	if _, err := ws.RegisterWorkspaceBinding(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "v", ServerId: "node-a", ReplicaId: "r1"}); err != nil {
		t.Fatal(err)
	}
	reviewHeartbeat(t, conn, ctx, "v", "node-a", b.DataAddr, b.CtrlAddr)
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "v")
	if established {
		establishVolume(t, h, "v")
	}
	id := &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r1", Epoch: line.Epoch, ServerId: "node-a"}
	if _, err := ws.RenewWorkspaceLease(ctx, id); err != nil {
		t.Fatalf("the primary's lease: %v", err)
	}
	if creating := h.inCreationPhase("v", line, nil) != ""; creating == established {
		t.Fatalf("fixture: creating=%t, want %t", creating, !established)
	}
	return h, control.NewLifecycleServiceClient(conn), ws, id
}

// A volume that never completed creation is deleted without its holder's
// cooperation: the reservation is admitted although the creating primary
// still holds a live lease (it served nothing and acknowledged nothing).
// From the reservation on the master grants that primary no lease, so a
// zombie that wakes cannot serve, and the delete's HEAD update still waits
// until no lease is live (the horizon), after which HEAD is marked deleting
// and no claim or commit applies to it.
func TestANeverCreatedVolumeIsDeletedWithoutItsHolder(t *testing.T) {
	_, lc, ws, id := neverCreatedVolume(t, false)
	req := &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "del-never-created"}
	if _, err := lc.ReserveVolumeDeletion(t.Context(), req); err != nil {
		t.Fatalf("delete reservation of a never-created volume with a live lease: %v", err)
	}
	if _, err := ws.RenewWorkspaceLease(t.Context(), id); err == nil {
		t.Fatal("the creating primary renewed its lease after the delete reservation: a zombie could keep serving")
	}
	if _, err := lc.CheckVolumeNoLiveLease(t.Context(), req); err == nil {
		t.Fatal("the delete's HEAD update was admitted while the last lease may still be live")
	}
}

// A volume that completed creation keeps the rule: its delete is refused
// while its holder's lease is live (the holder must be revoked first).
func TestAnEstablishedVolumeIsNotDeletedUnderALiveLease(t *testing.T) {
	_, lc, _, _ := neverCreatedVolume(t, true)
	_, err := lc.ReserveVolumeDeletion(t.Context(), &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "del-established"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("delete of an established volume under a live lease: %v; want FailedPrecondition", err)
	}
}
