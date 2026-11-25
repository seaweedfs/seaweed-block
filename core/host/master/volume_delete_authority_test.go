package master

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestVolumeDeleteAuthorityReservationSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	authDir, lifecycleDir := filepath.Join(root, "authority"), filepath.Join(root, "lifecycle")
	h, client, stop := hostOn(t, authDir, lifecycleDir)
	if _, err := client.CreateVolume(ctx, createReq("v")); err != nil {
		stop()
		t.Fatal(err)
	}
	a := h.VolumeDeleteAuthority("v", "delete-v")
	if err := a.NoLiveLease(ctx); err == nil {
		stop()
		t.Fatal("unreserved proof succeeded")
	}
	if _, err := client.ReserveVolumeDeletion(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "delete-v"}); err != nil {
		stop()
		t.Fatal(err)
	}
	if _, err := client.ReserveVolumeDeletion(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "delete-v"}); err != nil {
		stop()
		t.Fatal("replay", err)
	}
	if _, err := h.lifecycle.Deletions.Abort("v", "delete-v"); !errors.Is(err, lifecycle.ErrReservationCommitted) {
		stop()
		t.Fatalf("abort reopened reservation: %v", err)
	}
	if _, err := client.CheckVolumeNoLiveLease(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "delete-v"}); err != nil {
		stop()
		t.Fatal(err)
	}
	if _, err := client.AbortVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "delete-v"}); status.Code(err) != codes.FailedPrecondition {
		stop()
		t.Fatalf("RPC abort reopened committed reservation: %v", err)
	}
	stop()
	_, lc, closeLoaded := hostOn(t, authDir, lifecycleDir)
	defer closeLoaded()
	if _, err := lc.CheckVolumeNoLiveLease(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "delete-v"}); err != nil {
		t.Fatal("reloaded exclusion missing", err)
	}
	if _, err := lc.CreateVolume(ctx, createReq("v")); err == nil {
		t.Fatal("recreated a reserved volume")
	}
}

// The two RPCs the volume delete calls are the in-master authority: the
// check refuses before the reservation, the reservation commits (a replay
// is fine, it is never given back), the check then passes.
func TestVolumeDeleteAuthorityRPCs(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	_, client, stop := hostOn(t, filepath.Join(root, "authority"), filepath.Join(root, "lifecycle"))
	defer stop()
	if _, err := client.CreateVolume(ctx, createReq("v")); err != nil {
		t.Fatal(err)
	}
	req := &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "delete-v"}
	if _, err := client.CheckVolumeNoLiveLease(ctx, req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("check before the reservation: %v; want FailedPrecondition", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := client.ReserveVolumeDeletion(ctx, req); err != nil {
			t.Fatalf("reserve #%d: %v", i+1, err)
		}
	}
	if _, err := client.AbortVolumeDeleting(ctx, req); err == nil {
		t.Fatal("a committed reservation was given back")
	}
	if _, err := client.CheckVolumeNoLiveLease(ctx, req); err != nil {
		t.Fatalf("check after the reservation: %v", err)
	}
	if _, err := client.ReserveVolumeDeletion(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "v"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("reserve without a token: %v", err)
	}
}
