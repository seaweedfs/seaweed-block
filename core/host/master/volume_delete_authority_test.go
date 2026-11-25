package master

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

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
	for _, tc := range []struct {
		volume string
		known  bool
	}{{"v", true}, {"unknown", false}} {
		q := &control.MarkVolumeDeletingRequest{VolumeId: tc.volume, Token: "delete-v"}
		reserved, err := client.ReserveVolumeDeletion(ctx, q)
		if err != nil || reserved.GetKnown() != tc.known {
			t.Fatalf("reserve known(%s): %+v %v", tc.volume, reserved, err)
		}
		checked, err := client.CheckVolumeNoLiveLease(ctx, q)
		if err != nil || checked.GetKnown() != tc.known {
			t.Fatalf("check known(%s): %+v %v", tc.volume, checked, err)
		}
	}
}

// A failed reservation is never evidence that a durable write did not happen.
// These handler calls avoid a transport's own cancellation masking its status.
func TestVolumeDeleteAuthorityRPCFailureIsUnknown(t *testing.T) {
	svc := &services{host: &Host{}}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, stop := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer stop()
	req := &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "delete-v"}
	for _, ctx := range []context.Context{context.Background(), canceled, expired} {
		if _, err := svc.ReserveVolumeDeletion(ctx, req); status.Code(err) != codes.Unavailable {
			t.Fatalf("reservation failure: %v; want unknown/Unavailable", err)
		}
	}
	if _, err := svc.CheckVolumeNoLiveLease(context.Background(), &control.MarkVolumeDeletingRequest{VolumeId: "v"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("lease proof without token: %v", err)
	}
}

func TestReserveVolumeDeletionLiveLeaseIsDefiniteAndKeepsRenewing(t *testing.T) {
	h, conn, ctx := reviewBindingHost(t)
	reviewLegacyBind(t, h, conn, ctx, "live-delete")
	if _, err := h.Publisher().RenewWorkspaceLease(ctx, "live-delete", "r1", 1); err != nil {
		t.Fatal(err)
	}
	lc := control.NewLifecycleServiceClient(conn)
	if _, err := lc.ReserveVolumeDeletion(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "live-delete", Token: "typo"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("live reservation status: %v; want definite FailedPrecondition", err)
	}
	if h.lifecycle.Deletions.Reserved("live-delete") {
		t.Fatal("refusal persisted a reservation")
	}
	if _, err := h.Publisher().RenewWorkspaceLease(ctx, "live-delete", "r1", 1); err != nil {
		t.Fatalf("refused delete interrupted renewal: %v", err)
	}
}
