package master

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// INV-WS-DELETE-NO-LIVE-HOLDER-001 on the real master: a line whose holder
// never claimed HEAD at its epoch is admitted for delete without an ACK, on
// HEAD's evidence read before the locks; a HEAD claimed at the line's epoch,
// or one that cannot be read, is refused as before; the lifecycle
// DeleteVolume RPC (which reads no HEAD) refuses even the never-started line.
func TestDeleteAdmitsANeverStartedLine(t *testing.T) {
	var headEpoch atomic.Int64
	var headFails, headDeleting atomic.Bool
	h, conn, _ := reviewBindingHostBeforeStart(t, func(context.Context, string) (authority.WorkspaceHead, error) {
		if headFails.Load() {
			return authority.WorkspaceHead{}, errors.New("filer unreachable")
		}
		if headDeleting.Load() { // marked by the delete
			return authority.WorkspaceHead{Gen: 3, Deleting: true}, authority.ErrWorkspaceHeadDeleting
		}
		e := uint64(headEpoch.Load())
		if e == 0 { // the initial HEAD, as the filer reader returns it
			return authority.WorkspaceHead{Gen: 2}, authority.ErrWorkspaceHeadUnclaimed
		}
		return authority.WorkspaceHead{Gen: 2, Epoch: e, Holder: "node-a/r1"}, nil
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	for i, vol := range []string{"v", "w"} {
		reviewCreate(t, conn, ctx, vol)
		ws := control.NewWorkspaceServiceClient(conn)
		b := &control.WorkspaceBinding{VolumeId: vol, ServerId: "node-a", DataAddr: fmt.Sprintf("127.0.0.1:%d", 10000+2*i), CtrlAddr: fmt.Sprintf("127.0.0.1:%d", 10001+2*i)}
		if _, err := ws.RegisterWorkspaceBinding(ctx, b); err != nil {
			t.Fatal(err)
		}
		if _, err := ws.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: vol, ServerId: "node-a", ReplicaId: "r1"}); err != nil {
			t.Fatal(err)
		}
		reviewHeartbeat(t, conn, ctx, vol, "node-a", b.DataAddr, b.CtrlAddr)
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "v")
	waitAuthorityLine(t, h.Publisher(), "w")
	// A lease granted, HEAD never claimed: the stranded state the release
	// candidate could not delete.
	if _, err := control.NewWorkspaceServiceClient(conn).RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r1", Epoch: line.Epoch, ServerId: "node-a"}); err != nil {
		t.Fatal(err)
	}
	lc := control.NewLifecycleServiceClient(conn)
	mark := func(vol, token string) error {
		_, err := lc.MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: vol, Token: token})
		return err
	}
	refusedLive := func(what string, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "live authority requires clean revocation ACK") {
			t.Fatalf("%s: %v; want the live-authority refusal", what, err)
		}
	}

	headEpoch.Store(int64(line.Epoch)) // claimed at the line's epoch: live
	refusedLive("HEAD claimed at the line's epoch", mark("v", "del-live"))
	headFails.Store(true) // unreadable HEAD: fail closed
	refusedLive("HEAD unreadable", mark("v", "del-unreadable"))
	headFails.Store(false)
	headEpoch.Store(0) // never claimed
	_, err := lc.DeleteVolume(ctx, &control.DeleteVolumeRequest{VolumeId: "w"})
	refusedLive("the lifecycle DeleteVolume of a never-started line", err)
	res, err := lc.MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "del-never-started"})
	if err != nil || res.GetState() != lifecycle.ReservationAdmitted {
		t.Fatalf("delete of a never-started line: %+v %v; want admitted without an ACK", res, err)
	}
	// The delete stopped before it marked HEAD, and the holder then claimed
	// HEAD (its lease was still valid): a rerun of the same delete (its token
	// holds the reservation) is admitted without a new verdict, so it reaches
	// the catalog step, whose HEAD fence refuses the live HEAD and gives the
	// reservation back. A different delete is still decided, and refused.
	headEpoch.Store(int64(line.Epoch))
	res, err = lc.MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "del-never-started"})
	if err != nil || res.GetState() != lifecycle.ReservationAdmitted {
		t.Fatalf("rerun of an admitted delete after the holder claimed HEAD: %+v %v; want admitted (the catalog step decides)", res, err)
	}
	refusedLive("a new delete while the holder holds HEAD", mark("v", "del-other"))
	headEpoch.Store(0)
	// The delete marked HEAD deleting, then its commit timed out: the re-run
	// is admitted again (HEAD past the fence), not stranded as volume_in_use.
	headDeleting.Store(true)
	res, err = lc.MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "v", Token: "del-never-started"})
	if err != nil || res.GetState() != lifecycle.ReservationAdmitted {
		t.Fatalf("re-run after a commit that timed out: %+v %v; want admitted again", res, err)
	}
}
