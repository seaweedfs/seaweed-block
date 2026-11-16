package master

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// The master's deletion reservation (review of #152 r2): durable, separate
// from the desired-volume record, held by token, given back by Abort,
// permanent after Commit.

func markReq(vol, tok string) *control.MarkVolumeDeletingRequest {
	return &control.MarkVolumeDeletingRequest{VolumeId: vol, Token: tok}
}

func createReq(vol string) *control.CreateVolumeRequest {
	return &control.CreateVolumeRequest{VolumeId: vol, SizeBytes: 1 << 20, ReplicationFactor: 1}
}

// hostOn starts a master on fixed store directories (so a test can restart
// it) and returns it with a lifecycle client.
func hostOn(t *testing.T, authDir, lcDir string) (*Host, control.LifecycleServiceClient, func()) {
	t.Helper()
	h, err := New(Config{Listen: "127.0.0.1:0", AuthorityStoreDir: authDir, LifecycleStoreDir: lcDir})
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	c, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	stop := func() {
		c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.Close(ctx)
	}
	return h, control.NewLifecycleServiceClient(c), stop
}

func TestMarkVolumeDeleting_RefusedWhileAssigned(t *testing.T) {
	h, c, ctx := reviewBindingHost(t)
	reviewLegacyBind(t, h, c, ctx, "live")
	_, err := control.NewLifecycleServiceClient(c).MarkVolumeDeleting(ctx, markReq("live", "d1"))
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("mark of an assigned volume: %v; want FailedPrecondition", err)
	}
	if h.Lifecycle().Deletions.Reserved("live") {
		t.Fatal("a refused mark reserved the id")
	}
}

// P1: an id the master never knew is reserved too, and the ordinary
// DeleteVolume of a known record does not clear the reservation; both hold
// across a restart.
func TestMarkVolumeDeleting_ReservesUnknownIdsAndSurvivesDeleteAndRestart(t *testing.T) {
	authDir, lcDir := t.TempDir(), t.TempDir()
	ctx := context.Background()
	h, lc, stop := hostOn(t, authDir, lcDir)
	if resp, err := lc.MarkVolumeDeleting(ctx, markReq("forkvol", "d1")); err != nil || resp.GetKnown() || resp.GetState() != lifecycle.ReservationAdmitted {
		t.Fatalf("mark of an unknown id: %+v %v; want admitted, not known", resp, err)
	}
	if _, err := lc.CreateVolume(ctx, createReq("known")); err != nil {
		t.Fatal(err)
	}
	if resp, err := lc.MarkVolumeDeleting(ctx, markReq("known", "d2")); err != nil || !resp.GetKnown() {
		t.Fatalf("mark of a known volume: %+v %v", resp, err)
	}
	if _, err := lc.DeleteVolume(ctx, &control.DeleteVolumeRequest{VolumeId: "known"}); err != nil {
		t.Fatalf("ordinary delete of the record: %v", err)
	}
	for _, vol := range []string{"forkvol", "known"} {
		if _, err := lc.CreateVolume(ctx, createReq(vol)); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("create of reserved %s: %v; want FailedPrecondition", vol, err)
		}
	}
	_ = h
	stop()
	h, lc, stop = hostOn(t, authDir, lcDir)
	defer stop()
	for _, vol := range []string{"forkvol", "known"} {
		if !h.Lifecycle().Deletions.Reserved(vol) {
			t.Fatalf("%s's reservation lost on restart", vol)
		}
		if _, err := lc.CreateVolume(ctx, createReq(vol)); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("create of reserved %s after restart: %v; want FailedPrecondition", vol, err)
		}
	}
	// Abort gives the unknown id back (usable again); a committed one never.
	if resp, err := lc.AbortVolumeDeleting(ctx, markReq("forkvol", "d1")); err != nil || resp.GetState() != "" {
		t.Fatalf("abort: %+v %v; want the reservation gone", resp, err)
	}
	if _, err := lc.CreateVolume(ctx, createReq("forkvol")); err != nil {
		t.Fatalf("create after the abort: %v", err)
	}
	if _, err := lc.CommitVolumeDeleting(ctx, markReq("known", "d2")); err != nil {
		t.Fatal(err)
	}
	if _, err := lc.AbortVolumeDeleting(ctx, markReq("known", "d2")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("abort of a committed deletion: %v; want FailedPrecondition", err)
	}
	if _, err := lc.CommitVolumeDeleting(ctx, markReq("nothing", "dx")); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("commit of an unreserved id: %v; want FailedPrecondition", err)
	}
}

// P1: a reserved volume with an existing placement gets no assignment, and
// the reserved id is not placed, created or bound.
func TestMarkVolumeDeleting_StopsLaterAssignmentPlacementAndBinding(t *testing.T) {
	h, c, ctx := reviewBindingHost(t)
	lc := control.NewLifecycleServiceClient(c)
	for _, vol := range []string{"placed", "fresh"} {
		reviewCreate(t, c, ctx, vol)
	}
	if _, e := h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: "placed", DesiredRF: 1, Candidates: []lifecycle.PlacementCandidate{{VolumeID: "placed", ServerID: "node-a", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}}); e != nil {
		t.Fatal(e)
	}
	for _, vol := range []string{"placed", "fresh"} {
		if _, err := lc.MarkVolumeDeleting(ctx, markReq(vol, "d-"+vol)); err != nil {
			t.Fatalf("mark %s: %v", vol, err)
		}
	}
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
		t.Fatal("the reconciler placed a reserved volume")
	}
	if _, err := control.NewWorkspaceServiceClient(c).RegisterWorkspaceBinding(ctx, &control.WorkspaceBinding{VolumeId: "fresh", ServerId: "node-b", DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("binding of a reserved volume: %v; want FailedPrecondition", err)
	}
}
