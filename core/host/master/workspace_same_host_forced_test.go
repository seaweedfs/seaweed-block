package master

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/protobuf/proto"
)

func TestWorkspaceRPCSameHostForcedRecoveryRecordsLoss(t *testing.T) {
	var nowNS atomic.Int64
	nowNS.Store(time.Now().UnixNano())
	h, conn, ctx := reviewBindingHostBeforeStart(t, func(context.Context, string) (authority.WorkspaceHead, error) {
		return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42}, nil
	}, func(h *Host) {
		authority.WithWorkspaceLeaseConfig(30*time.Second, func() time.Time { return time.Unix(0, nowNS.Load()) })(h.Publisher())
	})
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
	if _, err := ws.RenewWorkspaceLease(ctx, leaseHolderIdentity(t, line.Epoch, "node-a")); err != nil {
		t.Fatal(err)
	}
	q := &control.WorkspaceMove{RequestId: "same-host-recovery", VolumeId: "v", ExpectedReplicaId: "r1", ExpectedEpoch: line.Epoch, TargetReplicaId: "r1", TargetServerId: "node-a", TargetDataAddr: b.DataAddr, TargetCtrlAddr: b.CtrlAddr, Kind: "forced", Force: true}
	if _, err := ws.MoveWorkspace(ctx, q); err == nil {
		t.Fatal("same-host recovery bypassed a live lease")
	}
	nowNS.Add(int64(24 * time.Hour))
	if preview, err := ws.PreviewWorkspaceMove(ctx, q); err != nil || !preview.Admitted {
		t.Fatalf("same-host recovery preview after lease barrier: %+v %v", preview, err)
	}
	grant, err := ws.MoveWorkspace(ctx, q)
	if err != nil {
		t.Fatal("same-host recovery must return a new epoch and loss record", err)
	}
	if grant.NewEpoch != line.Epoch+1 || grant.LostRange == nil || grant.LostRange.LostAfterLsn != 42 || grant.LostRange.OldEpoch != line.Epoch {
		t.Fatalf("recovery omitted the abandoned suffix: %+v", grant)
	}
	if bindings, ok, err := h.Lifecycle().WorkspaceBindings.Get("v"); err != nil || !ok || len(bindings) != 1 || bindings[0].Retired {
		t.Fatalf("recovery retired its own target reservation: %+v %v", bindings, err)
	}
	if replay, err := ws.MoveWorkspace(ctx, q); err != nil || !proto.Equal(replay, grant) {
		t.Fatalf("same-host recovery replay changed: %+v %v", replay, err)
	}
}
