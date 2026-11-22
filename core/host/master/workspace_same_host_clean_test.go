package master

import (
	"testing"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// INV-WS-SAME-HOST-CLEAN-MOVE-001: retirement belongs to an epoch, so the
// old epoch's replay cannot retire the re-prepared same-host binding.
func TestWorkspaceRPCSameHostCleanMoveKeepsRepreparedBinding(t *testing.T) {
	h, conn, ctx := reviewBindingHost(t)
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
	if line := waitAuthorityLine(t, h.Publisher(), "v"); line.Epoch != 1 {
		t.Fatal(line)
	}
	id := &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r1", Epoch: 1}
	if _, err := ws.RenewWorkspaceLease(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.RevokeWorkspace(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9}); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := h.Lifecycle().WorkspaceBindings.Get("v"); err != nil || !ok || len(got) != 1 || !got[0].Retired {
		t.Fatal("ACK did not retire the old binding", got, err)
	}
	if _, err := ws.RegisterWorkspaceBinding(ctx, b); err != nil {
		t.Fatal("same-host re-prepare refused", err)
	}
	req := &control.WorkspaceMove{RequestId: "return", VolumeId: "v", ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1", TargetServerId: "node-a", TargetDataAddr: b.DataAddr, TargetCtrlAddr: b.CtrlAddr, Kind: "clean"}
	if _, err := ws.MoveWorkspace(ctx, req); err != nil {
		t.Fatal("same-host clean move refused over RPC", err)
	}
	if _, err := ws.MoveWorkspace(ctx, req); err != nil {
		t.Fatal("same-host move replay refused", err)
	}
	if line, _ := h.Publisher().VolumeAuthorityLine("v"); line.Epoch != 2 || line.DataAddr != b.DataAddr || line.CtrlAddr != b.CtrlAddr {
		t.Fatal("same-host move did not publish exactly the next epoch", line)
	}
	if got, ok, err := h.Lifecycle().WorkspaceBindings.Get("v"); err != nil || !ok || len(got) != 1 || got[0].Retired {
		t.Fatal("old epoch retired the re-prepared binding", got, err)
	}
}
