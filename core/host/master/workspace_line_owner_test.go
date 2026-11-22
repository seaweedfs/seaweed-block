package master

import (
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"testing"
)

// TERM-HOLDER-001 / INV-AUTH-HOLDER-IDENTITY-001: current facts, leases and
// move-source history share the owner rule; placement does not override a move.
func TestWorkspaceLineOwnerIssuedAndHistorical(t *testing.T) {
	h, conn, ctx := reviewBindingHost(t)
	reviewLegacyBind(t, h, conn, ctx, "v")
	ws := control.NewWorkspaceServiceClient(conn)
	id := leaseHolderIdentity(t, 1, "node-a")
	if _, err := ws.RenewWorkspaceLease(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.RevokeWorkspace(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9}); err != nil {
		t.Fatal(err)
	}
	target := &control.WorkspaceBinding{VolumeId: "v", ServerId: "node-b", DataAddr: "127.0.0.1:20000", CtrlAddr: "127.0.0.1:20001"}
	if _, err := ws.RegisterWorkspaceBinding(ctx, target); err != nil {
		t.Fatal(err)
	}
	grant, err := ws.MoveWorkspace(ctx, &control.WorkspaceMove{RequestId: "owner-move", VolumeId: "v", ExpectedReplicaId: "r1", ExpectedEpoch: 1, TargetReplicaId: "r1", TargetServerId: "node-b", TargetDataAddr: target.DataAddr, TargetCtrlAddr: target.CtrlAddr, Kind: "clean"})
	if err != nil {
		t.Fatal(err)
	}
	if !grant.Issued || grant.NewEpoch != 2 {
		t.Fatal("move did not issue", grant)
	}
	svc := &services{host: h}
	matches := func(server string) bool {
		t.Helper()
		ok, err := svc.holderMatches(loopbackPeer(ctx), "v", "r1", server, 2)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if got := h.placementSourceHost("v", "r1"); got != "node-a" {
		t.Fatal("fixture placement no longer differs from issued owner", got)
	}
	// Both consumers must prefer the issued owner to the old placement.
	if got := h.workspaceSourceHost("v", "r1", 2); got != "node-b" || !matches(got) || matches("node-a") {
		t.Fatal("fact/lease issued owner mismatch", got)
	}
	stream, err := control.NewAssignmentServiceClient(conn).SubscribeAssignments(ctx, &control.SubscribeRequest{VolumeId: "v", ReplicaId: "r1", ServerId: "node-b"})
	if err != nil {
		t.Fatal(err)
	}
	fact, err := stream.Recv()
	if err != nil || fact.GetServerId() != "node-b" || fact.GetEpoch() != 2 {
		t.Fatal("wrong produced fact", fact, err)
	}
	if _, err := ws.RenewWorkspaceLease(ctx, leaseHolderIdentity(t, 2, "node-b")); err != nil {
		t.Fatal("issued owner lease refused", err)
	}
	// Change only desired-placement metadata. It must not rewrite the owner of
	// the historical e1 line retained in the actual issued move journal.
	if _, err := h.Lifecycle().Placements.ApplyPlan(lifecycle.PlacementPlan{VolumeID: "v", DesiredRF: 1, Candidates: []lifecycle.PlacementCandidate{{VolumeID: "v", ServerID: "node-c", ReplicaID: "r1", Source: lifecycle.PlacementSourceExistingReplica}}}); err != nil {
		t.Fatal(err)
	}
	if got := h.workspaceSourceHost("v", "r1", 1); got != "node-a" {
		t.Fatal("historical line guessed from current placement", got)
	}
	if got := h.workspaceSourceHost("v", "r1", 2); got != "node-b" || !matches(got) {
		t.Fatal("issued owner changed with placement", got)
	}
}
