package authority

import (
	"context"
	"testing"
	"time"
)

// INV-WS-SAME-HOST-CLEAN-MOVE-001: an unchanged sealed generation and its
// durable ACK remain a clean-move proof after the lease expires overnight.
func TestWorkspaceSameHostCleanMoveUsesOvernightSeal(t *testing.T) {
	disk, _ := newTempStore(t)
	clock := newTestClock()
	p := newLeasePublisher(t, disk, time.Second, clock)
	bindAndGrant(t, p, "v", "r")
	ctx := context.Background()
	req := WorkspaceMoveRequest{RequestID: "return", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "host", TargetHost: "host", ExpectedEpoch: 1, TargetDataAddr: "d", TargetCtrlAddr: "c", Kind: "clean"}
	head := WorkspaceHead{Gen: 7, Epoch: 1, Holder: "host/r", RemoteLSN: 42, Detached: true}
	if _, err := p.MoveWorkspace(ctx, req, head); err == nil {
		t.Fatal("same-host move accepted a seal without revocation ACK")
	}
	if err := p.BeginWorkspaceRevocation(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, HeadGen: 7}); err != nil {
		t.Fatal(err)
	}
	clock.at(86400)
	for _, bad := range []WorkspaceHead{
		{Gen: 7, Epoch: 1, Holder: "host/r", RemoteLSN: 42},
		{Gen: 8, Epoch: 1, Holder: "host/r", RemoteLSN: 42, Detached: true},
		{Gen: 7, Epoch: 1, Holder: "other/r", RemoteLSN: 42, Detached: true},
		{Gen: 7, Epoch: 2, Holder: "host/r", RemoteLSN: 42, Detached: true},
	} {
		if _, err := p.MoveWorkspace(ctx, req, bad); err == nil {
			t.Fatal("same-host move bypassed sealed-generation proof", bad)
		}
	}
	if line, _ := p.VolumeAuthorityLine("v"); line.Epoch != 1 || len(p.WorkspaceState("v").Moves) != 0 {
		t.Fatal("refusal reserved or published authority", line)
	}
	grant, err := p.MoveWorkspace(ctx, req, head)
	if err != nil {
		t.Fatal("sealed same-host clean move refused", err)
	}
	if !grant.Issued || grant.NewEpoch != 2 || grant.OldHost != "host" || grant.NewHost != "host" || grant.HeadGen != 7 || grant.RemoteLSN != 42 {
		t.Fatal("wrong journaled successor", grant)
	}
	again, err := p.MoveWorkspace(ctx, req, WorkspaceHead{})
	if err != nil || again != grant {
		t.Fatal("same-ID replay changed the successor", again, err)
	}
	loaded := NewPublisher(nil, WithStore(disk)).WorkspaceState("v")
	if loaded == nil || len(loaded.Moves) != 1 || loaded.Moves[0] != grant {
		t.Fatal("same-host grant not durable", loaded)
	}
}
