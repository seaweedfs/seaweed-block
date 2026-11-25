package master

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func TestWorkspaceAckRecordsTheAuthoritativeStopIdentity(t *testing.T) {
	head := authority.WorkspaceHead{Gen: 99, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42, Detached: true}
	var readErr error
	h, conn, ctx := reviewBindingHostBeforeStart(t, func(context.Context, string) (authority.WorkspaceHead, error) { return head, readErr }, nil)
	reviewLegacyBind(t, h, conn, ctx, "v")
	ws := control.NewWorkspaceServiceClient(conn)
	id := &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "v", ReplicaId: "r1", Epoch: 1}
	if _, err := ws.RenewWorkspaceLease(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.RevokeWorkspace(ctx, id); err != nil {
		t.Fatal(err)
	}
	good := head
	for _, tc := range []struct {
		name   string
		mutate func()
	}{
		{"unreadable", func() { readErr = errors.New("filer unavailable") }},
		{"live", func() { head.Detached = false }},
		{"other epoch", func() { head.Epoch++ }},
		{"other holder", func() { head.Holder = "node-b/r1" }},
		{"deleting", func() { head.Deleting = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			head, readErr = good, nil
			tc.mutate()
			if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 7}); err == nil {
				t.Fatal("unproven stopped image accepted")
			}
			if h.Publisher().WorkspaceState("v").Ack != nil {
				t.Fatal("refusal persisted an ACK")
			}
		})
	}
	head, readErr = good, nil
	if _, err := ws.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 7}); err != nil {
		t.Fatal("metadata generation changed before ACK", err)
	}
	ack := h.Publisher().WorkspaceState("v").Ack
	if ack == nil || ack.Epoch != 1 || ack.Holder != good.Holder || ack.RemoteLSN != 42 || ack.HeadGen != 7 {
		t.Fatalf("wrong persisted stop identity/readout: %+v", ack)
	}
}
