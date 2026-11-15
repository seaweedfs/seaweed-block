package authority

import (
	"context"
	"testing"
	"time"
)

func TestWorkspaceExpiredLeaseCannotAcknowledgeCLEAN(t *testing.T) {
	disk, _ := newTempStore(t)
	now := time.Unix(1000, 0)
	p := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(time.Second, func() time.Time { return now }))
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	if err := p.BeginWorkspaceRevocation(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, HeadGen: 7}); err == nil {
		t.Fatal("expired lease recorded a CLEAN revocation ACK")
	}
	if p.WorkspaceState("v").Ack != nil {
		t.Fatal("refused ACK persisted")
	}
}
