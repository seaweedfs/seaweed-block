package authority

import (
	"context"
	"testing"
	"time"
)

func TestWorkspaceAckUsesGrantedHalfSkewBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		elapsed time.Duration
		allowed bool
	}{
		{"before", 28*time.Second - time.Nanosecond, true},
		{"at", 28 * time.Second, false},
		{"after", 29 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk, _ := newTempStore(t)
			now := time.Unix(1000, 0)
			p := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(30*time.Second, func() time.Time { return now }), WithWorkspaceMaxClockSkew(4*time.Second))
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
			now = now.Add(tc.elapsed)
			err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, HeadGen: 7})
			if (err == nil) != tc.allowed {
				t.Fatalf("ACK allowed=%t, want %t: %v", err == nil, tc.allowed, err)
			}
			if (p.WorkspaceState("v").Ack != nil) != tc.allowed {
				t.Fatal("ACK persistence differs from admission")
			}
		})
	}
}
