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
		restart bool
	}{
		{"before", 28*time.Second - time.Nanosecond, true, false},
		{"at", 28 * time.Second, false, false},
		{"after", 29 * time.Second, false, false},
		{"horizon-before", 43*time.Second - time.Nanosecond, true, true},
		{"horizon-at", 43 * time.Second, false, true},
		{"horizon-after", 44 * time.Second, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk, _ := newTempStore(t)
			start := time.Unix(1000, 0)
			now := start
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
			if tc.restart {
				// Reopening at +20s gives a restart deadline of +50s, beyond
				// the persisted +45s horizon. ACK must stop at +43s.
				now = start.Add(20 * time.Second)
				p = NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(30*time.Second, func() time.Time { return now }), WithWorkspaceMaxClockSkew(4*time.Second))
				p.mu.Lock()
				deadline := p.leaseDeadlineLocked("v")
				horizon := p.workspace["v"].Lease.ExpiresAtNS
				p.mu.Unlock()
				if !deadline.After(time.Unix(0, horizon)) {
					t.Fatal("fixture deadline does not exceed persisted horizon")
				}
			}
			now = start.Add(tc.elapsed)
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
