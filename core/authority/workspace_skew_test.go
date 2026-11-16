package authority

import (
	"context"
	"testing"
	"time"
)

func TestWorkspaceClockSkewPersistsAndProtectsRestart(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "restart-smaller-config"}[restart], func(t *testing.T) {
			disk, _ := newTempStore(t)
			now := time.Unix(1000, 0)
			start := now
			p := NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(6*time.Second, func() time.Time { return now }), WithWorkspaceMaxClockSkew(4*time.Second))
			if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
				t.Fatal(err)
			}
			lease, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1)
			if err != nil {
				t.Fatal(err)
			}
			if lease.MaxClockSkewNS != int64(4*time.Second) {
				t.Fatal("configured margin not in durable grant", lease)
			}
			boundary := start.Add(10 * time.Second)
			if restart {
				now = start.Add(7 * time.Second)
				p = NewPublisher(nil, WithStore(disk), WithWorkspaceLeaseConfig(6*time.Second, func() time.Time { return now }), WithWorkspaceMaxClockSkew(time.Second))
				boundary = now.Add(6*time.Second + 4*time.Second)
			}
			q := WorkspaceMoveRequest{RequestID: "m", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "old", TargetHost: "new", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true}
			head := WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old/r"}
			now = boundary.Add(-time.Nanosecond)
			if _, err := p.MoveWorkspace(context.Background(), q, head); err == nil {
				t.Fatal("forced before skew boundary")
			}
			if len(p.WorkspaceState("v").Moves) != 0 {
				t.Fatal("early move wrote journal")
			}
			now = boundary
			if _, err := p.MoveWorkspace(context.Background(), q, head); err != nil {
				t.Fatal("margin elapsed", err)
			}
		})
	}
}
