package volume

import (
	"context"
	"strings"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// INV-WS-LEASE-001 / V1-SPEC r2.55 gap 9: invalid-reply fencing from a held,
// unexpired lease, rather than passing because a synthetic deadline expired.
func TestWorkspaceGuardFencesOnInvalidReply(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*control.WorkspaceLeaseFact) *control.WorkspaceLeaseFact
	}{
		{"nil", func(*control.WorkspaceLeaseFact) *control.WorkspaceLeaseFact { return nil }},
		{"volume", func(r *control.WorkspaceLeaseFact) *control.WorkspaceLeaseFact { r.VolumeId = "other"; return r }},
		{"replica", func(r *control.WorkspaceLeaseFact) *control.WorkspaceLeaseFact { r.ReplicaId = "other"; return r }},
		{"epoch", func(r *control.WorkspaceLeaseFact) *control.WorkspaceLeaseFact { r.Epoch++; return r }},
		{"zero validity", func(r *control.WorkspaceLeaseFact) *control.WorkspaceLeaseFact { r.ValidForNs = 0; return r }},
		{"negative validity", func(r *control.WorkspaceLeaseFact) *control.WorkspaceLeaseFact { r.ValidForNs = -1; return r }},
		{"over maximum validity", func(r *control.WorkspaceLeaseFact) *control.WorkspaceLeaseFact {
			r.ValidForNs = int64(30*time.Second) + 1
			return r
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			calls, stops, acks := 0, 0, 0
			client := workspaceTestClient{
				renew: func() (*control.WorkspaceLeaseFact, error) {
					calls++
					r := &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(10 * time.Second)}
					if calls == 2 {
						r = tc.change(r)
					}
					return r, nil
				},
				ack: func() error { acks++; return nil },
			}
			g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client,
				&control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", ServerId: "node", Epoch: 1},
				func(context.Context) (uint64, error) { stops++; return 7, nil }, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := g.Refresh(ctx); err != nil || !g.Held() || !g.Projection().Healthy {
				t.Fatalf("valid unexpired lease was not held before injection: %v cause=%s", err, g.FenceCause())
			}
			if err := g.Refresh(ctx); err == nil || !strings.Contains(err.Error(), "invalid workspace lease reply") {
				t.Fatalf("invalid reply did not trigger its own refusal: %v cause=%s", err, g.FenceCause())
			}
			if !strings.Contains(g.FenceCause(), "invalid lease reply") || g.Held() || g.Projection().Healthy || stops != 1 || acks != 0 {
				t.Fatalf("invalid-reply fence: cause=%s held=%v healthy=%v stops=%d acks=%d", g.FenceCause(), g.Held(), g.Projection().Healthy, stops, acks)
			}
			if err := g.Refresh(ctx); err == nil || g.Held() || g.Projection().Healthy || calls != 2 || stops != 1 || acks != 0 {
				t.Fatalf("fenced guard reopened or renewed: err=%v calls=%d stops=%d acks=%d", err, calls, stops, acks)
			}
		})
	}
}

func TestWorkspaceGuardAcceptsMaximumValidReply(t *testing.T) {
	now := time.Now()
	client := workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(30 * time.Second)}, nil
	}}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client,
		&control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", ServerId: "node", Epoch: 1},
		func(context.Context) (uint64, error) { t.Fatal("valid lease drained"); return 0, nil }, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err != nil || !g.Held() || !g.Projection().Healthy {
		t.Fatalf("maximum valid lease refused: %v cause=%s", err, g.FenceCause())
	}
}
