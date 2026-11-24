package volume

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/engine"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// Lease-grant rule: renewals refused before the first grant (a restart before the
// forced-move barrier) leave the lease not held; the first successful renewal
// is logged as a re-grant after the refused ones, with its time.
func TestWorkspaceLeaseGrantIsLoggedOnceAfterRefusedRenewals(t *testing.T) {
	logs := captureLog(t)
	now := time.Unix(1000, 0)
	calls := 0
	client := workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		calls++
		if calls <= 2 {
			return nil, errors.New("workspace forced move lease expiry plus clock-skew margin has not elapsed")
		}
		return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(10 * time.Second)}, nil
	}}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := g.Refresh(context.Background()); err == nil {
			t.Fatal("a refused renewal was not reported")
		}
		if g.Held() || g.Projection().Healthy {
			t.Fatal("the lease is held before any grant")
		}
	}
	if strings.Contains(logs.String(), "granted") {
		t.Fatalf("a grant was logged before any grant; log=%q", logs.String())
	}
	// One successful renewal: with the fake clock a second one would run
	// against a real-time context deadline in 1970.
	if err := g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !g.Held() || !g.Projection().Healthy {
		t.Fatal("the granted lease is not held")
	}
	out := logs.String()
	if n := strings.Count(out, "granted"); n != 1 || !strings.Contains(out, "workspace lease: re-granted after 2 refused renewal(s) volume=v replica=r epoch=1 valid for 10s at unix_ms=1000000") {
		t.Fatalf("want exactly one re-grant line with its time, got %d; log=%q", n, out)
	}
	now = now.Add(11 * time.Second)
	if g.Held() {
		t.Fatal("an expired lease is still held")
	}
}

// A first reply that revokes grants nothing: after refused renewals, a valid
// first reply with Revoking set fences the guard, is never held and logs no
// grant line (the line is a readiness timing witness).
func TestWorkspaceLeaseFirstRevokingReplyIsNotLoggedAsAGrant(t *testing.T) {
	logs := captureLog(t)
	now := time.Unix(1000, 0)
	calls := 0
	client := workspaceTestClient{
		renew: func() (*control.WorkspaceLeaseFact, error) {
			calls++
			if calls == 1 {
				return nil, errors.New("workspace forced move lease expiry plus clock-skew margin has not elapsed")
			}
			return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(10 * time.Second), Revoking: true}, nil
		},
		ack: func() error { return nil },
	}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	_ = g.Refresh(context.Background())
	_ = g.Refresh(context.Background())
	if g.Held() || g.Projection().Healthy {
		t.Fatal("a revoking first reply left the lease held")
	}
	if out := logs.String(); strings.Contains(out, "granted") || !strings.Contains(out, "revoked by the master") {
		t.Fatalf("want no grant line and the revocation fence; log=%q", out)
	}
}

// Lease-readiness rule: a workspace primary reports NotReadyReason=lease_not_held,
// first, while its guard is missing or holds no granted lease, even with the
// backend admitting I/O; ready once the lease is held; lease_not_held again
// after the guard fences.
func TestStatusServer_WorkspacePrimaryNotReadyWithoutHeldLease(t *testing.T) {
	now := time.Unix(1000, 0)
	p := stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 1, EndpointVersion: 1}}
	view := NewAdapterProjectionView(p, "v1", "r1", nil)
	view.workspaceRequired = true
	s := NewStatusServer(view)
	s.SetDurableStatusSource(stubDurableStatusSource{vols: []durable.VolumeStatus{{VolumeID: "v1", Operational: true}}})
	check := func(name string, wantReady bool) {
		t.Helper()
		got := s.statusProjection()
		if got.FrontendPrimaryReady != wantReady || (!wantReady && got.NotReadyReason != NotReadyLeaseNotHeld) || (wantReady && got.NotReadyReason != "") {
			t.Errorf("%s: ready=%v reason=%q, want ready=%v (reason %q when not ready)", name, got.FrontendPrimaryReady, got.NotReadyReason, wantReady, NotReadyLeaseNotHeld)
		}
	}
	check("no guard installed", false)
	g, err := NewWorkspaceLeaseGuard(view, workspaceTestClient{}, &control.WorkspaceIdentity{VolumeId: "v1", ReplicaId: "r1", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	view.workspaceGuard.Store(g)
	check("guard without a grant", false)
	g.mu.Lock()
	g.until = now.Add(10 * time.Second)
	g.mu.Unlock()
	check("lease held", true)
	g.mu.Lock()
	g.fenceLocked("test: revoked")
	g.mu.Unlock()
	check("guard fenced", false)
}
