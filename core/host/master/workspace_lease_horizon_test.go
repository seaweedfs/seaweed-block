package master

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// stalledGuardStore blocks every lease write once armed until release
// closes: a store that never comes back (issue #205, control 1).
type stalledGuardStore struct {
	authority.AuthorityStore
	armed   atomic.Bool
	puts    atomic.Int64
	release chan struct{}
}

func (s *stalledGuardStore) Put(r authority.DurableRecord) error {
	if s.armed.Load() && r.Workspace != nil && r.Workspace.Lease != nil {
		s.puts.Add(1)
		<-s.release
	}
	return s.AuthorityStore.Put(r)
}

// Control 1 (host side): with the master's store stalled for good, a real
// guard keeps serving on clamped grants and fences by the durable horizon
// minus half the skew bound, not earlier and not later than the horizon.
func TestWorkspaceLeaseGuardFencesByHorizonUnderStalledStore(t *testing.T) {
	disk, err := authority.NewFileAuthorityStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stall := &stalledGuardStore{AuthorityStore: disk, release: make(chan struct{})}
	var inflight sync.WaitGroup
	t.Cleanup(func() {
		inflight.Wait()
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	})
	dir := &skewDirective{asks: make(chan authority.AssignmentAsk, 1)}
	p := authority.NewPublisher(dir, authority.WithStore(stall), authority.WithWorkspaceLeaseConfig(3*time.Second, nil))
	seedSkewPublisher(t, p, dir)
	t.Cleanup(p.CloseWorkspaceExtenders)
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(stall.release) }) }) // runs first: unblock the extender
	var stops atomic.Int32
	g, err := volume.NewWorkspaceLeaseGuard(workspaceRPCView{frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: 1, Healthy: true}},
		deadlineLeaseClient{p: p, inflight: &inflight}, &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "v", ReplicaId: "r", Epoch: 1},
		func(context.Context) (uint64, error) { stops.Add(1); return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(10 * time.Second):
			t.Error("guard did not settle")
		}
	})
	for wait := time.Now().Add(3 * time.Second); !g.Projection().Healthy; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(wait) {
			t.Fatal("first grant did not arrive")
		}
	}
	lease := p.WorkspaceState("v").Lease
	horizon := time.Unix(0, lease.ExpiresAtNS)
	halfSkew := time.Duration(lease.MaxClockSkewNS) / 2
	stall.armed.Store(true)
	var fencedAt time.Time
	for deadline := horizon.Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if !g.Projection().Healthy {
			fencedAt = time.Now()
			break
		}
	}
	if fencedAt.IsZero() {
		t.Fatalf("guard still serving 2 s past the durable horizon %s", horizon.Format(time.RFC3339Nano))
	}
	if fencedAt.After(horizon.Add(300 * time.Millisecond)) {
		t.Fatalf("guard fenced %s after the durable horizon", fencedAt.Sub(horizon))
	}
	if early := horizon.Add(-halfSkew - 500*time.Millisecond); fencedAt.Before(early) {
		t.Fatalf("guard fenced %s before horizon - skew/2: grants did not clamp to the horizon", early.Sub(fencedAt))
	}
	if stall.puts.Load() == 0 {
		t.Fatal("no extender write reached the stalled store")
	}
	t.Logf("guard fenced %s before the durable horizon (half skew %s)", horizon.Sub(fencedAt).Round(time.Millisecond), halfSkew)
}

// Control 6a and the reply clamp: through the services layer, QueryWorkspace
// and RevokeWorkspace report the granted expiry, not the durable horizon; a
// renewal during a held reservation write of the SAME volume (revoked and
// ACKed, so deletion is admitted) returns the revoking lease without waiting,
// and once the reservation is published renewals are refused.
func TestWorkspaceRenewalRPCDuringHeldSameVolumeReservation(t *testing.T) {
	h, c, ctx := reviewBindingHost(t)
	reviewLegacyBind(t, h, c, ctx, "vol-b")
	line := waitAuthorityLine(t, h.Publisher(), "vol-b")
	svc := &services{host: h}
	id := &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "vol-b", ReplicaId: line.ReplicaID, Epoch: line.Epoch}
	first, err := svc.RenewWorkspaceLease(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	durable := h.Publisher().WorkspaceState("vol-b").Lease.ExpiresAtNS
	if first.ExpiresAtNs >= durable {
		t.Fatalf("fixture: grant %d not below the durable horizon %d", first.ExpiresAtNs, durable)
	}
	q, err := svc.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "vol-b"})
	if err != nil || q.Lease == nil || q.Lease.ExpiresAtNs != first.ExpiresAtNs {
		t.Fatalf("QueryWorkspace must report the grant %d, got %+v %v", first.ExpiresAtNs, q.GetLease(), err)
	}
	rv, err := svc.RevokeWorkspace(ctx, id)
	if err != nil || !rv.Revoking || rv.ExpiresAtNs != first.ExpiresAtNs {
		t.Fatalf("RevokeWorkspace must report the grant %d, got %+v %v", first.ExpiresAtNs, rv, err)
	}
	if _, err := svc.AckWorkspaceRevoked(ctx, &control.WorkspaceRevocationAck{Identity: id, HeadGen: 9}); err != nil {
		t.Fatal(err)
	}
	deletions := h.Lifecycle().Deletions
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	releaseHook := func() { releaseOnce.Do(func() { close(release) }) }
	deletions.SetPersistHookForTest(func() error {
		enterOnce.Do(func() { close(entered) })
		<-release
		return nil
	})
	marked := make(chan error, 1)
	go func() {
		_, err := svc.MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "vol-b", Token: "tok"})
		marked <- err
	}()
	renewed := make(chan *control.WorkspaceLeaseFact, 1)
	renewErr := make(chan error, 1)
	renewStarted := false
	t.Cleanup(func() { // bounded: release, settle both workers, then clear the hook
		releaseHook()
		settled := true
		select {
		case <-marked:
		case <-time.After(5 * time.Second):
			settled = false
			t.Error("held reservation write did not settle")
		}
		if renewStarted {
			select {
			case <-renewErr:
			case <-time.After(5 * time.Second):
				settled = false
				t.Error("renewal did not settle")
			}
		}
		if settled {
			deletions.SetPersistHookForTest(nil)
		}
	})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("reservation write of vol-b never reached the store")
	}
	renewStarted = true
	go func() {
		f, err := svc.RenewWorkspaceLease(ctx, id)
		renewed <- f
		renewErr <- err
	}()
	select {
	case f := <-renewed:
		err := <-renewErr
		renewErr <- err
		if err != nil || !f.Revoking || f.ExpiresAtNs != first.ExpiresAtNs {
			t.Fatalf("renewal during the held same-volume reservation: %+v %v; want the revoking lease with the granted expiry", f, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("renewal waited on the held reservation write")
	}
	releaseHook()
	select {
	case err := <-marked:
		marked <- err
		if err != nil {
			t.Fatal("mark deleting:", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("mark deleting did not finish")
	}
	if _, err := svc.RenewWorkspaceLease(ctx, id); err == nil || !strings.Contains(err.Error(), "being deleted") {
		t.Fatalf("renewal after the reservation was published: %v; want refused as being deleted", err)
	}
}
