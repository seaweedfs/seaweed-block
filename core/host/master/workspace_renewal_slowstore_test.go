package master

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

// slowGuardStore holds every workspace-lease Put, once armed, until a lease
// renewal has succeeded after the Put entered (issue #205): a slow store write,
// with the slowness defined by progress instead of a fixed sleep.
type slowGuardStore struct {
	authority.AuthorityStore
	armed    atomic.Bool
	renewals atomic.Int64  // successful renewals seen by the client
	kick     chan struct{} // a renewal succeeded (buffered 1)
	stop     chan struct{} // closed at cleanup: frees any held Put
	held     atomic.Int64  // lease writes that were held and then completed
}

func newSlowGuardStore(s authority.AuthorityStore) *slowGuardStore {
	return &slowGuardStore{AuthorityStore: s, kick: make(chan struct{}, 1), stop: make(chan struct{})}
}

func (s *slowGuardStore) renewed() {
	s.renewals.Add(1)
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

func (s *slowGuardStore) Put(r authority.DurableRecord) error {
	if s.armed.Load() && r.Workspace != nil && r.Workspace.Lease != nil {
		base := s.renewals.Load()
		for s.renewals.Load() <= base {
			select {
			case <-s.kick:
			case <-s.stop:
				return s.AuthorityStore.Put(r)
			}
		}
		defer s.held.Add(1)
	}
	return s.AuthorityStore.Put(r)
}

// deadlineLeaseClient calls the publisher through the master's leaseFact and,
// like a gRPC client, returns at the caller's deadline even when the master
// call is still blocked. In-flight calls are joined before the store closes.
type deadlineLeaseClient struct {
	p        *authority.Publisher
	inflight *sync.WaitGroup
	slow     *slowGuardStore // optional: told about each successful renewal
}

func (c deadlineLeaseClient) RenewWorkspaceLease(ctx context.Context, id *control.WorkspaceIdentity, _ ...grpc.CallOption) (*control.WorkspaceLeaseFact, error) {
	type result struct {
		fact *control.WorkspaceLeaseFact
		err  error
	}
	done := make(chan result, 1)
	c.inflight.Add(1)
	go func() {
		defer c.inflight.Done()
		l, err := c.p.RenewWorkspaceLease(context.Background(), id.VolumeId, id.ReplicaId, id.Epoch)
		if err != nil {
			done <- result{err: err}
			return
		}
		done <- result{fact: leaseFact(id.VolumeId, l)}
	}()
	select {
	case r := <-done:
		if r.err == nil && c.slow != nil {
			c.slow.renewed()
		}
		return r.fact, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (deadlineLeaseClient) AckWorkspaceRevoked(context.Context, *control.WorkspaceRevocationAck, ...grpc.CallOption) (*control.WorkspaceEmpty, error) {
	panic("a slow store must not lead to a clean ACK")
}

// A live host must keep its lease while the master's authority store is slow:
// a store write longer than the host's 2 s renewal timeout must not fence the
// guard and shut the volume down (issue #205). TTL 6 s gives the guard 4 s of
// validity (6 s minus half the default 4 s skew), renewed every ~1.3 s.
func TestWorkspaceLeaseGuardSurvivesSlowAuthorityStore(t *testing.T) {
	disk, err := authority.NewFileAuthorityStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	slow := newSlowGuardStore(disk)
	var inflight sync.WaitGroup
	t.Cleanup(func() {
		slow.armed.Store(false)
		close(slow.stop)
		inflight.Wait()
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	})
	dir := &skewDirective{asks: make(chan authority.AssignmentAsk, 1)}
	p := authority.NewPublisher(dir, authority.WithStore(slow), authority.WithWorkspaceLeaseConfig(6*time.Second, nil))
	t.Cleanup(p.CloseWorkspaceExtenders) // join extenders before the store closes
	seedSkewPublisher(t, p, dir)
	var stops atomic.Int32
	g, err := volume.NewWorkspaceLeaseGuard(workspaceRPCView{frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: 1, Healthy: true}},
		deadlineLeaseClient{p: p, inflight: &inflight, slow: slow}, &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1},
		func(context.Context) (uint64, error) { stops.Add(1); return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- g.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-runDone // Run returns on cancel; the go test timeout bounds a hang
	})
	for !g.Projection().Healthy { // progress: the first grant
		time.Sleep(20 * time.Millisecond)
	}

	// Every lease write is now held until a renewal succeeded during its hold,
	// so a renewal that waited for the store would deadlock this test. The
	// sampler records any fence while armed; three held writes must complete.
	var fenced atomic.Bool
	sampled := make(chan struct{})
	slow.armed.Store(true)
	go func() {
		defer close(sampled)
		for slow.armed.Load() {
			if !g.Projection().Healthy || stops.Load() != 0 {
				fenced.Store(true)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	for slow.held.Load() < 3 {
		if fenced.Load() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	slow.armed.Store(false)
	<-sampled
	if fenced.Load() || !g.Projection().Healthy || stops.Load() != 0 {
		t.Fatalf("guard fenced while lease writes were held behind renewals (held=%d renewals=%d stops=%d)", slow.held.Load(), slow.renewals.Load(), stops.Load())
	}
	t.Logf("guard stayed healthy through %d held lease writes (%d renewals)", slow.held.Load(), slow.renewals.Load())
}

// A durable deletion-reservation write of an unrelated volume must not block
// a lease renewal: the renewal RPC checks Deletions.Reserved and the volume
// record, and both lifecycle stores hold their mutex across file I/O
// (issue #205).
func TestWorkspaceRenewalRPCDoesNotWaitForHeldReservationWrite(t *testing.T) {
	h, c, ctx := reviewBindingHost(t)
	reviewLegacyBind(t, h, c, ctx, "vol-b")
	line := waitAuthorityLine(t, h.Publisher(), "vol-b")
	svc := &services{host: h}
	id := &control.WorkspaceIdentity{VolumeId: "vol-b", ReplicaId: line.ReplicaID, Epoch: line.Epoch}
	if _, err := svc.RenewWorkspaceLease(ctx, id); err != nil {
		t.Fatal(err) // first grant
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
	admitted := make(chan error, 1)
	go func() {
		_, err := deletions.Admit("vol-x", "tok") // an unrelated volume, a real durable write
		admitted <- err
	}()
	renewed := make(chan error, 1)
	renewStarted := false
	// Bounded cleanup: release the hook first, settle BOTH workers, and only
	// then clear the hook. Never call the setter after a timed-out join: the
	// store mutex may still be held.
	t.Cleanup(func() {
		releaseHook()
		settled := true
		select {
		case <-admitted:
		case <-time.After(5 * time.Second):
			settled = false
			t.Error("held reservation write did not settle within 5 s after release")
		}
		if renewStarted {
			select {
			case <-renewed:
			case <-time.After(5 * time.Second):
				settled = false
				t.Error("renewal did not settle within 5 s after release")
			}
		}
		if settled {
			deletions.SetPersistHookForTest(nil)
		}
	})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("reservation write never reached the store")
	}
	start := time.Now()
	renewStarted = true
	go func() { _, err := svc.RenewWorkspaceLease(ctx, id); renewed <- err }()
	select {
	case err := <-renewed:
		renewed <- err // keep it for the cleanup join
		if err != nil {
			t.Fatalf("renewal of vol-b: %v", err)
		}
		t.Logf("renewal RPC of vol-b took %s while vol-x's reservation write was held", time.Since(start))
	case <-time.After(2 * time.Second):
		t.Fatalf("renewal RPC of vol-b was still blocked %s behind the held reservation write of vol-x", time.Since(start).Round(time.Millisecond))
	}
}
