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
// renewal that STARTED after the Put entered has succeeded (issue #205): a slow
// store write, with the slowness defined by progress instead of a fixed sleep.
// Each held Put takes a generation; a renewal is credited with the generation
// it observed before its call, so a reply computed before the hold never
// releases it.
type slowGuardStore struct {
	authority.AuthorityStore
	armed    atomic.Bool
	gen      atomic.Int64  // generation of the latest held Put
	credited atomic.Int64  // highest generation a fresh renewal was started under
	renewals atomic.Int64  // successful renewals seen by the client
	kick     chan struct{} // a renewal succeeded (buffered 1)
	stop     chan struct{} // closed by release(): frees any held Put
	stopOnce sync.Once
	held     atomic.Int64 // lease writes that were held and then completed
	// Diagnostics only, printed when the test fails: when renewals succeeded.
	t0     time.Time
	lastOK atomic.Int64 // time since t0 of the latest successful renewal
	maxGap atomic.Int64 // longest time between two successful renewals
}

func newSlowGuardStore(s authority.AuthorityStore) *slowGuardStore {
	return &slowGuardStore{AuthorityStore: s, kick: make(chan struct{}, 1), stop: make(chan struct{}), t0: time.Now()}
}

// release disarms and frees every held Put; idempotent.
func (s *slowGuardStore) release() {
	s.armed.Store(false)
	s.stopOnce.Do(func() { close(s.stop) })
}

// renewed credits a successful renewal that started under generation g.
func (s *slowGuardStore) renewed(g int64) {
	s.renewals.Add(1)
	now := int64(time.Since(s.t0))
	if prev := s.lastOK.Swap(now); prev != 0 {
		for gap := now - prev; ; {
			m := s.maxGap.Load()
			if gap <= m || s.maxGap.CompareAndSwap(m, gap) {
				break
			}
		}
	}
	for {
		c := s.credited.Load()
		if g <= c || s.credited.CompareAndSwap(c, g) {
			break
		}
	}
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

func (s *slowGuardStore) Put(r authority.DurableRecord) error {
	if s.armed.Load() && r.Workspace != nil && r.Workspace.Lease != nil {
		g := s.gen.Add(1)
		for s.credited.Load() < g {
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
	var g int64
	if c.slow != nil {
		g = c.slow.gen.Load() // a reply can only release holds that entered before this call
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
		done <- result{fact: leaseFact(c.p, id.VolumeId, l)}
	}()
	select {
	case r := <-done:
		if r.err == nil && c.slow != nil {
			c.slow.renewed(g)
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
		slow.release()
		inflight.Wait()
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	})
	dir := &skewDirective{asks: make(chan authority.AssignmentAsk, 1)}
	p := authority.NewPublisher(dir, authority.WithStore(slow), authority.WithWorkspaceLeaseConfig(6*time.Second, nil))
	t.Cleanup(p.CloseWorkspaceExtenders) // join extenders before the store closes
	t.Cleanup(slow.release)              // runs before the join above: frees a held Put first
	seedSkewPublisher(t, p, dir)
	var stops atomic.Int32
	g, err := volume.NewWorkspaceLeaseGuard(workspaceRPCView{frontend.Projection{VolumeID: "v", ReplicaID: "r", Epoch: 1, Healthy: true}},
		deadlineLeaseClient{p: p, inflight: &inflight, slow: slow}, &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "v", ReplicaId: "r", Epoch: 1},
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
	t.Cleanup(slow.release)       // first of all cleanups: a held Put never blocks the joins
	for !g.Projection().Healthy { // progress: the first grant
		time.Sleep(20 * time.Millisecond)
	}

	// Every lease write is now held until a renewal succeeded during its hold,
	// so a renewal that waited for the store would deadlock this test. The
	// sampler records any fence while armed; three held writes must complete.
	var fenced atomic.Bool
	var sinceOKAtFence atomic.Int64 // time since the latest successful renewal when the fence was first seen
	sampled := make(chan struct{})
	slow.armed.Store(true)
	go func() {
		defer close(sampled)
		for slow.armed.Load() {
			if !g.Projection().Healthy || stops.Load() != 0 {
				if fenced.CompareAndSwap(false, true) {
					sinceOKAtFence.Store(int64(time.Since(slow.t0)) - slow.lastOK.Load())
				}
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
		// The gaps tell a starved runner (a gap near the 4 s validity) from a
		// guard that fenced although renewals kept succeeding in time.
		t.Fatalf("guard fenced while lease writes were held behind renewals (held=%d renewals=%d stops=%d); longest gap between successful renewals %s, time since the latest one when the fence was seen %s, validity 4s; the guard gives as cause: %q",
			slow.held.Load(), slow.renewals.Load(), stops.Load(), time.Duration(slow.maxGap.Load()), time.Duration(sinceOKAtFence.Load()), g.FenceCause())
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
	id := &control.WorkspaceIdentity{ServerId: "node-a", VolumeId: "vol-b", ReplicaId: line.ReplicaID, Epoch: line.Epoch}
	if _, err := svc.RenewWorkspaceLease(loopbackPeer(ctx), id); err != nil {
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
	go func() { _, err := svc.RenewWorkspaceLease(loopbackPeer(ctx), id); renewed <- err }()
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
