package volume

import (
	"bytes"
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func workspaceRuntimeHost(t *testing.T, addr string, required bool) *Host {
	t.Helper()
	h, err := New(Config{MasterAddr: addr, ServerID: "node-a", VolumeID: "v1", ReplicaID: "r1", DataAddr: "127.0.0.1:9100", CtrlAddr: "127.0.0.1:9101", EnableT1Readiness: true, WorkspaceLease: required})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.applyFixtureFact(&control.AssignmentFact{VolumeId: "v1", ReplicaId: "r1", Epoch: 3, EndpointVersion: 1, DataAddr: "127.0.0.1:9100", CtrlAddr: "127.0.0.1:9101"})
	end := time.Now().Add(time.Second)
	for h.realAdpt.Projection().Mode != engine.ModeHealthy && time.Now().Before(end) {
		time.Sleep(time.Millisecond)
	}
	if h.realAdpt.Projection().Mode != engine.ModeHealthy {
		t.Fatal("fixture did not reach ordinary assignment health")
	}
	return h
}

func TestWorkspaceHostRequiresLeaseBeforeFrontendAndCut(t *testing.T) {
	h := workspaceRuntimeHost(t, "127.0.0.1:1", true)
	if h.ProjectionView().Projection().Healthy {
		t.Fatal("workspace host reports healthy without ever acquiring a lease")
	}
	fixture := testutil.NewLayoutFixture(t, "v1", 64, 8)
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8, ChunkHolder: "node-a/r1"}
	fixture.Configure(&cfg)
	p, err := durable.NewDurableProvider(cfg, h.ProjectionView())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err = p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	be := p.Backend("v1")
	if _, err = be.Write(context.Background(), 0, make([]byte, 4096)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatal("unleased write", err)
	}
	if _, err = p.CaptureVolume(context.Background(), "v1"); err == nil {
		t.Fatal("unleased cut succeeded")
	}
	if head := runtimeLayoutHead(t, fixture); head.Gen != 1 || head.Holder != "" {
		t.Fatal("unleased HEAD mutation", head)
	}
	legacy := workspaceRuntimeHost(t, "127.0.0.1:1", false)
	if !legacy.ProjectionView().Projection().Healthy {
		t.Fatal("ordinary non-workspace host was gated")
	}
}

// Wire peer exposes only the public lease RPCs. Storage, provider, projection,
// real host identity and lease runtime below are the production implementations.
type runtimeLeaseServer struct {
	control.UnimplementedWorkspaceServiceServer
	mu                          sync.Mutex
	ttl                         time.Duration
	disabled, blocked, revoking bool
	blockedCalls                int // renewals that arrived while blocked
	renewals                    int // every renewal that arrived
	acks                        int
	ackGen                      uint64
	first                       chan struct{}
	once                        sync.Once
	stopped                     *atomic.Bool
}

func (s *runtimeLeaseServer) RenewWorkspaceLease(ctx context.Context, id *control.WorkspaceIdentity) (*control.WorkspaceLeaseFact, error) {
	s.once.Do(func() { close(s.first) })
	s.mu.Lock()
	s.renewals++
	ttl, disabled, blocked, revoking := s.ttl, s.disabled, s.blocked, s.revoking
	s.mu.Unlock()
	if disabled {
		return nil, status.Error(codes.FailedPrecondition, "workspace RPC disabled")
	}
	if blocked {
		s.mu.Lock()
		s.blockedCalls++
		s.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &control.WorkspaceLeaseFact{VolumeId: id.VolumeId, ReplicaId: id.ReplicaId, Epoch: id.Epoch, ValidForNs: int64(ttl), ExpiresAtNs: time.Now().Add(ttl).UnixNano(), Revoking: revoking}, nil
}
func (s *runtimeLeaseServer) AckWorkspaceRevoked(_ context.Context, q *control.WorkspaceRevocationAck) (*control.WorkspaceEmpty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped != nil && !s.stopped.Load() {
		return nil, status.Error(codes.FailedPrecondition, "ACK preceded physical drain")
	}
	s.acks++
	s.ackGen = q.HeadGen
	return &control.WorkspaceEmpty{}, nil
}
func runtimeWire(t *testing.T, s *runtimeLeaseServer) string {
	t.Helper()
	s.first = make(chan struct{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	control.RegisterWorkspaceServiceServer(srv, s)
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}
func runtimeAwait(t *testing.T, what string, fn func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for !fn() {
		select {
		case <-ctx.Done():
			t.Fatal(what, ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}
func runtimeLayoutHead(t *testing.T, fixture *testutil.LayoutFixture) layout.Head {
	t.Helper()
	head, found, err := fixture.Catalog.ReadHead(context.Background(), fixture.Ref)
	if err != nil || !found {
		t.Fatalf("layout HEAD: found=%v err=%v", found, err)
	}
	return head
}

func runtimePublishedBlock(t *testing.T, fixture *testutil.LayoutFixture, block uint64) []byte {
	t.Helper()
	head := runtimeLayoutHead(t, fixture)
	if head.Current == nil {
		t.Fatal("published HEAD has no image")
	}
	st, err := fixture.Catalog.Store(fixture.Ref)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := layout.NewReader("/provider-test", fixture.Ref.Dir(), st.Fetch)
	if err != nil {
		t.Fatal(err)
	}
	image, err := reader.Open(context.Background(), *head.Current)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := image.Chunk(context.Background(), block/8)
	if err != nil {
		t.Fatal(err)
	}
	if chunk.Zero {
		return make([]byte, 4096)
	}
	off := (block % 8) * 4096
	return chunk.Data[off : off+4096]
}

func runtimeStore(t *testing.T, h *Host) (*durable.DurableProvider, *testutil.LayoutFixture) {
	t.Helper()
	fixture := testutil.NewLayoutFixture(t, "v1", 64, 8)
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8, ChunkHolder: "node-a/r1"}
	fixture.Configure(&cfg)
	p, err := durable.NewDurableProvider(cfg, h.ProjectionView())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err = p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err = p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	return p, fixture
}
func runtimeDone(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("lease runtime did not finish")
		return nil
	}
}

func TestWorkspaceHostLeaseExpiryAndRevokeCloseRealProvider(t *testing.T) {
	for _, kind := range []string{"expiry-stalled-renewal", "detach-then-revoke", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			var stopped atomic.Bool
			// A 3 s lease while the volume is set up (#227: a 300 ms lease could
			// expire under load before the detach/cut steps ran); the expiry case
			// switches to 300 ms below.
			wire := &runtimeLeaseServer{ttl: 3 * time.Second, stopped: &stopped}
			h := workspaceRuntimeHost(t, runtimeWire(t, wire), true)
			p, fixture := runtimeStore(t, h)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- h.RunWorkspaceLease(ctx, func(c context.Context, id *control.WorkspaceIdentity) (uint64, error) {
					if err := p.Backend("v1").StopIO(c, "workspace self-fence"); err != nil {
						return 0, err
					}
					if err := p.Close(); err != nil {
						return 0, err
					}
					stopped.Store(true)
					head, found, err := fixture.Catalog.ReadHead(c, fixture.Ref)
					if err == nil && !found {
						err = errors.New("HEAD absent")
					}
					return head.Gen, err
				})
			}()
			runtimeAwait(t, "leased operational provider", func() bool {
				st := p.DurableStatuses()
				return h.ProjectionView().Projection().Healthy && len(st) == 1 && st[0].Operational
			})
			want := bytes.Repeat([]byte{0x73}, 4096)
			be := p.Backend("v1")
			if n, err := be.Write(ctx, 4096, want); err != nil || n != 4096 {
				t.Fatal(n, err)
			}
			if err := be.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := p.CaptureVolume(ctx, "v1"); err != nil {
				t.Fatal(err)
			}
			before := runtimeLayoutHead(t, fixture)
			if kind == "detach-then-revoke" {
				cut, _, err := p.DetachVolume(ctx, "v1")
				if err != nil {
					t.Fatal(err)
				}
				before = runtimeLayoutHead(t, fixture)
				if before.Gen != cut.Head.Gen {
					t.Fatal("detach result did not name HEAD generation", cut.Head.Gen, before.Gen)
				}
				wire.mu.Lock()
				wire.revoking = true
				wire.mu.Unlock()
			}
			if kind == "expiry-stalled-renewal" {
				// Shorten the lease and wait (progress) until the client guard
				// installed a 300 ms horizon; then stall renewals and wait until a
				// renewal arrived blocked: the client holds a short lease that is
				// not renewed.
				guard := h.view.workspaceGuard.Load()
				if guard == nil {
					t.Fatal("no workspace lease guard")
				}
				guard.mu.Lock()
				lastUntil := guard.until // the current 3 s horizon
				guard.mu.Unlock()
				wire.mu.Lock()
				wire.ttl = 300 * time.Millisecond
				wire.mu.Unlock()
				// Witness on the client: the host's guard installed a 300 ms
				// horizon (a newly installed deadline less than 1 s ahead; the 3 s
				// grants install about 3 s ahead).
				runtimeAwait(t, "the guard installed a 300 ms grant", func() bool {
					guard.mu.Lock()
					until, fenced := guard.until, guard.fenced
					guard.mu.Unlock()
					installed := !fenced && !until.Equal(lastUntil) && until.Sub(time.Now()) < time.Second
					lastUntil = until
					return installed
				})
				wire.mu.Lock()
				wire.blocked = true
				wire.mu.Unlock()
				runtimeAwait(t, "a renewal stalled after a 300 ms grant", func() bool {
					wire.mu.Lock()
					defer wire.mu.Unlock()
					return wire.blockedCalls > 0
				})
			}
			if kind == "cancel" {
				cancel()
				h.FenceWorkspaceLease()
				if h.ProjectionView().Projection().Healthy {
					t.Fatal("cancel fence was delayed")
				}
			}
			runtimeDone(t, done)
			if !stopped.Load() || h.ProjectionView().Projection().Healthy {
				t.Fatal("host/provider remained live")
			}
			if _, err := be.Write(context.Background(), 0, want); err == nil {
				t.Fatal("fenced write succeeded")
			}
			if _, err := p.CaptureVolume(context.Background(), "v1"); err == nil {
				t.Fatal("fenced cut succeeded")
			}
			after := runtimeLayoutHead(t, fixture)
			if after.Gen != before.Gen || after.Position != before.Position {
				t.Fatal("shutdown published an extra cut", before, after)
			}
			if data := runtimePublishedBlock(t, fixture, 1); !bytes.Equal(data, want) {
				t.Fatal("leased data changed")
			}
			wire.mu.Lock()
			acks, gen := wire.acks, wire.ackGen
			wire.mu.Unlock()
			if kind == "detach-then-revoke" {
				if acks != 1 || gen != before.Gen || before.State != layout.StateDetached {
					t.Fatal("ACK not joined to real detach", acks, gen, before)
				}
			} else if acks != 0 {
				t.Fatal("expiry/cancel ACKed clean", acks)
			}
		})
	}
}

func TestWorkspaceHostDisabledRPCNeverFallsBack(t *testing.T) {
	wire := &runtimeLeaseServer{ttl: time.Second, disabled: true}
	h := workspaceRuntimeHost(t, runtimeWire(t, wire), true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- h.RunWorkspaceLease(ctx, func(context.Context, *control.WorkspaceIdentity) (uint64, error) { return 0, nil })
	}()
	select {
	case <-wire.first:
	case <-time.After(time.Second):
		t.Fatal("no lease request")
	}
	if h.ProjectionView().Projection().Healthy {
		t.Fatal("disabled workspace RPC fell back to assignment-only serving")
	}
	cancel()
	runtimeDone(t, done)
	wire.mu.Lock()
	defer wire.mu.Unlock()
	if wire.acks != 0 {
		t.Fatal("disabled RPC cancellation ACKed clean")
	}
}

func TestWorkspaceHostCancelOrExpiryDuringRevokeDrainNeverACKs(t *testing.T) {
	for _, kind := range []string{"cancel", "expiry", "drain-error"} {
		t.Run(kind, func(t *testing.T) {
			wire := &runtimeLeaseServer{ttl: 150 * time.Millisecond}
			h := workspaceRuntimeHost(t, runtimeWire(t, wire), true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{})
			release := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- h.RunWorkspaceLease(ctx, func(context.Context, *control.WorkspaceIdentity) (uint64, error) {
					close(entered)
					<-release
					if kind == "drain-error" {
						return 0, errors.New("drain failed")
					}
					return 7, nil
				})
			}()
			runtimeAwait(t, "first lease", func() bool { return h.ProjectionView().Projection().Healthy })
			wire.mu.Lock()
			wire.revoking = true
			wire.mu.Unlock()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("revoke did not request drain")
			}
			if h.ProjectionView().Projection().Healthy {
				t.Fatal("revoke drain left admission open")
			}
			if kind == "cancel" {
				cancel()
			}
			if kind == "expiry" {
				time.Sleep(200 * time.Millisecond)
			}
			close(release)
			if err := runtimeDone(t, done); err == nil {
				t.Fatal("terminal drain unexpectedly succeeded")
			}
			wire.mu.Lock()
			defer wire.mu.Unlock()
			if wire.acks != 0 {
				t.Fatal("terminal drain sent clean ACK", wire.acks)
			}
		})
	}
}
