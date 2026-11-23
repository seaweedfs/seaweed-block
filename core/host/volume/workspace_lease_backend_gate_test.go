package volume

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// gateTestView is the host's projection before the lease guard: the backend's
// own lineage, healthy.
type gateTestView struct{}

func (gateTestView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, Healthy: true}
}

// countingStore is a block store that counts the writes reaching it (WAL
// appends) and can hold one write inside the store until released. It
// embeds the store as the LogicalStorage interface only, so every block
// write goes through Write.
type countingStore struct {
	storage.LogicalStorage
	mu      sync.Mutex
	writes  int
	hold    bool
	entered chan struct{}
	release chan struct{}
}

func (s *countingStore) Write(lba uint32, data []byte) (uint64, error) {
	s.mu.Lock()
	hold := s.hold
	s.hold = false
	s.mu.Unlock()
	if hold {
		close(s.entered)
		<-s.release
	}
	lsn, err := s.LogicalStorage.Write(lba, data)
	if err == nil {
		s.mu.Lock()
		s.writes++
		s.mu.Unlock()
	}
	return lsn, err
}

func (s *countingStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}

// gateClock is the guard's injected clock.
type gateClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *gateClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *gateClock) Add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// leaseGatedBackend is a durable backend whose projection is a workspace
// lease guard, as on a workspace host: the guard granted for 30 s.
type leaseGatedBackend struct {
	guard    *WorkspaceLeaseGuard
	backend  *durable.StorageBackend
	store    *countingStore
	clock    *gateClock
	revoking bool
}

func newLeaseGatedBackend(t *testing.T) *leaseGatedBackend {
	t.Helper()
	lg := &leaseGatedBackend{clock: &gateClock{now: time.Now()}}
	client := workspaceTestClient{
		renew: func() (*control.WorkspaceLeaseFact, error) {
			return &control.WorkspaceLeaseFact{VolumeId: "v1", ReplicaId: "r1", Epoch: 1, ValidForNs: int64(30 * time.Second), Revoking: lg.revoking}, nil
		},
		ack: func() error { return nil },
	}
	g, err := NewWorkspaceLeaseGuard(gateTestView{}, client, &control.WorkspaceIdentity{VolumeId: "v1", ReplicaId: "r1", Epoch: 1},
		func(context.Context) (uint64, error) { return 7, nil }, lg.clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	lg.guard = g
	lg.store = &countingStore{LogicalStorage: storage.NewBlockStore(16, 4096)}
	lg.backend = durable.NewStorageBackend(lg.store, g, frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1})
	lg.backend.SetOperational(true, "test")
	return lg
}

// fence makes the guard stop permitting: the lease runs out (expiry), or the
// master revokes it.
func (lg *leaseGatedBackend) fence(t *testing.T, how string) {
	t.Helper()
	switch how {
	case "expired":
		lg.clock.Add(31 * time.Second)
	case "revoked":
		lg.revoking = true
		if err := lg.guard.Refresh(context.Background()); err != nil {
			t.Fatalf("revocation: %v", err)
		}
	}
	if lg.guard.Projection().Healthy {
		t.Fatalf("the guard still permits after it was %s", how)
	}
}

func gateBlock(b byte) []byte { return bytes.Repeat([]byte{b}, 4096) }

// TestWorkspaceLeaseFenceRefusesAWriteArrivingAfterIt: a durable backend
// whose projection is the workspace lease guard admits a write while the
// lease is valid. Once the guard stops permitting (the lease expired, or the
// master revoked it), a guest write arriving after that is refused at the
// backend gate: ErrStalePrimary, nothing written (no ack: 0 bytes), no WAL
// append, the block still unwritten in the store; and it stays refused.
func TestWorkspaceLeaseFenceRefusesAWriteArrivingAfterIt(t *testing.T) {
	for _, how := range []string{"expired", "revoked"} {
		t.Run(how, func(t *testing.T) {
			lg := newLeaseGatedBackend(t)
			ctx := context.Background()
			if n, err := lg.backend.Write(ctx, 0, gateBlock(1)); err != nil || n != 4096 {
				t.Fatalf("a write under a valid lease: %d %v", n, err)
			}
			before := lg.store.count()

			lg.fence(t, how)
			for try := 0; try < 2; try++ {
				n, err := lg.backend.Write(ctx, 4096, gateBlock(2))
				if !errors.Is(err, frontend.ErrStalePrimary) || n != 0 {
					t.Fatalf("a write after the lease %s: %d %v; want 0 bytes, ErrStalePrimary", how, n, err)
				}
			}
			if got := lg.store.count(); got != before {
				t.Fatalf("%d WAL appends after the fence; want none", got-before)
			}
			if b, err := lg.store.Read(1); err != nil || !bytes.Equal(b, make([]byte, 4096)) {
				t.Fatalf("the refused block reached the store: %v", err)
			}
		})
	}
}

// TestWorkspaceLeaseWriteAdmittedBeforeTheFenceCompletes: a write admitted
// while the lease is valid (it passed the backend gate and is inside the
// store) when the guard stops permitting completes normally: all bytes,
// no error, its block in the store. Admission is decided once, at entry. A
// write arriving after the fence is refused.
func TestWorkspaceLeaseWriteAdmittedBeforeTheFenceCompletes(t *testing.T) {
	for _, how := range []string{"expired", "revoked"} {
		t.Run(how, func(t *testing.T) {
			lg := newLeaseGatedBackend(t)
			ctx := context.Background()
			lg.store.entered, lg.store.release = make(chan struct{}), make(chan struct{})
			lg.store.mu.Lock()
			lg.store.hold = true
			lg.store.mu.Unlock()
			type result struct {
				n   int
				err error
			}
			done := make(chan result, 1)
			go func() {
				n, err := lg.backend.Write(ctx, 0, gateBlock(3))
				done <- result{n, err}
			}()
			select {
			case <-lg.store.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the write never reached the store")
			}

			lg.fence(t, how) // the write is admitted and in flight
			close(lg.store.release)
			var r result
			select {
			case r = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("the admitted write did not complete")
			}
			if r.err != nil || r.n != 4096 {
				t.Fatalf("the write admitted before the fence: %d %v; want 4096 bytes, no error", r.n, r.err)
			}
			if b, err := lg.store.Read(0); err != nil || !bytes.Equal(b, gateBlock(3)) {
				t.Fatalf("the admitted write is not in the store: %v", err)
			}
			if n, err := lg.backend.Write(ctx, 4096, gateBlock(4)); !errors.Is(err, frontend.ErrStalePrimary) || n != 0 {
				t.Fatalf("a write after the fence: %d %v; want 0 bytes, ErrStalePrimary", n, err)
			}
		})
	}
}
