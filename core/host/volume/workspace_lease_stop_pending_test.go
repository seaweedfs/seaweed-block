package volume

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// stopPendingClient grants 30 s leases (normal renewal delay 10 s) and
// records when each renewal arrived; revoking makes the next reply revoke.
type stopPendingClient struct {
	mu       sync.Mutex
	at       []time.Time
	revoking bool
	acks     int
}

func (c *stopPendingClient) client() workspaceTestClient {
	return workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.at = append(c.at, time.Now())
		return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(30 * time.Second), Revoking: c.revoking}, nil
	}, ack: func() error {
		c.mu.Lock()
		c.acks++
		c.mu.Unlock()
		return nil
	}}
}

func (c *stopPendingClient) renewals() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.at...)
}

func awaitRenewals(t *testing.T, c *stopPendingClient, n int, within time.Duration) []time.Time {
	t.Helper()
	end := time.Now().Add(within)
	for {
		at := c.renewals()
		if len(at) >= n {
			return at
		}
		if time.Now().After(end) {
			t.Fatalf("%d renewals within %s, want %d", len(at), within, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// While no stop is pending a 30 s lease renews every 10 s; StopPending
// renews at once and then every stopPendingPoll, so the revocation that
// follows a detach fences within a poll, not within a renewal delay.
func TestWorkspaceLeaseStopPendingPollsFast(t *testing.T) {
	c := &stopPendingClient{}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, c.client(), &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	awaitRenewals(t, c, 1, 5*time.Second)

	// Normal cadence: nothing more for a while (the next renewal is ~10 s out).
	time.Sleep(time.Second)
	if n := len(c.renewals()); n != 1 {
		t.Fatalf("%d renewals in the first second of a 30 s lease, want 1", n)
	}

	kicked := time.Now()
	g.StopPending()
	g.StopPending() // idempotent, never blocks
	at := awaitRenewals(t, c, 2, time.Second)
	if d := at[1].Sub(kicked); d > 200*time.Millisecond {
		t.Fatalf("first renewal %s after StopPending, want immediate", d)
	}

	// Pending cadence: ~250 ms, so 1.5 s holds about 6 more renewals.
	time.Sleep(1500 * time.Millisecond)
	at = c.renewals()
	extra := len(at) - 2
	if extra < 3 || extra > 10 {
		t.Fatalf("%d renewals in 1.5 s while a stop is pending, want about 6 (every %s)", extra, stopPendingPoll)
	}

	c.mu.Lock()
	c.revoking = true
	c.mu.Unlock()
	revoked := time.Now()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("revocation ACK path: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("guard did not fence on the revocation within 2 s of it")
	}
	if d := time.Since(revoked); d > time.Second {
		t.Fatalf("fenced %s after the revocation, want within a poll", d)
	}
	if g.FenceCause() != "revoked by the master" || c.acks != 1 {
		t.Fatalf("cause %q acks %d", g.FenceCause(), c.acks)
	}
}

// StopPending before Run and on a guard that already fenced never blocks.
func TestWorkspaceLeaseStopPendingNeverBlocks(t *testing.T) {
	c := &stopPendingClient{}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, c.client(), &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	go func() {
		for i := 0; i < 5; i++ {
			g.StopPending()
		}
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("StopPending blocked")
	}
}

// Host wiring: the provider's detach hook, connected to the host, makes the
// real lease runtime see the revocation that follows a detach within a
// poll, where the 30 s lease's renewal delay would be 10 s.
func TestWorkspaceHostDetachHookFencesWithinPoll(t *testing.T) {
	var stopped atomic.Bool
	wire := &runtimeLeaseServer{ttl: 30 * time.Second, stopped: &stopped}
	h := workspaceRuntimeHost(t, runtimeWire(t, wire), true)
	remote := storage.NewMemChunkRemote()
	var hooked atomic.Int32
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8, ChunkHolder: "node-a/r1",
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
		OnChunkDetach:  func(id string) { hooked.Add(1); h.WorkspaceStopPending(id) }}, h.ProjectionView())
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopAt := make(chan time.Time, 1)
	done, finished := make(chan error, 1), make(chan struct{})
	// On a failed assertion the runtime still drains: let it finish before
	// the provider is closed (cleanups run in reverse order).
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
		}
	})
	go func() {
		defer close(finished)
		done <- h.RunWorkspaceLease(ctx, func(c context.Context, id *control.WorkspaceIdentity) (uint64, error) {
			stopAt <- time.Now()
			if err := p.Backend("v1").StopIO(c, "workspace lease fenced"); err != nil {
				return 0, err
			}
			stopped.Store(true)
			head, err := remote.GetHead(c)
			return head.Gen, err
		})
	}()
	runtimeAwait(t, "leased operational provider", func() bool {
		st := p.DurableStatuses()
		return h.ProjectionView().Projection().Healthy && len(st) == 1 && st[0].Operational
	})
	if _, _, err := p.DetachVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if hooked.Load() != 1 {
		t.Fatalf("detach hook called %d times, want 1", hooked.Load())
	}
	wire.mu.Lock()
	wire.revoking = true
	wire.mu.Unlock()
	revoked := time.Now()
	select {
	case at := <-stopAt:
		if d := at.Sub(revoked); d > 2*time.Second {
			t.Fatalf("host stopped %s after the revocation, want within a poll", d)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("host did not see the revocation within 3 s of a detach (renewal delay is 10 s)")
	}
	if err := runtimeDone(t, done); err != nil {
		t.Fatalf("revocation ACK path: %v", err)
	}
	wire.mu.Lock()
	acks := wire.acks
	wire.mu.Unlock()
	if acks != 1 {
		t.Fatalf("acks %d, want 1", acks)
	}
}
