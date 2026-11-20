package volume

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// joinTimeout bounds a test's wait for a lease worker it cancelled. The
// guard's own drain budget is 5 s for the stop callback plus 2 s for an ACK;
// the join allows that and leaves margin for scheduling and settlement.
const joinTimeout = 15 * time.Second

// startJoined runs run in a worker. Before the worker starts it registers the
// cleanup that cancels it, waits for it (at most joinTimeout; a timeout fails
// the test) and only after the worker is seen done runs teardown, so a failed
// assertion never closes resources under a still-running worker.
func startJoined(t *testing.T, run func(context.Context) error, teardown func()) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done, finished := make(chan error, 1), make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
			if teardown != nil {
				teardown()
			}
		case <-time.After(joinTimeout):
			t.Errorf("lease worker not joined within %s of its cancel; teardown skipped", joinTimeout)
		}
	})
	go func() {
		defer close(finished)
		done <- run(ctx)
	}()
	return done
}

// stopPendingClient grants 30 s leases (normal renewal delay 10 s) and counts
// renewals; revoking makes the next reply revoke.
type stopPendingClient struct {
	mu       sync.Mutex
	renewals int
	revoking bool
	acks     int
}

func (c *stopPendingClient) client() workspaceTestClient {
	return workspaceTestClient{renew: func() (*control.WorkspaceLeaseFact, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.renewals++
		return &control.WorkspaceLeaseFact{VolumeId: "v", ReplicaId: "r", Epoch: 1, ValidForNs: int64(30 * time.Second), Revoking: c.revoking}, nil
	}, ack: func() error {
		c.mu.Lock()
		c.acks++
		c.mu.Unlock()
		return nil
	}}
}

func (c *stopPendingClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.renewals
}

// scheduler replaces the guard's sleep: it records every delay Run chooses
// after a renewal and wakes Run only when the test fires it, so the cadence
// is asserted on the delay itself, with no wall-clock window.
type scheduler struct {
	delays chan time.Duration
	wake   chan time.Time
}

func newScheduler(g *WorkspaceLeaseGuard) *scheduler {
	s := &scheduler{delays: make(chan time.Duration, 16), wake: make(chan time.Time)}
	g.after = func(d time.Duration) <-chan time.Time {
		s.delays <- d
		return s.wake
	}
	return s
}

// next is the delay Run chose after its latest renewal. A Run that never
// renews again (a kick it ignored) fails here, after a generous bound.
func (s *scheduler) next(t *testing.T, what string) time.Duration {
	t.Helper()
	select {
	case d := <-s.delays:
		return d
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: Run did not renew and schedule its next sleep", what)
		return 0
	}
}

func (s *scheduler) fire(t *testing.T) {
	t.Helper()
	select {
	case s.wake <- time.Now():
	case <-time.After(5 * time.Second):
		t.Fatal("Run is not sleeping on the scheduler")
	}
}

// pendingSleepBound is the documented stop-pending bound
// (INV-WS-STOP-PENDING-001), written out so a changed stopPendingPoll fails.
const pendingSleepBound = 250 * time.Millisecond

// The schedule, step by step: a 30 s lease sleeps ~10 s; StopPending renews
// at once (the kick, without the sleep ending) and every following sleep is
// at most pendingSleepBound; ClearStopPending restores the normal sleep; a
// revocation then fences and ACKs.
func TestWorkspaceLeaseStopPendingSchedule(t *testing.T) {
	c := &stopPendingClient{}
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, c.client(), &control.WorkspaceIdentity{VolumeId: "v", ReplicaId: "r", Epoch: 1}, func(context.Context) (uint64, error) { return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := newScheduler(g)
	done := startJoined(t, g.Run, nil)

	if d := s.next(t, "first renewal"); d <= 5*time.Second {
		t.Fatalf("sleep %s after renewing a 30 s lease, want the normal ~10 s", d)
	}

	g.StopPending()
	// The kick alone wakes Run: the scheduled sleep is never fired here.
	if d := s.next(t, "renewal on the StopPending kick"); d > pendingSleepBound {
		t.Fatalf("sleep %s after the kicked renewal, want at most %s", d, pendingSleepBound)
	}
	if n := c.count(); n != 2 {
		t.Fatalf("%d renewals after the kick, want 2", n)
	}
	s.fire(t)
	if d := s.next(t, "renewal while pending"); d > pendingSleepBound {
		t.Fatalf("sleep %s after a renewal while a stop is pending, want at most %s", d, pendingSleepBound)
	}

	g.ClearStopPending()
	s.fire(t)
	if d := s.next(t, "renewal after clear"); d <= 5*time.Second {
		t.Fatalf("sleep %s after ClearStopPending, want the normal ~10 s again", d)
	}
	if n := c.count(); n != 4 {
		t.Fatalf("%d renewals, want 4 (a stale kick must not add one)", n)
	}

	c.mu.Lock()
	c.revoking = true
	c.mu.Unlock()
	s.fire(t)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("revocation ACK path: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("guard did not fence on the revocation")
	}
	c.mu.Lock()
	acks := c.acks
	c.mu.Unlock()
	if g.FenceCause() != "revoked by the master" || acks != 1 {
		t.Fatalf("cause %q acks %d", g.FenceCause(), acks)
	}
}

// StopPending and ClearStopPending before Run, repeated, never block.
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
			g.ClearStopPending()
			g.StopPending()
		}
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("StopPending/ClearStopPending blocked")
	}
}

func guardPending(g *WorkspaceLeaseGuard) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stopPending
}

// Detach attempts are numbered: only the latest attempt's resume clears
// stop-pending, on the host and on the installed guard; an older attempt's,
// another volume's or a repeated resume changes nothing.
func TestWorkspaceHostStopResumedClearsOnlyTheLatestAttempt(t *testing.T) {
	h := workspaceRuntimeHost(t, "127.0.0.1:1", true)
	g, err := NewWorkspaceLeaseGuard(workspaceTestView{}, (&stopPendingClient{}).client(), &control.WorkspaceIdentity{VolumeId: "v1", ReplicaId: "r1", Epoch: 3}, func(context.Context) (uint64, error) { return 7, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	h.view.workspaceGuard.Store(g)
	pending := func() (bool, bool) {
		h.workspaceLeaseMu.Lock()
		defer h.workspaceLeaseMu.Unlock()
		return h.workspaceStopPending, guardPending(g)
	}
	if a := h.WorkspaceStopPending("other"); a != 0 {
		t.Fatalf("another volume's detach got token %d, want 0", a)
	}
	if hp, gp := pending(); hp || gp {
		t.Fatal("another volume's detach set stop-pending")
	}
	older := h.WorkspaceStopPending("v1")
	newer := h.WorkspaceStopPending("v1")
	if older == 0 || newer == older {
		t.Fatalf("attempt tokens %d, %d: want distinct and non-zero", older, newer)
	}
	h.WorkspaceStopResumed("v1", older)
	if hp, gp := pending(); !hp || !gp {
		t.Fatalf("an older attempt's resume cleared a newer pending detach (host %v, guard %v)", hp, gp)
	}
	h.WorkspaceStopResumed("other", newer)
	h.WorkspaceStopResumed("v1", 0)
	if hp, gp := pending(); !hp || !gp {
		t.Fatal("a resume for another volume or token 0 cleared stop-pending")
	}
	h.WorkspaceStopResumed("v1", newer)
	if hp, gp := pending(); hp || gp {
		t.Fatalf("the latest attempt's resume left stop-pending (host %v, guard %v)", hp, gp)
	}
	if d := g.renewDelay(); d <= stopPendingPoll {
		t.Fatalf("sleep %s after the resume, want the normal delay", d)
	}
	h.WorkspaceStopPending("v1")
	h.WorkspaceStopResumed("v1", newer) // the resumed attempt again: stale now
	if hp, gp := pending(); !hp || !gp {
		t.Fatal("a stale resume cleared a later detach")
	}
}

// detachFaultRemote makes the next detach fail one way.
type detachFaultRemote struct {
	*storage.MemChunkRemote
	mu         sync.Mutex
	casMode    string // "refuse": precondition, not applied; "lost": not applied, error
	getHeadErr error
}

var errInjectedRemote = errors.New("injected remote fault")

func (r *detachFaultRemote) set(mode string, headErr error) {
	r.mu.Lock()
	r.casMode, r.getHeadErr = mode, headErr
	r.mu.Unlock()
}

func (r *detachFaultRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	r.mu.Lock()
	err := r.getHeadErr
	r.mu.Unlock()
	if err != nil {
		return storage.ChunkHead{}, err
	}
	return r.MemChunkRemote.GetHead(ctx)
}

func (r *detachFaultRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	r.mu.Lock()
	mode := r.casMode
	r.casMode = ""
	r.mu.Unlock()
	switch mode {
	case "refuse":
		return 0, storage.ErrChunkHeadPrecondition
	case "lost":
		return 0, errInjectedRemote
	}
	return r.MemChunkRemote.CASHead(ctx, expected, next)
}

// detachHost is a leased workspace host whose provider reports detaches to
// it through both hooks, as blockvolume wires them.
type detachHost struct {
	h      *Host
	p      *durable.DurableProvider
	wire   *runtimeLeaseServer
	remote *detachFaultRemote
	done   <-chan error
}

func newDetachHost(t *testing.T) *detachHost {
	t.Helper()
	var stopped atomic.Bool
	d := &detachHost{wire: &runtimeLeaseServer{ttl: 30 * time.Second, stopped: &stopped}, remote: &detachFaultRemote{MemChunkRemote: storage.NewMemChunkRemote()}}
	d.h = workspaceRuntimeHost(t, runtimeWire(t, d.wire), true)
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8, ChunkHolder: "node-a/r1",
		ChunkRemoteFor:       func(string) (storage.ChunkRemote, error) { return d.remote, nil },
		OnChunkDetach:        d.h.WorkspaceStopPending,
		OnChunkDetachResumed: d.h.WorkspaceStopResumed}, d.h.ProjectionView())
	if err != nil {
		t.Fatal(err)
	}
	d.p = p
	handedOver := false
	t.Cleanup(func() { // only while no runtime exists; see handedOver below
		if !handedOver {
			_ = p.Close()
		}
	})
	if _, err = p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err = p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	// From here the provider is closed only by the joined teardown, after the
	// runtime is seen done.
	handedOver = true
	d.done = startJoined(t, func(ctx context.Context) error {
		return d.h.RunWorkspaceLease(ctx, func(c context.Context, id *control.WorkspaceIdentity) (uint64, error) {
			if err := p.Backend("v1").StopIO(c, "workspace lease fenced"); err != nil {
				return 0, err
			}
			stopped.Store(true)
			head, err := d.remote.MemChunkRemote.GetHead(c)
			return head.Gen, err
		})
	}, func() { _ = p.Close() })
	runtimeAwait(t, "leased operational provider", func() bool {
		st := p.DurableStatuses()
		return d.h.ProjectionView().Projection().Healthy && len(st) == 1 && st[0].Operational && d.h.view.workspaceGuard.Load() != nil
	})
	return d
}

func (d *detachHost) pending() bool {
	d.h.workspaceLeaseMu.Lock()
	defer d.h.workspaceLeaseMu.Unlock()
	g := d.h.view.workspaceGuard.Load()
	return d.h.workspaceStopPending || (g != nil && guardPending(g))
}

func (d *detachHost) renewals() int {
	d.wire.mu.Lock()
	defer d.wire.mu.Unlock()
	return d.wire.renewals
}

// Host wiring: the provider's detach hook makes the real lease runtime see
// the revocation that follows a detach soon after it, where the 30 s lease's
// renewal delay would be 10 s.
func TestWorkspaceHostDetachHookFencesWithinPoll(t *testing.T) {
	d := newDetachHost(t)
	if _, _, err := d.p.DetachVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	if !d.pending() {
		t.Fatal("a detach did not set stop-pending")
	}
	d.wire.mu.Lock()
	d.wire.revoking = true
	d.wire.mu.Unlock()
	revoked := time.Now()
	if err := runtimeDone(t, d.done); err != nil {
		t.Fatalf("revocation ACK path: %v", err)
	}
	if took := time.Since(revoked); took > 3*time.Second {
		t.Fatalf("revocation ACKed %s after it, want well inside the 10 s renewal delay", took)
	}
	d.wire.mu.Lock()
	acks := d.wire.acks
	d.wire.mu.Unlock()
	if acks != 1 {
		t.Fatalf("acks %d, want 1", acks)
	}
}

// A failed detach that provably did not happen resumes serving and leaves
// stop-pending: the guard is back on its normal renewal delay. A detach whose
// outcome is unknown, or that is fenced, stays stopped and stays pending.
func TestWorkspaceHostFailedDetachStopPendingByOutcome(t *testing.T) {
	t.Run("aborted-resumes-normal-cadence", func(t *testing.T) {
		d := newDetachHost(t)
		d.remote.set("refuse", nil)
		if _, _, err := d.p.DetachVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrDetachAborted) {
			t.Fatalf("detach with a refused CAS: %v; want ErrDetachAborted", err)
		}
		if st := d.p.DurableStatuses(); len(st) != 1 || !st[0].Operational {
			t.Fatal("serving did not resume after the aborted detach")
		}
		if d.pending() {
			t.Fatal("stop-pending survived a detach that provably aborted and resumed serving")
		}
		if delay := d.h.view.workspaceGuard.Load().renewDelay(); delay <= stopPendingPoll {
			t.Fatalf("guard sleep %s after the resume, want the normal delay", delay)
		}
		// The sleep already running when the resume came is at most one poll;
		// after it the guard renews once more and then sleeps ~10 s. Pending,
		// it would renew about four times a second.
		time.Sleep(4 * stopPendingPoll)
		before := d.renewals()
		time.Sleep(8 * stopPendingPoll)
		if after := d.renewals(); after != before {
			t.Fatalf("%d renewals in %s after the resume, want none (normal cadence)", after-before, 8*stopPendingPoll)
		}
	})
	t.Run("unknown-stays-pending", func(t *testing.T) {
		d := newDetachHost(t)
		d.remote.set("lost", errInjectedRemote)
		if _, _, err := d.p.DetachVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrDetachOutcomeUnknown) {
			t.Fatalf("detach with an undecidable outcome: %v; want ErrDetachOutcomeUnknown", err)
		}
		if !d.pending() {
			t.Fatal("stop-pending cleared on an unknown detach outcome")
		}
		d.remote.set("", nil)
	})
	t.Run("fenced-stays-pending", func(t *testing.T) {
		d := newDetachHost(t)
		ctx := context.Background()
		cur, err := d.remote.MemChunkRemote.GetHead(ctx)
		if err != nil {
			t.Fatal(err)
		}
		newer := cur
		newer.Epoch, newer.Holder = max(cur.Epoch, 3)+1, "another-holder" // above the host's assignment epoch 3
		if _, err := d.remote.MemChunkRemote.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, newer); err != nil {
			t.Fatal(err)
		}
		if _, _, err := d.p.DetachVolume(ctx, "v1"); !errors.Is(err, storage.ErrChunkLayerFenced) {
			t.Fatalf("detach against a newer epoch: %v; want fenced", err)
		}
		if !d.pending() {
			t.Fatal("stop-pending cleared on a fenced detach")
		}
	})
}
