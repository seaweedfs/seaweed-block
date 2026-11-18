package authority

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// GREEN-gating controls for the #205 fix (design r1 section 8).

// hookStore runs before (an error is a known no-write) and after (an error
// after the real Put is an unknown outcome: the record landed) hooks.
type hookStore struct {
	AuthorityStore
	before atomic.Pointer[func(DurableRecord) error]
	after  atomic.Pointer[func(DurableRecord) error]
}

func (s *hookStore) Put(r DurableRecord) error {
	if f := s.before.Load(); f != nil {
		if err := (*f)(r); err != nil {
			return err
		}
	}
	if err := s.AuthorityStore.Put(r); err != nil {
		return err
	}
	if f := s.after.Load(); f != nil {
		return (*f)(r)
	}
	return nil
}

func (s *hookStore) setBefore(f func(DurableRecord) error) { s.before.Store(&f) }
func (s *hookStore) setAfter(f func(DurableRecord) error)  { s.after.Store(&f) }

// isExtension reports a lease write that raises the horizon above firstH and
// is not a revocation: the extender's write.
func isExtension(r DurableRecord, volume string, firstH int64) bool {
	return r.VolumeID == volume && r.Workspace != nil && r.Workspace.Lease != nil && !r.Workspace.Lease.Revoking && r.Workspace.Lease.ExpiresAtNS > firstH
}

type testClock struct{ ns atomic.Int64 }

func newTestClock() *testClock {
	c := &testClock{}
	c.ns.Store(time.Unix(1000, 0).UnixNano())
	return c
}
func (c *testClock) now() time.Time { return time.Unix(0, c.ns.Load()) }
func (c *testClock) at(sec float64) {
	c.ns.Store(time.Unix(1000, 0).Add(time.Duration(sec * float64(time.Second))).UnixNano())
}
func (c *testClock) sec(sec float64) time.Time {
	return time.Unix(1000, 0).Add(time.Duration(sec * float64(time.Second)))
}

// newLeasePublisher joins extenders on cleanup, before the store closes
// (newTempStore registers its Close first, so it runs later).
func newLeasePublisher(t *testing.T, store AuthorityStore, ttl time.Duration, c *testClock) *Publisher {
	t.Helper()
	p := NewPublisher(nil, WithStore(store), WithWorkspaceLeaseConfig(ttl, c.now))
	t.Cleanup(p.CloseWorkspaceExtenders)
	return p
}

func bindAndGrant(t *testing.T, p *Publisher, volume, replica string) WorkspaceLease {
	t.Helper()
	if err := p.apply(AssignmentAsk{VolumeID: volume, ReplicaID: replica, DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	l, err := p.RenewWorkspaceLease(context.Background(), volume, replica, 1)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// waitExtenders joins every admitted extender (tests only; no concurrent
// admission while waiting).
func waitExtenders(t *testing.T, p *Publisher) {
	t.Helper()
	done := make(chan struct{})
	go func() { p.extWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("extenders did not finish")
	}
}

// assertLeaseViewMatches checks INV-LEASE-VIEW-001: with p.mu held, every
// volume's view equals derive(published state).
func assertLeaseViewMatches(t *testing.T, p *Publisher, what string) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	vols := map[string]bool{}
	for k := range p.state {
		vols[k.volumeID] = true
	}
	for v := range p.workspace {
		vols[v] = true
	}
	for v := range p.workspaceUncertain {
		vols[v] = true
	}
	p.leaseMu.Lock()
	for v := range p.leaseViews {
		vols[v] = true
	}
	p.leaseMu.Unlock()
	for v := range vols {
		want := p.deriveLeaseLocked(v)
		p.leaseMu.Lock()
		var got leaseDerived
		if lv := p.leaseViews[v]; lv != nil {
			got = lv.d
		}
		p.leaseMu.Unlock()
		if got != want {
			t.Fatalf("%s: INV-LEASE-VIEW-001 broken for %s:\n view   %+v\n derive %+v", what, v, got, want)
		}
	}
}

func forcedMove(volume, id string) (WorkspaceMoveRequest, WorkspaceHead) {
	return WorkspaceMoveRequest{RequestID: id, VolumeID: volume, ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "old", TargetHost: "new", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "forced", Force: true},
		WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old/r", RemoteLSN: 42}
}

// The view equals derive(published) after every writer kind: bind, first
// grant, extension, endpoint refresh, revocation, ACK, clean move issue, a
// move reservation whose issue write fails (uncertain via apply), a failed
// extension (uncertain via persistWorkspaceLocked), reload and close.
func TestWorkspaceLeaseViewMatchesDeriveAfterEveryWriter(t *testing.T) {
	disk, _ := newTempStore(t)
	hs := &hookStore{AuthorityStore: disk}
	c := newTestClock()
	p := newLeasePublisher(t, hs, 30*time.Second, c)
	check := func(what string) { t.Helper(); assertLeaseViewMatches(t, p, what) }
	ctx := context.Background()
	check("empty")
	for _, v := range []string{"v", "w", "x"} {
		if err := p.apply(AssignmentAsk{VolumeID: v, ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
			t.Fatal(err)
		}
	}
	check("bind")
	if err := p.apply(AssignmentAsk{VolumeID: "x", ReplicaID: "r", DataAddr: "d1", CtrlAddr: "c", Intent: IntentRefreshEndpoint}); err != nil {
		t.Fatal(err)
	}
	check("endpoint refresh, not enrolled")
	for _, v := range []string{"v", "w", "x"} {
		if _, err := p.RenewWorkspaceLease(ctx, v, "r", 1); err != nil {
			t.Fatal(err)
		}
	}
	check("first grants")
	firstH := p.WorkspaceState("v").Lease.ExpiresAtNS
	c.at(10)
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	waitExtenders(t, p)
	if got := p.WorkspaceState("v").Lease.ExpiresAtNS; got != c.sec(55).UnixNano() || got <= firstH {
		t.Fatalf("extension did not publish trigger+1.5*TTL: %d", got)
	}
	check("extension")
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d2", CtrlAddr: "c", Intent: IntentRefreshEndpoint}); err != nil {
		t.Fatal(err)
	}
	check("endpoint refresh, enrolled")
	if err := p.BeginWorkspaceRevocation(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	check("revocation")
	if err := p.AckWorkspaceRevocation(ctx, "v", WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, HeadGen: 7}); err != nil {
		t.Fatal(err)
	}
	check("ack")
	clean := WorkspaceMoveRequest{RequestID: "clean", VolumeID: "v", ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "old", TargetHost: "new", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "clean"}
	if _, err := p.MoveWorkspace(ctx, clean, WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old/r", RemoteLSN: 42, Detached: true}); err != nil {
		t.Fatal(err)
	}
	check("clean move issued")
	if p.leaseDeadlineLockedForTest("v") != (time.Time{}) {
		t.Fatal("move issue kept the old deadline")
	}
	// w: forced move whose issue write fails after the reservation persisted.
	c.at(30 + 15 + 20 + 1) // past w's horizon (45) + skew (20)
	hs.setBefore(func(r DurableRecord) error {
		if r.VolumeID == "w" && r.Workspace != nil && len(r.Workspace.Moves) > 0 && r.Workspace.Moves[len(r.Workspace.Moves)-1].Issued {
			return errors.New("injected issue write failure")
		}
		return nil
	})
	q, h := forcedMove("w", "forced-w")
	if _, err := p.MoveWorkspace(ctx, q, h); err == nil {
		t.Fatal("injected issue failure ignored")
	}
	check("move reservation, failed issue (uncertain via apply)")
	if st := p.WorkspaceState("w"); len(st.Moves) != 1 || st.Moves[0].Issued {
		t.Fatalf("reservation not published: %+v", st)
	}
	// x: a failed extension latches uncertainty.
	c.at(0)
	p2Disk, _ := newTempStore(t)
	hs2 := &hookStore{AuthorityStore: p2Disk}
	p2 := newLeasePublisher(t, hs2, 30*time.Second, c)
	bindAndGrant(t, p2, "x", "r")
	firstX := p2.WorkspaceState("x").Lease.ExpiresAtNS
	hs2.setBefore(func(r DurableRecord) error {
		if isExtension(r, "x", firstX) {
			return errors.New("injected extension write failure")
		}
		return nil
	})
	c.at(10)
	if _, err := p2.RenewWorkspaceLease(ctx, "x", "r", 1); err != nil {
		t.Fatal(err)
	}
	waitExtenders(t, p2)
	assertLeaseViewMatches(t, p2, "failed extension (uncertain via persistWorkspaceLocked)")
	if !p2.workspaceUncertainForTest("x") {
		t.Fatal("failed extension did not latch uncertainty")
	}
	p.CloseWorkspaceExtenders()
	check("close")
	reloaded := newLeasePublisher(t, disk, 30*time.Second, c)
	assertLeaseViewMatches(t, reloaded, "reload")
}

func (p *Publisher) leaseDeadlineLockedForTest(v string) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.leaseDeadlineLocked(v)
}

func (p *Publisher) workspaceUncertainForTest(v string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.workspaceUncertain[v]
}

// Control 1: with the store stalled for good, grants shrink to the durable
// horizon, a renewal that would leave no host validity is refused, and a
// renewal at the horizon is refused. The stall is exercised by the extender
// (puts > 0), so a green cannot bypass it.
func TestWorkspaceLeaseClampsToHorizonUnderStalledStore(t *testing.T) {
	disk, _ := newTempStore(t)
	hs := &hookStore{AuthorityStore: disk}
	c := newTestClock()
	p := newLeasePublisher(t, hs, 30*time.Second, c)
	bindAndGrant(t, p, "v", "r")
	horizon := p.WorkspaceState("v").Lease.ExpiresAtNS // 1045
	var puts atomic.Int64
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	hs.setBefore(func(r DurableRecord) error {
		if isExtension(r, "v", horizon) {
			puts.Add(1)
			enterOnce.Do(func() { close(entered) })
			<-release
		}
		return nil
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) }) // before Close joins
	ctx := context.Background()
	c.at(10)
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("extender never reached the store")
	}
	c.at(20)
	start := time.Now()
	l, err := p.RenewWorkspaceLease(ctx, "v", "r", 1)
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("renewal waited %s behind the stalled extender", took)
	}
	if l.ExpiresAtNS > horizon {
		t.Fatalf("grant %s beyond the durable horizon %s", time.Unix(0, l.ExpiresAtNS), time.Unix(0, horizon))
	}
	if l.ExpiresAtNS != horizon {
		t.Fatalf("grant not clamped to the horizon: %s", time.Unix(0, l.ExpiresAtNS))
	}
	c.at(35) // horizon - skew/2 = 1035: no positive host validity
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err == nil || !strings.Contains(err.Error(), "no host validity") {
		t.Fatalf("renewal without host validity: %v", err)
	}
	c.at(45)
	if _, err := p.RenewWorkspaceLease(ctx, "v", "r", 1); err == nil {
		t.Fatal("renewal at the durable horizon accepted")
	}
	if puts.Load() == 0 {
		t.Fatal("the stall was never exercised by an extender write")
	}
}

// Control 2: a restarted master honours the persisted (extended) horizon in
// the forced barrier: max(horizon, restart+TTL) + skew.
func TestWorkspaceRestartForcedBarrierHonoursHorizon(t *testing.T) {
	disk, _ := newTempStore(t)
	c := newTestClock()
	p := newLeasePublisher(t, disk, 30*time.Second, c)
	bindAndGrant(t, p, "v", "r")
	c.at(10)
	if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	waitExtenders(t, p)
	if got := p.WorkspaceState("v").Lease.ExpiresAtNS; got != c.sec(55).UnixNano() {
		t.Fatalf("extension not durable: %s", time.Unix(0, got))
	}
	p.CloseWorkspaceExtenders()
	c.at(12)
	r := newLeasePublisher(t, disk, 30*time.Second, c) // reload deadline 1042 < horizon 1055
	q, h := forcedMove("v", "forced")
	c.ns.Store(c.sec(75).UnixNano() - 1)
	if _, err := r.MoveWorkspace(context.Background(), q, h); err == nil {
		t.Fatal("forced move before persisted horizon + skew after restart")
	}
	c.at(75)
	if _, err := r.MoveWorkspace(context.Background(), q, h); err != nil {
		t.Fatal("forced move at horizon + skew refused:", err)
	}
}

// Control 3: an extension whose rename landed but whose directory sync
// failed is an unknown write: the volume is uncertain, the next renewal is
// refused (INV-AUTH-UNCERTAIN-001), and a reload finds the landed horizon and
// bars a forced move until that horizon + skew.
func TestWorkspaceAmbiguousExtensionLatchesUncertainty(t *testing.T) {
	disk, _ := newTempStore(t)
	hs := &hookStore{AuthorityStore: disk}
	c := newTestClock()
	p := newLeasePublisher(t, hs, 30*time.Second, c)
	bindAndGrant(t, p, "v", "r")
	first := p.WorkspaceState("v").Lease.ExpiresAtNS
	hs.setAfter(func(r DurableRecord) error {
		if isExtension(r, "v", first) {
			return errors.New("injected: directory sync failed after rename")
		}
		return nil
	})
	c.at(10)
	if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	waitExtenders(t, p)
	if got := p.WorkspaceState("v").Lease.ExpiresAtNS; got != first {
		t.Fatalf("an unknown write was published: %s", time.Unix(0, got))
	}
	c.at(11)
	if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err == nil || !strings.Contains(err.Error(), "uncertain") {
		t.Fatalf("renewal after an unknown write: %v; want refused as uncertain", err)
	}
	// INV-AUTH-UNCERTAIN-001: every further authority change of the volume
	// is refused until a restart.
	if err := p.BeginWorkspaceRevocation(context.Background(), "v", "r", 1); err == nil {
		t.Fatal("revocation accepted on an uncertain volume")
	}
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d2", CtrlAddr: "c", Intent: IntentRefreshEndpoint}); err == nil {
		t.Fatal("authority change accepted on an uncertain volume")
	}
	p.CloseWorkspaceExtenders()
	c.at(12)
	r := newLeasePublisher(t, disk, 30*time.Second, c)
	landed := r.WorkspaceState("v").Lease.ExpiresAtNS
	if landed != c.sec(55).UnixNano() {
		t.Fatalf("reload did not find the landed horizon: %s", time.Unix(0, landed))
	}
	q, h := forcedMove("v", "forced")
	c.ns.Store(time.Unix(0, landed).Add(20*time.Second).UnixNano() - 1)
	if _, err := r.MoveWorkspace(context.Background(), q, h); err == nil {
		t.Fatal("forced move before the landed horizon + skew")
	}
	c.ns.Store(time.Unix(0, landed).Add(20 * time.Second).UnixNano())
	if _, err := r.MoveWorkspace(context.Background(), q, h); err != nil {
		t.Fatal(err)
	}
}

// Control 4: an extender held inside its Put holds p.mu; a revocation or a
// forced move of the same volume waits, then runs after the extension is
// published, so its record carries the extended horizon (ordered).
func TestWorkspaceExtenderHeldOrdersSameVolumeWriters(t *testing.T) {
	for _, op := range []string{"revocation", "forced-move"} {
		t.Run(op, func(t *testing.T) {
			disk, _ := newTempStore(t)
			hs := &hookStore{AuthorityStore: disk}
			c := newTestClock()
			p := newLeasePublisher(t, hs, 30*time.Second, c)
			bindAndGrant(t, p, "v", "r")
			first := p.WorkspaceState("v").Lease.ExpiresAtNS
			entered, release := make(chan struct{}), make(chan struct{})
			var enterOnce, releaseOnce sync.Once
			releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
			hs.setBefore(func(r DurableRecord) error {
				if isExtension(r, "v", first) {
					enterOnce.Do(func() { close(entered) })
					<-release
				}
				return nil
			})
			done := make(chan error, 1)
			started := false
			// Installed before the extender is triggered: on any failure the
			// held write is released and the writer settled (bounded) before
			// the publisher joins its extenders and the store closes.
			t.Cleanup(func() {
				releaseFn()
				if started {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("writer did not settle")
					}
				}
			})
			c.at(10)
			if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("extender never reached the store")
			}
			started = true
			if op == "revocation" {
				go func() { done <- p.BeginWorkspaceRevocation(context.Background(), "v", "r", 1) }()
			} else {
				c.at(66) // past the OLD barrier max(1045, 1040) + 20
				q, h := forcedMove("v", "forced")
				go func() { _, err := p.MoveWorkspace(context.Background(), q, h); done <- err }()
			}
			select {
			case err := <-done:
				done <- err // keep it for the cleanup join
				t.Fatalf("%s ran while the extender held p.mu: %v", op, err)
			case <-time.After(200 * time.Millisecond):
			}
			releaseFn()
			var err error
			select {
			case err = <-done:
				done <- err
			case <-time.After(5 * time.Second):
				t.Fatalf("%s did not finish after the extender", op)
			}
			want := c.sec(55).UnixNano()
			st := p.WorkspaceState("v")
			if op == "revocation" {
				if err != nil || st.Lease == nil || !st.Lease.Revoking || st.Lease.ExpiresAtNS != want {
					t.Fatalf("revocation not ordered after the extension: %v %+v", err, st.Lease)
				}
			} else if err == nil || len(st.Moves) != 0 || st.Lease.ExpiresAtNS != want {
				t.Fatalf("forced move did not see the extended horizon: %v %+v", err, st)
			}
			p.CloseWorkspaceExtenders()
			r := newLeasePublisher(t, disk, 30*time.Second, c)
			if got := r.WorkspaceState("v").Lease; got.ExpiresAtNS != want || got.Revoking != (op == "revocation") {
				t.Fatalf("reload: %+v", got)
			}
		})
	}
}

// Control 5: a renewal of the same volume during a held revocation write sees
// the last published state (a grant under the horizon), then the revoking
// lease with the granted expiry and no extension. During a held forced-move
// write (barrier passed) a renewal is refused.
func TestWorkspaceSameVolumeRenewalDuringHeldWrite(t *testing.T) {
	t.Run("revocation", func(t *testing.T) {
		disk, _ := newTempStore(t)
		hs := &hookStore{AuthorityStore: disk}
		c := newTestClock()
		p := newLeasePublisher(t, hs, 30*time.Second, c)
		bindAndGrant(t, p, "v", "r")
		horizon := p.WorkspaceState("v").Lease.ExpiresAtNS
		entered, release := make(chan struct{}), make(chan struct{})
		var enterOnce, releaseOnce sync.Once
		releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
		hs.setBefore(func(r DurableRecord) error {
			if r.VolumeID == "v" && r.Workspace != nil && r.Workspace.Lease != nil && r.Workspace.Lease.Revoking {
				enterOnce.Do(func() { close(entered) })
				<-release
			}
			return nil
		})
		done := make(chan error, 1)
		go func() { done <- p.BeginWorkspaceRevocation(context.Background(), "v", "r", 1) }()
		t.Cleanup(func() {
			releaseFn()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("revocation did not settle")
			}
		})
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("revocation write never reached the store")
		}
		c.at(5)
		l, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1)
		if err != nil || l.Revoking || l.ExpiresAtNS != c.sec(35).UnixNano() || l.ExpiresAtNS > horizon {
			t.Fatalf("renewal during the held revocation write: %+v %v", l, err)
		}
		releaseFn()
		select {
		case err := <-done:
			done <- err
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("revocation did not finish after the release")
		}
		for _, at := range []float64{6, 7} {
			c.at(at)
			l, err = p.RenewWorkspaceLease(context.Background(), "v", "r", 1)
			if err != nil || !l.Revoking || l.ExpiresAtNS != c.sec(35).UnixNano() {
				t.Fatalf("revoking renewal at %v: %+v %v; want the granted expiry, never extended", at, l, err)
			}
		}
	})
	t.Run("forced-move", func(t *testing.T) {
		disk, _ := newTempStore(t)
		hs := &hookStore{AuthorityStore: disk}
		c := newTestClock()
		p := newLeasePublisher(t, hs, 30*time.Second, c)
		bindAndGrant(t, p, "v", "r")
		entered, release := make(chan struct{}), make(chan struct{})
		var enterOnce, releaseOnce sync.Once
		releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
		hs.setBefore(func(r DurableRecord) error {
			if r.VolumeID == "v" && r.Workspace != nil && len(r.Workspace.Moves) > 0 && !r.Workspace.Moves[len(r.Workspace.Moves)-1].Issued {
				enterOnce.Do(func() { close(entered) })
				<-release
			}
			return nil
		})
		c.at(66) // past horizon 1045 + skew 20
		q, h := forcedMove("v", "forced")
		done := make(chan error, 1)
		go func() { _, err := p.MoveWorkspace(context.Background(), q, h); done <- err }()
		t.Cleanup(func() {
			releaseFn()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("forced move did not settle")
			}
		})
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("move reservation write never reached the store")
		}
		start := time.Now()
		if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err == nil {
			t.Fatal("renewal granted during a forced move past the barrier")
		}
		if took := time.Since(start); took > time.Second {
			t.Fatalf("renewal waited %s on the held move", took)
		}
		releaseFn()
		select {
		case err := <-done:
			done <- err
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("forced move did not finish after the release")
		}
		if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err == nil {
			t.Fatal("old-epoch renewal after the move")
		}
	})
}

// Control 7 (run with -race): N volumes renew while apply, revocation, ACK,
// a clean move, a forced move, extenders and Close run. A bounded handshake
// holds the first horizon-extension write inside the store while renewals
// keep succeeding and the writers and Close are in flight (queued on the
// publisher lock behind it); then it is released. Required afterwards: every
// eligible volume had successful grants, at least one extension write was
// observed (classified by content: a raised horizon at the same epoch, not
// an ordinary writer's record), no renewal waited 2 s, nothing deadlocked,
// every live lease keeps disk >= published horizon >= every grant (S1), and
// the view equals derive.
func TestWorkspaceLeaseConcurrentRenewalsWritersExtendersClose(t *testing.T) {
	const n = 8
	vol := func(i int) string { return fmt.Sprintf("v%d", i) }
	const forcedVol, cleanVol = 2, 1
	eligible := []int{0, 3, 4, 5, 6, 7} // stay leased throughout; v0 also takes endpoint refreshes
	disk, _ := newTempStore(t)
	hs := &hookStore{AuthorityStore: disk}

	// Extension classification and the overlap hold.
	var classMu sync.Mutex
	lastH := map[string]int64{}
	lastEpoch := map[string]uint64{}
	var extensions atomic.Int64
	heldEntered, heldRelease := make(chan struct{}), make(chan struct{})
	var holdOnce, releaseOnce sync.Once
	releaseHold := func() { releaseOnce.Do(func() { close(heldRelease) }) }
	var classifying atomic.Bool
	hs.setBefore(func(r DurableRecord) error {
		time.Sleep(5 * time.Millisecond) // a slow store
		if !classifying.Load() || r.Workspace == nil || r.Workspace.Lease == nil {
			return nil
		}
		l := r.Workspace.Lease
		pending := len(r.Workspace.Moves) > 0 && !r.Workspace.Moves[len(r.Workspace.Moves)-1].Issued
		classMu.Lock()
		ext := !l.Revoking && !pending && l.Epoch == lastEpoch[r.VolumeID] && l.ExpiresAtNS > lastH[r.VolumeID]
		if ext {
			lastH[r.VolumeID] = l.ExpiresAtNS
		}
		classMu.Unlock()
		if ext {
			extensions.Add(1)
			first := false
			holdOnce.Do(func() { first = true })
			if first {
				close(heldEntered)
				<-heldRelease
			}
		}
		return nil
	})
	// The publisher clock is real time plus an offset. The forced-move volume
	// is granted at offset -10 s, so by offset 0 its horizon + skew barrier has
	// passed; the clock only moves forward.
	var offNS atomic.Int64
	offNS.Store(int64(-10 * time.Second))
	p := NewPublisher(nil, WithStore(hs), WithWorkspaceLeaseConfig(2*time.Second, func() time.Time { return time.Now().Add(time.Duration(offNS.Load())) }))
	t.Cleanup(p.CloseWorkspaceExtenders)
	ctx := context.Background()
	bindAndGrant(t, p, vol(forcedVol), "r") // never renewed again
	offNS.Store(0)
	for i := 0; i < n; i++ {
		if i != forcedVol {
			bindAndGrant(t, p, vol(i), "r")
		}
	}
	for i := 0; i < n; i++ {
		st := p.WorkspaceState(vol(i))
		lastH[vol(i)], lastEpoch[vol(i)] = st.Lease.ExpiresAtNS, st.Lease.Epoch
	}
	classifying.Store(true)

	var grants [n]atomic.Int64
	var maxGrant [n]atomic.Int64
	var slowest atomic.Int64
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopRenewers := func() { stopOnce.Do(func() { close(stop) }) }
	var wg sync.WaitGroup
	writersDone := make(chan error, 1)
	closed := make(chan struct{})
	writersStarted, closeStarted := false, false
	// Bounded cleanup on every path: release the hold, stop renewers, settle
	// every worker, and only then let the fixtures close.
	t.Cleanup(func() {
		releaseHold()
		stopRenewers()
		settled := make(chan struct{})
		go func() { wg.Wait(); close(settled) }()
		select {
		case <-settled:
		case <-time.After(10 * time.Second):
			t.Error("renewers did not settle")
		}
		if writersStarted {
			select {
			case <-writersDone:
			case <-time.After(10 * time.Second):
				t.Error("writers did not settle")
			}
		}
		if closeStarted {
			select {
			case <-closed:
			case <-time.After(10 * time.Second):
				t.Error("Close did not settle")
			}
		}
	})
	for _, i := range append(append([]int{}, eligible...), cleanVol) {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				start := time.Now()
				l, err := p.RenewWorkspaceLease(ctx, vol(i), "r", 1)
				if d := int64(time.Since(start)); d > slowest.Load() {
					slowest.Store(d)
				}
				if err == nil && !l.Revoking {
					grants[i].Add(1)
					if l.ExpiresAtNS > maxGrant[i].Load() {
						maxGrant[i].Store(l.ExpiresAtNS)
					}
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}

	// Handshake 1: the first extension write is held inside the store.
	select {
	case <-heldEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("no horizon-extension write reached the store within 5 s")
	}
	// Handshake 2: renewals keep succeeding while it is held.
	var atHold [n]int64
	for _, i := range eligible {
		atHold[i] = grants[i].Load()
	}
	for wait := time.Now().Add(2 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		all := true
		for _, i := range eligible {
			if grants[i].Load() <= atHold[i] {
				all = false
			}
		}
		if all {
			break
		}
		if time.Now().After(wait) {
			t.Fatal("renewals did not succeed while an extension write was held")
		}
	}
	// Writers and Close start while the extension write is held.
	writersStarted = true
	go func() {
		for k := 0; k < 5; k++ {
			if err := p.apply(AssignmentAsk{VolumeID: vol(0), ReplicaID: "r", DataAddr: fmt.Sprintf("d%d", k), CtrlAddr: "c", Intent: IntentRefreshEndpoint}); err != nil {
				writersDone <- fmt.Errorf("endpoint refresh: %w", err)
				return
			}
		}
		if err := p.BeginWorkspaceRevocation(ctx, vol(cleanVol), "r", 1); err != nil {
			writersDone <- fmt.Errorf("revocation: %w", err)
			return
		}
		if err := p.AckWorkspaceRevocation(ctx, vol(cleanVol), WorkspaceRevokeAck{ReplicaID: "r", Epoch: 1, HeadGen: 7}); err != nil {
			writersDone <- fmt.Errorf("ack: %w", err)
			return
		}
		clean := WorkspaceMoveRequest{RequestID: "clean", VolumeID: vol(cleanVol), ExpectedReplicaID: "r", TargetReplicaID: "r", ExpectedHost: "old", TargetHost: "new", ExpectedEpoch: 1, TargetDataAddr: "nd", TargetCtrlAddr: "nc", Kind: "clean"}
		if _, err := p.MoveWorkspace(ctx, clean, WorkspaceHead{Gen: 7, Epoch: 1, Holder: "old/r", RemoteLSN: 42, Detached: true}); err != nil {
			writersDone <- fmt.Errorf("clean move: %w", err)
			return
		}
		q, h := forcedMove(vol(forcedVol), "forced")
		if _, err := p.MoveWorkspace(ctx, q, h); err != nil {
			writersDone <- fmt.Errorf("forced move: %w", err)
			return
		}
		writersDone <- nil
	}()
	closeStarted = true
	go func() { p.CloseWorkspaceExtenders(); close(closed) }()
	select {
	case err := <-writersDone:
		writersDone <- err // keep it for the cleanup join
		t.Fatalf("writers finished while the extension write held the publisher lock: %v", err)
	case <-closed:
		t.Fatal("Close finished while an extension write was held")
	case <-time.After(150 * time.Millisecond):
	}
	releaseHold()
	var werr error
	select {
	case werr = <-writersDone:
		writersDone <- werr // keep it for the cleanup join
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock: writers did not finish after the release")
	}
	if werr != nil {
		t.Fatal("writer:", werr)
	}
	select {
	case <-closed:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock: Close did not finish after the release")
	}
	time.Sleep(50 * time.Millisecond) // renewals continue after Close
	stopRenewers()
	settled := make(chan struct{})
	go func() { wg.Wait(); close(settled) }()
	select {
	case <-settled:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock: renewers did not finish")
	}

	if s := time.Duration(slowest.Load()); s >= 2*time.Second {
		t.Fatalf("slowest renewal %s", s)
	}
	for _, i := range eligible {
		if grants[i].Load() == 0 {
			t.Fatalf("%s had no successful grant", vol(i))
		}
	}
	if extensions.Load() == 0 {
		t.Fatal("no horizon-extension write was observed")
	}
	if st := p.WorkspaceState(vol(forcedVol)); st == nil || st.Lease != nil || len(st.Moves) != 1 || !st.Moves[0].Issued || st.Moves[0].Kind != "forced" {
		t.Fatalf("forced move not issued: %+v", st)
	}
	if st := p.WorkspaceState(vol(cleanVol)); st == nil || st.Lease != nil || len(st.Moves) != 1 || st.Moves[0].Kind != "clean" {
		t.Fatalf("clean move not issued: %+v", st)
	}
	assertLeaseViewMatches(t, p, "after concurrency")
	recs, err := disk.Load()
	if err != nil {
		t.Fatal(err)
	}
	onDisk := map[string]int64{}
	for _, r := range recs {
		if r.Workspace != nil && r.Workspace.Lease != nil {
			onDisk[r.VolumeID] = r.Workspace.Lease.ExpiresAtNS
		}
	}
	for i := 0; i < n; i++ {
		st := p.WorkspaceState(vol(i))
		if st == nil || st.Lease == nil {
			continue // a completed move cleared the lease (out of S1's scope)
		}
		if onDisk[vol(i)] < st.Lease.ExpiresAtNS || st.Lease.ExpiresAtNS < maxGrant[i].Load() {
			t.Fatalf("%s: S1 broken: disk %d, published horizon %d, max grant %d", vol(i), onDisk[vol(i)], st.Lease.ExpiresAtNS, maxGrant[i].Load())
		}
	}
	t.Logf("extension writes %d; grants per eligible volume %v", extensions.Load(), func() []int64 {
		out := []int64{}
		for _, i := range eligible {
			out = append(out, grants[i].Load())
		}
		return out
	}())
}

// Control 10a: Close waits for an extender held inside its Put; after Close
// no extension is admitted or written.
func TestWorkspaceCloseJoinsHeldExtender(t *testing.T) {
	disk, _ := newTempStore(t)
	hs := &hookStore{AuthorityStore: disk}
	c := newTestClock()
	p := newLeasePublisher(t, hs, 30*time.Second, c)
	bindAndGrant(t, p, "v", "r")
	first := p.WorkspaceState("v").Lease.ExpiresAtNS
	var puts atomic.Int64
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
	hs.setBefore(func(r DurableRecord) error {
		if isExtension(r, "v", first) {
			puts.Add(1)
			enterOnce.Do(func() { close(entered) })
			<-release
		}
		return nil
	})
	closed := make(chan struct{})
	closeStarted := false
	t.Cleanup(func() { // bounded: release, then settle Close before the store closes
		releaseFn()
		if closeStarted {
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Error("Close did not settle")
			}
		}
	})
	c.at(10)
	if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("extender never reached the store")
	}
	closeStarted = true
	go func() { p.CloseWorkspaceExtenders(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned while an extender was inside its write")
	case <-time.After(200 * time.Millisecond):
	}
	releaseFn()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the extender finished")
	}
	before := puts.Load()
	c.at(20) // would trigger an extension (horizon 1055 < 1020 + 37.5)
	if _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); err != nil {
		t.Fatal(err)
	}
	waitExtenders(t, p)
	if puts.Load() != before {
		t.Fatal("an extension was written after Close")
	}
}

// Control 10b: an extender admitted before Close that reaches p.mu after
// closing is set aborts with a known no-write. The test holds p.mu itself, so
// the renewal also shows it never needs p.mu.
func TestWorkspaceExtenderAdmittedBeforeCloseDoesNotWrite(t *testing.T) {
	disk, _ := newTempStore(t)
	hs := &hookStore{AuthorityStore: disk}
	c := newTestClock()
	p := newLeasePublisher(t, hs, 30*time.Second, c)
	bindAndGrant(t, p, "v", "r")
	first := p.WorkspaceState("v").Lease.ExpiresAtNS
	var puts atomic.Int64
	hs.setBefore(func(r DurableRecord) error {
		if isExtension(r, "v", first) {
			puts.Add(1)
		}
		return nil
	})
	c.at(10)
	p.mu.Lock()
	renewed := make(chan error, 1)
	go func() { _, err := p.RenewWorkspaceLease(context.Background(), "v", "r", 1); renewed <- err }()
	select {
	case err := <-renewed:
		if err != nil {
			p.mu.Unlock()
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		p.mu.Unlock()
		t.Fatal("renewal waited on p.mu")
	}
	p.leaseMu.Lock()
	admitted := p.leaseViews["v"].extending
	p.leaseClosing = true // Close's first step, while the extender waits for p.mu
	p.leaseMu.Unlock()
	p.mu.Unlock()
	if !admitted {
		t.Fatal("no extender was admitted")
	}
	waitExtenders(t, p)
	if puts.Load() != 0 || p.WorkspaceState("v").Lease.ExpiresAtNS != first {
		t.Fatal("an extender admitted before closing wrote after closing")
	}
	p.leaseMu.Lock()
	extending := p.leaseViews["v"].extending
	p.leaseMu.Unlock()
	if extending {
		t.Fatal("aborted extender left the single-flight flag set")
	}
}
