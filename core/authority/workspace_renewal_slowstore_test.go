package authority

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowLeaseStore delays every workspace-lease Put once armed, standing in for
// a master disk whose fsync stalls under heavy I/O (issue #205).
type slowLeaseStore struct {
	AuthorityStore
	delayNS atomic.Int64
	puts    atomic.Int64 // delayed (armed) lease writes: proves the stall was exercised
}

func (s *slowLeaseStore) Put(r DurableRecord) error {
	if d := time.Duration(s.delayNS.Load()); d > 0 && r.Workspace != nil && r.Workspace.Lease != nil {
		s.puts.Add(1)
		time.Sleep(d)
	}
	return s.AuthorityStore.Put(r)
}

// A renewal of a live lease must not wait for the authority store's fsync:
// the host's renewal RPC times out after 2 s, so a slower store write on the
// renewal path turns into a host fence and a volume shutdown (issue #205).
func TestWorkspaceRenewalDoesNotWaitForSlowStore(t *testing.T) {
	disk, _ := newTempStore(t)
	slow := &slowLeaseStore{AuthorityStore: disk}
	var nowNS atomic.Int64
	nowNS.Store(time.Unix(1000, 0).UnixNano())
	p := NewPublisher(nil, WithStore(slow), WithWorkspaceLeaseConfig(30*time.Second, func() time.Time { return time.Unix(0, nowNS.Load()) }))
	t.Cleanup(p.CloseWorkspaceExtenders) // join extenders before the store closes
	if err := p.apply(AssignmentAsk{VolumeID: "v", ReplicaID: "old", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
		t.Fatal(err)
	}
	first, err := p.RenewWorkspaceLease(context.Background(), "v", "old", 1)
	if err != nil {
		t.Fatal(err)
	}
	slow.delayNS.Store(int64(2500 * time.Millisecond))
	t.Cleanup(func() { slow.delayNS.Store(0) })
	nowNS.Add(int64(10 * time.Second))
	start := time.Now()
	second, err := p.RenewWorkspaceLease(context.Background(), "v", "old", 1)
	took := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if second.ExpiresAtNS <= first.ExpiresAtNS {
		t.Fatalf("renewal did not extend the grant: first=%d second=%d", first.ExpiresAtNS, second.ExpiresAtNS)
	}
	if took >= 2*time.Second {
		t.Fatalf("renewal of a live lease took %s behind a 2.5 s store write; the host's renewal RPC times out at 2 s", took.Round(time.Millisecond))
	}
	t.Logf("renewal took %s", took)
	waitExtenders(t, p)
	if slow.puts.Load() == 0 {
		t.Fatal("no lease write went through the 2.5 s stall: the test did not exercise it")
	}
}

// Renewals of different volumes must not queue behind each other's store
// writes: with six volumes and a 0.5 s write each, serializing them makes the
// last renewal wait about 3 s, past the host's 2 s renewal timeout.
func TestWorkspaceConcurrentRenewalsDoNotSerializeOnStore(t *testing.T) {
	disk, _ := newTempStore(t)
	slow := &slowLeaseStore{AuthorityStore: disk}
	var nowNS atomic.Int64
	nowNS.Store(time.Unix(1000, 0).UnixNano())
	p := NewPublisher(nil, WithStore(slow), WithWorkspaceLeaseConfig(30*time.Second, func() time.Time { return time.Unix(0, nowNS.Load()) }))
	t.Cleanup(p.CloseWorkspaceExtenders) // join extenders before the store closes
	const n = 6
	for i := 0; i < n; i++ {
		v := fmt.Sprintf("v%d", i)
		if err := p.apply(AssignmentAsk{VolumeID: v, ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.RenewWorkspaceLease(context.Background(), v, "r", 1); err != nil {
			t.Fatal(err)
		}
	}
	slow.delayNS.Store(int64(500 * time.Millisecond))
	t.Cleanup(func() { slow.delayNS.Store(0) })
	nowNS.Add(int64(10 * time.Second))
	var wg sync.WaitGroup
	took := make([]time.Duration, n)
	errs := make([]error, n)
	gate := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			start := time.Now()
			_, errs[i] = p.RenewWorkspaceLease(context.Background(), fmt.Sprintf("v%d", i), "r", 1)
			took[i] = time.Since(start)
		}(i)
	}
	close(gate)
	wg.Wait()
	var slowest time.Duration
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("v%d: %v", i, errs[i])
		}
		if took[i] > slowest {
			slowest = took[i]
		}
	}
	if slowest >= 2*time.Second {
		t.Fatalf("slowest of %d concurrent renewals took %s (all: %v); renewals serialize behind each other's store writes", n, slowest.Round(time.Millisecond), took)
	}
	t.Logf("slowest of %d concurrent renewals took %s", n, slowest)
	waitExtenders(t, p)
	if slow.puts.Load() == 0 {
		t.Fatal("no lease write went through the 0.5 s stall: the test did not exercise it")
	}
}

// heldWriteStore blocks the first Put that matches hold until release closes,
// standing in for one slow synchronous authority write.
type heldWriteStore struct {
	AuthorityStore
	hold    func(DurableRecord) bool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *heldWriteStore) Put(r DurableRecord) error {
	if s.hold != nil && s.hold(r) {
		held := false
		s.once.Do(func() { held = true })
		if held {
			close(s.entered)
			<-s.release
		}
	}
	return s.AuthorityStore.Put(r)
}

// A slow synchronous write of one volume (here the revocation of a) must not
// block the renewal of an unrelated volume b: renewal needs no write of its
// own, so it must not wait on a's store I/O (issue #205).
func TestWorkspaceRenewalDoesNotWaitForUnrelatedHeldWrite(t *testing.T) {
	disk, _ := newTempStore(t)
	held := &heldWriteStore{AuthorityStore: disk, entered: make(chan struct{}), release: make(chan struct{}),
		hold: func(r DurableRecord) bool {
			return r.VolumeID == "a" && r.Workspace != nil && r.Workspace.Lease != nil && r.Workspace.Lease.Revoking
		}}
	var nowNS atomic.Int64
	nowNS.Store(time.Unix(1000, 0).UnixNano())
	p := NewPublisher(nil, WithStore(held), WithWorkspaceLeaseConfig(30*time.Second, func() time.Time { return time.Unix(0, nowNS.Load()) }))
	t.Cleanup(p.CloseWorkspaceExtenders) // join extenders before the store closes
	for _, v := range []string{"a", "b"} {
		if err := p.apply(AssignmentAsk{VolumeID: v, ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.RenewWorkspaceLease(context.Background(), v, "r", 1); err != nil {
			t.Fatal(err)
		}
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(held.release) }) }
	revoked := make(chan error, 1)
	go func() { revoked <- p.BeginWorkspaceRevocation(context.Background(), "a", "r", 1) }()
	// Bounded cleanup: release the held write first, then settle the worker.
	t.Cleanup(func() {
		release()
		select {
		case <-revoked:
		case <-time.After(5 * time.Second):
			t.Error("held revocation of a did not settle within 5 s after release")
		}
	})
	select {
	case <-held.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("revocation of a never reached the store")
	}
	nowNS.Add(int64(10 * time.Second))
	renewed := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := p.RenewWorkspaceLease(context.Background(), "b", "r", 1)
		renewed <- err
	}()
	select {
	case err := <-renewed:
		if err != nil {
			t.Fatalf("renewal of b: %v", err)
		}
		t.Logf("renewal of b took %s while a's write was held", time.Since(start))
	case <-time.After(2 * time.Second):
		blocked := time.Since(start).Round(time.Millisecond)
		release()
		select {
		case err := <-renewed:
			t.Fatalf("renewal of b was still blocked %s behind the held store write of a (it returned only after a was released, err=%v)", blocked, err)
		case <-time.After(5 * time.Second):
			t.Fatalf("renewal of b was still blocked %s behind the held store write of a, and did not settle within 5 s after release", blocked)
		}
	}
}
