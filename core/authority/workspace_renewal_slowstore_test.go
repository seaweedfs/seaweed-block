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
}

func (s *slowLeaseStore) Put(r DurableRecord) error {
	if d := time.Duration(s.delayNS.Load()); d > 0 && r.Workspace != nil && r.Workspace.Lease != nil {
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
}
