package replication

import (
	"context"
	"testing"
	"time"
)

// The lag check is one barrier at the highest LSN the queue took for
// shipping, queued behind every earlier write, never blocking a writer and
// never more than one at a time.
func TestPeerLagCheckBarriersAtTheLastShippedLSNBehindTheWrites(t *testing.T) {
	order := make(chan uint64, 8)
	release := make(chan struct{})
	q := newPeerWorkQueueWithOps("r1", 4, peerWorkQueueOps{
		ship: func(_ context.Context, w LocalWrite) (bool, error) {
			order <- w.LSN
			return true, nil
		},
		barrier: func(_ context.Context, target uint64) (uint64, error) {
			order <- 1000 + target
			<-release // the check stays in flight until released
			return target, nil
		},
	})
	t.Cleanup(func() { close(release); q.closeAndWait() })

	if q.tryLagCheck() {
		t.Fatal("a lag check ran with nothing shipped: there is nothing to compare")
	}
	results := make(chan peerWorkResult, 4)
	for lsn := uint64(1); lsn <= 2; lsn++ {
		if _, err := q.enqueueWrite(LocalWrite{LSN: lsn}, results); err != nil {
			t.Fatal(err)
		}
	}
	if !q.tryLagCheck() {
		t.Fatal("the lag check was not queued")
	}
	if q.tryLagCheck() {
		t.Fatal("a second lag check was queued while the first is in flight")
	}
	for _, want := range []uint64{1, 2, 1002} {
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("worker ran %d, want %d (the check must follow every earlier write, at LSN 2)", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("worker never ran %d", want)
		}
	}
}

// A full queue skips the check: it never saturates the queue (which would
// degrade the peer) and never blocks the caller.
func TestPeerLagCheckSkipsAFullQueueWithoutSaturatingIt(t *testing.T) {
	hold := make(chan struct{})
	q := newPeerWorkQueueWithOps("r1", 1, peerWorkQueueOps{
		ship: func(_ context.Context, _ LocalWrite) (bool, error) {
			<-hold
			return true, nil
		},
		barrier: func(_ context.Context, target uint64) (uint64, error) { return target, nil },
	})
	t.Cleanup(func() { close(hold); q.closeAndWait() })
	results := make(chan peerWorkResult, 4)
	if _, err := q.enqueueWrite(LocalWrite{LSN: 1}, results); err != nil { // taken by the worker, held
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(q.jobs) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, err := q.enqueueWrite(LocalWrite{LSN: 2}, results); err != nil { // fills the one slot
		t.Fatal(err)
	}
	done := make(chan bool, 1)
	go func() { done <- q.tryLagCheck() }()
	select {
	case queued := <-done:
		if queued {
			t.Fatal("a lag check was queued into a full queue")
		}
	case <-time.After(time.Second):
		t.Fatal("tryLagCheck blocked on a full queue")
	}
	if q.isTerminal() {
		t.Fatal("the lag check saturated the queue")
	}
}
