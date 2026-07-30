package replication

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestPeerWorkQueue_PreservesWriteBarrierOrder(t *testing.T) {
	var (
		mu    sync.Mutex
		order []uint64
	)
	q := newPeerWorkQueueWithOps("r1", 8, peerWorkQueueOps{
		ship: func(_ context.Context, write LocalWrite) (bool, error) {
			mu.Lock()
			order = append(order, write.LSN)
			mu.Unlock()
			return true, nil
		},
		barrier: func(_ context.Context, targetLSN uint64) (uint64, error) {
			mu.Lock()
			order = append(order, 1000+targetLSN)
			mu.Unlock()
			return targetLSN, nil
		},
	})
	t.Cleanup(q.closeAndWait)

	results := make(chan peerWorkResult, 3)
	if _, err := q.enqueueWrite(LocalWrite{LSN: 1}, results); err != nil {
		t.Fatal(err)
	}
	if _, err := q.enqueueWrite(LocalWrite{LSN: 2}, results); err != nil {
		t.Fatal(err)
	}
	if _, err := q.enqueueBarrier(2, results); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if result := <-results; result.err != nil {
			t.Fatal(result.err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	want := []uint64{1, 2, 1002}
	if len(order) != len(want) {
		t.Fatalf("order=%v want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order=%v want %v", order, want)
		}
	}
}

func TestPeerWorkQueue_SaturationFailsClosed(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	invalidated := make(chan string, 1)
	q := newPeerWorkQueueWithOps("slow", 1, peerWorkQueueOps{
		ship: func(_ context.Context, _ LocalWrite) (bool, error) {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
			return true, nil
		},
		barrier: func(_ context.Context, targetLSN uint64) (uint64, error) {
			return targetLSN, nil
		},
		invalidate: func(reason string) {
			invalidated <- reason
		},
	})

	results := make(chan peerWorkResult, 3)
	if _, err := q.enqueueWrite(LocalWrite{LSN: 1}, results); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := q.enqueueWrite(LocalWrite{LSN: 2}, results); err != nil {
		t.Fatal(err)
	}
	if _, err := q.enqueueWrite(LocalWrite{LSN: 3}, results); !errors.Is(err, ErrPeerQueueSaturated) {
		t.Fatalf("saturation error=%v", err)
	}
	if _, err := q.enqueueWrite(LocalWrite{LSN: 4}, results); !errors.Is(err, ErrPeerQueueSaturated) {
		t.Fatalf("post-saturation error=%v", err)
	}
	if reason := <-invalidated; reason == "" {
		t.Fatal("empty invalidation reason")
	}
	close(release)
	q.closeAndWait()
}

func TestPeerWorkQueue_CloseFailsPendingWork(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	q := newPeerWorkQueueWithOps("r1", 2, peerWorkQueueOps{
		ship: func(_ context.Context, _ LocalWrite) (bool, error) {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
			return false, errors.New("stopped")
		},
	})

	results := make(chan peerWorkResult, 2)
	if _, err := q.enqueueWrite(LocalWrite{LSN: 1}, results); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := q.enqueueWrite(LocalWrite{LSN: 2}, results); err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() {
		q.closeAndWait()
		close(closed)
	}()
	for {
		q.mu.Lock()
		queueClosed := q.closed
		q.mu.Unlock()
		if queueClosed {
			break
		}
	}
	close(release)
	<-closed

	first := <-results
	second := <-results
	if first.err == nil {
		t.Fatal("in-flight work unexpectedly succeeded")
	}
	if !errors.Is(second.err, ErrPeerQueueClosed) {
		t.Fatalf("pending close error=%v", second.err)
	}
}

func TestPeerWorkQueue_BarrierBelowTargetDegradesPeer(t *testing.T) {
	addr, _ := replicaHarness(t, "below-target")
	v := volumeHarness(t, "below-target-volume")
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{targetFor("r1", addr, 1, 1)}); err != nil {
		t.Fatal(err)
	}

	v.mu.Lock()
	q := v.peerQueues["r1"]
	peer := v.peers["r1"]
	v.mu.Unlock()
	results := make(chan peerWorkResult, 1)
	if _, err := q.enqueueBarrier(1, results); err != nil {
		t.Fatal(err)
	}
	result := <-results
	if !errors.Is(result.err, ErrBarrierBelowTargetLSN) {
		t.Fatalf("barrier error=%v", result.err)
	}
	if peer.State() != ReplicaDegraded {
		t.Fatalf("peer state=%s want degraded", peer.State())
	}
}

func TestReplicationVolume_LineageReplacementWaitsForOldQueue(t *testing.T) {
	addr, _ := replicaHarness(t, "lineage-replace")
	v := volumeHarness(t, "lineage-replace-volume")
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{targetFor("r1", addr, 1, 1)}); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	finished := make(chan struct{})
	oldQueue := newPeerWorkQueueWithOps("r1", 1, peerWorkQueueOps{
		ship: func(ctx context.Context, _ LocalWrite) (bool, error) {
			close(started)
			<-ctx.Done()
			close(finished)
			return false, ctx.Err()
		},
	})
	v.mu.Lock()
	v.closePeerQueueLocked("r1")
	v.peerQueues["r1"] = oldQueue
	oldPeer := v.peers["r1"]
	v.mu.Unlock()

	results := make(chan peerWorkResult, 1)
	if _, err := oldQueue.enqueueWrite(LocalWrite{LSN: 1}, results); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := v.UpdateReplicaSet(2, []ReplicaTarget{targetFor("r1", addr, 1, 2)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("lineage replacement returned before old in-flight queue work stopped")
	}
	if result := <-results; result.err == nil {
		t.Fatal("old-lineage in-flight write unexpectedly succeeded")
	}

	v.mu.Lock()
	newPeer := v.peers["r1"]
	newQueue := v.peerQueues["r1"]
	v.mu.Unlock()
	if newPeer == oldPeer || newQueue == oldQueue {
		t.Fatal("lineage replacement reused old peer or queue")
	}
}
