package replication

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestReplicationVolume_ObserveBatchSyncQuorumDoesNotWaitForSlowNonQuorumPeer(t *testing.T) {
	v := NewReplicationVolume("batch-quorum", storage.NewBlockStore(64, 4096))
	v.SetDurabilityMode(DurabilitySyncQuorum)

	var (
		fastMu   sync.Mutex
		fastLSNs []uint64
		slowMu   sync.Mutex
		slowLSNs []uint64
	)
	slowStarted := make(chan struct{})
	slowDrained := make(chan struct{})
	releaseSlow := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSlow) }) }

	fast := newPeerWorkQueueWithOps("fast", 16, peerWorkQueueOps{
		ship: func(_ context.Context, write LocalWrite) (bool, error) {
			fastMu.Lock()
			fastLSNs = append(fastLSNs, write.LSN)
			fastMu.Unlock()
			return true, nil
		},
		barrier: func(_ context.Context, targetLSN uint64) (uint64, error) {
			return targetLSN, nil
		},
	})
	slow := newPeerWorkQueueWithOps("slow", 16, peerWorkQueueOps{
		ship: func(_ context.Context, write LocalWrite) (bool, error) {
			if write.LSN == 1 {
				select {
				case <-slowStarted:
				default:
					close(slowStarted)
				}
				<-releaseSlow
			}
			slowMu.Lock()
			slowLSNs = append(slowLSNs, write.LSN)
			if len(slowLSNs) == 4 {
				close(slowDrained)
			}
			slowMu.Unlock()
			return true, nil
		},
		barrier: func(_ context.Context, targetLSN uint64) (uint64, error) {
			return targetLSN, nil
		},
	})
	v.mu.Lock()
	v.peerQueues["fast"] = fast
	v.peerQueues["slow"] = slow
	v.mu.Unlock()
	t.Cleanup(func() { _ = v.Close() })
	t.Cleanup(release)

	blocks := make([][]byte, 4)
	for i := range blocks {
		blocks[i] = make([]byte, 4096)
		blocks[i][0] = byte(i + 1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := v.ObserveBatch(ctx, 0, []uint64{1, 2, 3, 4}, blocks); err != nil {
		t.Fatal(err)
	}
	select {
	case <-slowStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("slow peer never received the first queued write")
	}

	fastMu.Lock()
	if len(fastLSNs) != 4 {
		fastMu.Unlock()
		t.Fatalf("fast peer LSNs=%v want [1 2 3 4]", fastLSNs)
	}
	for i, lsn := range fastLSNs {
		if lsn != uint64(i+1) {
			fastMu.Unlock()
			t.Fatalf("fast peer LSNs=%v want [1 2 3 4]", fastLSNs)
		}
	}
	fastMu.Unlock()

	release()
	select {
	case <-slowDrained:
	case <-time.After(time.Second):
		t.Fatal("slow peer did not drain all queued writes")
	}
	slowMu.Lock()
	defer slowMu.Unlock()
	if len(slowLSNs) != 4 {
		t.Fatalf("slow peer LSNs=%v want [1 2 3 4]", slowLSNs)
	}
	for i, lsn := range slowLSNs {
		if lsn != uint64(i+1) {
			t.Fatalf("slow peer LSNs=%v want [1 2 3 4]", slowLSNs)
		}
	}
}

func TestReplicationVolume_SyncQuorumDoesNotWaitForSlowNonQuorumBarrier(t *testing.T) {
	v, releaseSlow, slowStarted := volumeWithControlledBarrierQueues(t, DurabilitySyncQuorum)
	defer releaseSlow()

	if err := v.OnLocalWrite(context.Background(), LocalWrite{LBA: 0, LSN: 1, Data: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := v.Sync(ctx, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-slowStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("slow peer barrier was not queued")
	}
}

func TestReplicationVolume_SyncAllWaitsForSlowBarrier(t *testing.T) {
	v, releaseSlow, slowStarted := volumeWithControlledBarrierQueues(t, DurabilitySyncAll)
	if err := v.OnLocalWrite(context.Background(), LocalWrite{LBA: 0, LSN: 1, Data: make([]byte, 4096)}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- v.Sync(context.Background(), 1)
	}()
	<-slowStarted
	select {
	case err := <-done:
		t.Fatalf("sync_all returned before slow barrier release: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	releaseSlow()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestReplicationVolume_QueueSaturationIsTypedAndCounted(t *testing.T) {
	addr, _ := replicaHarness(t, "queue-saturation")
	v := volumeHarness(t, "queue-saturation")
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{targetFor("slow", addr, 1, 1)}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	v.mu.Lock()
	oldQueue := v.peerQueues["slow"]
	peer := v.peers["slow"]
	v.mu.Unlock()
	oldQueue.closeAndWait()
	q := newPeerWorkQueueWithOps("slow", 1, peerWorkQueueOps{
		ship: func(ctx context.Context, _ LocalWrite) (bool, error) {
			select {
			case <-started:
			default:
				close(started)
			}
			select {
			case <-releaseCh:
				return true, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		},
		invalidate: peer.Invalidate,
	})
	v.mu.Lock()
	v.peerQueues["slow"] = q
	v.mu.Unlock()
	t.Cleanup(release)

	first, err := v.enqueueLocalWrite(LocalWrite{LBA: 0, LSN: 1, Data: make([]byte, 4096)})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	second, err := v.enqueueLocalWrite(LocalWrite{LBA: 1, LSN: 2, Data: make([]byte, 4096)})
	if err != nil {
		t.Fatal(err)
	}
	third, err := v.enqueueLocalWrite(LocalWrite{LBA: 2, LSN: 3, Data: make([]byte, 4096)})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-third.result:
		if !errors.Is(err, ErrPeerQueueSaturated) {
			t.Fatalf("third write error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("saturated write did not complete")
	}
	if got := v.Stats().PeerQueueSaturated; got != 1 {
		t.Fatalf("PeerQueueSaturated=%d want 1", got)
	}
	v.mu.Lock()
	current := v.peerQueues["slow"]
	v.mu.Unlock()
	if current != q {
		t.Fatal("saturated queue changed before peer recovery completed")
	}
	release()
	<-first.result
	<-second.result
}

func TestReplicationVolume_HealthyRecoveryReplacesTerminalPeerQueue(t *testing.T) {
	addr, _ := replicaHarness(t, "queue-recovery")
	v := volumeHarness(t, "queue-recovery-volume")
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{targetFor("r1", addr, 1, 1)}); err != nil {
		t.Fatal(err)
	}

	v.mu.Lock()
	oldQueue := v.peerQueues["r1"]
	peer := v.peers["r1"]
	v.mu.Unlock()
	oldQueue.closeAndWait()
	peer.SetState(ReplicaDegraded)
	peer.SetState(ReplicaHealthy)

	v.mu.Lock()
	newQueue := v.peerQueues["r1"]
	v.mu.Unlock()
	if newQueue == nil || newQueue == oldQueue {
		t.Fatal("healthy recovery did not replace terminal peer queue")
	}
	results := make(chan peerWorkResult, 1)
	if _, err := newQueue.enqueueWrite(LocalWrite{LBA: 0, LSN: 1, Data: make([]byte, 4096)}, results); err != nil {
		t.Fatalf("replacement queue rejected write: %v", err)
	}
	if result := <-results; result.err != nil {
		t.Fatalf("replacement queue write failed: %v", result.err)
	}
}

func TestReplicationVolume_RecoveryQueueResetCannotLoseConcurrentWrite(t *testing.T) {
	addr, replicaStore := replicaHarness(t, "queue-reset-race")
	v := volumeHarness(t, "queue-reset-race-volume")
	if err := v.UpdateReplicaSet(1, []ReplicaTarget{targetFor("r1", addr, 1, 1)}); err != nil {
		t.Fatal(err)
	}

	v.mu.Lock()
	oldQueue := v.peerQueues["r1"]
	peer := v.peers["r1"]
	v.mu.Unlock()
	oldQueue.closeAndWait()
	peer.SetState(ReplicaDegraded)

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	peer.setOnHealthy(func() {
		close(callbackStarted)
		<-releaseCallback
		v.resetTerminalPeerQueue(peer)
	})
	stateDone := make(chan struct{})
	go func() {
		peer.SetState(ReplicaHealthy)
		close(stateDone)
	}()
	<-callbackStarted

	data := make([]byte, 4096)
	data[0] = 0x7a
	acks := v.dispatchLocalWrite(LocalWrite{LBA: 0, LSN: 1, Data: data})
	if err := v.waitForWriteAcks(context.Background(), acks); !errors.Is(err, ErrPeerQueueClosed) {
		t.Fatalf("write error=%v want old terminal queue evidence", err)
	}
	close(releaseCallback)
	<-stateDone

	deadline := time.Now().Add(time.Second)
	for {
		got, err := replicaStore.Read(0)
		if err == nil && len(got) > 0 && got[0] == 0x7a {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("concurrent write was not delivered through the replacement queue")
		}
		time.Sleep(time.Millisecond)
	}

	v.mu.Lock()
	newQueue := v.peerQueues["r1"]
	v.mu.Unlock()
	if newQueue == nil || newQueue == oldQueue {
		t.Fatal("terminal queue was not replaced")
	}
}

func volumeWithControlledBarrierQueues(
	t *testing.T,
	mode DurabilityMode,
) (*ReplicationVolume, func(), <-chan struct{}) {
	t.Helper()
	v := NewReplicationVolume("controlled-barrier", storage.NewBlockStore(64, 4096))
	v.SetDurabilityMode(mode)

	slowStarted := make(chan struct{})
	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	newQueue := func(peerID string, slow bool) *peerWorkQueue {
		return newPeerWorkQueueWithOps(peerID, 16, peerWorkQueueOps{
			ship: func(_ context.Context, _ LocalWrite) (bool, error) {
				return true, nil
			},
			barrier: func(_ context.Context, targetLSN uint64) (uint64, error) {
				if slow {
					select {
					case <-slowStarted:
					default:
						close(slowStarted)
					}
					<-releaseCh
				}
				return targetLSN, nil
			},
		})
	}
	v.mu.Lock()
	v.peerQueues["fast"] = newQueue("fast", false)
	v.peerQueues["slow"] = newQueue("slow", true)
	v.mu.Unlock()
	t.Cleanup(func() { _ = v.Close() })
	t.Cleanup(release)
	return v, release, slowStarted
}
