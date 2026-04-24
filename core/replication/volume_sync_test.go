package replication

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// --- Test 1: BestEffort E2E smoke ---

// TestReplicationVolume_Sync_BestEffort_E2E verifies the full
// StorageBackend.Sync → observer.Sync → DurabilityCoordinator →
// peer.Barrier → replica path end-to-end in best_effort mode.
//
// Flow: primary writes N LBAs through StorageBackend.Write; those
// fan out to the replica via the existing T4a-6 Observe hook. Host
// then calls StorageBackend.Sync which delegates to the observer.
// Observer calls DurabilityCoordinator which runs local fsync +
// per-peer barrier in parallel. Replica's LogicalStorage.Boundaries()
// must reflect the synced frontier after the call returns.
func TestReplicationVolume_Sync_BestEffort_E2E(t *testing.T) {
	const blockSize = 4096

	replicaStore := storage.NewBlockStore(64, blockSize)
	listener, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(func() { listener.Stop() })

	// Primary setup: LogicalStorage + StorageBackend + ReplicationVolume.
	primaryStore := storage.NewBlockStore(64, blockSize)
	id := frontend.Identity{
		VolumeID:        "vol-sync",
		ReplicaID:       "primary-sync",
		Epoch:           1,
		EndpointVersion: 1,
	}
	view := &alwaysHealthySyncView{proj: frontend.Projection{
		VolumeID: id.VolumeID, ReplicaID: id.ReplicaID,
		Epoch: id.Epoch, EndpointVersion: id.EndpointVersion, Healthy: true,
	}}
	backend := durable.NewStorageBackend(primaryStore, view, id)
	backend.SetOperational(true, "test harness ready")
	repVol := NewReplicationVolume(id.VolumeID, primaryStore)
	repVol.SetDurabilityMode(DurabilityBestEffort)
	backend.SetWriteObserver(repVol)
	t.Cleanup(func() {
		_ = backend.Close()
		_ = repVol.Close()
	})

	// Add one replica peer via the fake-master path.
	if err := repVol.UpdateReplicaSet(1, []ReplicaTarget{{
		ReplicaID:       "r1",
		DataAddr:        listener.Addr(),
		ControlAddr:     listener.Addr(),
		Epoch:           1,
		EndpointVersion: 1,
	}}); err != nil {
		t.Fatalf("UpdateReplicaSet: %v", err)
	}

	// Write N LBAs through StorageBackend (host-style byte Write).
	const n = 10
	for i := 0; i < n; i++ {
		data := make([]byte, blockSize)
		data[0] = byte(i + 1)
		data[1] = byte(0xA0 + i)
		offset := int64(i) * int64(blockSize)
		if _, err := backend.Write(context.Background(), offset, data); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}

	// Wait for last ship to land so Sync sees the expected walHead.
	waitForReplicaLBA(t, replicaStore, n-1, byte(n), byte(0xA0+n-1), 2*time.Second)

	// Host-side Sync → observer path.
	if err := backend.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Replica frontier must have advanced. R/S/H from Boundaries():
	// after the primary syncs + barrier, R on the replica should
	// reflect that the replica has applied + fsynced.
	_, _, pH := primaryStore.Boundaries()
	rR, _, _ := replicaStore.Boundaries()
	if rR < pH {
		t.Fatalf("replica R=%d < primary H=%d after Sync — barrier did not drive replica frontier",
			rR, pH)
	}

	// Byte-exact cross-check.
	for i := uint32(0); i < n; i++ {
		pri, _ := primaryStore.Read(i)
		rep, _ := replicaStore.Read(i)
		if !bytes.Equal(pri, rep) {
			t.Fatalf("LBA %d byte mismatch: primary[0]=%d replica[0]=%d", i, pri[0], rep[0])
		}
	}

	// Peer must still be Healthy — best_effort with no failures.
	repVol.mu.Lock()
	state := repVol.peers["r1"].State()
	repVol.mu.Unlock()
	if state != ReplicaHealthy {
		t.Fatalf("peer state after happy-path Sync: got %s want healthy", state)
	}
}

// --- Test 2: Forward-carry fence for INV-REPL-LSN-ORDER-FANOUT-001 ---

// TestReplicationVolume_Sync_PreservesLSNOrderUnderConcurrency is
// the T4b-4/T4b-5 forward-carry fence for the T4a-4 LSN-order
// invariant. Concurrent OnLocalWrite and Sync calls must not
// interleave in a way that lets a later-LSN message reach the
// replica before an earlier-LSN one.
//
// Pattern mirrors TestReplicationVolume_OnLocalWrite_ConcurrentLSNs_
// OrderedAtReplica (T4a-4 test #6): N goroutines, each pre-assigned
// a unique LSN under a caller-side mutex, call OnLocalWrite and
// sometimes Sync concurrently. LBAs are interleaved with LSN
// ordering so any reorder is byte-detectable at the replica.
func TestReplicationVolume_Sync_PreservesLSNOrderUnderConcurrency(t *testing.T) {
	const blockSize = 4096

	replicaStore := storage.NewBlockStore(64, blockSize)
	listener, err := transport.NewReplicaListener("127.0.0.1:0", replicaStore)
	if err != nil {
		t.Fatal(err)
	}
	listener.Serve()
	t.Cleanup(func() { listener.Stop() })

	primary := storage.NewBlockStore(64, blockSize)
	v := NewReplicationVolume("vol-ordering", primary)
	v.SetDurabilityMode(DurabilityBestEffort)
	t.Cleanup(func() { _ = v.Close() })

	if err := v.UpdateReplicaSet(1, []ReplicaTarget{{
		ReplicaID:       "r1",
		DataAddr:        listener.Addr(),
		ControlAddr:     listener.Addr(),
		Epoch:           1,
		EndpointVersion: 1,
	}}); err != nil {
		t.Fatal(err)
	}

	const n = 40
	var callerMu sync.Mutex
	var nextLSN atomic.Uint64
	var wg sync.WaitGroup
	start := make(chan struct{})

	// Build a write plan where LSN ordering differs from LBA ordering
	// (same shape as T4a-4 test #6). LSN i writes to LBA (n-1-i).
	type plan struct {
		lba       uint32
		lsnMarker byte
	}
	plans := make([]plan, n)
	for i := 0; i < n; i++ {
		plans[i] = plan{lba: uint32(n - 1 - i), lsnMarker: byte(i + 1)}
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start

			// Caller-side serialized section: LSN allocation + write.
			// Sync may be requested randomly mid-stream to exercise
			// the write + sync interleave on v.mu.
			callerMu.Lock()
			lsn := nextLSN.Add(1)
			p := plans[lsn-1]
			data := make([]byte, blockSize)
			data[0] = p.lsnMarker
			data[1] = 0xCC
			err := v.OnLocalWrite(context.Background(), LocalWrite{
				LBA:  p.lba,
				Data: data,
				LSN:  lsn,
			})
			callerMu.Unlock()
			if err != nil {
				t.Errorf("OnLocalWrite[%d] lsn=%d: %v", idx, lsn, err)
				return
			}

			// Periodically request a Sync — this is the new T4b-5
			// code path that must coexist with OnLocalWrite without
			// breaking LSN ordering.
			if idx%4 == 0 {
				if err := v.Sync(context.Background(), lsn); err != nil {
					t.Errorf("Sync[%d] lsn=%d: %v", idx, lsn, err)
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	// Final sync to drain any in-flight ships.
	if err := v.Sync(context.Background(), uint64(n)); err != nil {
		t.Fatalf("final Sync: %v", err)
	}

	// Wait for last-written LBA to settle on replica.
	finalLBA0Marker := byte(n)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := replicaStore.Read(0)
		if got != nil && got[0] == finalLBA0Marker {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Every LBA must carry the LSN marker that plan dictated. Any
	// reorder (later-LSN arriving before earlier-LSN on the same
	// LBA) would leave a stale marker and fail here.
	for i := 0; i < n; i++ {
		p := plans[i]
		got, _ := replicaStore.Read(p.lba)
		if got == nil || got[0] != p.lsnMarker {
			t.Fatalf("LBA %d: got marker %02x, want %02x (LSN=%d) — out-of-order under concurrent Sync (INV-REPL-LSN-ORDER-FANOUT-001 regression)",
				p.lba, got[0], p.lsnMarker, i+1)
		}
	}
}

// --- helpers ---

type alwaysHealthySyncView struct {
	proj frontend.Projection
}

func (v *alwaysHealthySyncView) Projection() frontend.Projection { return v.proj }
