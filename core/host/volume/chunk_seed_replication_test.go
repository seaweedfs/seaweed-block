package volume

// Composition RED: a fresh walstore-chunk host restoring from a remote HEAD,
// in the production startup order of cmd/blockvolume, with the real RF=1
// ReplicationVolume. The volume recovers before the master's assignment is
// visible (the chunk layer attach is deferred); blockvolume then builds the
// ReplicationVolume, which captures the store's next LSN; the replayed
// assignment lets the deferred attach claim HEAD and seed the local frontier
// to HEAD.remote_lsn. A frontend write after that must complete.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/replication"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// RED: a remote HEAD at LSN 5000 is inherited, so the frontier jumps after
// the ReplicationVolume captured its next LSN.
func TestWALStoreChunk_RestoredHostWritesAfterFrontierSeed(t *testing.T) {
	restoredHostWrites(t, 5000, false)
}

// The same, with the write observer installed only after the seed landed.
func TestWALStoreChunk_RestoredHostWritesWhenObserverFollowsSeed(t *testing.T) {
	restoredHostWrites(t, 5000, true)
}

// Control: no remote HEAD, no frontier seed; the same order writes fine.
func TestWALStoreChunk_NewLineageHostWritesWithoutSeed(t *testing.T) {
	restoredHostWrites(t, 0, false)
}

func restoredHostWrites(t *testing.T, inheritedLSN uint64, observerAfterSeed bool) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	if inheritedLSN > 0 {
		// The remote HEAD a previous holder (epoch 1) published.
		if _, err := remote.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{
			Epoch: 1, Holder: "old/r1", RemoteLSN: inheritedLSN, Chunks: map[uint64]storage.ChunkRef{},
		}); err != nil {
			t.Fatal(err)
		}
	}

	h, err := New(Config{
		MasterAddr: "127.0.0.1:1", // never dialed: no Start
		ServerID:   "s1", VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "127.0.0.1:2", CtrlAddr: "127.0.0.1:3",
		EnableT1Readiness: true,
		Logger:            newDiscardLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// main.go order: readiness blocked, the assignment (epoch 5) retained.
	h.BlockLocalReadiness("awaiting durable recovery")
	h.applyFixtureFact(&control.AssignmentFact{VolumeId: "v1", ReplicaId: "r1", Epoch: 5, EndpointVersion: 2, PeerSetGeneration: 1})

	dp, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, ChunkHolder: "r1",
		StorageRoot:    t.TempDir(), // empty: a fresh host
		BlockSize:      4096,
		NumBlocks:      256,
		ChunkBlocks:    16,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, h.ProjectionView())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dp.Close() })
	if _, err := dp.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	recCtx, recCancel := context.WithTimeout(ctx, 10*time.Second)
	_, recErr := dp.RecoverVolume(recCtx, "v1")
	recCancel()
	if recErr != nil {
		t.Fatalf("RecoverVolume before the assignment: %v", recErr)
	}

	// main.go: clear readiness, build the RF=1 ReplicationVolume over the
	// store (it captures the store's next LSN), install it: the retained
	// assignment replays and the deferred attach seeds the frontier.
	store := dp.LogicalStorage("v1")
	rv := replication.NewReplicationVolume("v1", store)
	h.ClearLocalReadinessBlock()
	h.SetReplicationVolume(rv)
	// main.go: the backend's write observer is the ReplicationVolume.
	sb := dp.Backend("v1")
	if sb == nil {
		t.Fatal("no durable backend for v1")
	}
	if !observerAfterSeed {
		if err := sb.SetWriteObserver(rv); err != nil {
			t.Fatalf("install the write observer: %v", err)
		}
	}

	openCtx, openCancel := context.WithTimeout(ctx, 10*time.Second)
	defer openCancel()
	be, err := dp.Open(openCtx, "v1")
	if err != nil {
		t.Fatalf("frontend open: %v", err)
	}

	// Wait until the attach has seeded the frontier (writes are refused
	// until the layer is attached).
	deadline := time.Now().Add(10 * time.Second)
	for inheritedLSN > 0 && store.NextLSN() <= inheritedLSN {
		if time.Now().After(deadline) {
			t.Fatalf("frontier never seeded: next LSN %d, inherited %d", store.NextLSN(), inheritedLSN)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if observerAfterSeed {
		if err := sb.SetWriteObserver(rv); err != nil {
			t.Fatalf("install the write observer: %v", err)
		}
	}

	// A frontend write and flush must complete (bounded here so a hang
	// fails instead of blocking the test).
	data := bytes.Repeat([]byte{0x5A}, 4096)
	var werr error
	for start := time.Now(); time.Since(start) < 10*time.Second; time.Sleep(20 * time.Millisecond) {
		wctx, wcancel := context.WithTimeout(ctx, 3*time.Second)
		_, werr = be.Write(wctx, 40*4096, data)
		wcancel()
		if werr == nil {
			break
		}
	}
	if werr != nil {
		t.Fatalf("frontend write on the restored host after the frontier seed (next LSN %d): %v", store.NextLSN(), werr)
	}
	sctx, scancel := context.WithTimeout(ctx, 3*time.Second)
	defer scancel()
	if err := be.Sync(sctx); err != nil {
		t.Fatalf("flush on the restored host after the frontier seed: %v", err)
	}
}
