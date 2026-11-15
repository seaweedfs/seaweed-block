package volume

// Operation order in the control adapter (review of #115): a snapshot
// resolves its request before it cuts (§3.2 step 1), and a cut to a minLSN
// never written publishes nothing (§3.1, MakeRemoteDurable's refusal).

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// After a recorded snapshot, the cut path fails (detached layer): a retry of
// the same request still returns the snapshot and another request gets
// snapshot_exists, neither cutting; only a free name reaches the cut.
func TestChunkControl_SnapshotResolvesRequestBeforeCutting(t *testing.T) {
	ctx := context.Background()
	fp, ctl := adapterFixture(t)
	first, err := ctl.Snapshot(ctx, "v1", ChunkSnapshotRequest{Name: "s1", RequestID: "r"})
	if err != nil || fp.cuts != 1 {
		t.Fatalf("first snapshot: %+v %v (cuts %d)", first, err, fp.cuts)
	}
	fp.cutErr = storage.ErrChunkLayerDetached
	again, err := ctl.Snapshot(ctx, "v1", ChunkSnapshotRequest{Name: "s1", RequestID: "r"})
	if err != nil || !again.Replayed || again.HeadGen != first.HeadGen || fp.cuts != 1 {
		t.Fatalf("retry after the cut path failed: %+v %v (cuts %d); want the recorded snapshot without a cut", again, err, fp.cuts)
	}
	if _, err := ctl.Snapshot(ctx, "v1", ChunkSnapshotRequest{Name: "s1", RequestID: "other"}); chunkstore.CodeOf(err) != chunkstore.CodeSnapshotExists || fp.cuts != 1 {
		t.Fatalf("another request for the name: %v (cuts %d); want snapshot_exists without a cut", err, fp.cuts)
	}
	if _, err := ctl.Snapshot(ctx, "v1", ChunkSnapshotRequest{Name: "s2", RequestID: "r2"}); err == nil || fp.cuts != 2 {
		t.Fatalf("a free name with the cut path failing: %v (cuts %d); want the cut's error", err, fp.cuts)
	}
}

type healthyView struct{}

func (healthyView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1, Healthy: true}
}

// Through the real provider: a minLSN never written is refused with no
// upload and no new HEAD; a written one cuts.
func TestChunkControl_CutToUnwrittenMinLSNPublishesNothing(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	dp, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl:           durable.ImplWALStoreChunk,
		StorageRoot:    t.TempDir(),
		BlockSize:      4096,
		NumBlocks:      256,
		ChunkBlocks:    16,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, healthyView{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dp.Close() })
	s, err := dp.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dp.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(1, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	cat, err := chunkstore.NewCatalog(chunkstore.NewMemFiler(), "/swblock/chunks")
	if err != nil {
		t.Fatal(err)
	}
	ctl := NewChunkControl(dp, cat)
	last := s.NextLSN() - 1
	if _, err := ctl.Cut(ctx, "v1", last+5); err == nil {
		t.Fatal("a cut to an LSN never written succeeded")
	}
	if head, _ := remote.GetHead(ctx); head.Gen != 0 || remote.Puts() != 0 {
		t.Fatalf("a refused minLSN published: HEAD gen %d, %d chunks stored", head.Gen, remote.Puts())
	}
	if r, err := ctl.Cut(ctx, "v1", last); err != nil || r.CutLSN != last || r.HeadGen != 1 {
		t.Fatalf("cut to the last written LSN: %+v %v", r, err)
	}
}
