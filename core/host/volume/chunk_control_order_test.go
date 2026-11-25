package volume

// A refused minimum position never publishes an image.

import (
	"context"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

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
	claimed, err := remote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assigned := (healthyView{}).Projection()
	if claimed.Gen != 1 || claimed.Epoch != assigned.Epoch || claimed.Holder != assigned.ReplicaID ||
		claimed.State != storage.ChunkHeadActive || claimed.RemoteLSN != 0 || claimed.DetachedBy != nil ||
		len(claimed.Chunks) != 0 || len(claimed.Segments) != 0 || claimed.ManifestChunkCount != 0 || claimed.ChunkBytes != 0 || remote.Puts() != 0 {
		t.Fatalf("fresh open did not establish exactly one empty assigned active claim: HEAD=%+v PUTs=%d", claimed, remote.Puts())
	}
	if _, err := s.Write(1, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	ctl := NewChunkControl(dp, nil, chunkstore.VolumeRef{})
	last := s.NextLSN() - 1
	if _, err := ctl.Cut(ctx, "v1", last+5); err == nil {
		t.Fatal("a cut to an LSN never written succeeded")
	}
	if head, err := remote.GetHead(ctx); err != nil || !reflect.DeepEqual(head, claimed) || remote.Puts() != 0 {
		t.Fatalf("a refused minLSN published beyond the open claim: HEAD %+v (claim %+v), %d chunks stored, err=%v", head, claimed, remote.Puts(), err)
	}
	if r, err := ctl.Cut(ctx, "v1", last); err != nil || r.CutLSN != last || r.HeadGen != claimed.Gen+1 {
		t.Fatalf("cut to the last written LSN: %+v %v", r, err)
	}
}
