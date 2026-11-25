package volume

// A refused minimum position never publishes an image.

import (
	"context"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
)

type healthyView struct{}

func (healthyView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1, Healthy: true}
}

// Through the real provider: a minLSN never written is refused with no
// upload and no new HEAD; a written one cuts.
func TestChunkControl_CutToUnwrittenMinLSNPublishesNothing(t *testing.T) {
	ctx := context.Background()
	fixture := testutil.NewLayoutFixture(t, "v1", 256, 16)
	cfg := durable.ProviderConfig{
		Impl:        durable.ImplWALStoreChunk,
		StorageRoot: t.TempDir(),
		BlockSize:   4096,
		NumBlocks:   256,
		ChunkBlocks: 16,
	}
	fixture.Configure(&cfg)
	dp, err := durable.NewDurableProvider(cfg, healthyView{})
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
	claimed, found, err := fixture.Catalog.ReadHead(ctx, fixture.Ref)
	if err != nil || !found {
		t.Fatalf("claimed layout HEAD: found=%v err=%v", found, err)
	}
	assigned := (healthyView{}).Projection()
	if claimed.Epoch != assigned.Epoch || claimed.Holder != assigned.ReplicaID ||
		claimed.State != layout.StateLive || claimed.Position != 0 || claimed.Current != nil {
		t.Fatalf("fresh open did not establish an empty assigned live claim: HEAD=%+v", claimed)
	}
	if _, err := s.Write(1, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	ctl := NewChunkControl(dp, fixture.Catalog, fixture.Ref)
	last := s.NextLSN() - 1
	if _, err := ctl.Cut(ctx, "v1", last+5); err == nil {
		t.Fatal("a cut to an LSN never written succeeded")
	}
	if head, found, err := fixture.Catalog.ReadHead(ctx, fixture.Ref); err != nil || !found || !reflect.DeepEqual(head, claimed) {
		t.Fatalf("a refused minLSN published beyond the open claim: HEAD %+v (claim %+v), found=%v err=%v", head, claimed, found, err)
	}
	if r, err := ctl.Cut(ctx, "v1", last); err != nil || r.CutLSN != last || r.HeadGen <= claimed.Gen {
		t.Fatalf("cut to the last written LSN: %+v %v", r, err)
	}
}
