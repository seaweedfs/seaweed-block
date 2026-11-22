package volume

// Operation order in the control adapter (review of #115): a snapshot
// resolves its request before it cuts (§3.2 step 1), and a cut to a minLSN
// never written publishes nothing (§3.1, MakeRemoteDurable's refusal).

import (
	"context"
	"errors"
	"reflect"
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
	cat, err := chunkstore.NewCatalog(chunkstore.NewMemFiler(), "/swblock/chunks")
	if err != nil {
		t.Fatal(err)
	}
	ctl := NewChunkControl(dp, cat)
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

// A snapshot request with invalid labels is refused with invalid_label before
// any cut.
func TestChunkControl_InvalidLabelRefusedBeforeCutting(t *testing.T) {
	fp, ctl := adapterFixture(t)
	_, err := ctl.Snapshot(context.Background(), "v1", ChunkSnapshotRequest{Name: "s1", RequestID: "r", Labels: map[string]string{"Bad Key": "x"}})
	if chunkstore.CodeOf(err) != chunkstore.CodeInvalidLabel || fp.cuts != 0 {
		t.Fatalf("snapshot with an invalid label: %v (cuts %d); want invalid_label without a cut", err, fp.cuts)
	}
}

// ownerGuard records, at Begin, how many cuts the provider had taken.
type ownerGuard struct {
	cutsAtBegin []int
	finishes    []string
	fp          *fakeChunkProvider
	beginErr    error
}

func (g *ownerGuard) Begin(context.Context, string) (string, error) {
	g.cutsAtBegin = append(g.cutsAtBegin, g.fp.cuts)
	if g.beginErr != nil {
		return "", g.beginErr
	}
	return "owner", nil
}

func (g *ownerGuard) Finish(_ context.Context, vol, token string, head storage.ChunkHead) error {
	g.finishes = append(g.finishes, vol+":"+token)
	return nil
}

// The owner's seal begins the share BEFORE the cut that produces the sealed
// HEAD, and the catalog finishes it with that token (no second Begin).
func TestChunkControl_SnapshotBeginsShareBeforeTheCut(t *testing.T) {
	ctx := context.Background()
	fp, ctl := adapterFixture(t)
	g := &ownerGuard{fp: fp}
	ctl.(*chunkControl).cat.SetShareGuard(g)
	if _, err := ctl.Snapshot(ctx, "v1", ChunkSnapshotRequest{Name: "s1", RequestID: "r"}); err != nil {
		t.Fatal(err)
	}
	if len(g.cutsAtBegin) != 1 || g.cutsAtBegin[0] != 0 || fp.cuts != 1 {
		t.Fatalf("Begin saw %v cuts (cuts now %d); want one Begin before the one cut", g.cutsAtBegin, fp.cuts)
	}
	if len(g.finishes) != 1 || g.finishes[0] != "v1:owner" {
		t.Fatalf("finishes %v; want one Finish with the owner's token", g.finishes)
	}
}

// A refused Begin stops the owner's seal before its cut: no cut, no Finish,
// no snapshot.
func TestChunkControl_SnapshotBeginRefusedCutsNothing(t *testing.T) {
	ctx := context.Background()
	fp, ctl := adapterFixture(t)
	g := &ownerGuard{fp: fp, beginErr: errors.New("share refused")}
	ctl.(*chunkControl).cat.SetShareGuard(g)
	if _, err := ctl.Snapshot(ctx, "v1", ChunkSnapshotRequest{Name: "s1", RequestID: "r"}); err == nil {
		t.Fatal("snapshot after a refused Begin")
	}
	if fp.cuts != 0 || len(g.finishes) != 0 {
		t.Fatalf("cuts %d finishes %v after a refused Begin; want none", fp.cuts, g.finishes)
	}
}
