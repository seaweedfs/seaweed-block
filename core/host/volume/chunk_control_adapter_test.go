package volume

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type fakeChunkProvider struct {
	cut    storage.ChunkCutResult
	detach storage.ChunkCutResult
	last   uint64
	cuts   int
}

func (f *fakeChunkProvider) CutVolume(context.Context, string) (storage.ChunkCutResult, error) {
	f.cuts++
	return f.cut, nil
}

func (f *fakeChunkProvider) DetachVolume(context.Context, string) (storage.ChunkCutResult, uint64, error) {
	return f.detach, f.last, nil
}

func adapterFixture(t *testing.T) (*fakeChunkProvider, ChunkControl) {
	t.Helper()
	head := storage.ChunkHead{Gen: 3, Epoch: 2, Holder: "r1", RemoteLSN: 9, State: storage.ChunkHeadActive,
		Chunks: map[uint64]storage.ChunkRef{0: {Key: "chunks/e2/c0/k1"}, 5: {Key: "chunks/e2/c5/k2"}}}
	fp := &fakeChunkProvider{cut: storage.ChunkCutResult{H: 9, Head: head}}
	cat, err := chunkstore.NewCatalog(chunkstore.NewMemFiler(), "/swblock/chunks")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.EnsureVolume(context.Background(), "v1", chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	return fp, NewChunkControl(fp, cat)
}

// Snapshot validates before it cuts, then stores the HEAD its cut
// installed (§3.2); list and delete go to the catalog.
func TestChunkControl_SnapshotStoresTheCutHead(t *testing.T) {
	ctx := context.Background()
	fp, ctl := adapterFixture(t)
	for _, bad := range []ChunkSnapshotRequest{{Name: "a/b", RequestID: "r"}, {Name: "s1"}} {
		if _, err := ctl.Snapshot(ctx, "v1", bad); chunkstore.CodeOf(err) != chunkstore.CodeInvalidName {
			t.Fatalf("snapshot %+v: %v, want invalid_name", bad, err)
		}
	}
	if fp.cuts != 0 {
		t.Fatalf("an invalid snapshot request cut %d times", fp.cuts)
	}
	info, err := ctl.Snapshot(ctx, "v1", ChunkSnapshotRequest{Name: "s1", RequestID: "r", Labels: map[string]string{"agent.step": "x"}, Consistency: "app"})
	if err != nil || fp.cuts != 1 || info.HeadGen != 3 || info.HeadEpoch != 2 || info.CutLSN != 9 || info.ChunkCount != 2 ||
		info.Labels["agent.step"] != "x" || info.Consistency != "app" || info.Replayed {
		t.Fatalf("snapshot: %+v %v (cuts %d)", info, err, fp.cuts)
	}
	again, err := ctl.Snapshot(ctx, "v1", ChunkSnapshotRequest{Name: "s1", RequestID: "r"})
	if err != nil || !again.Replayed || again.HeadGen != 3 {
		t.Fatalf("snapshot retry: %+v %v", again, err)
	}
	if _, err := ctl.Snapshot(ctx, "v1", ChunkSnapshotRequest{Name: "s1", RequestID: "other"}); chunkstore.CodeOf(err) != chunkstore.CodeSnapshotExists {
		t.Fatalf("snapshot with another request id: %v, want snapshot_exists", err)
	}
	list, _, err := ctl.ListSnapshots(ctx, "v1", chunkstore.ListOptions{})
	if err != nil || len(list) != 1 || list[0].Name != "s1" {
		t.Fatalf("list: %+v %v", list, err)
	}
	if st, err := ctl.DeleteSnapshot(ctx, "v1", "s1"); err != nil || st != chunkstore.SnapDeleting {
		t.Fatalf("delete: %q %v", st, err)
	}
}

func TestChunkControl_CutMinLSNAndDetach(t *testing.T) {
	ctx := context.Background()
	fp, ctl := adapterFixture(t)
	if _, err := ctl.Cut(ctx, "v1", 10); err == nil {
		t.Fatal("a cut that reached LSN 9 satisfied minLSN 10")
	}
	r, err := ctl.Cut(ctx, "v1", 9)
	if err != nil || r != (ChunkCutResponse{VolumeID: "v1", CutLSN: 9, HeadGen: 3, HeadEpoch: 2, Holder: "r1"}) {
		t.Fatalf("cut: %+v %v", r, err)
	}
	fp.detach = storage.ChunkCutResult{H: 12, Head: storage.ChunkHead{Gen: 4, Epoch: 2, State: storage.ChunkHeadDetached}}
	fp.last = 12
	d, err := ctl.Detach(ctx, "v1")
	if err != nil || d != (ChunkDetachResponse{VolumeID: "v1", CutLSN: 12, HeadGen: 4, HeadEpoch: 2, LastWrittenLSN: 12}) {
		t.Fatalf("detach: %+v %v", d, err)
	}
}
