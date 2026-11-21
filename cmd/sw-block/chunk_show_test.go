package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ops chunk show prints a volume's VOLUME entry and HEAD from the filer;
// either is null when it does not exist (B9 W2 appendix).
func TestOpsChunk_Show(t *testing.T) {
	ctx := context.Background()
	mem := newManifestFixtureFiler(chunkstore.NewMemFiler())
	prev := dialChunkFiler
	dialChunkFiler = func(context.Context, string) (chunkstore.Filer, func() error, error) {
		return mem, func() error { return nil }, nil
	}
	t.Cleanup(func() { dialChunkFiler = prev })
	cat, err := chunkstore.NewCatalog(mem, defaultChunkPathCLI)
	if err != nil {
		t.Fatal(err)
	}
	geo := chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}
	if _, err := cat.EnsureVolume(ctx, "v1", geo); err != nil {
		t.Fatal(err)
	}
	head := storage.ChunkHead{Gen: 2, Epoch: 1, Holder: "s1/r1", RemoteLSN: 42, State: storage.ChunkHeadActive,
		Chunks: map[uint64]storage.ChunkRef{0: {Key: "chunks/e1/c0/k"}}}
	head = prepareFixtureHead(t, mem, head)
	if _, err := cat.CreateSnapshot(ctx, "v1", chunkstore.SnapshotRequest{Name: "s1", RequestID: "r"}, head); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.Fork(ctx, chunkstore.ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); err != nil {
		t.Fatal(err)
	}
	show := func(vol string) chunkstore.VolumeShowBody {
		t.Helper()
		code, out, errs := runChunk("show", "--volume", vol, "--chunk-filer", "f:1")
		var b chunkstore.VolumeShowBody
		if code != 0 || json.Unmarshal([]byte(out), &b) != nil {
			t.Fatalf("show %s: %d %q %q", vol, code, out, errs)
		}
		return b
	}
	fork := show("v2")
	if fork.Volume == nil || fork.Volume.Origin != chunkstore.OriginFork || fork.Volume.NumBlocks != 64 || fork.Volume.Parent == nil ||
		fork.Volume.Parent.VolumeID != "v1" || fork.Volume.Parent.Snapshot != "s1" {
		t.Fatalf("show v2 VOLUME: %+v", fork.Volume)
	}
	if fork.Head == nil || fork.Head.Gen != 1 || fork.Head.Epoch != 0 || fork.Head.RemoteLSN != 42 || fork.Head.State != "active" ||
		fork.Head.ChunkCount != 1 || fork.Head.DetachedBy != nil {
		t.Fatalf("show v2 HEAD: %+v", fork.Head)
	}
	// A created volume carries its initial HEAD (gen 1, epoch 0, no chunks).
	if src := show("v1"); src.Volume == nil || src.Volume.Origin != chunkstore.OriginCreate || src.Head == nil || src.Head.Gen != 1 ||
		src.Head.Epoch != 0 || src.Head.RemoteLSN != 0 || src.Head.ChunkCount != 0 || src.Head.State != "active" {
		t.Fatalf("show v1 (VOLUME and its initial HEAD): %+v %+v", src.Volume, src.Head)
	}
	if none := show("v9"); none.Volume != nil || none.Head != nil || none.VolumeID != "v9" {
		t.Fatalf("show of a volume that does not exist: %+v", none)
	}
	if code, out, _ := runChunk("show", "--chunk-filer", "f:1"); code != 2 || chunkErrorOut(t, out).Code != "bad_request" {
		t.Fatalf("show without --volume: exit %d %q", code, out)
	}
}
