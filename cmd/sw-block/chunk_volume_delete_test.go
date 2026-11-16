package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// volume-delete needs a volume and --master; it refuses an attached volume
// and deletes a free one (the real master runtime reports it unassigned);
// the deleted id cannot be created again.
func TestOpsChunk_VolumeDelete(t *testing.T) {
	ctx := context.Background()
	mem := chunkstore.NewMemFiler()
	prev := dialChunkFiler
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
	t.Cleanup(func() { dialChunkFiler = prev })
	cat, err := chunkstore.NewCatalog(mem, defaultChunkPathCLI)
	if err != nil {
		t.Fatal(err)
	}
	geo := chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}
	for _, vol := range []string{"v1", "v2"} {
		if _, err := cat.EnsureVolume(ctx, vol, geo); err != nil {
			t.Fatal(err)
		}
	}
	// v2 is attached: its HEAD names a holder.
	r, err := chunkstore.NewChunkRemote(mem, &gcCLIStore{MemFiler: mem}, defaultChunkPathCLI+"/v2")
	if err != nil {
		t.Fatal(err)
	}
	cur, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, storage.ChunkHead{Epoch: 1, Holder: "h1", Chunks: map[uint64]storage.ChunkRef{}, State: storage.ChunkHeadActive}); err != nil {
		t.Fatal(err)
	}

	for _, args := range [][]string{
		{"volume-delete", "v1", "--chunk-filer", "f:1"},                                      // no --master
		{"volume-delete", "--master", "m:1", "--chunk-filer", "f:1"},                         // no volume
		{"volume-delete", "--volume", "v1", "v2", "--master", "m:1", "--chunk-filer", "f:1"}, // two volumes
	} {
		if code, out, _ := runChunk(args...); code == 0 || !strings.Contains(out, codeBadRequestCLI) {
			t.Fatalf("%v: %d %q; want a bad request", args, code, out)
		}
	}

	cfg := gcTestMasterConfig(t)
	cfg.LifecycleStoreDir = t.TempDir() // MarkVolumeDeleting admits the delete
	h, err := master.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.Close(ctx)
	})
	// v2 is admitted by the master (it has no authority) but held in the
	// catalog: refused, and the partial result says what was done.
	code, out, _ := runChunk("volume-delete", "--chunk-filer", "f:1", "--master", h.Addr(), "v2")
	var partial struct {
		chunkstore.VolumeDeleteResult
		Error *chunkErrBody `json:"error"`
	}
	if code == 0 || json.Unmarshal([]byte(out), &partial) != nil || partial.Error == nil || partial.Error.Code != chunkstore.CodeVolumeInUse || !partial.Admitted || partial.HeadMarked || partial.Completed {
		t.Fatalf("delete of attached v2: %d %q; want volume_in_use with the partial result", code, out)
	}
	code, out, errs := runChunk("volume-delete", "--chunk-filer", "f:1", "--master", h.Addr(), "v1")
	var res chunkstore.VolumeDeleteResult
	if code != 0 || json.Unmarshal([]byte(out), &res) != nil || res.VolumeID != "v1" || res.Replayed || !res.Completed {
		t.Fatalf("delete v1: %d %q %q", code, out, errs)
	}
	if _, err := cat.EnsureVolume(ctx, "v1", geo); chunkstore.CodeOf(err) != chunkstore.CodeVolumeDeleted {
		t.Fatalf("recreate v1: %v; want volume_deleted", err)
	}
}
