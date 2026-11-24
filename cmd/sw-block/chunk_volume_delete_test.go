package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/host/master"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type volumeDeleteOutput struct {
	chunkstore.VolumeDeleteResult
	Error *chunkErrBody `json:"error"`
}

func lifecycleClient(t *testing.T, addr string) control.LifecycleServiceClient {
	t.Helper()
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return control.NewLifecycleServiceClient(c)
}

func startDeleteMaster(t *testing.T, cfg master.Config) (*master.Host, func()) {
	t.Helper()
	h, err := master.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	return h, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.Close(ctx)
	}
}

// volume-delete against a real master runtime: argument checks; a held
// volume and a volume with a snapshot are refused with the admission given
// back (usable again); a lost first admission is adopted and given back by
// a rerun with the same request id (usable after a master restart); a
// completed delete is committed (never created again).
func TestOpsChunk_VolumeDelete(t *testing.T) {
	ctx := context.Background()
	mem := chunkstore.NewMemFiler()
	prev := dialChunkFiler
	dialChunkFiler = func(chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
	t.Cleanup(func() { dialChunkFiler = prev })
	cat, err := chunkstore.NewCatalog(mem, defaultChunkPathCLI)
	if err != nil {
		t.Fatal(err)
	}
	geo := chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}
	for _, vol := range []string{"v1", "v2", "v3"} {
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
	// v3 has a snapshot.
	h3, _, err := cat.ReadHead(ctx, "v3")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.CreateSnapshot(ctx, "v3", chunkstore.SnapshotRequest{Name: "s", RequestID: "rs"}, h3); err != nil {
		t.Fatal(err)
	}

	for _, args := range [][]string{
		{"volume-delete", "--chunk-filer", "f:1", "--request-id", "x", "v1"},                       // no --master
		{"volume-delete", "--chunk-filer", "f:1", "--master", "m:1", "v1"},                         // no --request-id
		{"volume-delete", "--master", "m:1", "--request-id", "x", "--chunk-filer", "f:1"},          // no volume
		{"volume-delete", "--volume", "v1", "--master", "m:1", "--request-id", "x", "v2", "extra"}, // two volumes
	} {
		if code, out, _ := runChunk(args...); code == 0 || !strings.Contains(out, codeBadRequestCLI) {
			t.Fatalf("%v: %d %q; want a bad request", args, code, out)
		}
	}

	cfg := gcTestMasterConfig(t)
	cfg.LifecycleStoreDir = t.TempDir() // the reservation store lives here
	h, stop := startDeleteMaster(t, cfg)
	lc := lifecycleClient(t, h.Addr())
	run := func(vol, op string) (int, volumeDeleteOutput) {
		t.Helper()
		code, out, _ := runChunk("volume-delete", "--chunk-filer", "f:1", "--master", h.Addr(), "--request-id", op, vol)
		var got volumeDeleteOutput
		if err := json.Unmarshal([]byte(out), &got); err != nil {
			t.Fatalf("volume-delete %s: %q", vol, out)
		}
		return code, got
	}
	usable := func(vol string) {
		t.Helper()
		if _, err := lc.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: vol, SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
			t.Fatalf("create %s: %v; want usable", vol, err)
		}
	}

	// Held: refused, admission given back.
	if code, got := run("v2", "op-v2"); code == 0 || got.Error == nil || got.Error.Code != chunkstore.CodeVolumeInUse || !got.Admitted || !got.AdmissionAborted || got.Completed {
		t.Fatalf("delete of attached v2: %d %+v", code, got)
	}
	usable("v2")

	// A first invocation's admission landed and its reply was lost (the
	// master holds op-v3); the rerun with the same request id meets the
	// snapshot, gives back and so releases that hold too.
	if _, err := lc.MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: "v3", Token: "op-v3"}); err != nil {
		t.Fatal(err)
	}
	if code, got := run("v3", "op-v3"); code == 0 || got.Error == nil || got.Error.Code != chunkstore.CodeVolumeHasSnapshots || !got.Reverted || !got.AdmissionAborted {
		t.Fatalf("rerun of v3's delete: %d %+v; want volume_has_snapshots, reverted, given back", code, got)
	}
	stop()
	h, stop = startDeleteMaster(t, cfg) // restart: the given-back id stays usable
	defer stop()
	lc = lifecycleClient(t, h.Addr())
	usable("v3")
	if _, err := cat.EnsureVolume(ctx, "v3", geo); err != nil {
		t.Fatalf("attach v3 after the refused delete: %v", err)
	}

	// Completed: committed, never created again.
	code, got := run("v1", "op-v1")
	if code != 0 || got.VolumeID != "v1" || !got.Completed || !got.Committed {
		t.Fatalf("delete v1: %d %+v", code, got)
	}
	if _, err := cat.EnsureVolume(ctx, "v1", geo); chunkstore.CodeOf(err) != chunkstore.CodeVolumeDeleted {
		t.Fatalf("recreate v1: %v; want volume_deleted", err)
	}
	if _, err := lc.CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "v1", SizeBytes: 1 << 20, ReplicationFactor: 1}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("create v1 after its committed delete: %v; want FailedPrecondition", err)
	}
}
