package volume

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// A snapshot deleted through the holder's control route asks the volume's
// maintenance loop for a pass (its cleaning) at once; a refused delete asks
// for nothing.
func TestChunkControlSnapshotDeleteKicksTheCleaning(t *testing.T) {
	ctx := context.Background()
	f := &adapterManifestFiler{Filer: chunkstore.NewMemFiler(), blobs: map[string][]byte{}}
	cat, err := chunkstore.NewBCatalog(f, f, "/chunks")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := layout.ParseVolumeID("00112233445566778899aabbccddeeff")
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err := cat.CreateVolume(ctx, ref, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	h, _, err := cat.ReadHead(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	r := layout.RootName{Hash: layout.Sum([]byte("root"))}
	h.Snapshots = map[string]layout.Snapshot{"a": {Root: r}, "held": {Root: r, Dependents: []layout.Dependent{{Target: "t", TargetID: layout.VolumeID{9}, Root: r}}}}
	s, _ := cat.Store(ref)
	if err := s.UpdateHead(ctx, h.Gen, h); err != nil {
		t.Fatal(err)
	}
	p := &fakeChunkProvider{}
	ctl := NewChunkControl(p, cat, ref)
	if _, err := ctl.DeleteSnapshot(ctx, "v1", "held"); !errors.Is(err, layout.ErrHasDependents) {
		t.Fatalf("delete of a snapshot with a dependent: %v; want ErrHasDependents", err)
	}
	if _, err := ctl.DeleteSnapshot(ctx, "other", "a"); !errors.Is(err, layout.ErrIdentity) {
		t.Fatalf("delete on another volume: %v; want ErrIdentity", err)
	}
	if len(p.kicks) != 0 {
		t.Fatalf("refused deletes asked for maintenance: %v", p.kicks)
	}
	if _, err := ctl.DeleteSnapshot(ctx, "v1", "a"); err != nil {
		t.Fatal(err)
	}
	if len(p.kicks) != 1 || p.kicks[0] != "v1" {
		t.Fatalf("kicks after a snapshot delete: %v; want one for v1", p.kicks)
	}
}
