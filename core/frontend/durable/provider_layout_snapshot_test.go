package durable_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// SnapshotVolume through the attached layout runtime: a named snapshot of
// written data is in HEAD with its facts; a replay returns those facts; a
// snapshot of an image with nothing new to publish is still named (its own
// commit); a name the kernel refuses is the typed refusal.
func TestLayoutProviderSnapshotVolume(t *testing.T) {
	ctx := context.Background()
	cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), &layoutProviderBlobs{blobs: map[string][]byte{}}, "/layout-test")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	v := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err = cat.CreateVolume(ctx, v); err != nil {
		t.Fatal(err)
	}
	view := newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}))
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r1",
		ChunkLayoutHeadFor: func(string) (layout.HeadStore, error) { return cat.Store(v) },
		ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			return chunkstore.NewLayoutRemote(cat, v, me)
		},
	}, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	ws := s.(*storage.WALStore)
	if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err = ws.Write(0, bytes.Repeat([]byte{'A'}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err = ws.Sync(); err != nil {
		t.Fatal(err)
	}
	head := func() layout.Head {
		h, _, err := cat.ReadHead(ctx, v)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	meta := layout.SnapshotMeta{CreatedNs: 11, Class: layout.ClassSemantic, Labels: map[string]string{"k": "v"}}
	hd, err := p.SnapshotVolume(ctx, "v1", "daily", meta)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	info := chunkstore.SnapshotInfoFromHead("v1", "daily", hd)
	h := head()
	sn, ok := h.Snapshots["daily"]
	if !ok || sn.Meta.CreatedNs != 11 || sn.Meta.Labels["k"] != "v" || info.Name != "daily" || info.CreatedNs != 11 || info.CutLSN != h.Position || info.Root != sn.Root.String() {
		t.Fatalf("snapshot daily: info %+v, HEAD %+v", info, sn)
	}
	if again, err := p.SnapshotVolume(ctx, "v1", "daily", layout.SnapshotMeta{CreatedNs: 99}); err != nil || again.Snapshots["daily"].Meta.CreatedNs != 11 {
		t.Fatalf("replay: %+v %v; want the recorded facts", again, err)
	}
	// Nothing new written: the name still lands on the current image.
	if _, err := p.SnapshotVolume(ctx, "v1", "same", layout.SnapshotMeta{CreatedNs: 12}); err != nil {
		t.Fatalf("snapshot of an unchanged image: %v", err)
	}
	h2 := head()
	if s2, ok := h2.Snapshots["same"]; !ok || s2.Root != h2.Snapshots["daily"].Root || h2.Activity.Kind != layout.ActIdle {
		t.Fatalf("unchanged image snapshot: %+v", h2)
	}
	if _, err := p.SnapshotVolume(ctx, "v1", strings.Repeat("n", layout.MaxSnapshotNameBytes+1), layout.SnapshotMeta{}); !errors.Is(err, layout.ErrMalformed) {
		t.Fatalf("too-long name: %v; want ErrMalformed", err)
	}
	// The cap: snapshots up to it are named; past it, the cut still commits
	// (new data reaches HEAD, unnamed) and the request gets the typed error.
	for i := len(head().Snapshots); i < layout.MaxSnapshots; i++ {
		if _, err := p.SnapshotVolume(ctx, "v1", "s"+string(rune('a'+i)), layout.SnapshotMeta{CreatedNs: int64(20 + i)}); err != nil {
			t.Fatalf("snapshot %d: %v", i+1, err)
		}
	}
	before := head()
	if _, err = ws.Write(1, bytes.Repeat([]byte{'B'}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err = ws.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.SnapshotVolume(ctx, "v1", "over", layout.SnapshotMeta{CreatedNs: 50}); !errors.Is(err, layout.ErrTooManySnapshots) {
		t.Fatalf("snapshot past the cap: %v; want ErrTooManySnapshots", err)
	}
	after := head()
	if _, named := after.Snapshots["over"]; named || after.Position <= before.Position || *after.Current == *before.Current {
		t.Fatalf("past the cap: named %t, position %d -> %d; want the cut committed unnamed", named, before.Position, after.Position)
	}
}
