package durable_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

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
	if _, err = cat.CreateVolume(ctx, v, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	view := newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}))
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r1",
		ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return cat.Store(v) },
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
	// The cap: snapshots up to it are named; past it the request gets the
	// typed error before its cut opens a session, so HEAD is unchanged (same
	// generation, activity idle, same root and position), with nothing new
	// to publish and with new data waiting.
	for i := len(head().Snapshots); i < layout.MaxSnapshots; i++ {
		if _, err := p.SnapshotVolume(ctx, "v1", "s"+string(rune('a'+i)), layout.SnapshotMeta{CreatedNs: int64(20 + i)}); err != nil {
			t.Fatalf("snapshot %d: %v", i+1, err)
		}
	}
	refusedPastCap := func(what string) {
		t.Helper()
		before := head()
		if _, err := p.SnapshotVolume(ctx, "v1", "over", layout.SnapshotMeta{CreatedNs: 50}); !errors.Is(err, layout.ErrTooManySnapshots) {
			t.Fatalf("%s: snapshot past the cap: %v; want ErrTooManySnapshots", what, err)
		}
		if after := head(); !reflect.DeepEqual(after, before) || after.Activity.Kind != layout.ActIdle {
			t.Fatalf("%s: a refused snapshot changed HEAD: before %+v, after %+v", what, before, after)
		}
	}
	refusedPastCap("nothing new")
	before := head()
	if _, err = ws.Write(1, bytes.Repeat([]byte{'B'}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err = ws.Sync(); err != nil {
		t.Fatal(err)
	}
	refusedPastCap("new data waiting")
	// The waiting data reaches HEAD by the next ordinary cut.
	if _, err := p.CutVolume(ctx, "v1", 0); err != nil {
		t.Fatalf("cut after the refusal: %v", err)
	}
	if after := head(); after.Position <= before.Position || *after.Current == *before.Current || len(after.Snapshots) != layout.MaxSnapshots {
		t.Fatalf("cut after the refusal: position %d -> %d, %d snapshots", before.Position, after.Position, len(after.Snapshots))
	}
	// A replay after later cuts answers with the facts recorded when the
	// name was taken, not HEAD's current position or root.
	again, err := p.SnapshotVolume(ctx, "v1", "daily", layout.SnapshotMeta{CreatedNs: 99})
	if err != nil {
		t.Fatalf("replay after later cuts: %v", err)
	}
	if ri := chunkstore.SnapshotInfoFromHead("v1", "daily", again); ri.CutLSN != info.CutLSN || ri.Root != info.Root || ri.CreatedNs != 11 || ri.CutLSN == again.Position {
		t.Fatalf("replay after later cuts: cut %d root %s created %d; want the recorded cut %d root %s (HEAD now at %d)", ri.CutLSN, ri.Root, ri.CreatedNs, info.CutLSN, info.Root, again.Position)
	}
}

// A snapshot request whose cut fails returns the error and withdraws its
// name: the cut keeps its session and staged uploads for its retry, and
// that later cut must not name the image the requester was told was not
// taken.
func TestLayoutProviderFailedSnapshotNamesNoLaterImage(t *testing.T) {
	ctx := context.Background()
	blobs := &layoutProviderBlobs{blobs: map[string][]byte{}}
	cat, err := chunkstore.NewBCatalog(chunkstore.NewMemFiler(), blobs, "/layout-test")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	v := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err = cat.CreateVolume(ctx, v, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	view := newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}))
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r1",
		ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return cat.Store(v) },
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
	blobs.Lock()
	blobs.fail = errors.New("blob store down")
	blobs.Unlock()
	shortCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if _, err := p.SnapshotVolume(shortCtx, "v1", "weekly", layout.SnapshotMeta{CreatedNs: 7}); err == nil {
		t.Fatal("a snapshot whose cut could not upload succeeded")
	}
	blobs.Lock()
	blobs.fail = nil
	blobs.Unlock()
	if _, err = ws.Write(1, bytes.Repeat([]byte{'B'}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err = ws.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CutVolume(ctx, "v1", 0); err != nil {
		t.Fatalf("the later cut: %v", err)
	}
	h, _, err := cat.ReadHead(ctx, v)
	if err != nil {
		t.Fatal(err)
	}
	if _, named := h.Snapshots["weekly"]; named {
		t.Fatalf("the failed request's name rode onto a later cut: %+v", h.Snapshots)
	}
}
