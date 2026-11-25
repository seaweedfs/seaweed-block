package volume

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

func TestChunkControlUsesTheBoundLayoutIdentity(t *testing.T) {
	ctx := context.Background()
	f := &adapterManifestFiler{Filer: chunkstore.NewMemFiler(), blobs: map[string][]byte{}}
	cat, err := chunkstore.NewBCatalog(f, f, "/chunks")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := layout.ParseVolumeID("00112233445566778899aabbccddeeff")
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	h, err := cat.CreateVolume(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	h, _, err = cat.ReadHead(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	r := layout.RootName{Hash: layout.Sum([]byte("root"))}
	h.Snapshots = map[string]layout.Snapshot{"a": {Root: r}, "b": {Root: r}, "c": {Root: r}}
	s, _ := cat.Store(ref)
	if err := s.UpdateHead(ctx, h.Gen, h); err != nil {
		t.Fatal(err)
	}
	ctl := NewChunkControl(&fakeChunkProvider{}, cat, ref)
	list, next, err := ctl.ListSnapshots(ctx, "v1", chunkstore.ListOptions{Limit: 1})
	if err != nil || len(list) != 1 || list[0].Name != "a" || next != "a" {
		t.Fatalf("first page: %+v %q %v", list, next, err)
	}
	if list[0].Root != r.String() {
		t.Fatalf("snapshot root lost: %+v", list[0])
	}
	server := chunkServer(t, ctl, true)
	w := serve(t, server, http.MethodGet, "/chunk/snapshots?volume=v1&limit=1", "", "")
	var body chunkstore.SnapshotListBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != http.StatusOK || len(body.Snapshots) != 1 || body.Snapshots[0].Root != r.String() {
		t.Fatalf("HTTP root: %d %s %v", w.Code, w.Body, err)
	}
	list, next, err = ctl.ListSnapshots(ctx, "v1", chunkstore.ListOptions{StartAfter: next, Limit: 2})
	if err != nil || len(list) != 2 || list[0].Name != "b" || list[1].Name != "c" || next != "" {
		t.Fatalf("next page: %+v %q %v", list, next, err)
	}
	if _, _, err := ctl.ListSnapshots(ctx, "other", chunkstore.ListOptions{}); !errors.Is(err, layout.ErrIdentity) {
		t.Fatalf("wrong name: %v", err)
	}
	if _, err := ctl.DeleteSnapshot(ctx, "v1", "b"); err != nil {
		t.Fatal(err)
	}
	h, found, err := cat.ReadHead(ctx, ref)
	if err != nil || !found || len(h.Snapshots) != 2 {
		t.Fatalf("HEAD after delete: %+v %v", h, err)
	}
	u, err := ctl.Usage(ctx, "v1")
	if err != nil || u.Snapshots != 2 || u.OwnChunks != 0 || u.ForeignChunks != 0 {
		t.Fatalf("usage: %+v %v", u, err)
	}
	missing := NewChunkControl(&fakeChunkProvider{}, cat, chunkstore.VolumeRef{Name: "v1", ID: layout.VolumeID{1}})
	if _, _, err := missing.ListSnapshots(ctx, "v1", chunkstore.ListOptions{}); !errors.Is(err, layout.ErrHeadAbsent) {
		t.Fatalf("another incarnation fell back: %v", err)
	}
	h.Snapshots["a"] = layout.Snapshot{Root: r, Dependents: []layout.Dependent{{Target: "child", TargetID: layout.VolumeID{2}, Root: r}}}
	if err := s.UpdateHead(ctx, h.Gen, h); err != nil {
		t.Fatal(err)
	}
	if _, err := ctl.DeleteSnapshot(ctx, "v1", "a"); !errors.Is(err, layout.ErrHasDependents) {
		t.Fatalf("dependent snapshot deleted: %v", err)
	}
	if _, err := ctl.DeleteSnapshot(ctx, "other", "a"); !errors.Is(err, layout.ErrIdentity) {
		t.Fatalf("wrong-name delete: %v", err)
	}
	u, err = ctl.Usage(ctx, "v1")
	if err != nil || u.Dependents != 1 {
		t.Fatalf("dependents missing: %+v %v", u, err)
	}
}

type unavailableCatalogFiler struct {
	chunkstore.Filer
	err error
}

func (f unavailableCatalogFiler) LookupEntry(context.Context, string, string) (*filerpb.Entry, error) {
	return nil, f.err
}

func TestChunkControlCatalogLookupErrorIsNotAbsence(t *testing.T) {
	ctx := context.Background()
	want := errors.New("filer lookup failed")
	f := &adapterManifestFiler{Filer: chunkstore.NewMemFiler(), blobs: map[string][]byte{}}
	cat, err := chunkstore.NewBCatalog(unavailableCatalogFiler{Filer: f, err: want}, f, "/chunks")
	if err != nil {
		t.Fatal(err)
	}
	ctl := NewChunkControl(&fakeChunkProvider{}, cat, chunkstore.VolumeRef{Name: "v1", ID: layout.VolumeID{1}})
	if _, _, err := ctl.ListSnapshots(ctx, "v1", chunkstore.ListOptions{}); !errors.Is(err, want) {
		t.Fatalf("lookup became empty list: %v", err)
	}
	if _, err := ctl.Usage(ctx, "v1"); !errors.Is(err, want) {
		t.Fatalf("lookup became empty usage: %v", err)
	}
	if _, err := ctl.DeleteSnapshot(ctx, "v1", "s"); !errors.Is(err, want) {
		t.Fatalf("unknown deletion reported success: %v", err)
	}
}
