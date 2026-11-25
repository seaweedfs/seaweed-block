package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"path"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

func layoutStoreFixture(t *testing.T) (*LayoutStore, *fakeFiler, *fakeBlobs) {
	t.Helper()
	f, b := newFakeFiler(), newFakeBlobs()
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewLayoutStore(f, b, "/blocks/v", id)
	if err != nil {
		t.Fatal(err)
	}
	return s, f, b
}

func TestLayoutStoreMissingHEADRefusesStart(t *testing.T) {
	s, f, _ := layoutStoreFixture(t)
	ctx := context.Background()
	if _, found, err := s.ReadHead(ctx); found || err != nil {
		t.Fatal("missing HEAD did not remain absent", found, err)
	}
	if _, err := layout.Start(ctx, s, okClaimLease{}, layout.HolderID{Epoch: 1, Holder: "s/r"}, layout.LocalRoot{ClaimID: "root", Fresh: true}); !errors.Is(err, layout.ErrHeadAbsent) {
		t.Fatal("missing HEAD must not start", err)
	}
	if len(f.written) != 0 {
		t.Fatal("Start recreated a missing HEAD")
	}
}

func TestLayoutStoreObjectsAreFilerFiles(t *testing.T) {
	s, f, b := layoutStoreFixture(t)
	ctx := context.Background()
	raw := bytes.Repeat([]byte("new"), 100)
	n := layout.NameChunk(10000, raw)
	if err := layout.EnsureObject(ctx, s, n.Path(), raw, "writer"); err != nil {
		t.Fatal(err)
	}
	e, err := f.LookupEntry(ctx, "/blocks/v/pool/1", n.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Content) != 0 || len(e.Chunks) != 1 || e.Chunks[0].Offset != 0 || e.Chunks[0].Size != 300 || e.Attributes.FileSize != 300 {
		t.Fatalf("not an ordinary complete filer file: %+v", e)
	}
	got, found, err := s.Fetch(ctx, path.Join(s.root, n.Path()))
	if err != nil || !found || !bytes.Equal(got, raw) {
		t.Fatalf("readback %q %t %v", got, found, err)
	}
	puts, _, _ := b.counts()
	if err := layout.EnsureObject(ctx, s, n.Path(), raw, "writer"); err != nil {
		t.Fatal(err)
	}
	after, _, _ := b.counts()
	if after != puts {
		t.Fatal("same-session object was uploaded again")
	}
	if err := s.Create(ctx, n.Path(), []byte("wrong bytes"), "writer"); !errors.Is(err, layout.ErrCorrupt) {
		t.Fatalf("wrong content name: %v", err)
	}
}

func TestLayoutStoreLateCleanCannotDeleteReusedFile(t *testing.T) {
	s, _, _ := layoutStoreFixture(t)
	ctx := context.Background()
	raw := []byte("keep this image")
	n := layout.NameChunk(4, raw)
	if err := layout.EnsureObject(ctx, s, n.Path(), raw, "snapshot-a"); err != nil {
		t.Fatal(err)
	}
	plan, ok, err := s.Stat(ctx, n.Path())
	if err != nil || !ok {
		t.Fatal(plan, ok, err)
	}
	if err := s.Mark(ctx, n.Path(), plan, "clean-a"); err != nil {
		t.Fatal(err)
	}
	if err := layout.EnsureObject(ctx, s, n.Path(), raw, "snapshot-b"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteMarked(ctx, n.Path(), "clean-a"); !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("late delete: %v", err)
	}
	if err := s.Mark(ctx, n.Path(), plan, "clean-a"); !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("late mark: %v", err)
	}
	meta, _, err := s.Stat(ctx, n.Path())
	if err != nil || meta.Session != "snapshot-b" || meta.Mark != "" {
		t.Fatal(meta, err)
	}
	got, found, err := s.Fetch(ctx, path.Join(s.root, n.Path()))
	if err != nil || !found || !bytes.Equal(got, raw) {
		t.Fatal("reuse lost file data", found, err)
	}
	if err := s.Mark(ctx, n.Path(), meta, "clean-b"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteMarked(ctx, n.Path(), "clean-b"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteMarked(ctx, n.Path(), "clean-b"); err != nil {
		t.Fatal("missing delete must replay", err)
	}
}

func TestLayoutStoreHEADGenerationAndVolumeIdentity(t *testing.T) {
	s, f, _ := layoutStoreFixture(t)
	ctx := context.Background()
	h := layout.Head{VolumeID: s.id, Epoch: 1, Holder: "server/r", ClaimID: "local-root", State: layout.StateLive, Activity: layout.Activity{Kind: layout.ActIdle}}
	if err := s.UpdateHead(ctx, 0, h); err != nil {
		t.Fatal(err)
	}
	read, found, err := s.ReadHead(ctx)
	if err != nil || !found || read.Gen != 1 || read.ClaimID != "local-root" {
		t.Fatal(read, found, err)
	}
	me := layout.HolderID{Epoch: 1, Holder: "server/r", ClaimID: "local-root"}
	if _, err := layout.RunBegin(ctx, s, me, "snap-a"); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateHead(ctx, 1, h); !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("stale HEAD replaced session: %v", err)
	}
	read, _, err = s.ReadHead(ctx)
	if err != nil || read.Gen != 2 || read.Activity.Session != "snap-a" {
		t.Fatal(read, err)
	}
	f.failUpdateAfterApply = func(string) error { return errors.New("reply lost") }
	if _, err := layout.RunAbort(ctx, s, me, "snap-a"); err == nil {
		t.Fatal("unknown write was reported successful")
	}
	f.failUpdateAfterApply = nil
	read, err = layout.RunAbort(ctx, s, me, "snap-a")
	if err != nil || read.Gen != 3 || read.Activity.Kind != layout.ActIdle {
		t.Fatal("lost reply did not settle on replay", read, err)
	}
	id, _ := layout.NewVolumeID()
	other, err := NewLayoutStore(f, s.b, s.root, id)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := other.ReadHead(ctx); !errors.Is(err, layout.ErrIdentity) {
		t.Fatalf("wrong volume accepted HEAD: %v", err)
	}
	h.VolumeID = id
	if err := other.UpdateHead(ctx, 3, h); !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("old generation crossed volume identity: %v", err)
	}
}

func TestLayoutStoreRefusesLegacyHEADAndUnsafePaths(t *testing.T) {
	s, f, _ := layoutStoreFixture(t)
	ctx := context.Background()
	if err := f.CreateEntry(ctx, s.root, &filerpb.Entry{Name: "HEAD", Content: []byte(`{"format":1,"epoch":1}`)}, true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ReadHead(ctx); !errors.Is(err, ErrLegacyStore) {
		t.Fatalf("legacy format needs recreate refusal: %v", err)
	}
	for _, rel := range []string{"../pool/0/x", "/pool/0/x", "keys/old", "intents/old", "pool/01/0-bad", "root/UPPER"} {
		if _, _, err := s.Stat(ctx, rel); !errors.Is(err, layout.ErrMalformed) {
			t.Fatalf("path %q: %v", rel, err)
		}
	}
	if _, _, err := s.Fetch(ctx, "/outside/VOLUME"); err == nil {
		t.Fatal("read escaped the store base")
	}
}

func TestLayoutStoreRecreatedVolumeRefusesOldDelete(t *testing.T) {
	s, f, b := layoutStoreFixture(t)
	ctx := context.Background()
	raw := []byte("same content in a new volume")
	n := layout.NameChunk(1, raw)
	if err := s.Create(ctx, n.Path(), raw, "old"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteOfVolume(ctx, n.Path(), s.id); err != nil {
		t.Fatal(err)
	}
	id, _ := layout.NewVolumeID()
	next, err := NewLayoutStore(f, b, s.root, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Create(ctx, n.Path(), raw, "new"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteOfVolume(ctx, n.Path(), s.id); !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("old volume deleted its replacement: %v", err)
	}
	got, found, err := next.Fetch(ctx, path.Join(next.root, n.Path()))
	if err != nil || !found || !bytes.Equal(got, raw) {
		t.Fatal("new volume image lost", found, err)
	}
}
