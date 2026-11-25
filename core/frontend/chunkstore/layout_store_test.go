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
	if _, err := layout.Start(ctx, s, okClaimLease{}, layout.HolderID{Epoch: 1, Holder: "s/r"}, layout.LocalRoot{Geometry: layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "root", Fresh: true, CanStartFrom: anyStart}); !errors.Is(err, layout.ErrHeadAbsent) {
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

func TestLayoutStoreListsOnePoolPageAtATime(t *testing.T) {
	s, _, _ := layoutStoreFixture(t)
	ctx := context.Background()
	var want []layout.ChunkName
	for _, idx := range []uint64{2, 3, 4, 10000} {
		n := layout.NameChunk(idx, []byte("bytes"))
		if err := s.Create(ctx, n.Path(), []byte("bytes"), "writer"); err != nil {
			t.Fatal(err)
		}
		if layout.Shard(idx) == 0 {
			want = append(want, n)
		}
	}
	after := ""
	for _, n := range want {
		got, err := s.ListChunkCandidates(ctx, 0, after, 1)
		if err != nil || len(got) != 1 || got[0] != n {
			t.Fatalf("page after %q: %v %v; want %v", after, got, err, n)
		}
		after = n.String()
	}
	if got, err := s.ListChunkCandidates(ctx, 0, after, 1); err != nil || len(got) != 0 {
		t.Fatal("page crossed into the next shard", got, err)
	}
	if _, err := s.ListChunkCandidates(ctx, 0, "", 0); !errors.Is(err, layout.ErrMalformed) {
		t.Fatal("unbounded page accepted", err)
	}
	for _, cursor := range []string{"../HEAD", layout.NameChunk(10000, []byte("bytes")).String()} {
		if _, err := s.ListChunkCandidates(ctx, 0, cursor, 1); !errors.Is(err, layout.ErrMalformed) {
			t.Fatalf("unsafe cursor %q: %v", cursor, err)
		}
	}
}

type layoutListFiler struct {
	Filer
	entries []*filerpb.Entry
	err     error
}

func (f layoutListFiler) ListEntries(context.Context, string, string, string, uint32) ([]*filerpb.Entry, error) {
	return f.entries, f.err
}

func TestLayoutStoreListErrorsAreNotAnEmptyPool(t *testing.T) {
	s, f, _ := layoutStoreFixture(t)
	ctx := context.Background()
	n := layout.NameChunk(1, []byte("bytes"))
	unreachable := errors.New("filer unreachable")
	for _, tc := range []struct {
		name    string
		entries []*filerpb.Entry
		err     error
		want    error
	}{
		{"transport", nil, unreachable, unreachable},
		{"wrong shard", []*filerpb.Entry{{Name: layout.NameChunk(10000, nil).String()}}, nil, layout.ErrCorrupt},
		{"directory", []*filerpb.Entry{{Name: n.String(), IsDirectory: true}}, nil, layout.ErrCorrupt},
		{"noncanonical", []*filerpb.Entry{{Name: "garbage"}}, nil, layout.ErrCorrupt},
		{"nil entry", []*filerpb.Entry{nil}, nil, layout.ErrCorrupt},
		{"repeated cursor", []*filerpb.Entry{{Name: n.String()}}, nil, layout.ErrCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.f = layoutListFiler{Filer: f, entries: tc.entries, err: tc.err}
			after := ""
			if tc.name == "repeated cursor" {
				after = n.String()
			}
			got, err := s.ListChunkCandidates(ctx, 0, after, 10)
			if !errors.Is(err, tc.want) || len(got) != 0 {
				t.Fatalf("got %v %v; want no candidates and %v", got, err, tc.want)
			}
		})
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

func TestLayoutStoreDeleteNeverRemovesControlRecords(t *testing.T) {
	s, f, _ := layoutStoreFixture(t)
	ctx := context.Background()
	for _, name := range []string{"HEAD", "VOLUME"} {
		if err := f.CreateEntry(ctx, s.root, &filerpb.Entry{Name: name, Content: []byte("keep")}, true); err != nil {
			t.Fatal(err)
		}
	}
	for _, rel := range []string{"HEAD", "VOLUME", ".", "pool", "pool/0", "seg", "root", "../other/HEAD"} {
		if err := s.DeleteMarked(ctx, rel, "clean"); !errors.Is(err, layout.ErrMalformed) {
			t.Fatalf("marked deletion of %q: %v", rel, err)
		}
		if err := s.DeleteOfVolume(ctx, rel, s.id); !errors.Is(err, layout.ErrMalformed) {
			t.Fatalf("volume deletion of %q: %v", rel, err)
		}
	}
	for _, name := range []string{"HEAD", "VOLUME"} {
		got, err := f.LookupEntry(ctx, s.root, name)
		if err != nil || string(got.Content) != "keep" {
			t.Fatal("control record changed", name, err)
		}
	}
}

// The first remover dies after Mark. The next cleaning must replace that
// mark, and the dead remover's delayed delete must not match the replacement.
type abandonedMarkStore struct {
	*LayoutStore
	t                *testing.T
	oldDeleteRefused bool
}

func (s *abandonedMarkStore) DeleteMarked(ctx context.Context, rel, mark string) error {
	if err := s.LayoutStore.DeleteMarked(ctx, rel, "dead-clean"); !errors.Is(err, layout.ErrPrecondition) {
		s.t.Fatalf("old delete after re-mark: %v", err)
	}
	s.oldDeleteRefused = true
	return s.LayoutStore.DeleteMarked(ctx, rel, mark)
}

func TestLayoutStoreCleaningReclaimsAnAbandonedMark(t *testing.T) {
	s, _, _ := layoutStoreFixture(t)
	ctx := context.Background()
	if _, err := layout.CreateVolume(ctx, s, layout.Volume{ID: s.id, Name: "v", Geometry: layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, Format: layout.VolumeFormat}); err != nil {
		t.Fatal(err)
	}
	h, _, err := s.ReadHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h.Epoch, h.Holder, h.ClaimID = 1, "server/r", "root"
	h.Activity = layout.Activity{Kind: layout.ActClean, Session: "next-clean"}
	if err := s.UpdateHead(ctx, h.Gen, h); err != nil {
		t.Fatal(err)
	}
	raw := []byte("unreachable")
	n := layout.NameChunk(4, raw)
	if err := s.Create(ctx, n.Path(), raw, "writer"); err != nil {
		t.Fatal(err)
	}
	meta, _, err := s.Stat(ctx, n.Path())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Mark(ctx, n.Path(), meta, "dead-clean"); err != nil {
		t.Fatal(err)
	}
	r, err := layout.NewReader("/blocks", "v", s.Fetch)
	if err != nil {
		t.Fatal(err)
	}
	os := &abandonedMarkStore{LayoutStore: s, t: t}
	removed, err := layout.RemoveUnreachable(ctx, s, r, os, layout.HolderID{Epoch: 1, Holder: "server/r", ClaimID: "root"}, "next-clean", 0, []layout.ChunkName{n})
	if err != nil || len(removed) != 1 || removed[0] != n || !os.oldDeleteRefused {
		t.Fatalf("next cleaning: removed=%v old_delete_refused=%v err=%v", removed, os.oldDeleteRefused, err)
	}
	if _, found, err := s.Stat(ctx, n.Path()); err != nil || found {
		t.Fatalf("orphan retained: %v %v", found, err)
	}
}

type layoutCleanupBlobs struct {
	*fakeBlobs
	deleted   []string
	deleteErr error
}

func (b *layoutCleanupBlobs) DeleteBlob(_ context.Context, fid string) error {
	b.deleted = append(b.deleted, fid)
	if b.deleteErr != nil {
		return b.deleteErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.data, fid)
	return nil
}

func TestLayoutStoreCreateExistsReclaimsOnlyItsUnusedUpload(t *testing.T) {
	for _, failedCleanup := range []bool{false, true} {
		t.Run(map[bool]string{false: "reclaimed", true: "cleanup unavailable"}[failedCleanup], func(t *testing.T) {
			s, _, base := layoutStoreFixture(t)
			b := &layoutCleanupBlobs{fakeBlobs: base}
			s.b = b
			ctx := context.Background()
			raw := []byte("referenced bytes")
			n := layout.NameChunk(3, raw)
			if err := s.Create(ctx, n.Path(), raw, "winner"); err != nil {
				t.Fatal(err)
			}
			if failedCleanup {
				b.deleteErr = errors.New("volume server unavailable")
			}
			if err := s.Create(ctx, n.Path(), raw, "loser"); !errors.Is(err, layout.ErrExists) {
				t.Fatal(err)
			}
			if len(b.deleted) != 1 {
				t.Fatalf("unused upload cleanup calls: %v", b.deleted)
			}
			_, _, stored := b.counts()
			want := 1
			if failedCleanup {
				want = 2
			}
			if stored != want {
				t.Fatalf("stored blobs=%d, want %d", stored, want)
			}
			got, found, err := s.Fetch(ctx, path.Join(s.root, n.Path()))
			if err != nil || !found || !bytes.Equal(got, raw) {
				t.Fatalf("winner lost: %q %v %v", got, found, err)
			}
		})
	}
}

func TestLayoutStoreCreateUnknownOutcomeKeepsItsUpload(t *testing.T) {
	s, f, base := layoutStoreFixture(t)
	b := &layoutCleanupBlobs{fakeBlobs: base}
	s.b = b
	unknown := errors.New("create reply lost")
	f.failCreateAfterApply = func(string) error { return unknown }
	raw := []byte("applied bytes")
	n := layout.NameChunk(3, raw)
	ctx := context.Background()
	if err := s.Create(ctx, n.Path(), raw, "writer"); !errors.Is(err, unknown) {
		t.Fatal(err)
	}
	if len(b.deleted) != 0 {
		t.Fatalf("unknown upload deleted: %v", b.deleted)
	}
	got, found, err := s.Fetch(ctx, path.Join(s.root, n.Path()))
	if err != nil || !found || !bytes.Equal(got, raw) {
		t.Fatalf("applied data lost: %q %v %v", got, found, err)
	}
}
