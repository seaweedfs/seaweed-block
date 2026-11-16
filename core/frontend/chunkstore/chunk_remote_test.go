package chunkstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const testRoot = "/swblock/chunks/v1"

func newTestRemote(t *testing.T, f *fakeFiler, b *fakeBlobs) *ChunkRemote {
	t.Helper()
	r, err := NewChunkRemote(f, b, testRoot)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func ref(key string, data []byte) storage.ChunkRef {
	return storage.ChunkRef{Key: key, SHA256: sha256.Sum256(data)}
}

func head(epoch uint64, holder string, lsn uint64, chunks map[uint64]storage.ChunkRef) storage.ChunkHead {
	return storage.ChunkHead{Epoch: epoch, Holder: holder, RemoteLSN: lsn, Chunks: chunks}
}

func isPrecondition(err error) bool { return errors.Is(err, storage.ErrChunkHeadPrecondition) }

func TestChunkRemote_RootMustBeCleanAbsolute(t *testing.T) {
	for _, root := range []string{"", "/", "rel/dir", "/a/../b", "/a/"} {
		if _, err := NewChunkRemote(newFakeFiler(), newFakeBlobs(), root); err == nil {
			t.Errorf("root %q accepted", root)
		}
	}
}

func TestChunkRemote_HeadRoundTripAndCAS(t *testing.T) {
	ctx := context.Background()
	f, b := newFakeFiler(), newFakeBlobs()
	r := newTestRemote(t, f, b)

	h0, err := r.GetHead(ctx)
	if err != nil || h0.Gen != 0 || len(h0.Chunks) != 0 {
		t.Fatalf("absent HEAD = %+v, %v; want the zero HEAD", h0, err)
	}
	d := bytes.Repeat([]byte{7}, 64)
	if err := r.Put(ctx, "chunks/e1/c3/n-1", d); err != nil {
		t.Fatal(err)
	}
	want := head(1, "a", 42, map[uint64]storage.ChunkRef{3: ref("chunks/e1/c3/n-1", d)})
	gen, err := r.CASHead(ctx, storage.ChunkHeadVersion{}, want)
	if err != nil || gen != 1 {
		t.Fatalf("first CAS = %d, %v; want gen 1", gen, err)
	}
	got, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Gen != 1 || got.Epoch != 1 || got.Holder != "a" || got.RemoteLSN != 42 || len(got.Chunks) != 1 || got.Chunks[3] != want.Chunks[3] {
		t.Fatalf("HEAD read back = %+v; want gen 1 of %+v", got, want)
	}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{}, want); !isPrecondition(err) {
		t.Fatalf("second first-HEAD CAS: %v; want precondition", err)
	}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: 1, Epoch: 0}, want); !isPrecondition(err) {
		t.Fatalf("CAS at a wrong epoch: %v; want precondition", err)
	}
	gen, err = r.CASHead(ctx, storage.ChunkHeadVersion{Gen: 1, Epoch: 1}, head(1, "a", 50, nil))
	if err != nil || gen != 2 {
		t.Fatalf("CAS at the current version = %d, %v; want gen 2", gen, err)
	}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: 1, Epoch: 1}, head(1, "a", 60, nil)); !isPrecondition(err) {
		t.Fatalf("CAS at a stale generation: %v; want precondition", err)
	}
	got, _ = r.GetHead(ctx)
	if got.Gen != 2 || got.RemoteLSN != 50 || len(got.Chunks) != 0 {
		t.Fatalf("HEAD after refused CAS = %+v; want gen 2 at LSN 50", got)
	}
	for _, e := range f.allWritten() {
		if len(e.GetChunks()) != 0 {
			t.Fatalf("entry %s written with filer chunks", e.Name)
		}
	}
}

// An absent HEAD has epoch 0; an expectation of gen 0 at another epoch can
// never match and must not create HEAD.
func TestChunkRemote_AbsentHeadOnlyMatchesZeroVersion(t *testing.T) {
	ctx := context.Background()
	f := newFakeFiler()
	r := newTestRemote(t, f, newFakeBlobs())
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Epoch: 2}, head(2, "a", 1, nil)); !isPrecondition(err) {
		t.Fatalf("CAS {0,2} on an absent HEAD: %v; want precondition", err)
	}
	if h, _ := r.GetHead(ctx); h.Gen != 0 {
		t.Fatalf("HEAD created by a refused CAS: %+v", h)
	}
}

func TestChunkRemote_EpochNeverLowers(t *testing.T) {
	ctx := context.Background()
	r := newTestRemote(t, newFakeFiler(), newFakeBlobs())
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{}, head(5, "b", 1, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: 1, Epoch: 5}, head(4, "a", 2, nil)); !isPrecondition(err) {
		t.Fatalf("CAS lowering the epoch: %v; want precondition", err)
	}
	if h, _ := r.GetHead(ctx); h.Gen != 1 || h.Epoch != 5 || h.Holder != "b" {
		t.Fatalf("HEAD after refused lowering = %+v", h)
	}
}

// A takeover raises the epoch; the old holder's next CAS, at the version
// it read, is refused (the chunk layer then fences).
func TestChunkRemote_TakeoverRefusesOldHolder(t *testing.T) {
	ctx := context.Background()
	f, b := newFakeFiler(), newFakeBlobs()
	a, nb := newTestRemote(t, f, b), newTestRemote(t, f, b)
	if _, err := a.CASHead(ctx, storage.ChunkHeadVersion{}, head(1, "a", 1, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := nb.CASHead(ctx, storage.ChunkHeadVersion{Gen: 1, Epoch: 1}, head(2, "b", 1, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CASHead(ctx, storage.ChunkHeadVersion{Gen: 1, Epoch: 1}, head(1, "a", 9, nil)); !isPrecondition(err) {
		t.Fatalf("old holder's CAS after takeover: %v; want precondition", err)
	}
}

// Only a definite refusal is a precondition failure. An error whose
// outcome is unknown must come back as something else, whether or not
// the update was applied, so the chunk layer re-reads HEAD.
func TestChunkRemote_UncertainCASIsNotPrecondition(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("transport reset")
	for _, applied := range []bool{false, true} {
		f := newFakeFiler()
		r := newTestRemote(t, f, newFakeBlobs())
		if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{}, head(1, "a", 1, nil)); err != nil {
			t.Fatal(err)
		}
		if applied {
			f.failUpdateAfterApply = func(string) error { return boom }
		} else {
			f.failUpdate = func(string) error { return boom }
		}
		_, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: 1, Epoch: 1}, head(1, "a", 2, nil))
		if err == nil || isPrecondition(err) || !errors.Is(err, boom) {
			t.Fatalf("applied=%v: uncertain CAS returned %v; want the transport error, not a precondition", applied, err)
		}
		f.failUpdate, f.failUpdateAfterApply = nil, nil
		h, err := r.GetHead(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if wantGen := map[bool]uint64{false: 1, true: 2}[applied]; h.Gen != wantGen {
			t.Fatalf("applied=%v: HEAD re-read gen %d; want %d", applied, h.Gen, wantGen)
		}
	}
}

func TestChunkRemote_KeysAreWriteOnce(t *testing.T) {
	ctx := context.Background()
	f, b := newFakeFiler(), newFakeBlobs()
	r := newTestRemote(t, f, b)
	first, second := []byte("first"), []byte("second")
	if err := r.Put(ctx, "chunks/e1/c0/n-1", first); err != nil {
		t.Fatal(err)
	}
	if err := r.Put(ctx, "chunks/e1/c0/n-1", second); !errors.Is(err, storage.ErrChunkKeyExists) {
		t.Fatalf("rebinding a key on the same client: %v; want ErrChunkKeyExists", err)
	}
	if puts, _, _ := b.counts(); puts != 1 { // a known key uploads no orphan needle
		t.Fatalf("refused rebind on the same client stored %d needles; want 1", puts)
	}
	// A second client has no cache: the exclusive create is the authority.
	other := newTestRemote(t, f, b)
	if err := other.Put(ctx, "chunks/e1/c0/n-1", second); !errors.Is(err, storage.ErrChunkKeyExists) {
		t.Fatalf("rebinding a key from another client: %v; want ErrChunkKeyExists", err)
	}
	for _, c := range []*ChunkRemote{r, other, newTestRemote(t, f, b)} {
		got, err := c.Get(ctx, "chunks/e1/c0/n-1")
		if err != nil || !bytes.Equal(got, first) {
			t.Fatalf("Get after refused rebind = %q, %v; want %q", got, err, first)
		}
	}
	if _, err := r.Get(ctx, "chunks/e1/c0/never"); err == nil {
		t.Fatal("Get of an unbound key succeeded")
	}
	if err := r.Delete(ctx, "chunks/e1/c0/n-1"); !errors.Is(err, ErrDeleteUnsupported) {
		t.Fatalf("Delete: %v; want ErrDeleteUnsupported", err)
	}
}

// A HEAD entry that exists but cannot be trusted is an error, never the
// zero HEAD (which would let a new holder start over the volume).
func TestChunkRemote_UntrustedHeadIsAnError(t *testing.T) {
	ctx := context.Background()
	cases := map[string]func(e *filerpb.Entry){
		"filer chunks":     func(e *filerpb.Entry) { e.Chunks = []*filerpb.FileChunk{{FileId: "3,01"}} },
		"wrong kind":       func(e *filerpb.Entry) { e.Extended[extKind] = []byte("head") },
		"gen disagrees":    func(e *filerpb.Entry) { e.Extended[extGen] = []byte("9") },
		"epoch disagrees":  func(e *filerpb.Entry) { e.Extended[extEpoch] = []byte("9") },
		"holder disagrees": func(e *filerpb.Entry) { e.Extended[extHolder] = []byte("z") },
		"not json":         func(e *filerpb.Entry) { e.Content = []byte("{") },
		"no content":       func(e *filerpb.Entry) { e.Content = nil },
		"generation 0": func(e *filerpb.Entry) {
			e.Content = bytes.Replace(e.Content, []byte(`"gen":1`), []byte(`"gen":0`), 1)
			e.Extended[extGen] = []byte("0")
		},
		"bad chunk hash": func(e *filerpb.Entry) {
			e.Content = bytes.Replace(e.Content, []byte(`"sha256":"`), []byte(`"sha256":"zz`), 1)
		},
		"unsupported format": func(e *filerpb.Entry) {
			e.Content = bytes.Replace(e.Content, []byte(`"format":1`), []byte(`"format":2`), 1)
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFakeFiler()
			r := newTestRemote(t, f, newFakeBlobs())
			h := head(1, "a", 1, map[uint64]storage.ChunkRef{0: ref("k", []byte("x"))})
			if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{}, h); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			corrupt(f.entries[testRoot+"/"+headName])
			f.mu.Unlock()
			got, err := r.GetHead(ctx)
			if err == nil {
				t.Fatalf("untrusted HEAD read as %+v", got)
			}
		})
	}
}

// The chunk layer end to end over this remote: a host writes and captures,
// then a fresh host reads every block lazily from HEAD.
func TestChunkRemote_ChunkLayerCaptureThenFreshHost(t *testing.T) {
	ctx := context.Background()
	const blocks, chunkBlocks = 64, 8
	f, b := newFakeFiler(), newFakeBlobs()
	dir := t.TempDir()

	attach := func(name string, fresh bool) (*storage.WALStore, storage.ChunkLayer) {
		p := filepath.Join(dir, name)
		s, err := storage.CreateWALStore(p, blocks, 4096)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Recover(); err != nil {
			t.Fatal(err)
		}
		r := newTestRemote(t, f, b)
		l, err := storage.AttachChunkLayer(s, r, storage.ChunkLayerConfig{
			ChunkBlocks: chunkBlocks, Epoch: 1, Holder: name, FreshFromHead: fresh,
			Bitmap: storage.NewFileChunkBitmapStore(p + ".chunkbitmap"),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s, l
	}

	src, layer := attach("a", true) // a new, empty source volume
	want := map[uint32][]byte{}
	for _, lba := range []uint32{0, 9, 17, 63} {
		want[lba] = bytes.Repeat([]byte{byte(lba + 1)}, 4096)
		if _, err := src.Write(lba, want[lba]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := src.Sync(); err != nil {
		t.Fatal(err)
	}
	h, err := layer.Capture(ctx)
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if layer.RemoteLSN() != h {
		t.Fatalf("RemoteLSN %d after capture at %d", layer.RemoteLSN(), h)
	}
	puts, _, _ := b.counts()
	if puts != 5 { // chunks 0, 1, 2 and 7, plus their manifest segment
		t.Fatalf("capture stored %d needles; want 4 data chunks plus one manifest", puts)
	}
	_ = src.Close()

	dst, _ := attach("b", true)
	zero := make([]byte, 4096)
	for lba := uint32(0); lba < blocks; lba++ {
		got, err := dst.Read(lba)
		if err != nil {
			t.Fatalf("fresh host read %d: %v", lba, err)
		}
		exp, ok := want[lba]
		if !ok {
			exp = zero
		}
		if !bytes.Equal(got, exp) {
			t.Fatalf("fresh host block %d = %x...; want %x...", lba, got[:4], exp[:4])
		}
	}
	if _, gets, _ := b.counts(); gets != 4 { // each published chunk fetched once
		t.Fatalf("fresh host fetched %d needles; want 4", gets)
	}
}
