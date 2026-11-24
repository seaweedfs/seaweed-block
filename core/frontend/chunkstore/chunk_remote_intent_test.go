package chunkstore

import (
	"context"
	"path"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Intents live in <root>/intents/<gen>-<nonce>, carry no filer chunks, list
// back with what was written, and DeleteIntent removes only its own id.
func TestChunkRemote_IntentsRoundTripAndNonce(t *testing.T) {
	f, b := newFakeFiler(), newFakeBlobs()
	r, err := NewChunkRemote(f, b, "/swblock/chunks/v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	a, err := r.PutIntent(ctx, storage.ChunkIntent{Gen: 3, Epoch: 1, Holder: "h1", Keys: []string{"chunks/e1/c0/x-1", "@v0/chunks/e1/c1/y-2"}})
	if err != nil {
		t.Fatal(err)
	}
	c, err := r.PutIntent(ctx, storage.ChunkIntent{Gen: 3, Epoch: 1, Holder: "h1", Keys: []string{"chunks/e1/c2/z-3"}})
	if err != nil {
		t.Fatal(err)
	}
	if a == c || !strings.HasPrefix(a, "3-") || !strings.HasPrefix(c, "3-") {
		t.Fatalf("ids %q %q: want distinct \"3-<nonce>\"", a, c)
	}
	if _, err := f.LookupEntry(ctx, "/swblock/chunks/v1/intents", a); err != nil {
		t.Fatalf("intent %q not at <root>/intents/: %v", a, err)
	}
	assertNoFilerChunks(t, f)
	got, err := r.ListIntents(ctx)
	if err != nil || len(got) != 2 {
		t.Fatalf("ListIntents: %+v %v", got, err)
	}
	for _, in := range got {
		if in.ID == a && (in.Gen != 3 || in.Epoch != 1 || in.Holder != "h1" || len(in.Keys) != 2 || in.Keys[1] != "@v0/chunks/e1/c1/y-2" || in.CreatedNs == 0) {
			t.Fatalf("intent %q listed as %+v", a, in)
		}
	}
	if err := r.DeleteIntent(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := r.DeleteIntent(ctx, a); err != nil {
		t.Fatalf("deleting a missing intent: %v", err)
	}
	got, _ = r.ListIntents(ctx)
	if len(got) != 1 || got[0].ID != c {
		t.Fatalf("after DeleteIntent(%q): %+v", a, got)
	}
	if err := r.DeleteIntent(ctx, "../HEAD"); err == nil {
		t.Fatal("DeleteIntent accepted an id that is not \"<gen>-<nonce>\"")
	}
	if _, err := f.LookupEntry(ctx, "/swblock/chunks/v1", "HEAD"); err == nil {
		t.Fatal("unexpected HEAD")
	}
}

// A malformed intent entry fails the listing: GC must never miss a root.
func TestChunkRemote_ListIntentsRefusesMalformed(t *testing.T) {
	f, b := newFakeFiler(), newFakeBlobs()
	r, _ := NewChunkRemote(f, b, "/swblock/chunks/v1")
	ctx := context.Background()
	if _, err := r.PutIntent(ctx, storage.ChunkIntent{Gen: 1, Epoch: 1, Holder: "h", Keys: []string{"k"}}); err != nil {
		t.Fatal(err)
	}
	got, _ := r.ListIntents(ctx)
	f.mu.Lock()
	f.entries[path.Join("/swblock/chunks/v1/intents", got[0].ID)].Content = []byte("{not json")
	f.mu.Unlock()
	if _, err := r.ListIntents(ctx); err == nil {
		t.Fatal("a malformed intent listed without error")
	}
}
