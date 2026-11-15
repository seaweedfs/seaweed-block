package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The SeaweedFS remote enforces the chunk key domain (non-empty valid UTF-8)
// at Put and PutIntent before anything is written, and keys inside the
// domain, Unicode and owner-qualified ones included, round-trip exactly
// through the key bindings and the intent documents.
func TestChunkRemote_KeyDomainEnforcedBeforePersistence(t *testing.T) {
	f, b := newFakeFiler(), newFakeBlobs()
	r, err := NewChunkRemote(f, b, "/swblock/chunks/v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, bad := range []string{"chunks/\xff/c0", "a\xc3"} {
		if err := r.Put(ctx, bad, []byte("x")); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Put(%q): %v, want ErrInvalidKey", bad, err)
		}
		if _, err := r.PutIntent(ctx, storage.ChunkIntent{Gen: 1, Epoch: 1, Holder: "h", Keys: []string{"ok", bad}}); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("PutIntent with %q: %v, want ErrInvalidKey", bad, err)
		}
	}
	if puts, _, _ := b.counts(); puts != 0 || len(f.allWritten()) != 0 {
		t.Fatalf("refused calls wrote %d needles and %d entries", puts, len(f.allWritten()))
	}

	data := bytes.Repeat([]byte{0x33}, 4096)
	if err := r.Put(ctx, "chunks/é/ü-1", data); err != nil {
		t.Fatalf("Put of a Unicode key: %v", err)
	}
	fresh, _ := NewChunkRemote(f, b, "/swblock/chunks/v1") // no cached binding
	if got, err := fresh.Get(ctx, "chunks/é/ü-1"); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Get of a Unicode key through its stored binding: %v", err)
	}
	keys := []string{"chunks/é/ü-1", "@v0/chunks/e1/c0/x-1", "日本/鍵"}
	if _, err := r.PutIntent(ctx, storage.ChunkIntent{Gen: 2, Epoch: 1, Holder: "h", Keys: keys}); err != nil {
		t.Fatal(err)
	}
	got, err := fresh.ListIntents(ctx)
	if err != nil || len(got) != 1 {
		t.Fatalf("ListIntents: %+v %v", got, err)
	}
	for i, k := range keys {
		if got[0].Keys[i] != k {
			t.Fatalf("intent key %d listed as %q, stored %q", i, got[0].Keys[i], k)
		}
	}
}
