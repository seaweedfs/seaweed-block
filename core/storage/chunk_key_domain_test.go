package storage

import (
	"context"
	"testing"
)

// The chunk key domain is non-empty valid UTF-8, enforced at Put and at
// PutIntent by every remote, so a key protected by an intent always lists
// back byte-identical to the key stored (review of #110).
func checkKeyDomain(t *testing.T, r chunkRemote) {
	t.Helper()
	ctx := context.Background()
	for _, bad := range []string{"", "chunks/\xff/c0", "a\xc3"} {
		if err := r.Put(ctx, bad, []byte("x")); err == nil {
			t.Errorf("Put(%q) accepted a key outside the domain", bad)
		}
		if _, err := r.PutIntent(ctx, chunkIntent{Gen: 1, Epoch: 1, Holder: "h", Keys: []string{"ok", bad}}); err == nil {
			t.Errorf("PutIntent with key %q accepted a key outside the domain", bad)
		}
	}
	if got, err := r.ListIntents(ctx); err != nil || len(got) != 0 {
		t.Fatalf("a refused PutIntent left intents: %+v %v", got, err)
	}
	keys := []string{"chunks/é/ü-1", "@v1/chunks/e1/c0/x-1", "日本/鍵"}
	if err := r.Put(ctx, keys[0], []byte("y")); err != nil {
		t.Fatalf("Put of a Unicode key: %v", err)
	}
	if _, err := r.PutIntent(ctx, chunkIntent{Gen: 2, Epoch: 1, Holder: "h", Keys: keys}); err != nil {
		t.Fatalf("PutIntent with Unicode and qualified keys: %v", err)
	}
	got, err := r.ListIntents(ctx)
	if err != nil || len(got) != 1 || len(got[0].Keys) != len(keys) {
		t.Fatalf("ListIntents: %+v %v", got, err)
	}
	for i, k := range keys {
		if got[0].Keys[i] != k {
			t.Fatalf("intent key %d listed as %q, stored %q", i, got[0].Keys[i], k)
		}
	}
}

func TestMemChunkRemote_KeyDomain(t *testing.T)  { checkKeyDomain(t, NewMemChunkRemote()) }
func TestFakeChunkRemote_KeyDomain(t *testing.T) { checkKeyDomain(t, newFakeChunkRemote()) }
