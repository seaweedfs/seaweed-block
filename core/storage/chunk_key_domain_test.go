package storage

import (
	"context"
	"testing"
)

// The chunk key domain is non-empty valid UTF-8, enforced at Put by every
// remote, so a stored key is byte-identical to the key named (review of
// #110).
func checkKeyDomain(t *testing.T, r chunkRemote) {
	t.Helper()
	ctx := context.Background()
	for _, bad := range []string{"", "chunks/\xff/c0", "a\xc3"} {
		if err := r.Put(ctx, bad, []byte("x")); err == nil {
			t.Errorf("Put(%q) accepted a key outside the domain", bad)
		}
	}
	for i, k := range []string{"chunks/é/ü-1", "@v1/chunks/e1/c0/x-1", "日本/鍵"} {
		data := []byte{byte(i), 'y'}
		if err := r.Put(ctx, k, data); err != nil {
			t.Fatalf("Put of a Unicode or qualified key %q: %v", k, err)
		}
		if got, err := r.Get(ctx, k); err != nil || string(got) != string(data) {
			t.Fatalf("key %q read back as %q (%v)", k, got, err)
		}
	}
}

func TestMemChunkRemote_KeyDomain(t *testing.T)  { checkKeyDomain(t, NewMemChunkRemote()) }
func TestFakeChunkRemote_KeyDomain(t *testing.T) { checkKeyDomain(t, newFakeChunkRemote()) }
