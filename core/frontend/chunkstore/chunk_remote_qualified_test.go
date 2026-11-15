package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func qualifiedRemotes(t *testing.T) (*fakeFiler, *fakeBlobs, *ChunkRemote, *ChunkRemote) {
	t.Helper()
	f, b := newFakeFiler(), newFakeBlobs()
	src, err := NewChunkRemote(f, b, "/swblock/chunks/v1")
	if err != nil {
		t.Fatal(err)
	}
	dst, err := NewChunkRemote(f, b, "/swblock/chunks/v2")
	if err != nil {
		t.Fatal(err)
	}
	return f, b, src, dst
}

// A fork's HEAD names its parent's chunks as "@<owner>/<key>"; Get must find
// the binding in the owner volume's keys/ directory.
func TestChunkRemote_GetResolvesQualifiedKey(t *testing.T) {
	_, _, src, dst := qualifiedRemotes(t)
	ctx := context.Background()
	data := bytes.Repeat([]byte{0x5a}, 4096)
	if err := src.Put(ctx, "chunks/e1/c0/n-1", data); err != nil {
		t.Fatal(err)
	}
	got, err := dst.Get(ctx, "@v1/chunks/e1/c0/n-1")
	if err != nil {
		t.Fatalf("Get of a key owned by v1 from v2: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("qualified Get returned other bytes")
	}
	// The plain key in v2's own directory does not exist.
	if _, err := dst.Get(ctx, "chunks/e1/c0/n-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unqualified Get of v1's key from v2: %v, want ErrNotFound", err)
	}
}

// Writes only ever go to the volume's own directory: a qualified key is
// refused before any needle or binding is written.
func TestChunkRemote_PutRefusesQualifiedKey(t *testing.T) {
	f, b, _, dst := qualifiedRemotes(t)
	err := dst.Put(context.Background(), "@v1/chunks/e1/c0/n-1", []byte("x"))
	if !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Put of a qualified key: %v, want ErrInvalidKey", err)
	}
	if puts, _, _ := b.counts(); puts != 0 {
		t.Fatalf("a refused Put stored %d needles", puts)
	}
	if n := len(f.allWritten()); n != 0 {
		t.Fatalf("a refused Put wrote %d filer entries", n)
	}
}

// The owner of a qualified key is one plain path element, so a key can never
// reach outside the chunk base or into the volume's own HEAD.
func TestChunkRemote_QualifiedKeyOwnerIsValidated(t *testing.T) {
	_, _, _, dst := qualifiedRemotes(t)
	for _, key := range []string{"@", "@v1", "@v1/", "@/k", "@../v1/k", "@./k", `@v\1/k`} {
		if _, err := dst.Get(context.Background(), key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Get(%q): %v, want ErrInvalidKey", key, err)
		}
	}
}
