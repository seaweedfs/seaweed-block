package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ReclaimKey is GC's step for one own key: the binding is tombstoned and
// kept, the needle deleted; repeating it is a no-op; a qualified key is
// refused.
func TestChunkRemote_ReclaimKey(t *testing.T) {
	f, b := newFakeFiler(), newFakeBlobs()
	r, err := NewChunkRemote(f, b, catBase+"/v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := r.Put(ctx, "k1", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	fid := r.fids["k1"]
	if err := r.ReclaimKey(ctx, "k1", gcBlobs{b}); err != nil {
		t.Fatal(err)
	}
	e, err := f.LookupEntry(ctx, r.keyDir(), "k1")
	if err != nil {
		t.Fatalf("binding removed: %v", err)
	}
	if string(e.Extended[extGCDeleted]) != "done" || string(e.Extended[extFid]) != fid {
		t.Fatalf("binding after reclaim: %q fid %q; want done, same fid", e.Extended[extGCDeleted], e.Extended[extFid])
	}
	if _, err := b.Get(ctx, fid); err == nil {
		t.Fatal("needle still readable after reclaim")
	}
	if err := r.ReclaimKey(ctx, "k1", gcBlobs{b}); err != nil {
		t.Fatalf("repeated reclaim: %v", err)
	}
	if err := r.Put(ctx, "k1", []byte("again")); !errors.Is(err, storage.ErrChunkKeyExists) {
		t.Fatalf("rebinding a reclaimed key: %v", err)
	}
	if err := r.ReclaimKey(ctx, "@v0/k1", gcBlobs{b}); err == nil {
		t.Fatal("reclaim of an owner-qualified key accepted")
	}
}
