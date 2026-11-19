package chunkstore

import (
	"context"
	"errors"
	"path"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type rootCASFiler struct {
	Filer
	roots, children int
	refuseChildren  bool
}

func (f *rootCASFiler) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	if name == headName {
		f.roots++
	} else {
		f.children++
		if f.refuseChildren {
			return nil, errors.New("segment service unavailable")
		}
	}
	return f.Filer.LookupEntry(ctx, dir, name)
}

// A HEAD CAS validates root authority, not the availability of every immutable
// segment. Ordinary GetHead still resolves and verifies all segment contents.
func TestManifest_CASReadsOnlyRoot(t *testing.T) {
	f, b, _, _, _ := gcFixture(t)
	_, h := publishSegmented(t, f, b)
	tracked := &rootCASFiler{Filer: f, refuseChildren: true}
	r, err := NewChunkRemote(tracked, b, path.Join(catBase, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	next := h
	next.RemoteLSN++
	gen, err := r.CASHead(context.Background(), storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, next)
	if err != nil {
		t.Fatalf("root-only CAS must not depend on segments: %v", err)
	}
	if gen != h.Gen+1 || tracked.roots != 1 || tracked.children != 0 {
		t.Fatalf("CAS gen=%d root reads=%d child reads=%d", gen, tracked.roots, tracked.children)
	}
	if _, err := r.GetHead(context.Background()); err == nil {
		t.Fatal("GetHead skipped segment resolution")
	}
	tracked.refuseChildren = false
	got, err := r.GetHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Gen != gen || got.RemoteLSN != next.RemoteLSN || !reflect.DeepEqual(got.Chunks, h.Chunks) || !reflect.DeepEqual(got.Segments, h.Segments) {
		t.Fatalf("CAS changed content or lost frontier: %+v", got)
	}
}
