package chunkstore

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// INV-WS-LOCAL-ROOT-IDENTITY-001: HEAD's HolderRoot (and its detached proof's)
// survives the S3 document; a pre-release document without it reads as an
// empty root; a detached proof naming another root of the same holder is
// refused.
func TestHeadDoc_HolderRootRoundTrip(t *testing.T) {
	f, b := newFakeFiler(), newFakeBlobs()
	r, _ := NewChunkRemote(f, b, catBase+"/v1")
	ctx := context.Background()
	gen, err := r.CASHead(ctx, storage.ChunkHeadVersion{}, storage.ChunkHead{Epoch: 1, Holder: "s1/r1", HolderRoot: "aaaa", HolderRootGen: 1, RemoteLSN: 5, Chunks: map[uint64]storage.ChunkRef{}, State: storage.ChunkHeadActive})
	if err != nil {
		t.Fatal(err)
	}
	if h, err := r.GetHead(ctx); err != nil || h.HolderRoot != "aaaa" {
		t.Fatalf("active HEAD read back as %+v %v, want holder root aaaa", h, err)
	}
	det := storage.ChunkHead{Epoch: 1, Holder: "s1/r1", HolderRoot: "aaaa", HolderRootGen: gen + 1, RemoteLSN: 5, Chunks: map[uint64]storage.ChunkRef{}, State: storage.ChunkHeadDetached,
		DetachedBy: &storage.ChunkDetachedBy{Holder: "s1/r1", HolderRoot: "aaaa", Epoch: 1, Gen: gen + 1}}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: gen, Epoch: 1}, det); err != nil {
		t.Fatal(err)
	}
	h, err := r.GetHead(ctx)
	if err != nil || h.DetachedBy == nil || h.DetachedBy.HolderRoot != "aaaa" || h.HolderRoot != "aaaa" {
		t.Fatalf("detached HEAD read back as %+v %v", h, err)
	}

	e, _ := f.LookupEntry(ctx, catBase+"/v1", "HEAD")
	if !strings.Contains(string(e.Content), `"holder_root":"aaaa"`) {
		t.Fatalf("HEAD document without holder_root: %s", e.Content)
	}
	var doc map[string]any
	_ = json.Unmarshal(e.Content, &doc)
	set := func(d map[string]any) {
		body, _ := json.Marshal(d)
		f.mu.Lock()
		f.entries[catBase+"/v1/HEAD"].Content = body
		f.mu.Unlock()
	}
	// A detached proof naming another root of the same holder is refused.
	doc["detachedBy"].(map[string]any)["holder_root"] = "bbbb"
	set(doc)
	if _, err := r.GetHead(ctx); err == nil {
		t.Fatal("a detached HEAD whose proof names another local root was accepted")
	}
	// The format number goes with the root (the leads' ruling on #428): an
	// old format number carrying root fields is refused, and so is format 4
	// without its root; a pre-release document (format 1, no root anywhere)
	// reads as an empty root.
	if doc["format"] != float64(headFormatRooted) {
		t.Fatalf("a rooted HEAD was written as format %v, want %d", doc["format"], headFormatRooted)
	}
	doc["detachedBy"].(map[string]any)["holder_root"] = "aaaa"
	doc["format"] = float64(headFormat)
	set(doc)
	if _, err := r.GetHead(ctx); err == nil {
		t.Fatal("a format 1 HEAD carrying root fields was accepted")
	}
	doc["format"] = float64(headFormatRooted)
	delete(doc, "holder_root")
	set(doc)
	if _, err := r.GetHead(ctx); err == nil {
		t.Fatal("a format 4 HEAD without its local root was accepted")
	}
	doc["format"] = float64(headFormat)
	delete(doc, "holder_root_gen")
	delete(doc["detachedBy"].(map[string]any), "holder_root")
	set(doc)
	if h, err := r.GetHead(ctx); err != nil || h.HolderRoot != "" || h.DetachedBy == nil || h.DetachedBy.HolderRoot != "" {
		t.Fatalf("a pre-release HEAD: %+v %v, want empty roots", h, err)
	}
}
