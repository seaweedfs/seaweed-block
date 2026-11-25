package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"path"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// A layout key round-trips, the owner may hold ':', and anything that is
// not exactly a canonical key is ErrMalformed.
func TestLayoutKeyRoundTripAndRefusals(t *testing.T) {
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []string{"v1", "a:b"} {
		ref := layout.Ref{Owner: owner, OwnerID: id, Chunk: layout.NameChunk(12345, []byte("x"))}
		got, err := ParseLayoutKey(LayoutKey(ref))
		if err != nil || got != ref {
			t.Fatalf("round trip of %+v: %+v %v", ref, got, err)
		}
	}
	good := LayoutKey(layout.Ref{Owner: "v1", OwnerID: id, Chunk: layout.NameChunk(7, []byte("y"))})
	name := layout.NameChunk(7, []byte("y")).String()
	for _, key := range []string{
		"",
		"v1:" + id.String() + ":" + name,      // no prefix
		"layout:" + id.String() + ":" + name,  // no owner
		"layout::" + id.String() + ":" + name, // empty owner
		"layout:v1:" + id.String(),            // no chunk name
		"layout:v1:zz:" + name,                // bad id
		"layout:v1:" + strings.ToUpper(id.String()) + ":" + name,              // not canonical
		"layout:v1:" + id.String() + ":07-" + strings.SplitN(name, "-", 2)[1], // non-canonical index
		"layout:a/b:" + id.String() + ":" + name,                              // owner is not a path element
		"layout:..:" + id.String() + ":" + name,                               // owner is not a path element
		"layout:v1:" + layout.VolumeID{}.String() + ":" + name,                // zero owner id
		good + "x",
	} {
		if _, err := ParseLayoutKey(key); !errors.Is(err, layout.ErrMalformed) {
			t.Fatalf("ParseLayoutKey(%q): %v; want ErrMalformed", key, err)
		}
	}
}

// A restore target's manifest names its source's chunk; the key alone reads
// it from the source's pool through ReadRef. A reference to another id of
// the owner is ErrIdentity, a chunk the pool lacks ErrMissing, and bytes
// that do not hash to the name ErrCorrupt.
func TestRestoreTargetReadsItsSourceThroughItsManifest(t *testing.T) {
	ctx := context.Background()
	f := newFakeFiler()
	c, err := NewBCatalog(f, newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := newRef(t, "src"), newRef(t, "t")
	if _, err := c.CreateVolume(ctx, src); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{'s'}, 4096)
	commitSnapshot(t, c, src, data, "daily")
	sh, _, err := c.ReadHead(ctx, src)
	if err != nil {
		t.Fatal(err)
	}
	th, err := c.Restore(ctx, src, "daily", tgt)
	if err != nil {
		t.Fatal(err)
	}
	_, tr, err := c.open(tgt)
	if err != nil {
		t.Fatal(err)
	}
	m, err := LayoutManifest(ctx, tr, th)
	if err != nil || len(m) != 1 {
		t.Fatalf("target manifest: %+v %v; want one chunk", m, err)
	}
	cr := m[1]
	ref, err := ParseLayoutKey(cr.Key)
	if err != nil || ref.Owner != src.Name || ref.OwnerID != src.ID || cr.SHA256 != [32]byte(ref.Chunk.Hash) || cr.Encoding != "full" || cr.BirthLSN != th.Position {
		t.Fatalf("target manifest entry %+v (%+v, %v); want the source's chunk", cr, ref, err)
	}
	got, err := tr.ReadRef(ctx, ref)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("ReadRef: %v; want the source's bytes", err)
	}
	// The source's own manifest names the same reference.
	_, sr, err := c.open(src)
	if err != nil {
		t.Fatal(err)
	}
	if sm, err := LayoutManifest(ctx, sr, sh); err != nil || sm[1].Key != cr.Key || sm[1].BirthLSN != sh.Position {
		t.Fatalf("source manifest %+v %v; want the same key", sm, err)
	}

	other, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.ReadRef(ctx, layout.Ref{Owner: ref.Owner, OwnerID: other, Chunk: ref.Chunk}); !errors.Is(err, layout.ErrIdentity) && !errors.Is(err, layout.ErrMissing) {
		t.Fatalf("ReadRef of another id: %v; want ErrIdentity or ErrMissing (its directory does not exist)", err)
	}
	p := path.Join("/blocks", src.Dir(), ref.Chunk.Path())
	e := f.entries[p]
	f.entries[p] = layoutEntry(path.Base(p), []byte("not the chunk"))
	if _, err := tr.ReadRef(ctx, ref); !errors.Is(err, layout.ErrCorrupt) {
		t.Fatalf("ReadRef of replaced bytes: %v; want ErrCorrupt", err)
	}
	delete(f.entries, p)
	if _, err := tr.ReadRef(ctx, ref); !errors.Is(err, layout.ErrMissing) {
		t.Fatalf("ReadRef of a removed chunk: %v; want ErrMissing", err)
	}
	f.entries[p] = e
}

// A HEAD with no current image is an empty manifest; a root that cannot be
// read is an error, never a partial manifest.
func TestLayoutManifestEmptyAndUnreadable(t *testing.T) {
	ctx := context.Background()
	f := newFakeFiler()
	c, err := NewBCatalog(f, newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	v := newRef(t, "v1")
	h, err := c.CreateVolume(ctx, v)
	if err != nil {
		t.Fatal(err)
	}
	_, r, err := c.open(v)
	if err != nil {
		t.Fatal(err)
	}
	if m, err := LayoutManifest(ctx, r, h); err != nil || len(m) != 0 {
		t.Fatalf("fresh HEAD: %+v %v; want empty", m, err)
	}
	commitSnapshot(t, c, v, []byte("z"), "s")
	h, _, err = c.ReadHead(ctx, v)
	if err != nil {
		t.Fatal(err)
	}
	delete(f.entries, path.Join("/blocks", v.Dir(), h.Current.Path()))
	if m, err := LayoutManifest(ctx, r, h); !errors.Is(err, layout.ErrMissing) || m != nil {
		t.Fatalf("missing root: %+v %v; want ErrMissing and no manifest", m, err)
	}
}
