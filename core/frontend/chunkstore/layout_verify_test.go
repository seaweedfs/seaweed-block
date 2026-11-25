package chunkstore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"google.golang.org/protobuf/proto"
)

type filerBlobs struct {
	*fakeFiler
	*fakeBlobs
}

func putFile(t *testing.T, f *fakeFiler, p string, b []byte) {
	t.Helper()
	if err := f.CreateEntry(context.Background(), path.Dir(p), &filerpb.Entry{Name: path.Base(p), Content: b}, true); err != nil {
		t.Fatal(err)
	}
}

func putDir(t *testing.T, f *fakeFiler, p string) {
	t.Helper()
	if err := f.CreateEntry(context.Background(), path.Dir(p), &filerpb.Entry{Name: path.Base(p), IsDirectory: true}, false); err != nil {
		t.Fatal(err)
	}
}

// a layout volume "v" under /blocks: VOLUME, HEAD, one root naming chunks 0 and 5
func verifyFixtureVolume(t *testing.T) (filerBlobs, layout.VolumeID) {
	f, b := newFakeFiler(), newFakeBlobs()
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	vb, err := layout.Volume{ID: id, Name: "v", Geometry: layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, Format: layout.VolumeFormat}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	putFile(t, f, "/blocks/v/"+layout.VolumeName, vb)
	manifest := map[uint64]layout.Ref{}
	putDir(t, f, "/blocks/v/pool/0")
	for _, i := range []uint64{0, 5} {
		d := []byte(strings.Repeat(string(rune('a'+i)), 64))
		c := layout.NameChunk(i, d)
		putFile(t, f, path.Join("/blocks/v", c.Path()), d)
		manifest[i] = layout.Ref{Owner: "v", OwnerID: id, Chunk: c}
	}
	root, segs, err := layout.Build(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for n, sb := range segs {
		putFile(t, f, path.Join("/blocks/v", n.Path()), sb)
	}
	rn, rb, err := layout.NameRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	putFile(t, f, path.Join("/blocks/v", rn.Path()), rb)
	hb, err := layout.Head{VolumeID: id, Epoch: 1, Holder: "s/r", ClaimID: "c", Activity: layout.Activity{Kind: layout.ActIdle}, Current: &rn}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	putFile(t, f, "/blocks/v/"+layout.HeadName, hb)
	return filerBlobs{f, b}, id
}

func TestVerifyLayoutThroughTheAdapter(t *testing.T) {
	fb, _ := verifyFixtureVolume(t)
	ctx := context.Background()
	before := snapshotStore(fb)
	r, err := VerifyLayout(ctx, fb, "/blocks", "v", layout.VerifyOptions{Depth: layout.DepthFull, Orphans: true})
	if err != nil || !r.Complete || len(r.Findings) != 0 || r.Chunks != 2 || !r.OrphansChecked {
		t.Fatalf("clean: %+v %v", r, err)
	}
	if after := snapshotStore(fb); after != before {
		t.Fatalf("verify changed the store:\nbefore %s\nafter  %s", before, after)
	}
	extra := layout.NameChunk(9, []byte("zz"))
	putFile(t, fb.fakeFiler, path.Join("/blocks/v", extra.Path()), []byte("zz"))
	c0 := layout.NameChunk(0, []byte(strings.Repeat("a", 64)))
	delete(fb.fakeFiler.entries, path.Join("/blocks/v", c0.Path()))
	r, err = VerifyLayout(ctx, fb, "/blocks", "v", layout.VerifyOptions{Orphans: true})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, f := range r.Findings {
		got[f.Path] = f.Class
	}
	if got["v/"+c0.Path()] != layout.FindMissing || got["v/"+extra.Path()] != layout.FindOrphan || len(got) != 2 {
		t.Fatalf("findings: %+v", r.Findings)
	}
}

func TestLayoutStoreListPagesAndStaysInBase(t *testing.T) {
	fb, id := verifyFixtureVolume(t)
	s, err := NewLayoutStore(fb.fakeFiler, fb.fakeBlobs, "/blocks/v", id)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < verifyListPage+3; i++ {
		putFile(t, fb.fakeFiler, path.Join("/blocks/v/seg", strings.Repeat("x", 4)+string(rune('A'+i%26))+string(rune('a'+i/26%26))+string(rune('a'+i/676))), []byte("s"))
	}
	names, err := s.List(context.Background(), "/blocks/v/seg")
	if err != nil {
		t.Fatal(err)
	}
	segs := 0
	for n := range fb.fakeFiler.entries {
		if path.Dir(n) == "/blocks/v/seg" {
			segs++
		}
	}
	if len(names) != segs || segs <= verifyListPage {
		t.Fatalf("listed %d of %d entries (page %d)", len(names), segs, verifyListPage)
	}
	if _, err := s.List(context.Background(), "/elsewhere"); err == nil {
		t.Fatal("a path outside the store base must be refused")
	}
}

func TestVerifyLayoutNeedsBlobs(t *testing.T) {
	if _, err := VerifyLayout(context.Background(), newFakeFiler(), "/blocks", "v", layout.VerifyOptions{}); err == nil {
		t.Fatal("a filer without blobs must be refused")
	}
}

// snapshotStore is every filer entry (path and bytes) and the blob counters
// except reads, as one comparable string.
func snapshotStore(fb filerBlobs) string {
	fb.fakeFiler.mu.Lock()
	var keys []string
	for p, e := range fb.fakeFiler.entries {
		raw, _ := proto.Marshal(e)
		keys = append(keys, fmt.Sprintf("%s=%x", p, sha256.Sum256(raw)))
	}
	fb.fakeFiler.mu.Unlock()
	sort.Strings(keys)
	puts, _, stored := fb.fakeBlobs.counts()
	return fmt.Sprintf("puts=%d stored=%d %s", puts, stored, strings.Join(keys, ","))
}
