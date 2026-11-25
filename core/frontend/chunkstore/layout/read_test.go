package layout

import (
	"bytes"
	"context"
	"errors"
	"path"
	"strings"
	"testing"
)

// objStore is a fake store of whole objects by full path.
type objStore struct {
	obj   map[string][]byte
	fail  string // a path whose fetch fails
	asked []string
}

func (s *objStore) fetch(_ context.Context, p string) ([]byte, bool, error) {
	s.asked = append(s.asked, p)
	if p == s.fail {
		return nil, false, errors.New("filer unavailable")
	}
	b, ok := s.obj[p]
	return b, ok, nil
}

const base = "/swblock/chunks"

// The test volumes' directories: <name>-<first 8 hex of the volume id>.
var (
	tDir   = VolumeDir("t", idFor("t"))
	srcDir = VolumeDir("src", idFor("src"))
)

// readFixture: volume "t" (a restore target) whose root names chunk 3
// ("own") in its own pool, chunk 10002 ("src data") in the pool of "src",
// and nothing else; every object is written.
func readFixture(t *testing.T) (*objStore, RootName, map[uint64]Ref) {
	t.Helper()
	s := &objStore{obj: map[string][]byte{}}
	put := func(vol, rel string, b []byte) { s.obj[path.Join(base, vol, rel)] = b }
	own := NameChunk(3, []byte("own"))
	foreign := NameChunk(10002, []byte("src data"))
	put(tDir, own.Path(), []byte("own"))
	put(srcDir, foreign.Path(), []byte("src data"))
	m := map[uint64]Ref{3: {Owner: "t", OwnerID: idFor("t"), Chunk: own}, 10002: {Owner: "src", OwnerID: idFor("src"), Chunk: foreign}}
	putVolume(t, put, "t", tDir)
	putVolume(t, put, "src", srcDir)
	root, segs, err := Build(m)
	if err != nil {
		t.Fatal(err)
	}
	for n, b := range segs {
		put(tDir, n.Path(), b)
	}
	rn, rb, err := NameRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	put(tDir, rn.Path(), rb)
	return s, rn, m
}

func newReader(t *testing.T, s *objStore) *Reader {
	t.Helper()
	r, err := NewReader(base, tDir, s.fetch)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRead_OwnChunk(t *testing.T) {
	s, rn, _ := readFixture(t)
	r := newReader(t, s)
	img, err := r.Open(context.Background(), rn)
	if err != nil {
		t.Fatal(err)
	}
	c, err := img.Chunk(context.Background(), 3)
	if err != nil || c.Zero || !bytes.Equal(c.Data, []byte("own")) || c.Ref.Owner != "t" {
		t.Fatalf("chunk 3: %+v %v", c, err)
	}
}

// An index the root does not name reads as zeros: in a shard with a
// segment, and in a shard with none. Zeros never touch the pool.
func TestRead_AbsentIndexIsZeros(t *testing.T) {
	s, rn, _ := readFixture(t)
	r := newReader(t, s)
	img, err := r.Open(context.Background(), rn)
	if err != nil {
		t.Fatal(err)
	}
	for _, idx := range []uint64{4, 9999, 10001, 50000} {
		s.asked = nil
		c, err := img.Chunk(context.Background(), idx)
		if err != nil || !c.Zero || c.Data != nil {
			t.Fatalf("index %d: %+v %v; want zeros", idx, c, err)
		}
		for _, p := range s.asked {
			if strings.Contains(p, "/"+PoolDir+"/") {
				t.Fatalf("index %d read the pool (%s) for a zero chunk", idx, p)
			}
		}
	}
}

// A foreign name resolves in the source volume's pool, never in the
// reading volume's.
func TestRead_ForeignChunkFromSourcePool(t *testing.T) {
	s, rn, m := readFixture(t)
	// A same-named object in t's own pool must not be what is read.
	s.obj[path.Join(base, tDir, m[10002].Chunk.Path())] = []byte("decoy")
	r := newReader(t, s)
	img, err := r.Open(context.Background(), rn)
	if err != nil {
		t.Fatal(err)
	}
	s.asked = nil
	c, err := img.Chunk(context.Background(), 10002)
	if err != nil || c.Zero || !bytes.Equal(c.Data, []byte("src data")) || c.Ref.Owner != "src" {
		t.Fatalf("chunk 10002: %+v %v", c, err)
	}
	want := path.Join(base, srcDir, m[10002].Chunk.Path())
	if got := s.asked[len(s.asked)-1]; got != want {
		t.Fatalf("read %s; want the source pool path %s", got, want)
	}
}

// A missing segment or chunk is an error, never zeros; so is a missing
// root.
func TestRead_MissingObjectIsAnErrorNeverZeros(t *testing.T) {
	ctx := context.Background()
	t.Run("root", func(t *testing.T) {
		s, rn, _ := readFixture(t)
		delete(s.obj, path.Join(base, tDir, rn.Path()))
		if _, err := newReader(t, s).Open(ctx, rn); !errors.Is(err, ErrMissing) {
			t.Fatalf("open of a missing root: %v; want ErrMissing", err)
		}
	})
	t.Run("segment", func(t *testing.T) {
		s, rn, _ := readFixture(t)
		img, err := newReader(t, s).Open(ctx, rn)
		if err != nil {
			t.Fatal(err)
		}
		delete(s.obj, path.Join(base, tDir, img.root.Segments[1].Path()))
		for _, idx := range []uint64{10002, 10003} { // named, and not named, in that shard
			c, err := img.Chunk(ctx, idx)
			if !errors.Is(err, ErrMissing) || c.Zero {
				t.Fatalf("index %d with its segment missing: %+v %v; want ErrMissing", idx, c, err)
			}
		}
	})
	t.Run("own chunk", func(t *testing.T) {
		s, rn, m := readFixture(t)
		delete(s.obj, path.Join(base, tDir, m[3].Chunk.Path()))
		img, err := newReader(t, s).Open(ctx, rn)
		if err != nil {
			t.Fatal(err)
		}
		if c, err := img.Chunk(ctx, 3); !errors.Is(err, ErrMissing) || c.Zero {
			t.Fatalf("missing own chunk: %+v %v; want ErrMissing", c, err)
		}
	})
	t.Run("foreign chunk", func(t *testing.T) {
		s, rn, m := readFixture(t)
		delete(s.obj, path.Join(base, srcDir, m[10002].Chunk.Path()))
		img, err := newReader(t, s).Open(ctx, rn)
		if err != nil {
			t.Fatal(err)
		}
		if c, err := img.Chunk(ctx, 10002); !errors.Is(err, ErrMissing) || c.Zero {
			t.Fatalf("missing foreign chunk: %+v %v; want ErrMissing", c, err)
		}
	})
}

// A failed fetch is an error and not ErrMissing; bytes that do not match
// their name are ErrCorrupt.
func TestRead_FetchFailureAndCorruption(t *testing.T) {
	ctx := context.Background()
	s, rn, m := readFixture(t)
	r := newReader(t, s)
	img, err := r.Open(ctx, rn)
	if err != nil {
		t.Fatal(err)
	}
	s.fail = path.Join(base, tDir, m[3].Chunk.Path())
	if _, err := img.Chunk(ctx, 3); err == nil || errors.Is(err, ErrMissing) {
		t.Fatalf("failed fetch: %v; want an error that is not ErrMissing", err)
	}
	s.fail = ""
	for name, p := range map[string]string{
		"chunk":   path.Join(base, tDir, m[3].Chunk.Path()),
		"segment": path.Join(base, tDir, img.root.Segments[0].Path()),
	} {
		s2, rn2, _ := readFixture(t)
		s2.obj[p] = append(append([]byte(nil), s2.obj[p]...), 'x')
		img2, err := newReader(t, s2).Open(ctx, rn2)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := img2.Chunk(ctx, 3); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s altered: %v; want ErrCorrupt", name, err)
		}
	}
	s3, rn3, _ := readFixture(t)
	rp := path.Join(base, tDir, rn3.Path())
	s3.obj[rp] = append(append([]byte(nil), s3.obj[rp]...), 0)
	if _, err := newReader(t, s3).Open(ctx, rn3); !errors.Is(err, ErrCorrupt) {
		t.Errorf("root altered: %v; want ErrCorrupt", err)
	}
}

// A segment stored under one shard's name but holding another shard is
// refused (its bytes match its hash, so only the shard check catches it).
func TestRead_SegmentOfAnotherShardRefused(t *testing.T) {
	ctx := context.Background()
	s := &objStore{obj: map[string][]byte{}}
	seg := Segment{Shard: 2, Chunks: map[uint64]Ref{20000: {Owner: "t", OwnerID: idFor("t"), Chunk: NameChunk(20000, []byte("z"))}}}
	sn, sb, err := NameSegment(seg)
	if err != nil {
		t.Fatal(err)
	}
	lie := SegName{Shard: 0, Hash: sn.Hash}
	s.obj[path.Join(base, tDir, lie.Path())] = sb
	root := Root{Segments: map[uint64]SegName{0: lie}}
	rn, rb, err := NameRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	s.obj[path.Join(base, tDir, rn.Path())] = rb
	img, err := newReader(t, s).Open(ctx, rn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := img.Chunk(ctx, 5); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("segment of shard 2 under shard 0: %v; want ErrCorrupt", err)
	}
}

func TestNewReader_RefusesBadInput(t *testing.T) {
	f := func(context.Context, string) ([]byte, bool, error) { return nil, false, nil }
	if _, err := NewReader(base, "a/b", f); err == nil {
		t.Error("volume with a slash accepted")
	}
	if _, err := NewReader(base, "t", nil); err == nil {
		t.Error("nil fetch accepted")
	}
}

// putVolume writes vol's VOLUME record with its test id.
func putVolume(t *testing.T, put func(dir, rel string, b []byte), name, dir string) {
	t.Helper()
	b, err := Volume{ID: idFor(name), Name: name, Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, Format: VolumeFormat}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	put(dir, VolumeName, b)
}

// A foreign chunk is read only if its owner's VOLUME carries the id the
// reference names: a source deleted and created again under its name has
// another id (ErrIdentity); a source without VOLUME is ErrMissing. The
// chunk's bytes are never fetched in either case.
func TestRead_ForeignChunkNeedsTheOwnersVolumeID(t *testing.T) {
	ctx := context.Background()
	s, rn, m := readFixture(t)
	b, err := Volume{ID: idFor("src again"), Name: "src", Geometry: Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, Format: VolumeFormat}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	s.obj[path.Join(base, srcDir, VolumeName)] = b
	img, err := newReader(t, s).Open(ctx, rn)
	if err != nil {
		t.Fatal(err)
	}
	s.asked = nil
	if _, err := img.Chunk(ctx, 10002); !errors.Is(err, ErrIdentity) {
		t.Fatalf("chunk of a re-created source: %v; want ErrIdentity", err)
	}
	for _, p := range s.asked {
		if p == path.Join(base, srcDir, m[10002].Chunk.Path()) {
			t.Fatal("the chunk was fetched from a source whose id does not match")
		}
	}
	delete(s.obj, path.Join(base, srcDir, VolumeName))
	img, _ = newReader(t, s).Open(ctx, rn)
	if _, err := img.Chunk(ctx, 10002); !errors.Is(err, ErrMissing) {
		t.Fatalf("chunk of a source without VOLUME: %v; want ErrMissing", err)
	}
	if c, err := img.Chunk(ctx, 3); err != nil || c.Zero {
		t.Fatalf("own chunk beside it: %+v %v", c, err)
	}
}
