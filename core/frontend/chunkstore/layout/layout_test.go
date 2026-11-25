package layout

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"path"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// SHA-256("abc"), FIPS 180-2 test vector.
const abcHash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

func mustHash(t *testing.T, s string) Hash {
	t.Helper()
	h, err := parseHash(s)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestChunkName_GoldenNameAndPath(t *testing.T) {
	c := NameChunk(12345, []byte("abc"))
	if got := c.String(); got != "12345-"+abcHash {
		t.Fatalf("name %q", got)
	}
	if got := c.Path(); got != "pool/1/12345-"+abcHash {
		t.Fatalf("path %q", got)
	}
	if err := c.Verify([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := c.Verify([]byte("abd")); err == nil {
		t.Fatal("Verify accepted other bytes")
	}
}

func TestShard_Boundaries(t *testing.T) {
	for _, tc := range []struct{ idx, shard uint64 }{
		{0, 0}, {9999, 0}, {10000, 1}, {19999, 1}, {20000, 2}, {math.MaxUint64, math.MaxUint64 / 10000},
	} {
		if got := Shard(tc.idx); got != tc.shard {
			t.Errorf("Shard(%d) = %d; want %d", tc.idx, got, tc.shard)
		}
		c := ChunkName{Index: tc.idx}
		if want := path.Join("pool", strconv.FormatUint(tc.shard, 10)); c.Dir() != want {
			t.Errorf("Dir(%d) = %q; want %q", tc.idx, c.Dir(), want)
		}
	}
}

func TestParseChunkName_RoundTripAndRefusals(t *testing.T) {
	for _, idx := range []uint64{0, 7, 10000, math.MaxUint64} {
		c := ChunkName{Index: idx, Hash: mustHash(t, abcHash)}
		got, err := ParseChunkName(c.String())
		if err != nil || got != c {
			t.Fatalf("round trip of %s: %+v %v", c, got, err)
		}
	}
	for _, bad := range []string{
		"",
		abcHash,                           // no index
		"12345",                           // no hash
		"012-" + abcHash,                  // leading zero
		"+1-" + abcHash,                   // sign
		"-" + abcHash,                     // empty index
		"18446744073709551616-" + abcHash, // overflows uint64
		"1-" + strings.ToUpper(abcHash),   // upper-case hex
		"1-" + abcHash[:63],               // short
		"1-" + abcHash + "0",              // long
		"1-" + abcHash[:63] + "g",         // not hex
		"1-" + abcHash + "-x",             // trailing part
	} {
		if _, err := ParseChunkName(bad); !errors.Is(err, ErrMalformed) {
			t.Errorf("ParseChunkName(%q): %v; want ErrMalformed", bad, err)
		}
	}
}

// idFor is a fixed test volume id per volume name.
func idFor(name string) VolumeID {
	var id VolumeID
	h := Sum([]byte("volume " + name))
	copy(id[:], h[:])
	return id
}

func ref(owner string, idx uint64, data string) Ref {
	return Ref{Owner: owner, OwnerID: idFor(owner), Chunk: NameChunk(idx, []byte(data))}
}

// goldenSegment is the encoding of shard 1 holding chunk 10000 ("abc", own)
// and chunk 10001 (from volume "src"), written out by hand.
func goldenSegment(t *testing.T) []byte {
	t.Helper()
	h := mustHash(t, abcHash)
	h2 := Sum([]byte("x"))
	var b []byte
	b = append(b, "SWBSEG\x02"...)
	b = binary.AppendUvarint(b, 1) // shard
	b = binary.AppendUvarint(b, 2) // entries
	b = binary.AppendUvarint(b, 10000)
	b = binary.AppendUvarint(b, 2)
	b = append(b, "v1"...)
	v1 := idFor("v1")
	b = append(b, v1[:]...)
	b = append(b, h[:]...)
	b = binary.AppendUvarint(b, 10001)
	b = binary.AppendUvarint(b, 3)
	b = append(b, "src"...)
	src := idFor("src")
	b = append(b, src[:]...)
	b = append(b, h2[:]...)
	return b
}

func TestSegment_GoldenEncodingRoundTrip(t *testing.T) {
	s := Segment{Shard: 1, Chunks: map[uint64]Ref{10001: ref("src", 10001, "x"), 10000: ref("v1", 10000, "abc")}}
	got, err := s.Encode()
	if err != nil {
		t.Fatal(err)
	}
	want := goldenSegment(t)
	if !bytes.Equal(got, want) {
		t.Fatalf("encoding\n got %x\nwant %x", got, want)
	}
	back, err := DecodeSegment(got)
	if err != nil || !reflect.DeepEqual(back, s) {
		t.Fatalf("decode: %+v %v", back, err)
	}
	n, b, err := NameSegment(s)
	if err != nil || !bytes.Equal(b, want) || n.Hash != Sum(want) || n.Shard != 1 {
		t.Fatalf("name %s %v", n, err)
	}
	if n.Path() != "seg/1-"+Sum(want).String() {
		t.Fatalf("path %q", n.Path())
	}
	if p, err := ParseSegName(n.String()); err != nil || p != n {
		t.Fatalf("ParseSegName: %+v %v", p, err)
	}
}

func TestSegment_EncodeRefusals(t *testing.T) {
	for name, s := range map[string]Segment{
		"empty":                {Shard: 0, Chunks: map[uint64]Ref{}},
		"chunk outside shard":  {Shard: 0, Chunks: map[uint64]Ref{10000: ref("v1", 10000, "a")}},
		"key is not the index": {Shard: 0, Chunks: map[uint64]Ref{1: ref("v1", 2, "a")}},
		"owner empty":          {Shard: 0, Chunks: map[uint64]Ref{1: ref("", 1, "a")}},
		"owner with slash":     {Shard: 0, Chunks: map[uint64]Ref{1: ref("a/b", 1, "a")}},
		"owner dot-dot":        {Shard: 0, Chunks: map[uint64]Ref{1: ref("..", 1, "a")}},
		"owner without id":     {Shard: 0, Chunks: map[uint64]Ref{1: {Owner: "v1", Chunk: NameChunk(1, []byte("a"))}}},
	} {
		if _, err := s.Encode(); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v; want ErrMalformed", name, err)
		}
	}
}

// segBytes writes entries in the given order, without the checks Encode
// makes: the hostile inputs DecodeSegment must refuse.
func segBytes(shard uint64, entries ...Ref) []byte {
	var b []byte
	b = append(b, "SWBSEG\x02"...)
	b = binary.AppendUvarint(b, shard)
	b = binary.AppendUvarint(b, uint64(len(entries)))
	for _, r := range entries {
		b = binary.AppendUvarint(b, r.Chunk.Index)
		b = binary.AppendUvarint(b, uint64(len(r.Owner)))
		b = append(b, r.Owner...)
		b = append(b, r.OwnerID[:]...)
		b = append(b, r.Chunk.Hash[:]...)
	}
	return b
}

func TestDecodeSegment_RefusesNonCanonical(t *testing.T) {
	a, c := ref("v1", 1, "a"), ref("v1", 2, "b")
	good := segBytes(0, a, c)
	if _, err := DecodeSegment(good); err != nil {
		t.Fatalf("control: %v", err)
	}
	nonMinimal := append([]byte("SWBSEG\x02"), 0x80, 0x00) // shard 0 in two bytes
	nonMinimal = append(nonMinimal, segBytes(0, a, c)[len("SWBSEG\x02")+1:]...)
	huge := append([]byte("SWBSEG\x02"), 0)
	huge = binary.AppendUvarint(huge, ShardSize+1)
	for name, b := range map[string][]byte{
		"wrong magic":         append([]byte("SWBSEG\x01"), good[7:]...),
		"unsorted":            segBytes(0, c, a),
		"duplicate":           segBytes(0, a, a),
		"empty":               segBytes(0),
		"outside shard":       segBytes(1, a),
		"bad owner":           segBytes(0, ref("a/b", 1, "a")),
		"trailing byte":       append(append([]byte(nil), good...), 0),
		"truncated":           good[:len(good)-1],
		"non-minimal varint":  nonMinimal,
		"count above a shard": huge,
	} {
		if _, err := DecodeSegment(b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v; want ErrMalformed", name, err)
		}
	}
}

func TestRoot_GoldenEncodingRoundTrip(t *testing.T) {
	s0 := SegName{Shard: 0, Hash: mustHash(t, abcHash)}
	s3 := SegName{Shard: 3, Hash: Sum([]byte("x"))}
	r := Root{Segments: map[uint64]SegName{3: s3, 0: s0}}
	var want []byte
	want = append(want, "SWBROOT\x01"...)
	want = binary.AppendUvarint(want, 2)
	want = binary.AppendUvarint(want, 0)
	want = append(want, s0.Hash[:]...)
	want = binary.AppendUvarint(want, 3)
	want = append(want, s3.Hash[:]...)
	got, err := r.Encode()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("encoding %x %v\nwant %x", got, err, want)
	}
	back, err := DecodeRoot(got)
	if err != nil || !reflect.DeepEqual(back, r) {
		t.Fatalf("decode %+v %v", back, err)
	}
	n, _, err := NameRoot(r)
	if err != nil || n.Hash != Sum(want) || n.Path() != "root/"+Sum(want).String() {
		t.Fatalf("name %s %v", n, err)
	}
	if p, err := ParseRootName(n.String()); err != nil || p != n {
		t.Fatalf("ParseRootName %+v %v", p, err)
	}
	empty, err := Root{}.Encode() // an all-zero image
	if err != nil {
		t.Fatal(err)
	}
	if back, err := DecodeRoot(empty); err != nil || len(back.Segments) != 0 {
		t.Fatalf("empty root: %+v %v", back, err)
	}
}

func TestRoot_Refusals(t *testing.T) {
	if _, err := (Root{Segments: map[uint64]SegName{1: {Shard: 2}}}).Encode(); !errors.Is(err, ErrMalformed) {
		t.Errorf("root keyed by another shard: %v", err)
	}
	h := Sum([]byte("x"))
	raw := func(shards ...uint64) []byte {
		b := append([]byte(nil), "SWBROOT\x01"...)
		b = binary.AppendUvarint(b, uint64(len(shards)))
		for _, s := range shards {
			b = binary.AppendUvarint(b, s)
			b = append(b, h[:]...)
		}
		return b
	}
	good := raw(0, 5)
	if _, err := DecodeRoot(good); err != nil {
		t.Fatalf("control: %v", err)
	}
	huge := binary.AppendUvarint(append([]byte(nil), "SWBROOT\x01"...), 1<<40)
	for name, b := range map[string][]byte{
		"wrong magic":   append([]byte("SWBSEG\x01"), good[8:]...),
		"unsorted":      raw(5, 0),
		"duplicate":     raw(5, 5),
		"trailing byte": append(append([]byte(nil), good...), 0),
		"truncated":     good[:len(good)-1],
		"huge count":    huge,
	} {
		if _, err := DecodeRoot(b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v; want ErrMalformed", name, err)
		}
	}
	for _, bad := range []string{"", abcHash[:63], strings.ToUpper(abcHash), "1-" + abcHash} {
		if _, err := ParseRootName(bad); !errors.Is(err, ErrMalformed) {
			t.Errorf("ParseRootName(%q): %v", bad, err)
		}
	}
}

// A manifest's root does not depend on map order, and changing one chunk
// changes only its shard's segment name and the root name: the unchanged
// shards' segments are reused as they are.
func TestBuild_ShardsAndIncrementalReuse(t *testing.T) {
	m := map[uint64]Ref{
		0: ref("v1", 0, "a"), 9999: ref("v1", 9999, "b"),
		10000: ref("src", 10000, "c"),
		30005: ref("v1", 30005, "d"),
	}
	root, segs, err := Build(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Segments) != 3 || len(segs) != 3 {
		t.Fatalf("shards %v, segments %d; want shards 0, 1, 3", root.Segments, len(segs))
	}
	got := map[uint64]Ref{}
	for sh, n := range root.Segments {
		s, err := DecodeSegment(segs[n])
		if err != nil || s.Shard != sh || Sum(segs[n]) != n.Hash {
			t.Fatalf("segment %s: %+v %v", n, s, err)
		}
		for i, r := range s.Chunks {
			got[i] = r
		}
	}
	if !reflect.DeepEqual(got, m) {
		t.Fatalf("manifest read back %v; want %v", got, m)
	}
	r1, _, _ := NameRoot(root)
	for k := 0; k < 20; k++ { // map order never shows
		again, _, _ := Build(m)
		if r, _, _ := NameRoot(again); r != r1 {
			t.Fatal("the root name depends on map order")
		}
	}
	m2 := map[uint64]Ref{}
	for i, r := range m {
		m2[i] = r
	}
	m2[30005] = ref("v1", 30005, "d2")
	root2, _, err := Build(m2)
	if err != nil {
		t.Fatal(err)
	}
	for _, sh := range []uint64{0, 1} {
		if root2.Segments[sh] != root.Segments[sh] {
			t.Errorf("shard %d segment changed although none of its chunks did", sh)
		}
	}
	if root2.Segments[3] == root.Segments[3] {
		t.Error("shard 3 segment unchanged although its chunk changed")
	}
	if r2, _, _ := NameRoot(root2); r2 == r1 {
		t.Error("root name unchanged although the image changed")
	}
}

func TestEncodings_RandomRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	owners := []string{"v1", "src", "a b"} // ids may hold spaces
	for k := 0; k < 200; k++ {
		m := map[uint64]Ref{}
		for j := rng.Intn(50); j > 0; j-- {
			i := uint64(rng.Int63n(100000))
			var d [8]byte
			rng.Read(d[:])
			o := owners[rng.Intn(len(owners))]
			m[i] = Ref{Owner: o, OwnerID: idFor(o), Chunk: NameChunk(i, d[:])}
		}
		root, segs, err := Build(m)
		if err != nil {
			t.Fatal(err)
		}
		_, rb, err := NameRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		back, err := DecodeRoot(rb)
		if err != nil || !reflect.DeepEqual(back.Segments, root.Segments) {
			t.Fatalf("root round trip: %v", err)
		}
		for n, b := range segs {
			s, err := DecodeSegment(b)
			if err != nil {
				t.Fatal(err)
			}
			if again, _ := s.Encode(); !bytes.Equal(again, b) || Sum(b) != n.Hash {
				t.Fatalf("segment %s does not re-encode to its name", n)
			}
		}
	}
}

type fakeStore struct {
	have  map[string]bool
	fail  string // a path whose lookup fails
	asked []string
}

func (f *fakeStore) look(_ context.Context, dir, name string) (bool, error) {
	p := path.Join(dir, name)
	f.asked = append(f.asked, p)
	if p == f.fail {
		return false, errors.New("filer unavailable")
	}
	return f.have[p], nil
}

func TestPresentAndMissingOwn(t *testing.T) {
	ctx := context.Background()
	const vr = "/swblock/chunks/v1"
	a, b, foreign := ref("v1", 3, "a"), ref("v1", 10007, "b"), ref("src", 5, "c")
	m := map[uint64]Ref{3: a, 10007: b, 5: foreign}
	f := &fakeStore{have: map[string]bool{path.Join(vr, a.Chunk.Path()): true}}
	if ok, err := Present(ctx, f.look, vr, a.Chunk.Path()); err != nil || !ok {
		t.Fatalf("Present of an uploaded chunk: %t %v", ok, err)
	}
	if ok, err := Present(ctx, f.look, vr, b.Chunk.Path()); err != nil || ok {
		t.Fatalf("Present of a missing chunk: %t %v", ok, err)
	}
	f.asked = nil
	missing, err := MissingOwn(ctx, f.look, vr, "v1", m)
	if err != nil || len(missing) != 1 || missing[0] != b.Chunk {
		t.Fatalf("MissingOwn: %v %v; want [%s]", missing, err, b.Chunk)
	}
	for _, p := range f.asked {
		if strings.Contains(p, foreign.Chunk.String()) {
			t.Fatalf("MissingOwn looked up the foreign chunk at %s", p)
		}
	}
	if want := path.Join(vr, "pool/1", b.Chunk.String()); f.asked[len(f.asked)-1] != want {
		t.Fatalf("looked up %v; want the shard directory path %s last", f.asked, want)
	}
	// A failed lookup is an error, never "absent" and never "present".
	f.fail = path.Join(vr, b.Chunk.Path())
	if got, err := MissingOwn(ctx, f.look, vr, "v1", m); err == nil {
		t.Fatalf("MissingOwn with a failing lookup: %v, no error", got)
	}
	if _, err := Present(ctx, f.look, vr, b.Chunk.Path()); err == nil {
		t.Fatal("Present with a failing lookup: no error")
	}
}
