package chunkstore

import (
	"context"
	"errors"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// A batch offset names needle key base+i with the base's cookie; the stock
// form drops leading zero key bytes.
func TestBinding_CanonicalFid(t *testing.T) {
	for in, want := range map[string]string{
		"1,0a0b0c0d0e":         "1,0a0b0c0d0e",
		"1,0a0b0c0d0e_1":       "1,0b0b0c0d0e",
		"1,ff0b0c0d0e_1":       "1,01000b0c0d0e",
		"3,0000000001deadbeef": "3,01deadbeef",
	} {
		got, id, err := canonicalFid(in)
		if err != nil || got != want {
			t.Errorf("%s: got %q, %v; want %q", in, got, err, want)
			continue
		}
		if again, _, _ := canonicalFid(got); again != got || id.GetCookie() == 0 {
			t.Errorf("%s: canonical form %q not stable (%q) or cookie lost", in, got, again)
		}
	}
	for _, bad := range []string{"", "1", "1,", "1,abc", "x,0a0b0c0d0e", "1,0a0b0c0d0e_0", "1,0a0b0c0d0e_x",
		"1,ffffffffffffffff0b0c0d0e_1", "7,01020304050607080900000001"} {
		if got, _, err := canonicalFid(bad); err == nil {
			t.Errorf("%q accepted as %q", bad, got)
		}
	}
}

func liveBinding(t *testing.T) *filerpb.Entry {
	t.Helper()
	c, err := bindingChunk("1,0a0b0c0d0e_2", 10)
	if err != nil {
		t.Fatal(err)
	}
	return &filerpb.Entry{Name: "k", Chunks: []*filerpb.FileChunk{c}, Attributes: &filerpb.FuseAttributes{FileSize: 10, Crtime: 1},
		Extended: map[string][]byte{extKind: []byte(kindChunkKey)}}
}

// The decoder accepts exactly the live and done shapes.
func TestBinding_DecoderAcceptsOnlyTwoShapes(t *testing.T) {
	b, err := decodeBinding(liveBinding(t))
	if err != nil || b.done || b.fid != "1,0c0b0c0d0e" || b.size != 10 {
		t.Fatalf("live: %+v %v", b, err)
	}
	done := liveBinding(t)
	done.Chunks, done.Extended[extGCDeleted] = nil, []byte("done")
	if b, err = decodeBinding(done); err != nil || !b.done || b.fid != "" {
		t.Fatalf("done: %+v %v", b, err)
	}
	bad := map[string]func(e *filerpb.Entry){
		"two chunks":        func(e *filerpb.Entry) { e.Chunks = append(e.Chunks, e.Chunks[0]) },
		"no chunk":          func(e *filerpb.Entry) { e.Chunks = nil },
		"content and chunk": func(e *filerpb.Entry) { e.Content = []byte("x") },
		"size 0":            func(e *filerpb.Entry) { e.Chunks[0].Size = 0 },
		"offset":            func(e *filerpb.Entry) { e.Chunks[0].Offset = 1 },
		"no fid":            func(e *filerpb.Entry) { e.Chunks[0].Fid = nil },
		"batch file_id":     func(e *filerpb.Entry) { e.Chunks[0].FileId = "1,0a0b0c0d0e_2" },
		"other file_id":     func(e *filerpb.Entry) { e.Chunks[0].FileId = "1,0d0b0c0d0e" },
		"manifest":          func(e *filerpb.Entry) { e.Chunks[0].IsChunkManifest = true },
		"compressed":        func(e *filerpb.Entry) { e.Chunks[0].IsCompressed = true },
		"ciphered":          func(e *filerpb.Entry) { e.Chunks[0].CipherKey = []byte{1} },
		"old swb.fid":       func(e *filerpb.Entry) { e.Extended["swb.fid"] = []byte("1,0c0b0c0d0e") },
		"reclaim state 1":   func(e *filerpb.Entry) { e.Extended[extGCDeleted] = []byte("1") },
		"done with a chunk": func(e *filerpb.Entry) { e.Extended[extGCDeleted] = []byte("done") },
		"other kind":        func(e *filerpb.Entry) { e.Extended[extKind] = []byte("head") },
		"directory":         func(e *filerpb.Entry) { e.IsDirectory = true },
	}
	for name, edit := range bad {
		e := liveBinding(t)
		edit(e)
		if b, err := decodeBinding(e); err == nil {
			t.Errorf("%s: accepted as %+v", name, b)
		}
	}
}

// An empty object is refused before anything is uploaded or bound.
func TestChunkRemote_EmptyObjectRefusedBeforeUpload(t *testing.T) {
	ctx := context.Background()
	f, b := newFakeFiler(), newFakeBlobs()
	r, err := NewChunkRemote(f, b, "/swblock/chunks/v1")
	if err != nil {
		t.Fatal(err)
	}
	written := len(f.allWritten())
	if err := r.Put(ctx, "chunks/e1/c0/k", nil); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("empty put: %v", err)
	}
	if b.puts != 0 || len(f.allWritten()) != written {
		t.Fatalf("empty put uploaded %d, wrote %d entries", b.puts, len(f.allWritten())-written)
	}
	rb := newRangeFake()
	rr := roundRemote(t, rb)
	if err := rr.BeginPutRound(1, 4096).Put(ctx, 0, roundKey(0), []byte{}); err == nil {
		t.Fatal("empty round put accepted")
	}
	if got := rb.putList(); len(got) != 0 {
		t.Fatalf("empty round put uploaded %v", got)
	}
}

// reclaimFixture is an orphan whose first mark is two grace periods old: the
// next applied pass reclaims it.
func reclaimFixture(t *testing.T) (*fakeFiler, gcBlobs, *ChunkRemote, *Collector, *time.Time, string, string) {
	t.Helper()
	f, b, _, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err := r.Put(ctx, "orphan", []byte("exact orphan")); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Run(ctx); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2*time.Hour + g.cfg.ClockSkew)
	name, err := r.ownKeyName("orphan")
	if err != nil {
		t.Fatal(err)
	}
	e, err := f.LookupEntry(ctx, r.keyDir(), name)
	if err != nil {
		t.Fatal(err)
	}
	live, err := decodeBinding(e)
	if err != nil || live.done {
		t.Fatalf("orphan binding %+v %v", live, err)
	}
	return f, b, r, g, now, path.Join(r.keyDir(), name), live.fid
}

// GC reclaims by one conditional UpdateEntry to the done shape; the filer's
// queue (not GC) deletes exactly that needle.
func TestGC_ReclaimIsOneConditionalUpdate(t *testing.T) {
	f, b, r, g, _, p, fid := reclaimFixture(t)
	ctx := context.Background()
	result, err := g.Run(ctx)
	if err != nil || result.Reclaimed != 1 {
		t.Fatalf("reclaim: %+v %v", result, err)
	}
	e, err := f.LookupEntry(ctx, path.Dir(p), path.Base(p))
	if err != nil {
		t.Fatal(err)
	}
	if bound, err := decodeBinding(e); err != nil || !bound.done || len(e.Chunks) != 0 {
		t.Fatalf("reclaimed binding %+v %v chunks=%d", bound, err, len(e.Chunks))
	}
	if len(f.dropped) != 1 || f.dropped[0] != fid {
		t.Fatalf("filer deletion queue %v; want exactly %s", f.dropped, fid)
	}
	if _, err := b.Get(ctx, fid); err == nil {
		t.Fatal("reclaimed needle still stored")
	}
	if v, has := f.lastExpected[extGCDeleted]; len(f.lastExpected) != 2 || string(f.lastExpected[extKind]) != kindChunkKey || !has || len(v) != 0 {
		t.Fatalf("reclaim condition %q; want %s=%s and %s absent", f.lastExpected, extKind, kindChunkKey, extGCDeleted)
	}
	fresh, _ := NewChunkRemote(f, b, r.root) // r still holds the fid it uploaded
	if _, err := fresh.Get(ctx, "orphan"); !errors.Is(err, ErrBindingReclaimed) {
		t.Fatalf("get of a reclaimed key: %v", err)
	}
}

// A second reclaim of the same binding loses the condition: one drop.
func TestGC_StaleReclaimLosesTheCondition(t *testing.T) {
	f, _, _, _, _, p, _ := reclaimFixture(t)
	ctx := context.Background()
	stale, err := f.LookupEntry(ctx, path.Dir(p), path.Base(p))
	if err != nil {
		t.Fatal(err)
	}
	if err := reclaimBinding(ctx, f, path.Dir(p), stale); err != nil {
		t.Fatal(err)
	}
	if err := reclaimBinding(ctx, f, path.Dir(p), stale); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("second reclaim: %v; want a precondition failure", err)
	}
	if len(f.dropped) != 1 {
		t.Fatalf("dropped %v; want one needle", f.dropped)
	}
}

// Two collectors, each with its own two marks: one reclaims, the other then
// skips the done binding.
func TestGC_TwoCollectorsReclaimOnce(t *testing.T) {
	f, b, _, g, now, _, _ := reclaimFixture(t)
	ctx := context.Background()
	other, err := NewCollector(g.cat, b, g.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Run(ctx); err != nil { // other's first mark
		t.Fatal(err)
	}
	if r, err := g.Run(ctx); err != nil || r.Reclaimed != 1 {
		t.Fatalf("first collector: %+v %v", r, err)
	}
	*now = now.Add(2*time.Hour + g.cfg.ClockSkew)
	if r, err := other.Run(ctx); err != nil || r.Reclaimed != 0 {
		t.Fatalf("second collector: %+v %v", r, err)
	}
	if len(f.dropped) != 1 {
		t.Fatalf("dropped %v; want one needle", f.dropped)
	}
}

// A reclaim whose reply was lost fails the pass uncounted; the next pass
// finds the done binding and skips it.
func TestGC_LostReclaimReplySkipsNextPass(t *testing.T) {
	f, _, _, g, _, p, _ := reclaimFixture(t)
	ctx := context.Background()
	lost := true
	f.failUpdateAfterApply = func(q string) error {
		if q == p && lost {
			lost = false
			return errors.New("connection reset")
		}
		return nil
	}
	if r, err := g.Run(ctx); err == nil || r.Reclaimed != 0 {
		t.Fatalf("lost reply: %+v %v; want the pass to fail uncounted", r, err)
	}
	if r, err := g.Run(ctx); err != nil || r.Reclaimed != 0 {
		t.Fatalf("next pass: %+v %v", r, err)
	}
	if len(f.dropped) != 1 {
		t.Fatalf("dropped %v; want one needle", f.dropped)
	}
}
