package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// chunkedRecords writes every control record as filer chunks for this test.
func chunkedRecords(t *testing.T) {
	t.Helper()
	old := recordInline
	recordInline = 0
	t.Cleanup(func() { recordInline = old })
}

func recordEntry(t *testing.T, f *fakeFiler, p string) *filerpb.Entry {
	t.Helper()
	e, err := f.LookupEntry(context.Background(), path.Dir(p), path.Base(p))
	if err != nil {
		t.Fatalf("%s: %v", p, err)
	}
	return e
}

func wantChunked(t *testing.T, f *fakeFiler, p string) {
	t.Helper()
	e := recordEntry(t, f, p)
	if len(e.GetContent()) != 0 || len(e.GetChunks()) == 0 {
		t.Fatalf("%s: content %d bytes, %d chunks; want a chunked record", p, len(e.GetContent()), len(e.GetChunks()))
	}
	if _, err := recordChunks(e); err != nil {
		t.Fatalf("%s: %v", p, err)
	}
}

// Below the threshold a payload is inline; at it, chunks of at most 4 MiB
// contiguous from 0 to FileSize.
func TestRecord_Threshold(t *testing.T) {
	ctx := context.Background()
	f, b := newFakeFiler(), newFakeBlobs()
	rf := recordFiler{f, b}
	for _, n := range []int{recordInlineMax - 1, recordInlineMax, 2*recordChunkMax + 5} {
		body := bytes.Repeat([]byte{byte(n)}, n)
		e := &filerpb.Entry{Name: "r", Content: body, Attributes: &filerpb.FuseAttributes{FileSize: uint64(n)}}
		if err := rf.CreateEntry(ctx, "/d", e, false); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(e.Content, body) || len(e.Chunks) != 0 {
			t.Fatalf("%d bytes: the caller's entry was changed", n)
		}
		got := recordEntry(t, f, "/d/r")
		chunks := len(got.GetChunks())
		switch {
		case n < recordInlineMax && (chunks != 0 || !bytes.Equal(got.GetContent(), body)):
			t.Fatalf("%d bytes: %d chunks; want inline", n, chunks)
		case n >= recordInlineMax && (len(got.GetContent()) != 0 || chunks != (n+recordChunkMax-1)/recordChunkMax):
			t.Fatalf("%d bytes: content %d, %d chunks", n, len(got.GetContent()), chunks)
		}
		_, read, err := readRecord(ctx, f, b, "/d", got)
		if err != nil || !bytes.Equal(read, body) {
			t.Fatalf("%d bytes: read %d bytes, %v", n, len(read), err)
		}
	}
}

// The decoder accepts exactly inline or contiguous plain chunks.
func TestRecord_ShapeRefusals(t *testing.T) {
	good := func() *filerpb.Entry {
		c0, _ := bindingChunk(testFid(3, 1), 4)
		c1, _ := bindingChunk(testFid(3, 2), 6)
		c1.Offset = 4
		return &filerpb.Entry{Name: "r", Chunks: []*filerpb.FileChunk{c0, c1}, Attributes: &filerpb.FuseAttributes{FileSize: 10}}
	}
	if _, err := recordChunks(good()); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*filerpb.Entry){
		"content and chunks": func(e *filerpb.Entry) { e.Content = []byte("x") },
		"gap":                func(e *filerpb.Entry) { e.Chunks[1].Offset = 5 },
		"short file size":    func(e *filerpb.Entry) { e.Attributes.FileSize = 9 },
		"size 0":             func(e *filerpb.Entry) { e.Chunks[0].Size = 0 },
		"manifest":           func(e *filerpb.Entry) { e.Chunks[1].IsChunkManifest = true },
		"compressed":         func(e *filerpb.Entry) { e.Chunks[0].IsCompressed = true },
		"ciphered":           func(e *filerpb.Entry) { e.Chunks[0].CipherKey = []byte{1} },
		"no file id":         func(e *filerpb.Entry) { e.Chunks[0].FileId = "" },
	} {
		e := good()
		edit(e)
		if _, err := recordChunks(e); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Every record of a volume's life works chunked: VOLUME, HEAD (CAS), a
// snapshot and its name record, a fork from it, intents.
func TestRecord_ChunkedLifecycle(t *testing.T) {
	chunkedRecords(t)
	f, b, c, h := catalogFixture(t)
	f.deleteChunk = gcBlobs{b}.deleteChunk
	ctx := context.Background()
	for _, p := range []string{"VOLUME", "HEAD"} {
		wantChunked(t, f, catBase+"/v1/"+p)
	}
	if got, ok, err := c.ReadHead(ctx, "v1"); err != nil || !ok || got.Gen != h.Gen || got.RemoteLSN != 42 {
		t.Fatalf("read HEAD: %+v %t %v", got, ok, err)
	}
	snap, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h)
	if err != nil {
		t.Fatal(err)
	}
	list, _, err := c.ListSnapshots(ctx, "v1", ListOptions{})
	if err != nil || len(list) != 1 || list[0].ManifestBytes == 0 || list[0].ManifestBytes != snap.ManifestBytes {
		t.Fatalf("snapshots %+v %v; want one with its record's byte size", list, err)
	}
	wantChunked(t, f, catBase+"/v1/snapnames/s1")
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "fork-1"}); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := c.ReadHead(ctx, "v2"); err != nil || !ok || got.ManifestChunkCount != 2 || len(got.Segments) != 1 {
		t.Fatalf("fork HEAD: %+v %t %v", got, ok, err)
	}
	r, err := NewChunkRemote(f, b, catBase+"/v1")
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.PutIntent(ctx, storage.ChunkIntent{Gen: 3, Epoch: 1, Holder: "h1", Keys: []string{"chunks/e1/c2/z-3"}})
	if err != nil {
		t.Fatal(err)
	}
	wantChunked(t, f, catBase+"/v1/intents/"+id)
	in, err := r.ListIntents(ctx)
	if err != nil || len(in) != 1 || in[0].ID != id {
		t.Fatalf("intents %+v %v", in, err)
	}
	// An intent is deleted with its data.
	intentChunks := recordEntry(t, f, catBase+"/v1/intents/"+id).GetChunks()
	dropped := len(f.dropped)
	if err := r.DeleteIntent(ctx, id); err != nil {
		t.Fatal(err)
	}
	if got := f.dropped[dropped:]; len(got) != len(intentChunks) || got[0] != intentChunks[0].GetFileId() {
		t.Fatalf("intent delete dropped %v; want its %d chunks", got, len(intentChunks))
	}
}

// A HEAD rewrite drops the old payload needle; the filer deletes it, and a
// reader that held the old entry reads the new version (R-READ, a race).
func TestRecord_ReaderRacingARewrite(t *testing.T) {
	chunkedRecords(t)
	f, b, _, h := catalogFixture(t)
	f.deleteChunk = gcBlobs{b}.deleteChunk
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, catBase+"/v1")
	if err != nil {
		t.Fatal(err)
	}
	old := recordEntry(t, f, catBase+"/v1/HEAD")
	next := h
	next.RemoteLSN = 50
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, next); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(f.dropped, " "), old.GetChunks()[0].GetFileId()) {
		t.Fatalf("rewrite dropped %v; want the old payload needle", f.dropped)
	}
	lookups := 0
	counted := &lookupCounter{Filer: f, n: &lookups}
	got, body, err := readRecord(ctx, counted, b, catBase+"/v1", old)
	if err != nil {
		t.Fatal(err)
	}
	head, err := decodeHead("HEAD", got, body)
	if err != nil || head.RemoteLSN != 50 || lookups != 1 {
		t.Fatalf("read %+v %v after %d re-reads; want the new HEAD after one", head, err, lookups)
	}
}

// A payload needle missing under an unchanged chunk list is a loss: an
// error, never an absent or empty record.
func TestRecord_LostPayloadIsAnError(t *testing.T) {
	chunkedRecords(t)
	f, b, c, _ := catalogFixture(t)
	ctx := context.Background()
	e := recordEntry(t, f, catBase+"/v1/HEAD")
	gcBlobs{b}.deleteChunk(e.GetChunks()[0].GetFileId())
	_, ok, err := c.ReadHead(ctx, "v1")
	if err == nil || ok || errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "payload lost") {
		t.Fatalf("read HEAD with its payload lost: ok=%t %v", ok, err)
	}
	r, _ := NewChunkRemote(f, b, catBase+"/v1")
	if h, err := r.GetHead(ctx); err == nil {
		t.Fatalf("remote read the lost HEAD as %+v", h)
	}
}

// Under sustained rewrites (every re-read names another missing needle) the
// read gives up after recordLookups lookups with a retryable error.
func TestRecord_BusyAfterThreeLookups(t *testing.T) {
	ctx := context.Background()
	b := newFakeBlobs()
	n := 0
	f := &rewritingFiler{next: func() *filerpb.Entry {
		n++
		c, _ := bindingChunk(testFid(3, uint64(1000+n)), 3) // never stored
		return &filerpb.Entry{Name: "HEAD", Chunks: []*filerpb.FileChunk{c}, Attributes: &filerpb.FuseAttributes{FileSize: 3}}
	}}
	_, _, err := lookupRecord(ctx, f, b, "/d", "HEAD")
	if !errors.Is(err, ErrRecordBusy) || n != 3 { // R-READ: at most 3 lookups
		t.Fatalf("err %v after %d lookups; want busy after 3", err, n)
	}
}

// An intent settled between its listing and its read is skipped.
func TestRecord_IntentGoneDuringListing(t *testing.T) {
	chunkedRecords(t)
	f, b := newFakeFiler(), newFakeBlobs()
	f.deleteChunk = gcBlobs{b}.deleteChunk
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, "/swblock/chunks/v1")
	if err != nil {
		t.Fatal(err)
	}
	gone, _ := r.PutIntent(ctx, storage.ChunkIntent{Gen: 2, Epoch: 1, Holder: "h1", Keys: []string{"a"}})
	kept, _ := r.PutIntent(ctx, storage.ChunkIntent{Gen: 3, Epoch: 1, Holder: "h1", Keys: []string{"b"}})
	f.beforeList = func() { _ = f.deleteLocked("/swblock/chunks/v1/intents/" + gone) }
	in, err := r.ListIntents(ctx)
	if err != nil || len(in) != 1 || in[0].ID != kept {
		t.Fatalf("intents %+v %v; want only %s", in, err, kept)
	}
}

// The GC mark is written and read back chunked.
func TestRecord_ChunkedGCMark(t *testing.T) {
	chunkedRecords(t)
	f, b, c, _, now := gcFixture(t)
	ctx := context.Background()
	cfg := GCConfig{Grace: time.Hour, IntentTTL: time.Hour, Now: func() time.Time { return *now }, IssuedEpoch: func(context.Context, string) (uint64, error) { return 1, nil }}
	for i := 0; i < 2; i++ {
		out, err := c.ExecuteGC(ctx, b, cfg, true)
		if err != nil || !out.MarkSaved {
			t.Fatalf("pass %d: %+v %v", i, out, err)
		}
		wantChunked(t, f, catBase+"/_GC_MARK")
		*now = now.Add(2 * time.Hour)
	}
}

// Verify reads chunked roots.
func TestRecord_ChunkedVerify(t *testing.T) {
	chunkedRecords(t)
	fx := newVerifyFixture(t, "v1")
	fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	wantChunked(t, fx.f, catBase+"/v1/HEAD")
	wantClasses(t, "chunked roots", fx.verify("v1", VerifyOptions{Depth: VerifyData}))
}

type lookupCounter struct {
	Filer
	n *int
}

func (l *lookupCounter) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	*l.n++
	return l.Filer.LookupEntry(ctx, dir, name)
}

// rewritingFiler returns a new version of its one record at every lookup.
type rewritingFiler struct {
	Filer
	next func() *filerpb.Entry
}

func (r *rewritingFiler) LookupEntry(context.Context, string, string) (*filerpb.Entry, error) {
	return r.next(), nil
}
