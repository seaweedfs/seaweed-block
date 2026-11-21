package chunkstore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A geometry with three segments' worth of chunks (one segment covers a
// logical GiB: 32768 chunks of 32 KiB), so a test can damage one segment
// and see the others still checked.
var verifyGeo = Geometry{NumBlocks: 3 * 32768 * 8, BlockSize: 4096, ChunkBlocks: 8}

const (
	verifyChunkBytes = 8 * 4096
	verifyPer        = 32768 // chunks per segment at verifyGeo
)

// countingBlobs counts reads per fid, and fails reads of a fid on request
// with a transient (not "not found") error.
type countingBlobs struct {
	gcBlobs
	mu   sync.Mutex
	gets map[string]int
	fail map[string]int // reads of fid still to fail; negative: every read
}

var errTransient = errors.New("read tcp 127.0.0.1:8080: connection reset by peer")

func (b *countingBlobs) Get(ctx context.Context, fid string) ([]byte, error) {
	b.mu.Lock()
	b.gets[fid]++
	n := b.fail[fid]
	if n > 0 {
		b.fail[fid] = n - 1
	}
	b.mu.Unlock()
	if n != 0 {
		return nil, fmt.Errorf("chunkstore: read %s: %w", fid, errTransient)
	}
	return b.gcBlobs.Get(ctx, fid)
}

// failReads makes the next n reads of fid fail (n < 0: all of them).
func (b *countingBlobs) failReads(fid string, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.fail[fid] = n
}

func (b *countingBlobs) reads(fid string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gets[fid]
}

type verifyFixture struct {
	t   *testing.T
	f   *fakeFiler
	b   gcBlobs
	cb  *countingBlobs
	c   *Catalog
	tag int
}

func newVerifyFixture(t *testing.T, vols ...string) *verifyFixture {
	t.Helper()
	fx := &verifyFixture{t: t, f: newFakeFiler(), b: gcBlobs{newFakeBlobs()}}
	fx.cb = &countingBlobs{gcBlobs: fx.b, gets: map[string]int{}, fail: map[string]int{}}
	c, err := NewCatalog(manifestFilerBlobs{fx.f, fx.cb}, catBase)
	if err != nil {
		t.Fatal(err)
	}
	fx.c = c
	for _, vol := range vols {
		if err := fx.f.CreateEntry(context.Background(), catBase, &filerpb.Entry{Name: vol, IsDirectory: true}, true); err != nil {
			t.Fatal(err)
		}
		if _, err := c.EnsureVolume(context.Background(), vol, verifyGeo); err != nil {
			t.Fatal(err)
		}
	}
	return fx
}

func verifyData(fill byte) []byte {
	b := make([]byte, verifyChunkBytes)
	for i := range b {
		b[i] = fill + byte(i%7)
	}
	return b
}

// publish installs a new HEAD for vol: the current chunks when keep is set,
// with set written under fresh keys over them.
func (fx *verifyFixture) publish(vol string, set map[uint64][]byte, keep bool) storage.ChunkHead {
	fx.t.Helper()
	ctx := context.Background()
	r, err := NewChunkRemote(fx.f, fx.b, path.Join(catBase, vol))
	if err != nil {
		fx.t.Fatal(err)
	}
	cur, err := r.GetHead(ctx)
	if err != nil {
		fx.t.Fatal(err)
	}
	next := storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: max(cur.RemoteLSN, 42) + 1, Chunks: map[uint64]storage.ChunkRef{}}
	if keep {
		for idx, ref := range cur.Chunks {
			next.Chunks[idx] = ref
		}
	}
	for idx, data := range set {
		fx.tag++
		key := fmt.Sprintf("chunks/t%d/c%d", fx.tag, idx)
		if err := r.Put(ctx, key, data); err != nil {
			fx.t.Fatal(err)
		}
		next.Chunks[idx] = storage.ChunkRef{Key: key, SHA256: sha256.Sum256(data)}
	}
	next, ids, err := r.PrepareManifest(ctx, cur, next, verifyChunkBytes)
	if err != nil {
		fx.t.Fatal(err)
	}
	gen, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, next)
	if err != nil {
		fx.t.Fatal(err)
	}
	next.Gen = gen
	for _, id := range ids {
		if err := r.DeleteIntent(ctx, id); err != nil {
			fx.t.Fatal(err)
		}
	}
	return next
}

// binding returns the directory and name of a qualified key's binding.
func bindingOf(t *testing.T, qkey string) (string, string) {
	t.Helper()
	owner, own, ok := strings.Cut(strings.TrimPrefix(qkey, "@"), "/")
	if !ok || !strings.HasPrefix(qkey, "@") {
		t.Fatalf("key %q is not owner-qualified", qkey)
	}
	return path.Join(catBase, owner, keysDir), url.PathEscape(own)
}

func (fx *verifyFixture) fid(qkey string) string {
	fx.t.Helper()
	dir, name := bindingOf(fx.t, qkey)
	e, err := fx.f.LookupEntry(context.Background(), dir, name)
	if err != nil {
		fx.t.Fatalf("binding of %s: %v", qkey, err)
	}
	return string(e.Extended[extFid])
}

func (fx *verifyFixture) deleteBinding(qkey string) {
	fx.t.Helper()
	dir, name := bindingOf(fx.t, qkey)
	if err := fx.f.DeleteEntry(context.Background(), dir, name, nil); err != nil {
		fx.t.Fatal(err)
	}
}

func (fx *verifyFixture) tombstone(qkey string) {
	fx.t.Helper()
	dir, name := bindingOf(fx.t, qkey)
	fx.f.bumpExtended(path.Join(dir, name), extGCDeleted, "done")
}

// needle edits the stored bytes of fid (nil deletes the needle).
func (fx *verifyFixture) needle(fid string, edit func([]byte) []byte) {
	fx.t.Helper()
	fx.b.mu.Lock()
	defer fx.b.mu.Unlock()
	d, ok := fx.b.data[fid]
	if !ok {
		fx.t.Fatalf("no needle %s", fid)
	}
	if out := edit(append([]byte(nil), d...)); out == nil {
		delete(fx.b.data, fid)
	} else {
		fx.b.data[fid] = out
	}
}

func flipByte(d []byte) []byte { d[len(d)/2] ^= 0xff; return d }
func truncate(d []byte) []byte { return d[:len(d)-4096] }
func dropNeedle([]byte) []byte { return nil }

func (fx *verifyFixture) verify(vol string, opts VerifyOptions) VerifyResult {
	fx.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := fx.c.Verify(ctx, vol, opts)
	if err != nil {
		fx.t.Fatalf("verify %s %+v: %v", vol, opts, err)
	}
	if !res.Complete {
		fx.t.Fatalf("verify %s: incomplete without an error: %+v", vol, res)
	}
	return res
}

// classes renders findings as "CLASS@index" (or "CLASS@seg<g>"), in order.
func classes(res VerifyResult) []string {
	var out []string
	for _, f := range res.Findings {
		switch {
		case f.Index != nil:
			out = append(out, fmt.Sprintf("%s@%d", f.Class, *f.Index))
		case f.Segment != nil:
			out = append(out, fmt.Sprintf("%s@seg%d", f.Class, *f.Segment))
		default:
			out = append(out, f.Class)
		}
	}
	return out
}

func wantClasses(t *testing.T, what string, res VerifyResult, want ...string) {
	t.Helper()
	got := classes(res)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s: findings %v, want %v\n%+v", what, got, want, res.Findings)
	}
	n := 0
	for _, c := range res.FindingsByClass {
		n += c
	}
	if n != len(res.Findings) {
		t.Fatalf("%s: findingsByClass %v does not count %d findings", what, res.FindingsByClass, len(res.Findings))
	}
}

func allDepths() []VerifyDepth { return []VerifyDepth{VerifyMeta, VerifyBindings, VerifyData} }

// A healthy volume with three segments and a snapshot verifies clean at
// every depth; the shared segments are read once.
func TestVerify_CleanVolumeAllDepths(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), verifyPer: verifyData(2), 2 * verifyPer: verifyData(3)}, false)
	if _, err := fx.c.CreateSnapshot(context.Background(), "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h); err != nil {
		t.Fatal(err)
	}
	for _, d := range allDepths() {
		res := fx.verify("v1", VerifyOptions{Depth: d, AllSnapshots: true})
		wantClasses(t, string(d), res)
		if len(res.HeadGens) != 1 || res.HeadGens[0] != h.Gen || len(res.Snapshots) != 1 || !strings.HasPrefix(res.Snapshots[0], "s1/") ||
			res.SegmentsChecked != 6 || res.SegmentReads != 3 || res.ChunksChecked != 6 || res.DistinctChunks != 3 {
			t.Fatalf("%s: summary %+v", d, res)
		}
		wantRead := 0
		if d == VerifyData {
			wantRead = 3
		}
		if res.DistinctChunksRead != wantRead || res.BytesRead != int64(wantRead*verifyChunkBytes) {
			t.Fatalf("%s: read %d chunks, %d bytes; want %d chunks", d, res.DistinctChunksRead, res.BytesRead, wantRead)
		}
	}
}

// The gap the default depth closes: GC's mark never looks a rooted key up,
// so a HEAD key whose binding is missing passes it; verify reports it.
func TestVerify_MissingBindingGCPassesVerifyReports(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	fx.deleteBinding(h.Chunks[1].Key)

	now := time.Now().Add(24 * time.Hour)
	g, err := NewCollector(fx.c, fx.b, GCConfig{Grace: time.Hour, IntentTTL: time.Hour, Now: func() time.Time { return now },
		IssuedEpoch: func(context.Context, string) (uint64, error) { return 1, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Preview(context.Background()); err != nil {
		t.Fatalf("GC mark: %v; the gap is that it passes", err)
	}

	wantClasses(t, "meta", fx.verify("v1", VerifyOptions{Depth: VerifyMeta}))
	res := fx.verify("v1", VerifyOptions{})
	wantClasses(t, "default depth", res, "BINDING_MISSING@1")
	f := res.Findings[0]
	if res.Depth != VerifyBindings || f.Volume != "v1" || f.Root != headLabel(h.Gen) || f.Owner != "v1" || !strings.HasPrefix(h.Chunks[1].Key, "@v1/"+f.Key) {
		t.Fatalf("finding %+v (depth %s)", f, res.Depth)
	}
}

// A flipped byte is invisible to metadata; depth data catches it.
func TestVerify_FlippedByteDataOnly(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	fx.needle(fx.fid(h.Chunks[0].Key), flipByte)
	wantClasses(t, "bindings", fx.verify("v1", VerifyOptions{Depth: VerifyBindings}))
	res := fx.verify("v1", VerifyOptions{Depth: VerifyData})
	wantClasses(t, "data", res, "CONTENT_MISMATCH@0")
	if res.Findings[0].FID != fx.fid(h.Chunks[0].Key) {
		t.Fatalf("finding fid %q", res.Findings[0].FID)
	}
}

func TestVerify_TruncatedNeedleSizeMismatch(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	fx.needle(fx.fid(h.Chunks[1].Key), truncate)
	wantClasses(t, "data", fx.verify("v1", VerifyOptions{Depth: VerifyData}), "SIZE_MISMATCH@1")
}

func TestVerify_MissingNeedle(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	fx.needle(fx.fid(h.Chunks[0].Key), dropNeedle)
	wantClasses(t, "bindings", fx.verify("v1", VerifyOptions{Depth: VerifyBindings}))
	res := fx.verify("v1", VerifyOptions{Depth: VerifyData})
	wantClasses(t, "data", res, "NEEDLE_MISSING@0")
	// The store said "not found": that answer is not retried.
	if res.ReadRetries != 0 || res.ChunksNotRead != 0 || fx.cb.reads(fx.fid(h.Chunks[0].Key)) != 1 {
		t.Fatalf("not-found needle: %d retries, %d not read, %d reads; want 0, 0, 1",
			res.ReadRetries, res.ChunksNotRead, fx.cb.reads(fx.fid(h.Chunks[0].Key)))
	}
}

// A read that fails once with an error other than "not found" is retried
// and then verifies: no finding.
func TestVerify_TransientReadIsRetried(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	chunk, seg := fx.fid(h.Chunks[0].Key), fx.fid(h.Segments[0].Key)
	fx.cb.failReads(chunk, 1)
	fx.cb.failReads(seg, 1)
	res := fx.verify("v1", VerifyOptions{Depth: VerifyData})
	wantClasses(t, "transient", res)
	if res.ReadRetries != 2 || res.ChunksNotRead != 0 || res.DistinctChunksRead != 2 || fx.cb.reads(chunk) != 2 || fx.cb.reads(seg) != 2 {
		t.Fatalf("transient errors: %d retries, %d not read, %d chunks read, reads chunk %d segment %d; want 2, 0, 2, 2, 2",
			res.ReadRetries, res.ChunksNotRead, res.DistinctChunksRead, fx.cb.reads(chunk), fx.cb.reads(seg))
	}
}

// A read that keeps failing with an error other than "not found" is
// READ_ERROR (not verified), never NEEDLE_MISSING or SEGMENT_UNREADABLE.
func TestVerify_PersistentReadErrorIsNotAMissingNeedle(t *testing.T) {
	t.Run("chunk", func(t *testing.T) {
		fx := newVerifyFixture(t, "v1")
		h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
		fid := fx.fid(h.Chunks[0].Key)
		fx.cb.failReads(fid, -1)
		res := fx.verify("v1", VerifyOptions{Depth: VerifyData})
		wantClasses(t, "chunk read error", res, "READ_ERROR@0")
		f := res.Findings[0]
		if res.ChunksNotRead != 1 || res.ReadRetries != 2 || fx.cb.reads(fid) != 3 || f.FID != fid ||
			!strings.Contains(f.Detail, "not verified") || !strings.Contains(f.Detail, errTransient.Error()) {
			t.Fatalf("chunk read error: %d not read, %d retries, %d reads, finding %+v", res.ChunksNotRead, res.ReadRetries, fx.cb.reads(fid), f)
		}
	})
	t.Run("segment", func(t *testing.T) {
		fx := newVerifyFixture(t, "v1")
		h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), verifyPer: verifyData(2)}, false)
		fx.cb.failReads(fx.fid(h.Segments[0].Key), -1)
		res := fx.verify("v1", VerifyOptions{})
		wantClasses(t, "segment read error", res, "READ_ERROR@seg0")
		if res.ChunksNotRead != 1 || res.ChunksChecked != 1 {
			t.Fatalf("segment read error: %+v", res)
		}
	})
}

// SWFS reports a needle absent (ErrBlobNotFound) only when every location
// answers 404; the error text is what it was before.
func TestSWFS_GetNotFoundIsErrBlobNotFound(t *testing.T) {
	server := func(status int) string {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		t.Cleanup(s.Close)
		return strings.TrimPrefix(s.URL, "http://")
	}
	missing, broken := server(http.StatusNotFound), server(http.StatusInternalServerError)
	// dropped closes every connection without an answer: a transport error.
	dropped := func() string {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
		}))
		t.Cleanup(s.Close)
		return strings.TrimPrefix(s.URL, "http://")
	}()
	statusOf := map[string]int{missing: http.StatusNotFound, broken: http.StatusInternalServerError}
	for _, tc := range []struct {
		name     string
		hosts    []string
		notFound bool
	}{
		{"every location 404", []string{missing, missing}, true},
		{"one location 500", []string{missing, broken}, false},
		{"only 500", []string{broken}, false},
		// Only the last answer is 404: still not proof of absence.
		{"500 then 404", []string{broken, missing}, false},
		{"transport error then 404", []string{dropped, missing}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var locs []*filerpb.Location
			for _, h := range tc.hosts {
				locs = append(locs, &filerpb.Location{Url: h})
			}
			c := startFakeFiler(t, &fakeFilerGRPC{lookupVol: &filerpb.LookupVolumeResponse{LocationsMap: map[string]*filerpb.Locations{"7": {Locations: locs}}}})
			_, err := c.Get(context.Background(), "7,0000dead")
			last := tc.hosts[len(tc.hosts)-1]
			status := statusOf[last]
			if err == nil || errors.Is(err, ErrBlobNotFound) != tc.notFound || err.Error() != fmt.Sprintf("chunkstore: read 7,0000dead: http %d from %s", status, last) {
				t.Fatalf("Get: %v (not found %v), want not found %v", err, errors.Is(err, ErrBlobNotFound), tc.notFound)
			}
		})
	}
}

// volumeAt is a LookupVolume answer placing volume vid at hosts.
func volumeAt(vid string, hosts ...string) *filerpb.LookupVolumeResponse {
	var locs []*filerpb.Location
	for _, h := range hosts {
		locs = append(locs, &filerpb.Location{Url: h})
	}
	return &filerpb.LookupVolumeResponse{LocationsMap: map[string]*filerpb.Locations{vid: {Locations: locs}}}
}

func (f *fakeFilerGRPC) volumeLookups() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.volLookups
}

// Cached locations can be stale (the volume moved): 404 from every cached
// location is not proof of absence. Get looks the volume up again and reads
// from the fresh locations; only 404 from every fresh location is
// ErrBlobNotFound. A read the cache serves makes no lookup.
func TestSWFS_GetStaleCachedLocationLooksUpAgain(t *testing.T) {
	body := []byte("needle bytes")
	server := func(status int) string {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			if status == http.StatusOK {
				_, _ = w.Write(body)
			}
		}))
		t.Cleanup(s.Close)
		return strings.TrimPrefix(s.URL, "http://")
	}
	oldLoc, newLoc, alsoMissing := server(http.StatusNotFound), server(http.StatusOK), server(http.StatusNotFound)
	const fid = "7,0000dead"
	for _, tc := range []struct {
		name        string
		cached      []string // nil: nothing cached
		fresh       string
		wantBody    bool
		wantLookups int
	}{
		{"cached location 404, fresh location has it", []string{oldLoc}, newLoc, true, 1},
		{"cached and fresh locations 404", []string{oldLoc}, alsoMissing, false, 1},
		{"warm cache", []string{newLoc}, newLoc, true, 0},
		{"nothing cached, fresh location 404", nil, alsoMissing, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeFilerGRPC{lookupVol: volumeAt("7", tc.fresh)}
			c := startFakeFiler(t, f)
			if tc.cached != nil {
				c.mu.Lock()
				c.vidCache["7"] = tc.cached
				c.mu.Unlock()
			}
			b, err := c.Get(context.Background(), fid)
			if n := f.volumeLookups(); n != tc.wantLookups {
				t.Fatalf("Get made %d volume lookups, want %d (err %v)", n, tc.wantLookups, err)
			}
			if tc.wantBody {
				if err != nil || string(b) != string(body) {
					t.Fatalf("Get: %q, %v; want the needle from %s", b, err, tc.fresh)
				}
				return
			}
			if !errors.Is(err, ErrBlobNotFound) || err.Error() != "chunkstore: read "+fid+": http 404 from "+tc.fresh {
				t.Fatalf("Get: %v (not found %v); want ErrBlobNotFound from the fresh location %s", err, errors.Is(err, ErrBlobNotFound), tc.fresh)
			}
		})
	}
}

// A volume that moved after its location was cached is not missing: depth
// data through SWFS reads every needle from the new location, no finding.
func TestVerify_MovedVolumeIsNotAMissingNeedle(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	var mu sync.Mutex
	moved := false
	serve := func(old bool) string {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			gone := old && moved
			mu.Unlock()
			fx.b.mu.Lock()
			d, ok := fx.b.data[strings.TrimPrefix(r.URL.Path, "/")]
			fx.b.mu.Unlock()
			if gone || !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write(d)
		}))
		t.Cleanup(s.Close)
		return strings.TrimPrefix(s.URL, "http://")
	}
	oldLoc, newLoc := serve(true), serve(false)
	vid, _, _ := strings.Cut(fx.fid(h.Chunks[0].Key), ",")
	f := &fakeFilerGRPC{lookupVol: volumeAt(vid, oldLoc)}
	sw := startFakeFiler(t, f)
	ctx := context.Background()
	// Warm the location cache at the old location, then move the volume.
	if _, err := sw.Get(ctx, fx.fid(h.Chunks[0].Key)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	moved = true
	mu.Unlock()
	f.mu.Lock()
	f.lookupVol = volumeAt(vid, newLoc)
	f.mu.Unlock()

	cat, err := NewCatalog(manifestFilerBlobs{fx.f, sw}, catBase)
	if err != nil {
		t.Fatal(err)
	}
	res, err := cat.Verify(ctx, "v1", VerifyOptions{Depth: VerifyData, BytesPerSecond: -1})
	if err != nil || !res.Complete {
		t.Fatalf("verify: %v, %+v", err, res)
	}
	wantClasses(t, "moved volume", res)
	if res.DistinctChunksRead != 2 || res.SegmentReads != 1 {
		t.Fatalf("read %d chunks, %d segments; want 2, 1", res.DistinctChunksRead, res.SegmentReads)
	}
}

// A root that names a binding GC tombstoned: the chunk is gone or going.
func TestVerify_TombstonedBinding(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	fx.tombstone(h.Chunks[0].Key)
	fx.needle(fx.fid(h.Chunks[0].Key), dropNeedle)
	wantClasses(t, "bindings", fx.verify("v1", VerifyOptions{}), "BINDING_TOMBSTONED@0")
	// No read of a tombstoned binding's needle: one finding, not two.
	res := fx.verify("v1", VerifyOptions{Depth: VerifyData})
	wantClasses(t, "data", res, "BINDING_TOMBSTONED@0")
	if res.DistinctChunksRead != 1 {
		t.Fatalf("read %d chunks, want only the sound one", res.DistinctChunksRead)
	}
}

// A corrupt segment and a segment whose binding is gone are each a finding;
// the pass goes on to the other segments and their chunks.
func TestVerify_UnreadableSegmentContinues(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), verifyPer: verifyData(2), 2 * verifyPer: verifyData(3)}, false)
	fx.needle(fx.fid(h.Segments[0].Key), flipByte)
	fx.deleteBinding(h.Segments[1].Key)
	fx.deleteBinding(h.Chunks[2*verifyPer].Key)
	res := fx.verify("v1", VerifyOptions{})
	wantClasses(t, "bindings", res, "SEGMENT_UNREADABLE@seg0", "SEGMENT_UNREADABLE@seg1", fmt.Sprintf("BINDING_MISSING@%d", 2*verifyPer))
	if res.SegmentsChecked != 3 || res.ChunksChecked != 1 {
		t.Fatalf("summary %+v", res)
	}
	if !strings.Contains(res.Findings[0].Detail, "hash") || res.Findings[0].Key != h.Segments[0].Key {
		t.Fatalf("corrupt segment finding %+v", res.Findings[0])
	}
}

// A chunk that leaves HEAD and is collected while the pass runs is not a
// finding under HEAD (it is counted as superseded during verify); the same
// miss under a snapshot that still names it is one.
func TestVerify_SupersededDuringPass(t *testing.T) {
	type damage struct {
		name  string
		depth VerifyDepth
		apply func(fx *verifyFixture, qkey string)
		class string
	}
	cases := []damage{
		{"binding deleted", VerifyBindings, func(fx *verifyFixture, k string) {
			fid := fx.fid(k)
			fx.deleteBinding(k)
			fx.needle(fid, dropNeedle)
		}, FindingBindingMissing},
		{"binding tombstoned", VerifyBindings, func(fx *verifyFixture, k string) {
			fx.tombstone(k)
			fx.needle(fx.fid(k), dropNeedle)
		}, FindingBindingTombstoned},
		{"needle deleted", VerifyData, func(fx *verifyFixture, k string) { fx.needle(fx.fid(k), dropNeedle) }, FindingNeedleMissing},
		{"needle overwritten", VerifyData, func(fx *verifyFixture, k string) { fx.needle(fx.fid(k), flipByte) }, FindingContentMismatch},
		{"needle unreadable", VerifyData, func(fx *verifyFixture, k string) { fx.cb.failReads(fx.fid(k), -1) }, FindingReadError},
	}
	for _, tc := range cases {
		for _, withSnap := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/snapshot=%v", tc.name, withSnap), func(t *testing.T) {
				fx := newVerifyFixture(t, "v1")
				h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
				if withSnap {
					if _, err := fx.c.CreateSnapshot(context.Background(), "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h); err != nil {
						t.Fatal(err)
					}
				}
				old := h.Chunks[0].Key
				hooked := 0
				opts := VerifyOptions{Depth: tc.depth, AllSnapshots: withSnap}
				opts.hook = func(stage string) {
					if stage != verifyHookHeadWalked {
						return
					}
					hooked++
					// After verify read HEAD: the owner replaces chunk 0 and GC
					// collects the old one.
					fx.publish("v1", map[uint64][]byte{0: verifyData(9)}, true)
					tc.apply(fx, old)
				}
				res := fx.verify("v1", opts)
				if !withSnap {
					wantClasses(t, "HEAD only", res)
				} else {
					wantClasses(t, "with the snapshot", res, tc.class+"@0")
					if !strings.HasPrefix(res.Findings[0].Root, "snapshot s1/") {
						t.Fatalf("finding root %q, want the snapshot", res.Findings[0].Root)
					}
				}
				if hooked != 1 || res.SupersededDuringVerify != 1 || len(res.HeadGens) != 1 || res.HeadGens[0] != h.Gen {
					t.Fatalf("hooked %d, superseded %d, gens %v; want 1, 1, [%d]", hooked, res.SupersededDuringVerify, res.HeadGens, h.Gen)
				}
			})
		}
	}
}

// A miss under a snapshot is a finding only while that attempt is still
// active: a snapshot deleted during the pass, whose chunk is then
// collected, is counted as root deleted during verify and listed as not
// fully covered, not reported.
func TestVerify_SnapshotDeletedDuringPass(t *testing.T) {
	cases := []struct {
		name  string
		depth VerifyDepth
		apply func(fx *verifyFixture, qkey string)
		class string
	}{
		{"binding deleted", VerifyBindings, func(fx *verifyFixture, k string) {
			fid := fx.fid(k)
			fx.deleteBinding(k)
			fx.needle(fid, dropNeedle)
		}, FindingBindingMissing},
		{"needle overwritten", VerifyData, func(fx *verifyFixture, k string) { fx.needle(fx.fid(k), flipByte) }, FindingContentMismatch},
		{"needle truncated", VerifyData, func(fx *verifyFixture, k string) { fx.needle(fx.fid(k), truncate) }, FindingSizeMismatch},
		{"needle unreadable", VerifyData, func(fx *verifyFixture, k string) { fx.cb.failReads(fx.fid(k), -1) }, FindingReadError},
	}
	for _, tc := range cases {
		for _, deleteSnap := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deleted=%v", tc.name, deleteSnap), func(t *testing.T) {
				fx := newVerifyFixture(t, "v1")
				ctx := context.Background()
				h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
				s, err := fx.c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h)
				if err != nil {
					t.Fatal(err)
				}
				// HEAD moves on before the pass: only the snapshot names the old chunk 0.
				fx.publish("v1", map[uint64][]byte{0: verifyData(9)}, true)
				old := h.Chunks[0].Key
				opts := VerifyOptions{Depth: tc.depth, AllSnapshots: true}
				opts.hook = func(stage string) {
					if stage != verifyHookRootsWalked {
						return
					}
					if deleteSnap {
						if st, err := fx.c.DeleteSnapshot(ctx, "v1", "s1"); err != nil || st != SnapDeleting {
							t.Errorf("delete s1: %q %v", st, err)
						}
					}
					tc.apply(fx, old)
				}
				res := fx.verify("v1", opts)
				if len(res.Snapshots) != 1 {
					t.Fatalf("snapshots checked %v", res.Snapshots)
				}
				if !deleteSnap {
					wantClasses(t, "active snapshot", res, tc.class+"@0")
					if res.RootDeletedDuringVerify != 0 || len(res.SnapshotsDeletedDuringVerify) != 0 || !strings.HasPrefix(res.Findings[0].Root, "snapshot s1/") {
						t.Fatalf("active snapshot: %+v", res)
					}
					return
				}
				wantClasses(t, "deleted snapshot", res)
				if res.RootDeletedDuringVerify != 1 || res.SupersededDuringVerify != 0 ||
					len(res.SnapshotsDeletedDuringVerify) != 1 || res.SnapshotsDeletedDuringVerify[0] != res.Snapshots[0] {
					t.Fatalf("deleted snapshot: root deleted %d, superseded %d, deleted list %v (checked %v, created %+v)",
						res.RootDeletedDuringVerify, res.SupersededDuringVerify, res.SnapshotsDeletedDuringVerify, res.Snapshots, s.Name)
				}
			})
		}
	}
}

// A chunk named by HEAD (superseded during the pass) and by a snapshot that
// is still active is one finding, under the snapshot.
func TestVerify_SharedChunkReportedUnderStillNamingRoot(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	ctx := context.Background()
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	if _, err := fx.c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h); err != nil {
		t.Fatal(err)
	}
	opts := VerifyOptions{Depth: VerifyData, AllSnapshots: true}
	opts.hook = func(stage string) {
		if stage == verifyHookRootsWalked {
			fx.publish("v1", map[uint64][]byte{0: verifyData(9)}, true)
			fx.needle(fx.fid(h.Chunks[0].Key), dropNeedle)
		}
	}
	res := fx.verify("v1", opts)
	wantClasses(t, "shared chunk", res, "NEEDLE_MISSING@0")
	if !strings.HasPrefix(res.Findings[0].Root, "snapshot s1/") || res.SupersededDuringVerify != 1 || res.RootDeletedDuringVerify != 0 {
		t.Fatalf("finding under %q, superseded %d, root deleted %d; want the snapshot, 1, 0",
			res.Findings[0].Root, res.SupersededDuringVerify, res.RootDeletedDuringVerify)
	}
}

// A HEAD that advances mid-pass: the result lists both generations, and
// only the segment whose key changed is read again.
func TestVerify_HeadRefreshRewalksChangedSegments(t *testing.T) {
	for _, d := range []VerifyDepth{VerifyBindings, VerifyData} {
		t.Run(string(d), func(t *testing.T) {
			fx := newVerifyFixture(t, "v1")
			h1 := fx.publish("v1", map[uint64][]byte{0: verifyData(1), verifyPer: verifyData(2), 2 * verifyPer: verifyData(3)}, false)
			var h2 storage.ChunkHead
			opts := VerifyOptions{Depth: d, RefreshInterval: time.Millisecond}
			opts.hook = func(stage string) {
				if stage == verifyHookHeadWalked {
					h2 = fx.publish("v1", map[uint64][]byte{verifyPer: verifyData(7)}, true)
					time.Sleep(3 * time.Millisecond) // past the refresh interval
				}
			}
			res := fx.verify("v1", opts)
			wantClasses(t, "refresh", res)
			if len(res.HeadGens) != 2 || res.HeadGens[0] != h1.Gen || res.HeadGens[1] != h2.Gen {
				t.Fatalf("covered gens %v, want [%d %d]", res.HeadGens, h1.Gen, h2.Gen)
			}
			if h2.Segments[0] != h1.Segments[0] || h2.Segments[2] != h1.Segments[2] || h2.Segments[1] == h1.Segments[1] {
				t.Fatal("fixture: the new HEAD should change only segment 1")
			}
			if res.SegmentReads != 4 || res.SegmentsChecked != 6 {
				t.Fatalf("segment reads %d (checked %d); want 3 + 1 changed", res.SegmentReads, res.SegmentsChecked)
			}
			if res.DistinctChunks != 4 || res.ChunksChecked != 4 {
				t.Fatalf("chunks %d distinct, %d checked; want the 3 first and the 1 new", res.DistinctChunks, res.ChunksChecked)
			}
			if d == VerifyData && (res.DistinctChunksRead != 4 || fx.cb.reads(fx.fid(h2.Chunks[verifyPer].Key)) != 1) {
				t.Fatalf("read %d chunks; want 4, the new one included", res.DistinctChunksRead)
			}
		})
	}
}

// A chunk shared by HEAD and two snapshots is fetched once.
func TestVerify_DedupSharedChunkReadOnce(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	for _, n := range []string{"s1", "s2"} {
		if _, err := fx.c.CreateSnapshot(context.Background(), "v1", SnapshotRequest{Name: n, RequestID: "r-" + n}, h); err != nil {
			t.Fatal(err)
		}
	}
	res := fx.verify("v1", VerifyOptions{Depth: VerifyData, AllSnapshots: true})
	wantClasses(t, "dedup", res)
	if res.ChunksChecked != 6 || res.DistinctChunks != 2 || res.DistinctChunksRead != 2 || len(res.Snapshots) != 2 {
		t.Fatalf("summary %+v", res)
	}
	for _, idx := range []uint64{0, 1} {
		if n := fx.cb.reads(fx.fid(h.Chunks[idx].Key)); n != 1 {
			t.Fatalf("chunk %d read %d times, want once", idx, n)
		}
	}
	// One named snapshot works the same way.
	res = fx.verify("v1", VerifyOptions{Depth: VerifyBindings, Snapshot: "s2"})
	if len(res.Snapshots) != 1 || !strings.HasPrefix(res.Snapshots[0], "s2/") || res.ChunksChecked != 4 {
		t.Fatalf("named snapshot: %+v", res)
	}
	if _, err := fx.c.Verify(context.Background(), "v1", VerifyOptions{Snapshot: "nope"}); CodeOf(err) != CodeSnapshotNotFound {
		t.Fatalf("missing snapshot: %v", err)
	}
}

// A fork's qualified keys are checked in the owner volume's keys/, and its
// fork record against the parent snapshot.
func TestVerify_ForkQualifiedKeysAndRecord(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	ctx := context.Background()
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	if _, err := fx.c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "snap"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "fork"}); err != nil {
		t.Fatal(err)
	}
	for _, d := range allDepths() {
		wantClasses(t, "clean fork "+string(d), fx.verify("v2", VerifyOptions{Depth: d}))
	}
	// The fork wrote no binding of its own: every chunk resolves to v1.
	fx.deleteBinding(h.Chunks[1].Key)
	res := fx.verify("v2", VerifyOptions{})
	wantClasses(t, "fork, parent binding gone", res, "BINDING_MISSING@1")
	if res.Findings[0].Owner != "v1" || res.Findings[0].Volume != "v2" {
		t.Fatalf("finding %+v, want owner v1 in volume v2", res.Findings[0])
	}

	// A fork record whose digest is not the parent snapshot's.
	e, err := fx.f.LookupEntry(ctx, path.Join(catBase, "v2"), volumeName)
	if err != nil {
		t.Fatal(err)
	}
	var vi VolumeInfo
	if err := json.Unmarshal(e.Content, &vi); err != nil {
		t.Fatal(err)
	}
	vi.Fork.HeadSHA256 = strings.Repeat("0", 64)
	if e.Content, err = json.Marshal(vi); err != nil {
		t.Fatal(err)
	}
	if err := fx.f.UpdateEntry(ctx, path.Join(catBase, "v2"), e, nil); err != nil {
		t.Fatal(err)
	}
	res = fx.verify("v2", VerifyOptions{Depth: VerifyMeta})
	wantClasses(t, "tampered fork record", res, "FORK_RECORD_MISMATCH")
	if res.Findings[0].Owner != "v1" || !strings.Contains(res.Findings[0].Detail, "s1") {
		t.Fatalf("finding %+v", res.Findings[0])
	}
}

// A fork whose source volume was deleted afterwards has nothing to compare
// its fork record with. That is not damage: no finding, and the result
// says the source is gone. While the source volume still exists, a missing
// parent snapshot attempt is a finding.
func TestVerify_ForkSourceVolumeDeleted(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	ctx := context.Background()
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1)}, false)
	if _, err := fx.c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "snap"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "fork"}); err != nil {
		t.Fatal(err)
	}
	// The parent's snapshot attempts vanish, the source volume remains.
	dir := path.Join(catBase, "v1", snapsDir, "s1")
	es, err := fx.f.ListEntries(ctx, dir, "", "", 100)
	if err != nil || len(es) == 0 {
		t.Fatalf("attempts of s1: %d, %v", len(es), err)
	}
	for _, e := range es {
		if err := fx.f.DeleteEntry(ctx, dir, e.GetName(), nil); err != nil {
			t.Fatal(err)
		}
	}
	res := fx.verify("v2", VerifyOptions{Depth: VerifyMeta})
	wantClasses(t, "attempts gone, source present", res, "FORK_RECORD_MISMATCH")
	if res.ForkSourceGone {
		t.Fatal("ForkSourceGone set while the source volume exists")
	}
	// Now the source volume record goes too.
	if err := fx.f.DeleteEntry(ctx, path.Join(catBase, "v1"), volumeName, nil); err != nil {
		t.Fatal(err)
	}
	res = fx.verify("v2", VerifyOptions{Depth: VerifyMeta})
	wantClasses(t, "source volume deleted", res)
	if !res.ForkSourceGone {
		t.Fatal("ForkSourceGone not set")
	}
}

// Intent bookkeeping: an old capture intent below HEAD's epoch naming keys
// HEAD does not reference is reported; one naming a key HEAD binds, a
// current one and a copy intent are not.
func TestVerify_IntentStale(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	ctx := context.Background()
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1)}, false)
	root := path.Join(catBase, "v1")
	headKey := strings.TrimPrefix(h.Chunks[0].Key, "@v1/")
	stale, err := putIntent(ctx, fx.f, root, storage.ChunkIntent{Gen: 1, Epoch: 0, Holder: "old/r1", Keys: []string{"chunks/lost/k"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range []storage.ChunkIntent{
		{Gen: 1, Epoch: 0, Holder: "old/r1", Keys: []string{headKey}},                          // HEAD binds its key
		{Gen: 2, Epoch: 1, Holder: "h1", Keys: []string{"chunks/next/k"}},                      // the current epoch
		{Gen: 0, Epoch: 0, Holder: "snapshot:v1/s9/1-0000000000000000", Keys: []string{"x/y"}}, // a copy intent
	} {
		if _, err := putIntent(ctx, fx.f, root, in); err != nil {
			t.Fatal(err)
		}
	}
	wantClasses(t, "young intents", fx.verify("v1", VerifyOptions{}))
	fx.c.SetClock(func() time.Time { return time.Now().Add(2 * time.Hour) })
	res := fx.verify("v1", VerifyOptions{Depth: VerifyMeta})
	wantClasses(t, "old intents", res, "INTENT_STALE")
	if res.Findings[0].Root != "intent "+stale {
		t.Fatalf("stale intent finding %+v, want intent %s", res.Findings[0], stale)
	}
}

// recordingStore counts every write the catalog makes through it.
type recordingStore struct {
	Filer
	blobs                        *countingBlobs
	mu                           sync.Mutex
	creates, updates, deletes    int
	blobPuts, blobDeletes, reads int
}

func (r *recordingStore) CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, o bool) error {
	r.mu.Lock()
	r.creates++
	r.mu.Unlock()
	return r.Filer.CreateEntry(ctx, dir, e, o)
}
func (r *recordingStore) UpdateEntry(ctx context.Context, dir string, e *filerpb.Entry, x map[string][]byte) error {
	r.mu.Lock()
	r.updates++
	r.mu.Unlock()
	return r.Filer.UpdateEntry(ctx, dir, e, x)
}
func (r *recordingStore) DeleteEntry(ctx context.Context, dir, name string, x map[string][]byte) error {
	r.mu.Lock()
	r.deletes++
	r.mu.Unlock()
	return r.Filer.DeleteEntry(ctx, dir, name, x)
}
func (r *recordingStore) Put(ctx context.Context, d []byte) (string, error) {
	r.mu.Lock()
	r.blobPuts++
	r.mu.Unlock()
	return r.blobs.Put(ctx, d)
}
func (r *recordingStore) Get(ctx context.Context, fid string) ([]byte, error) {
	r.mu.Lock()
	r.reads++
	r.mu.Unlock()
	return r.blobs.Get(ctx, fid)
}
func (r *recordingStore) DeleteBlob(ctx context.Context, fid string) error {
	r.mu.Lock()
	r.blobDeletes++
	r.mu.Unlock()
	return r.blobs.DeleteBlob(ctx, fid)
}

// Verify writes nothing, at any depth, also on the miss, superseded and
// refresh paths and for a fork.
func TestVerify_ReadOnlyAtEveryDepth(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	ctx := context.Background()
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2), verifyPer: verifyData(3)}, false)
	if _, err := fx.c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "snap"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := fx.c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "fork"}); err != nil {
		t.Fatal(err)
	}
	if _, err := putIntent(ctx, fx.f, path.Join(catBase, "v1"), storage.ChunkIntent{Gen: 1, Holder: "old", Keys: []string{"k/old"}}); err != nil {
		t.Fatal(err)
	}
	fx.needle(fx.fid(h.Chunks[1].Key), flipByte)
	rec := &recordingStore{Filer: fx.f, blobs: fx.cb}
	cat, err := NewCatalog(rec, catBase)
	if err != nil {
		t.Fatal(err)
	}
	cat.SetClock(func() time.Time { return time.Now().Add(2 * time.Hour) })
	for _, d := range allDepths() {
		for _, vol := range []string{"v1", "v2"} {
			opts := VerifyOptions{Depth: d, AllSnapshots: true, RefreshInterval: time.Millisecond}
			opts.hook = func(stage string) {
				if vol == "v1" && stage == verifyHookHeadWalked {
					// Writes by the owner (through the fixture, not the
					// recorder): a new HEAD, and the old chunk 0 collected.
					r, _ := NewChunkRemote(fx.f, fx.b, path.Join(catBase, "v1"))
					cur, err := r.GetHead(ctx)
					if err != nil {
						t.Error(err)
						return
					}
					oldFid := fx.fid(cur.Chunks[0].Key)
					fx.publish("v1", map[uint64][]byte{0: verifyData(byte(10 + len(d)))}, true)
					fx.needle(oldFid, dropNeedle)
					time.Sleep(3 * time.Millisecond)
				}
			}
			res, err := cat.Verify(ctx, vol, opts)
			if err != nil || !res.Complete {
				t.Fatalf("%s %s: %v", d, vol, err)
			}
			if d == VerifyData && rec.reads == 0 {
				t.Fatalf("%s %s: no blob reads recorded; the recorder is not in the path", d, vol)
			}
		}
	}
	if rec.creates+rec.updates+rec.deletes+rec.blobPuts+rec.blobDeletes != 0 {
		t.Fatalf("verify wrote: %d creates, %d updates, %d deletes, %d blob puts, %d blob deletes",
			rec.creates, rec.updates, rec.deletes, rec.blobPuts, rec.blobDeletes)
	}
}

// A chunk index past the end of VOLUME's geometry is a GEOMETRY finding.
func TestVerify_GeometryOutsideVolume(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	ctx := context.Background()
	fx.publish("v1", map[uint64][]byte{0: verifyData(1), 2 * verifyPer: verifyData(2)}, false)
	e, err := fx.f.LookupEntry(ctx, path.Join(catBase, "v1"), volumeName)
	if err != nil {
		t.Fatal(err)
	}
	var vi VolumeInfo
	if err := json.Unmarshal(e.Content, &vi); err != nil {
		t.Fatal(err)
	}
	vi.NumBlocks = 2 * verifyPer * 8
	if e.Content, err = json.Marshal(vi); err != nil {
		t.Fatal(err)
	}
	if err := fx.f.UpdateEntry(ctx, path.Join(catBase, "v1"), e, nil); err != nil {
		t.Fatal(err)
	}
	wantClasses(t, "meta", fx.verify("v1", VerifyOptions{Depth: VerifyMeta}), fmt.Sprintf("GEOMETRY@%d", 2*verifyPer))
}

// A HEAD that does not decode is a finding, not a failure to run.
func TestVerify_InvalidHeadRoot(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	fx.publish("v1", map[uint64][]byte{0: verifyData(1)}, false)
	fx.f.bumpExtended(path.Join(catBase, "v1", headName), extGen, "999")
	res := fx.verify("v1", VerifyOptions{})
	wantClasses(t, "bad HEAD", res, "ROOT_INVALID")
	if len(res.HeadGens) != 0 {
		t.Fatalf("covered gens %v of an invalid HEAD", res.HeadGens)
	}
}

// Only an inability to continue stops the pass: the result then says it is
// incomplete and keeps what was covered.
func TestVerify_ContextEndedIsIncomplete(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := VerifyOptions{Depth: VerifyData}
	opts.hook = func(stage string) { cancel() }
	res, err := fx.c.Verify(ctx, "v1", opts)
	if !errors.Is(err, context.Canceled) || res.Complete || len(res.HeadGens) != 1 || res.HeadGens[0] != h.Gen {
		t.Fatalf("cancelled pass: %v, %+v", err, res)
	}
	if _, err := fx.c.Verify(context.Background(), "nope", VerifyOptions{}); CodeOf(err) != CodeNotChunkVolume {
		t.Fatalf("missing volume: %v", err)
	}
}

// heldReads holds the read of one needle open until the reader's context
// ends (or the test releases it), and counts the reads in progress.
type heldReads struct {
	Blobs
	fid     string
	onEnter func()        // runs once, when the held read has started
	entered chan struct{} // closed when the held read has started
	once    sync.Once

	released    chan struct{} // closed by release: the escape when cancellation fails
	releaseOnce sync.Once

	mu     sync.Mutex
	active int
	ended  error // the held read's context error, once its context ended
}

func newHeldReads(b Blobs, fid string) *heldReads {
	return &heldReads{Blobs: b, fid: fid, entered: make(chan struct{}), released: make(chan struct{})}
}

func (h *heldReads) release() { h.releaseOnce.Do(func() { close(h.released) }) }

// verifyEscape bounds a test's wait for a Verify that should return at
// once; it only turns a hang into a failure, it is not a success criterion.
const verifyEscape = 30 * time.Second

type verifyOutcome struct {
	res VerifyResult
	err error
}

// verifyBounded runs cat.Verify in a goroutine and returns its result, or
// fails the test when it has not returned within verifyEscape. The cleanup,
// registered before the launch, releases the held read and joins the call.
func verifyBounded(t *testing.T, cat *Catalog, ctx context.Context, opts VerifyOptions, held *heldReads) (VerifyResult, error) {
	t.Helper()
	done := make(chan verifyOutcome, 1)
	t.Cleanup(func() {
		held.release()
		select {
		case out := <-done:
			done <- out
		case <-time.After(verifyEscape):
			t.Error("Verify did not return even after its held read was released")
		}
	})
	go func() {
		res, err := cat.Verify(ctx, "v1", opts)
		done <- verifyOutcome{res, err}
	}()
	select {
	case out := <-done:
		done <- out // for the cleanup's join
		return out.res, out.err
	case <-time.After(verifyEscape):
		t.Fatalf("Verify did not return within %s: the read in progress was not cancelled and joined", verifyEscape)
		return VerifyResult{}, nil
	}
}

func (h *heldReads) Get(ctx context.Context, fid string) ([]byte, error) {
	h.mu.Lock()
	h.active++
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		h.active--
		h.mu.Unlock()
	}()
	if fid != h.fid {
		return h.Blobs.Get(ctx, fid)
	}
	h.once.Do(func() {
		close(h.entered)
		if h.onEnter != nil {
			h.onEnter()
		}
	})
	select {
	case <-ctx.Done():
	case <-h.released:
		return nil, fmt.Errorf("chunkstore: read %s: released by the test", fid)
	}
	h.mu.Lock()
	h.ended = ctx.Err()
	h.mu.Unlock()
	return nil, fmt.Errorf("chunkstore: read %s: %w", fid, ctx.Err())
}

func (h *heldReads) state() (active int, ended error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.active, h.ended
}

var errFilerDown = errors.New("filer: connection refused")

// tickingClock advances by step at every reading: with a refresh interval
// of step, a pass refreshes HEAD at every opportunity, without waiting.
func tickingClock(step time.Duration) func() time.Time {
	var mu sync.Mutex
	now := time.Now()
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(step)
		return now
	}
}

// headFailsDuringRead fails HEAD reads made while the held read is in
// progress. The first HEAD read after the chunk key listing passes (it is
// the refresh before the first chunk, the held one, is handed to a reader);
// later ones wait until the held read has started, then fail.
type headFailsDuringRead struct {
	Filer
	root, keys string
	entered    <-chan struct{}
	released   <-chan struct{} // the test's escape

	mu     sync.Mutex
	listed bool
	after  int // HEAD reads since the listing
}

func (f *headFailsDuringRead) ListEntries(ctx context.Context, dir, prefix, after string, limit uint32) ([]*filerpb.Entry, error) {
	if dir == f.keys {
		f.mu.Lock()
		f.listed = true
		f.mu.Unlock()
	}
	return f.Filer.ListEntries(ctx, dir, prefix, after, limit)
}

func (f *headFailsDuringRead) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	if dir == f.root && name == headName {
		f.mu.Lock()
		n := 0
		if f.listed {
			f.after++
			n = f.after
		}
		f.mu.Unlock()
		if n >= 2 {
			select {
			case <-f.entered:
				return nil, errFilerDown
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-f.released:
				return nil, errors.New("released by the test")
			}
		}
	}
	return f.Filer.LookupEntry(ctx, dir, name)
}

// A HEAD refresh that fails while a chunk read is in progress stops that
// read at once: Verify returns the filer's error (not the cancellation it
// caused in the reader), incomplete, with no read left running, and long
// before the caller's own deadline.
func TestVerify_FailedRefreshCancelsReadInProgress(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	held := newHeldReads(fx.cb, fx.fid(h.Chunks[0].Key))
	filer := &headFailsDuringRead{Filer: fx.f, root: path.Join(catBase, "v1"), keys: path.Join(catBase, "v1", keysDir),
		entered: held.entered, released: held.released}
	cat, err := NewCatalog(manifestFilerBlobs{filer, held}, catBase)
	if err != nil {
		t.Fatal(err)
	}
	cat.SetClock(tickingClock(time.Minute))
	// The caller's deadline tells a read cancelled by the failed refresh
	// from one that ran into it: Verify must end well before it.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	res, err := verifyBounded(t, cat, ctx, VerifyOptions{Depth: VerifyData, RefreshInterval: time.Minute, BytesPerSecond: -1}, held)
	active, ended := held.state()
	if !errors.Is(err, errFilerDown) || !strings.Contains(err.Error(), "refresh HEAD") {
		t.Fatalf("verify: %v; want the HEAD refresh error", err)
	}
	if ctx.Err() != nil || !errors.Is(ended, context.Canceled) {
		t.Fatalf("held read ended with %v (caller context %v); want cancelled by the failed refresh", ended, ctx.Err())
	}
	if res.Complete || active != 0 {
		t.Fatalf("complete %v, %d reads still running; want incomplete, none", res.Complete, active)
	}
}

// Cancelling the caller's context while a chunk read is in progress ends
// that read and the pass: incomplete, the cancellation reported, no read
// left running.
func TestVerify_CancelDuringReadJoinsReaders(t *testing.T) {
	fx := newVerifyFixture(t, "v1")
	h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
	held := newHeldReads(fx.cb, fx.fid(h.Chunks[0].Key))
	cat, err := NewCatalog(manifestFilerBlobs{fx.f, held}, catBase)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	held.onEnter = cancel
	res, err := verifyBounded(t, cat, ctx, VerifyOptions{Depth: VerifyData, BytesPerSecond: -1}, held)
	active, ended := held.state()
	select {
	case <-held.entered:
	default:
		t.Fatal("the held read never started")
	}
	if !errors.Is(err, context.Canceled) || res.Complete || !errors.Is(ended, context.Canceled) || active != 0 {
		t.Fatalf("verify: %v, complete %v; held read ended with %v, %d reads still running", err, res.Complete, ended, active)
	}
	if len(res.HeadGens) != 1 || res.HeadGens[0] != h.Gen {
		t.Fatalf("covered gens %v, want [%d]", res.HeadGens, h.Gen)
	}
}

// republish installs a new HEAD for vol with the current chunk references
// as edit leaves them (keys are not uploaded again).
func (fx *verifyFixture) republish(vol string, edit func(map[uint64]storage.ChunkRef)) storage.ChunkHead {
	fx.t.Helper()
	ctx := context.Background()
	r, err := NewChunkRemote(fx.f, fx.b, path.Join(catBase, vol))
	if err != nil {
		fx.t.Fatal(err)
	}
	cur, err := r.GetHead(ctx)
	if err != nil {
		fx.t.Fatal(err)
	}
	next := storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: cur.RemoteLSN + 1, Chunks: map[uint64]storage.ChunkRef{}}
	for idx, ref := range cur.Chunks {
		next.Chunks[idx] = ref
	}
	edit(next.Chunks)
	next, ids, err := r.PrepareManifest(ctx, cur, next, verifyChunkBytes)
	if err != nil {
		fx.t.Fatal(err)
	}
	gen, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, next)
	if err != nil {
		fx.t.Fatal(err)
	}
	next.Gen = gen
	for _, id := range ids {
		if err := r.DeleteIntent(ctx, id); err != nil {
			fx.t.Fatal(err)
		}
	}
	return next
}

// wrongHash flips the reference SHA-256 of chunk idx, keeping its key.
func wrongHash(idx uint64) func(map[uint64]storage.ChunkRef) {
	return func(c map[uint64]storage.ChunkRef) {
		ref := c[idx]
		ref.SHA256[0] ^= 0xff
		c[idx] = ref
	}
}

// beforeRead runs fn once, before the first read of fid.
type beforeRead struct {
	Blobs
	fid  string
	fn   func()
	once sync.Once
}

func (b *beforeRead) Get(ctx context.Context, fid string) ([]byte, error) {
	if fid == b.fid {
		b.once.Do(b.fn)
	}
	return b.Blobs.Get(ctx, fid)
}

// A reference to a chunk already checked, under the same key but with
// another SHA-256, is a finding: at depth data it is compared with the
// content read once for that chunk (the chunk is not read again), below
// data with the other reference. This holds for a HEAD refreshed during
// the pass and for a snapshot.
func TestVerify_SameKeyWithAnotherHashIsAFinding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		depth  VerifyDepth
		when   string // "walked": HEAD changes once the first view is walked; "read": once chunk 0 was read; "snapshot": before the pass
		detail string
	}{
		{"refreshed HEAD, data, before the read", VerifyData, "walked", "needle SHA-256"},
		{"refreshed HEAD, data, after the read", VerifyData, "read", "needle SHA-256"},
		{"refreshed HEAD, bindings", VerifyBindings, "walked", "disagree"},
		{"snapshot, data", VerifyData, "snapshot", "needle SHA-256"},
		{"snapshot, bindings", VerifyBindings, "snapshot", "disagree"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newVerifyFixture(t, "v1")
			ctx := context.Background()
			h1 := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
			fid0 := fx.fid(h1.Chunks[0].Key)
			var h2 storage.ChunkHead
			opts := VerifyOptions{Depth: tc.depth, BytesPerSecond: -1, RefreshInterval: time.Minute, Concurrency: 1}
			cat := fx.c
			switch tc.when {
			case "walked":
				opts.hook = func(stage string) {
					if stage == verifyHookHeadWalked {
						h2 = fx.republish("v1", wrongHash(0))
					}
				}
			case "read":
				// One reader: chunk 0 is read completely before chunk 1 is
				// handed out, so its content is known when HEAD changes.
				br := &beforeRead{Blobs: fx.cb, fid: fx.fid(h1.Chunks[1].Key), fn: func() { h2 = fx.republish("v1", wrongHash(0)) }}
				var err error
				if cat, err = NewCatalog(manifestFilerBlobs{fx.f, br}, catBase); err != nil {
					t.Fatal(err)
				}
			case "snapshot":
				if _, err := fx.c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h1); err != nil {
					t.Fatal(err)
				}
				h2 = fx.republish("v1", wrongHash(0))
				opts.AllSnapshots = true
			}
			cat.SetClock(tickingClock(time.Minute))
			res, err := cat.Verify(ctx, "v1", opts)
			if err != nil || !res.Complete {
				t.Fatalf("verify: %v, %+v", err, res)
			}
			wantClasses(t, tc.name, res, "CONTENT_MISMATCH@0")
			f := res.Findings[0]
			wantRoot := headLabel(h2.Gen)
			if tc.when == "snapshot" && tc.depth != VerifyData {
				// HEAD is walked first; the snapshot's reference is the one
				// that disagrees with it.
				wantRoot = "snapshot s1/"
			}
			if !strings.HasPrefix(f.Root, wantRoot) || !strings.Contains(f.Detail, tc.detail) || f.Key == "" {
				t.Fatalf("finding %+v; want under %q, detail with %q", f, wantRoot, tc.detail)
			}
			if tc.when != "snapshot" && (len(res.HeadGens) != 2 || res.HeadGens[0] != h1.Gen || res.HeadGens[1] != h2.Gen) {
				t.Fatalf("covered gens %v, want [%d %d]", res.HeadGens, h1.Gen, h2.Gen)
			}
			if tc.depth == VerifyData && (res.DistinctChunksRead != 2 || fx.cb.reads(fid0) != 1) {
				t.Fatalf("read %d chunks, chunk 0 %d times; want 2, chunk 0 once", res.DistinctChunksRead, fx.cb.reads(fid0))
			}
		})
	}
}

// manualClock stands still until advanced.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) read() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// A chunk's outcome (needle missing, read error, binding missing) applies
// to every reference to it, also to those a refreshed HEAD added before or
// after that outcome: a reference whose root still names the chunk there is
// a finding, under that root. The chunk is not read again.
func TestVerify_OutcomeAppliesToRefreshedReferences(t *testing.T) {
	dropK := func(fx *verifyFixture, k string) { fx.needle(fx.fid(k), dropNeedle) }
	failK := func(fx *verifyFixture, k string) { fx.cb.failReads(fx.fid(k), -1) }
	unbindK := func(fx *verifyFixture, k string) { fx.deleteBinding(k) }
	for _, tc := range []struct {
		name   string
		depth  VerifyDepth
		after  bool // the refresh comes after the outcome, not before
		damage func(fx *verifyFixture, qkey string)
		want   string
		reads  int // reads of the damaged chunk
	}{
		{"refresh before, needle missing", VerifyData, false, dropK, "NEEDLE_MISSING@1", 1},
		{"refresh before, read error", VerifyData, false, failK, "READ_ERROR@1", verifyReadAttempts},
		{"refresh before, binding missing", VerifyBindings, false, unbindK, "BINDING_MISSING@1", 0},
		{"refresh after, needle missing", VerifyData, true, dropK, "NEEDLE_MISSING@1", 1},
		{"refresh after, read error", VerifyData, true, failK, "READ_ERROR@1", verifyReadAttempts},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newVerifyFixture(t, "v1")
			h1 := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
			k := h1.Chunks[0].Key
			fidK := fx.fid(k)
			var h2 storage.ChunkHead
			// The owner moves chunk K from index 0 to index 1 (index 0 and
			// the old chunk 1 leave HEAD), and K is damaged.
			move := func() {
				h2 = fx.republish("v1", func(c map[uint64]storage.ChunkRef) {
					c[1] = c[0]
					delete(c, 0)
				})
				tc.damage(fx, k)
			}
			opts := VerifyOptions{Depth: tc.depth, BytesPerSecond: -1, RefreshInterval: time.Minute, Concurrency: 1}
			cat := fx.c
			if !tc.after {
				// Refreshed at every opportunity: the bindings phase sees
				// the move before K is checked.
				cat.SetClock(tickingClock(time.Minute))
				opts.hook = func(stage string) {
					if stage == verifyHookHeadWalked {
						move()
					}
				}
			} else {
				// No refresh until K's read has failed (one reader: K, at
				// index 0, is read completely before the old chunk 1), then
				// time passes and the pass refreshes after that batch.
				clock := &manualClock{now: time.Now()}
				moveFirst := &beforeRead{Blobs: fx.cb, fid: fidK, fn: move}
				thenAdvance := &beforeRead{Blobs: moveFirst, fid: fx.fid(h1.Chunks[1].Key), fn: func() { clock.advance(time.Hour) }}
				var err error
				if cat, err = NewCatalog(manifestFilerBlobs{fx.f, thenAdvance}, catBase); err != nil {
					t.Fatal(err)
				}
				cat.SetClock(clock.read)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			res, err := cat.Verify(ctx, "v1", opts)
			if err != nil || !res.Complete {
				t.Fatalf("verify: %v, %+v", err, res)
			}
			wantClasses(t, tc.name, res, tc.want)
			if res.Findings[0].Root != headLabel(h2.Gen) || res.SupersededDuringVerify != 1 {
				t.Fatalf("finding under %q, superseded %d; want under %q (the index 0 reference superseded)",
					res.Findings[0].Root, res.SupersededDuringVerify, headLabel(h2.Gen))
			}
			if len(res.HeadGens) != 2 || res.HeadGens[0] != h1.Gen || res.HeadGens[1] != h2.Gen {
				t.Fatalf("covered gens %v, want [%d %d]", res.HeadGens, h1.Gen, h2.Gen)
			}
			if n := fx.cb.reads(fidK); n != tc.reads {
				t.Fatalf("chunk K read %d times, want %d", n, tc.reads)
			}
		})
	}
}

// retagSnapshot rewrites one field of the document of the attempt snapshot
// name resolves to, leaving it otherwise valid.
func (fx *verifyFixture) retagSnapshot(vol, name, field, value string) string {
	fx.t.Helper()
	ctx := context.Background()
	ref, ok, err := fx.c.resolveSnap(ctx, vol, name)
	if err != nil || !ok || ref.entry == nil {
		fx.t.Fatalf("snapshot %s/%s: %v %v", vol, name, ok, err)
	}
	dir := path.Join(catBase, vol, snapsDir, name)
	e, err := fx.f.LookupEntry(ctx, dir, ref.rec.Attempt)
	if err != nil {
		fx.t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(e.Content, &doc); err != nil {
		fx.t.Fatal(err)
	}
	if doc[field], err = json.Marshal(value); err != nil {
		fx.t.Fatal(err)
	}
	if e.Content, err = json.Marshal(doc); err != nil {
		fx.t.Fatal(err)
	}
	if err := fx.f.UpdateEntry(ctx, dir, e, nil); err != nil {
		fx.t.Fatal(err)
	}
	return ref.rec.Attempt
}

// A snapshot is checked under the name and volume it was found at. A
// document that names another snapshot or volume is ROOT_INVALID, and a
// miss under that still published attempt is reported under the name it
// was found at, never excused as a snapshot deleted during the pass.
func TestVerify_SnapshotDocumentNamingAnotherSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name, field, value string
		opts               VerifyOptions
	}{
		{"named, another name", "name", "s2", VerifyOptions{Snapshot: "s1"}},
		{"all, another name", "name", "s2", VerifyOptions{AllSnapshots: true}},
		{"named, another volume", "volume_id", "v9", VerifyOptions{Snapshot: "s1"}},
		{"all, another volume", "volume_id", "v9", VerifyOptions{AllSnapshots: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newVerifyFixture(t, "v1")
			ctx := context.Background()
			h := fx.publish("v1", map[uint64][]byte{0: verifyData(1), 1: verifyData(2)}, false)
			if _, err := fx.c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h); err != nil {
				t.Fatal(err)
			}
			// HEAD moves on: only the snapshot names the old chunk 0, whose binding goes.
			fx.publish("v1", map[uint64][]byte{0: verifyData(9)}, true)
			attempt := fx.retagSnapshot("v1", "s1", tc.field, tc.value)
			fx.deleteBinding(h.Chunks[0].Key)
			res := fx.verify("v1", tc.opts)
			wantClasses(t, tc.name, res, "ROOT_INVALID", "BINDING_MISSING@0")
			label := "snapshot s1/" + attempt
			if res.Findings[0].Root != label || res.Findings[1].Root != label || !strings.Contains(res.Findings[0].Detail, tc.value) {
				t.Fatalf("findings %+v; want both under %q", res.Findings, label)
			}
			if len(res.Snapshots) != 1 || res.Snapshots[0] != "s1/"+attempt || res.RootDeletedDuringVerify != 0 || len(res.SnapshotsDeletedDuringVerify) != 0 {
				t.Fatalf("snapshots %v, root deleted %d, deleted %v; want [s1/%s], 0, none",
					res.Snapshots, res.RootDeletedDuringVerify, res.SnapshotsDeletedDuringVerify, attempt)
			}
		})
	}
}
