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
	for _, tc := range []struct {
		name     string
		hosts    []string
		notFound bool
	}{
		{"every location 404", []string{missing, missing}, true},
		{"one location 500", []string{missing, broken}, false},
		{"only 500", []string{broken}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var locs []*filerpb.Location
			for _, h := range tc.hosts {
				locs = append(locs, &filerpb.Location{Url: h})
			}
			c := startFakeFiler(t, &fakeFilerGRPC{lookupVol: &filerpb.LookupVolumeResponse{LocationsMap: map[string]*filerpb.Locations{"7": {Locations: locs}}}})
			_, err := c.Get(context.Background(), "7,0000dead")
			last := tc.hosts[len(tc.hosts)-1]
			status := 404
			if last == broken {
				status = 500
			}
			if err == nil || errors.Is(err, ErrBlobNotFound) != tc.notFound || err.Error() != fmt.Sprintf("chunkstore: read 7,0000dead: http %d from %s", status, last) {
				t.Fatalf("Get: %v (not found %v), want not found %v", err, errors.Is(err, ErrBlobNotFound), tc.notFound)
			}
		})
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
