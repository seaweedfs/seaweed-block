package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type gcBlobs struct{ *fakeBlobs }

func (b gcBlobs) DeleteBlob(_ context.Context, fid string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.data, fid)
	return nil
}

type heldGCRemote struct {
	*ChunkRemote
	entered chan struct{}
	release chan struct{}
}

func (r *heldGCRemote) CASHead(ctx context.Context, v storage.ChunkHeadVersion, h storage.ChunkHead) (uint64, error) {
	close(r.entered)
	select {
	case <-r.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return r.ChunkRemote.CASHead(ctx, v, h)
}

func gcFixture(t *testing.T) (*fakeFiler, gcBlobs, *Catalog, *Collector, *time.Time) {
	t.Helper()
	f, b := newFakeFiler(), gcBlobs{newFakeBlobs()}
	c := newCat(t, f)
	if e := f.CreateEntry(context.Background(), catBase, &filerpb.Entry{Name: "v1", IsDirectory: true}, true); e != nil {
		t.Fatal(e)
	}
	if _, e := c.EnsureVolume(context.Background(), "v1", catGeo); e != nil {
		t.Fatal(e)
	}
	now := time.Now().Add(24 * time.Hour)
	g, e := NewCollector(c, b, GCConfig{Grace: time.Hour, IntentTTL: time.Hour, Now: func() time.Time { return now }, IssuedEpoch: func(context.Context, string) (uint64, error) { return 1, nil }})
	if e != nil {
		t.Fatal(e)
	}
	return f, b, c, g, &now
}

func TestGC_StalledCutRemainsReadableAfterTwoMarks(t *testing.T) {
	f, b, _, g, now := gcFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	remote, e := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if e != nil {
		t.Fatal(e)
	}
	held := &heldGCRemote{remote, make(chan struct{}), make(chan struct{})}
	p := filepath.Join(t.TempDir(), "volume")
	s, e := storage.CreateWALStore(p, 64, 4096)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = s.Recover(); e != nil {
		t.Fatal(e)
	}
	layer, e := storage.AttachChunkLayer(s, held, storage.ChunkLayerConfig{ChunkBlocks: 8, Epoch: 1, Holder: "h1", Bitmap: storage.NewFileChunkBitmapStore(p + ".bitmap")})
	if e != nil {
		t.Fatal(e)
	}
	want := bytes.Repeat([]byte{0x71}, 4096)
	if _, e = s.Write(0, want); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, e := layer.Capture(ctx); done <- e }()
	select {
	case <-held.entered:
	case <-ctx.Done():
		t.Fatal("cut never reached post-upload CAS")
	}
	// Always release the owned cut before Close, including a failing assertion.
	released := false
	defer func() {
		if !released {
			close(held.release)
			<-done
		}
	}()
	for i := 0; i < 2; i++ {
		result, e := g.Run(ctx)
		if e != nil {
			t.Fatal(e)
		}
		t.Logf("mark %d: %+v", i+1, result)
		*now = now.Add(time.Hour + time.Second)
	}
	close(held.release)
	released = true
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	h, e := remote.GetHead(ctx)
	if e != nil || h.Gen == 0 {
		t.Fatalf("published HEAD: %+v %v", h, e)
	}
	for _, ref := range h.Chunks {
		got, e := remote.Get(ctx, ref.Key)
		if e != nil || !bytes.Equal(got[:min(len(got), 4096)], want) {
			t.Fatalf("published HEAD references deleted or corrupt stalled-cut chunk: key=%q bytes=%d err=%v", ref.Key, len(got), e)
		}
	}
}

func TestGC_UnreachableRequiresTwoMarksAndUploadAge(t *testing.T) {
	f, b, _, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if e := r.Put(ctx, "orphan", []byte("exact orphan")); e != nil {
		t.Fatal(e)
	}
	if _, e := g.Run(ctx); e != nil {
		t.Fatal(e)
	}
	*now = now.Add(time.Hour - time.Second)
	if result, e := g.Run(ctx); e != nil || result.Reclaimed != 0 {
		t.Fatalf("early second mark: %+v %v", result, e)
	}
	if _, e := r.Get(ctx, "orphan"); e != nil {
		t.Fatal("orphan deleted before grace", e)
	}
	*now = now.Add(2 * time.Second)
	result, e := g.Run(ctx)
	if e != nil || result.Reclaimed != 1 {
		t.Fatalf("old unreachable chunk was not reclaimed after grace-separated marks: %+v %v", result, e)
	}
	if _, e := r.Get(ctx, "orphan"); e == nil {
		t.Fatal("reclaimed orphan remains readable")
	}
	fresh, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if e := fresh.Put(ctx, "orphan", []byte("reused")); !errors.Is(e, storage.ErrChunkKeyExists) {
		t.Fatalf("immutable key reused after GC: %v", e)
	}
	// A freshly uploaded chunk cannot enter its first candidate mark yet.
	*now = time.Now()
	next, e := NewCollector(g.cat, b, g.cfg)
	if e != nil {
		t.Fatal(e)
	}
	if e := r.Put(ctx, "young", []byte("young")); e != nil {
		t.Fatal(e)
	}
	if result, e := next.Run(ctx); e != nil || result.Candidates != 0 {
		t.Fatalf("young upload admitted: %+v %v", result, e)
	}
}

func TestGC_IntentExpiryNeedsTTLAuthorityAndRemoteFence(t *testing.T) {
	for _, tc := range []struct {
		name         string
		issued, head uint64
		age          time.Duration
		authorityErr bool
		expire       bool
	}{
		{"current-holder-old-intent", 1, 1, 2 * time.Hour, false, false},
		{"higher-issued-but-no-takeover", 2, 1, 2 * time.Hour, false, false},
		{"takeover-but-no-authority", 1, 2, 2 * time.Hour, false, false},
		{"unknown-authority", 2, 2, 2 * time.Hour, true, false},
		{"TTL-not-exceeded", 2, 2, time.Hour, false, false},
		{"higher-issued-and-fenced", 2, 2, time.Hour + time.Nanosecond, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, b, _, g, now := gcFixture(t)
			ctx := context.Background()
			r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
			if e := r.Put(ctx, "inflight", []byte("protected")); e != nil {
				t.Fatal(e)
			}
			id, e := r.PutIntent(ctx, storage.ChunkIntent{Gen: 0, Epoch: 1, Holder: "h1", Keys: []string{"inflight"}})
			if e != nil {
				t.Fatal(e)
			}
			ins, e := r.ListIntents(ctx)
			if e != nil || len(ins) != 1 {
				t.Fatalf("intent: %+v %v", ins, e)
			}
			*now = time.Unix(0, ins[0].CreatedNs).Add(tc.age)
			// A real remote HEAD fence. The publisher's original expected version
			// can no longer install after epoch 2 was published.
			cur, e := r.GetHead(ctx)
			if e != nil {
				t.Fatal(e)
			}
			old := storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}
			if _, e = r.CASHead(ctx, old, storage.ChunkHead{Epoch: tc.head, Holder: "new", Chunks: map[uint64]storage.ChunkRef{}}); e != nil {
				t.Fatal(e)
			}
			g.cfg.IssuedEpoch = func(context.Context, string) (uint64, error) {
				if tc.authorityErr {
					return 0, errors.New("authority unavailable")
				}
				return tc.issued, nil
			}
			result, e := g.Run(ctx)
			if e != nil {
				t.Fatal(e)
			}
			ins, e = r.ListIntents(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if (len(ins) == 0) != tc.expire {
				t.Fatalf("intent %s expired=%v want=%v; %+v", id, len(ins) == 0, tc.expire, result)
			}
			if result.Reclaimed != 0 {
				t.Fatal("one mark reclaimed bytes")
			}
			if tc.expire {
				_, e = r.CASHead(ctx, old, storage.ChunkHead{Epoch: 1, Holder: "h1"})
				if !errors.Is(e, storage.ErrChunkHeadPrecondition) {
					t.Fatalf("expired publisher can still CAS: %v", e)
				}
			}
		})
	}
}

type failingGCListing struct {
	Filer
	fail bool
}

func (f *failingGCListing) ListEntries(ctx context.Context, dir, prefix, after string, limit uint32) ([]*filerpb.Entry, error) {
	if f.fail {
		return nil, errors.New("listing unavailable")
	}
	return f.Filer.ListEntries(ctx, dir, prefix, after, limit)
}
func TestGC_FailedMarkAndRestartNeverReuseCoverage(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if e := r.Put(ctx, "orphan", []byte("orphan")); e != nil {
		t.Fatal(e)
	}
	wrapper := &failingGCListing{Filer: f}
	c.f = wrapper
	if _, e := g.Run(ctx); e != nil {
		t.Fatal(e)
	}
	*now = now.Add(2 * time.Hour)
	wrapper.fail = true
	if _, e := g.Run(ctx); e == nil {
		t.Fatal("failed listing accepted")
	}
	wrapper.fail = false
	if result, e := g.Run(ctx); e != nil || result.Reclaimed != 0 {
		t.Fatalf("failed mark preserved coverage: %+v %v", result, e)
	}
	fresh, e := NewCollector(c, b, g.cfg)
	if e != nil {
		t.Fatal(e)
	}
	*now = now.Add(2 * time.Hour)
	if result, e := fresh.Run(ctx); e != nil || result.Reclaimed != 0 {
		t.Fatalf("restart inferred prior mark: %+v %v", result, e)
	}
}

func TestGC_SnapshotAndQualifiedForkAreRoots(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x21)})
	if _, e := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "req"}, h); e != nil {
		t.Fatal(e)
	}
	if e := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v2", IsDirectory: true}, true); e != nil {
		t.Fatal(e)
	}
	if _, e := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "fork"}); e != nil {
		t.Fatal(e)
	}
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if _, e := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, storage.ChunkHead{Epoch: 1, Holder: "h1", Chunks: map[uint64]storage.ChunkRef{}}); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if result, e := g.Run(ctx); e != nil || result.Reclaimed != 0 {
			t.Fatalf("rooted data reclaimed: %+v %v", result, e)
		}
		*now = now.Add(2 * time.Hour)
	}
	for _, ref := range h.Chunks {
		if got, e := r.Get(ctx, ref.Key); e != nil || !bytes.Equal(got, chunkBytes(0x21)) {
			t.Fatalf("shared bytes: %v", e)
		}
	}
}

func TestGC_CopyIntentSurvivesTakeover(t *testing.T) {
	f, b, _, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	for _, holder := range []string{"snapshot:v1/s1/req", "fork:v1/req"} {
		if _, e := r.PutIntent(ctx, storage.ChunkIntent{Gen: 0, Epoch: 1, Holder: holder, Keys: []string{"k"}}); e != nil {
			t.Fatal(e)
		}
	}
	cur, e := r.GetHead(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, storage.ChunkHead{Epoch: 2, Holder: "new"}); e != nil {
		t.Fatal(e)
	}
	g.cfg.IssuedEpoch = func(context.Context, string) (uint64, error) { return 2, nil }
	*now = now.Add(24 * time.Hour)
	if result, e := g.Run(ctx); e != nil || result.ExpiredIntents != 0 {
		t.Fatalf("epochless copy publication lost its protection: %+v %v", result, e)
	}
	intents, e := r.ListIntents(ctx)
	if e != nil || len(intents) != 2 {
		t.Fatalf("copy intents: %+v %v", intents, e)
	}
}

func TestSWFS_DeleteBlobStatusesAndExactPath(t *testing.T) {
	for _, status := range []int{200, 204, 404, 401, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "DELETE" || r.URL.Path != "/7,01abcdef" {
					t.Errorf("unexpected delete %s %s", r.Method, r.URL.Path)
				}
				w.WriteHeader(status)
			}))
			defer server.Close()
			c := &SWFS{http: server.Client(), vidCache: map[string][]string{"7": {strings.TrimPrefix(server.URL, "http://")}}}
			e := c.DeleteBlob(context.Background(), "7,01abcdef")
			if (e == nil) != (status == 200 || status == 204 || status == 404) {
				t.Fatalf("status %d: %v", status, e)
			}
			if calls != 1 {
				t.Fatalf("calls %d", calls)
			}
			for _, bad := range []string{"7,../bad", "7,a?bad", "7,a%2fb", "", ",a"} {
				if e := c.DeleteBlob(context.Background(), bad); e == nil {
					t.Errorf("invalid fid accepted: %q", bad)
				}
			}
			if calls != 1 {
				t.Fatal("invalid fid dispatched HTTP")
			}
		})
	}
}

func TestGC_UnboundNeedleRetainedAndCountUnknown(t *testing.T) {
	_, b, _, g, now := gcFixture(t)
	ctx := context.Background()
	fid, e := b.Put(ctx, []byte("pre-binding orphan"))
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		result, e := g.Run(ctx)
		if e != nil || !result.UnknownUnbound || result.Reclaimed != 0 {
			t.Fatalf("unbound inventory claim: %+v %v", result, e)
		}
		*now = now.Add(2 * time.Hour)
	}
	if got, e := b.Get(ctx, fid); e != nil || string(got) != "pre-binding orphan" {
		t.Fatalf("unbound orphan deleted: %q %v", got, e)
	}
}
