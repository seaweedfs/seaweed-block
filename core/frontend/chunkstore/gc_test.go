package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"path"
	"path/filepath"
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
