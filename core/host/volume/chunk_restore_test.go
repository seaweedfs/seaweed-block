package volume

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const restoreBase = "/swblock/chunks"

var restoreGeo = chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}

// sourceReadGuard fails every catalog read of the source volume's own
// entries (VOLUME, HEAD, snapshots) once armed; chunk bindings stay readable
// (hydration reads the source's chunks through T's qualified refs).
type sourceReadGuard struct {
	*adapterManifestFiler
	armed bool
	reads []string
}

func (g *sourceReadGuard) denied(dir string) bool {
	src := path.Join(restoreBase, "v1")
	return g.armed && (dir == src || strings.HasPrefix(dir, src+"/snap"))
}

func (g *sourceReadGuard) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	if g.denied(dir) {
		g.reads = append(g.reads, path.Join(dir, name))
		return nil, errors.New("test: the restore driver read the source")
	}
	return g.adapterManifestFiler.LookupEntry(ctx, dir, name)
}

func (g *sourceReadGuard) ListEntries(ctx context.Context, dir, prefix, after string, limit uint32) ([]*filerpb.Entry, error) {
	if g.denied(dir) {
		g.reads = append(g.reads, dir)
		return nil, errors.New("test: the restore driver listed the source")
	}
	return g.adapterManifestFiler.ListEntries(ctx, dir, prefix, after, limit)
}

// fakeRestoreProvider stands in for the chunk layer: the walk makes every
// chunk local, and a batch is one HEAD CAS replacing up to n foreign keys.
type fakeRestoreProvider struct {
	r           *chunkstore.ChunkRemote
	local       bool
	hydrations  int
	batches     int
	failAtBatch int // a batch with this 1-based number fails (0: never)
}

func (p *fakeRestoreProvider) HydrateVolume(ctx context.Context, vol string, bps int64) (storage.HydrateProgress, error) {
	p.hydrations++
	if bps != DefaultRestoreBytesPerSecond {
		return storage.HydrateProgress{}, fmt.Errorf("walker pace %d, want the default %d", bps, DefaultRestoreBytesPerSecond)
	}
	if p.local {
		return storage.HydrateProgress{}, nil
	}
	h, err := p.r.GetHead(ctx)
	if err != nil {
		return storage.HydrateProgress{}, err
	}
	p.local = true
	return storage.HydrateProgress{Filled: len(h.Chunks)}, nil
}

func (p *fakeRestoreProvider) OwnVolumeBatch(ctx context.Context, vol string, n int, foreign func(string) bool) (storage.OwnProgress, error) {
	h, err := p.r.GetHead(ctx)
	if err != nil {
		return storage.OwnProgress{}, err
	}
	var todo []uint64
	for idx, ref := range h.Chunks {
		if foreign(ref.Key) {
			todo = append(todo, idx)
		}
	}
	sort.Slice(todo, func(i, j int) bool { return todo[i] < todo[j] })
	if !p.local || len(todo) == 0 {
		nl := 0
		if !p.local {
			nl = len(todo)
		}
		return storage.OwnProgress{Remaining: len(todo), NotLocal: nl}, nil
	}
	p.batches++
	if p.batches == p.failAtBatch {
		return storage.OwnProgress{Remaining: len(todo)}, errors.New("test: batch cut failed")
	}
	next := storage.ChunkHead{Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.RemoteLSN, Chunks: map[uint64]storage.ChunkRef{}}
	for idx, ref := range h.Chunks {
		next.Chunks[idx] = ref
	}
	owned := 0
	for _, idx := range todo {
		if owned == n {
			break
		}
		data, err := p.r.Get(ctx, h.Chunks[idx].Key)
		if err != nil {
			return storage.OwnProgress{}, err
		}
		key := fmt.Sprintf("chunks/e3/c%d/b%d", idx, p.batches)
		if err := p.r.Put(ctx, key, data); err != nil {
			return storage.OwnProgress{}, err
		}
		next.Chunks[idx] = storage.ChunkRef{Key: key, SHA256: sha256.Sum256(data)}
		owned++
	}
	next, intents, err := p.r.PrepareManifest(ctx, h, next, uint64(restoreGeo.BlockSize)*uint64(restoreGeo.ChunkBlocks))
	if err != nil {
		return storage.OwnProgress{}, err
	}
	if _, err := p.r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, next); err != nil {
		return storage.OwnProgress{}, err
	}
	for _, id := range intents {
		_ = p.r.DeleteIntent(ctx, id)
	}
	return storage.OwnProgress{Owned: owned, Remaining: len(todo) - owned}, nil
}

// restoreFixtureHost publishes source v1 (chunks), snapshots it and restores
// it into v2; it returns the guard filer, the catalog and v2's provider.
func restoreFixtureHost(t *testing.T, chunks int) (*sourceReadGuard, *chunkstore.Catalog, *fakeRestoreProvider) {
	t.Helper()
	ctx := context.Background()
	f := &sourceReadGuard{adapterManifestFiler: &adapterManifestFiler{Filer: chunkstore.NewMemFiler(), blobs: map[string][]byte{}}}
	cat, err := chunkstore.NewCatalog(f, restoreBase)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.EnsureVolume(ctx, "v1", restoreGeo); err != nil {
		t.Fatal(err)
	}
	r1, err := chunkstore.NewChunkRemote(f, f, path.Join(restoreBase, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	cur, err := r1.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h := storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: 42, Chunks: map[uint64]storage.ChunkRef{}}
	for idx := 0; idx < chunks; idx++ {
		data := make([]byte, int(restoreGeo.BlockSize)*int(restoreGeo.ChunkBlocks))
		data[0] = byte(0x60 + idx)
		key := fmt.Sprintf("chunks/e1/c%d/k", idx)
		if err := r1.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		h.Chunks[uint64(idx)] = storage.ChunkRef{Key: key, SHA256: sha256.Sum256(data)}
	}
	h, intents, err := r1.PrepareManifest(ctx, cur, h, uint64(restoreGeo.BlockSize)*uint64(restoreGeo.ChunkBlocks))
	if err != nil {
		t.Fatal(err)
	}
	if h.Gen, err = r1.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, h); err != nil {
		t.Fatal(err)
	}
	for _, id := range intents {
		_ = r1.DeleteIntent(ctx, id)
	}
	if _, err := cat.CreateSnapshot(ctx, "v1", chunkstore.SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.Restore(ctx, chunkstore.ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "rs"}); err != nil {
		t.Fatal(err)
	}
	r2, err := chunkstore.NewChunkRemote(f, f, path.Join(restoreBase, "v2"))
	if err != nil {
		t.Fatal(err)
	}
	return f, cat, &fakeRestoreProvider{r: r2}
}

func noReport(RestoreStatus) {}

// The driver walks, cuts bounded batches until no foreign ref is left and
// writes the marker, reading only the target's facts (Restoring>Hydrating>
// Owning>Owning>Independent).
func TestRestoreDriver_RunsToIndependenceFromTheTargetsFactsOnly(t *testing.T) {
	f, cat, p := restoreFixtureHost(t, 5)
	f.armed = true // from here, any read of the source's catalog entries fails
	ctx := context.Background()
	st, err := runRestore(ctx, cat, p, "v2", RestoreConfig{BatchChunks: 2}, noReport)
	if err != nil {
		t.Fatalf("restore: %+v %v (source reads %v)", st, err, f.reads)
	}
	if len(f.reads) != 0 {
		t.Fatalf("the driver read the source: %v", f.reads)
	}
	if st.Phase != RestoreIndependent || st.Owned != 5 || st.Foreign != 0 || st.Batches != 3 || st.Filled != 5 {
		t.Fatalf("status %+v, want independent after 3 batches of at most 2", st)
	}
	m, ok, err := cat.ReadIndependence(ctx, "v2")
	if err != nil || !ok || m.HeadGen != st.MarkedGen {
		t.Fatalf("marker: %+v ok=%v %v", m, ok, err)
	}
	again, err := runRestore(ctx, cat, p, "v2", RestoreConfig{}, noReport)
	if err != nil || again.Phase != RestoreIndependent || p.batches != 3 {
		t.Fatalf("a rerun of an independent restore must do nothing: %+v %v", again, err)
	}
}

// A failed batch leaves HEAD as it was; the next run resumes from the
// target's HEAD and finishes.
func TestRestoreDriver_ResumesAfterAFailedBatch(t *testing.T) {
	_, cat, p := restoreFixtureHost(t, 5)
	ctx := context.Background()
	p.failAtBatch = 2
	st, err := runRestore(ctx, cat, p, "v2", RestoreConfig{BatchChunks: 2}, noReport)
	if err == nil || st.Owned != 2 || st.Foreign != 3 {
		t.Fatalf("first run: %+v %v, want the failure after 2 owned", st, err)
	}
	if _, ok, _ := cat.ReadIndependence(ctx, "v2"); ok {
		t.Fatal("marker written with foreign refs left")
	}
	p.failAtBatch = 0
	st, err = runRestore(ctx, cat, p, "v2", RestoreConfig{BatchChunks: 2}, noReport)
	if err != nil || st.Phase != RestoreIndependent || st.Owned != 3 {
		t.Fatalf("resumed run: %+v %v, want the 3 left owned and the marker", st, err)
	}
}

// Only a restore volume is driven.
func TestRestoreDriver_RefusesAVolumeNotMadeByRestore(t *testing.T) {
	_, cat, p := restoreFixtureHost(t, 1)
	if _, err := runRestore(context.Background(), cat, p, "v1", RestoreConfig{}, noReport); !errors.Is(err, errNotRestore) {
		t.Fatalf("driving the source volume: %v, want not a restore", err)
	}
}

// POST /chunk/restore starts one background run; GET reports it.
func TestRestoreDriver_ChunkControlStartsOneRunAndReportsIt(t *testing.T) {
	_, cat, p := restoreFixtureHost(t, 3)
	ctl := &chunkControl{p: restoreOnlyProvider{p}, cat: cat}
	ctx := context.Background()
	if st, err := ctl.Restore(ctx, "v2", false); err != nil || st.Phase != "" {
		t.Fatalf("status before any run: %+v %v", st, err)
	}
	if _, err := ctl.Restore(ctx, "v2", true); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := ctl.Restore(ctx, "v2", false)
		if err != nil {
			t.Fatal(err)
		}
		if !st.Running {
			if st.Phase != RestoreIndependent || st.Owned != 3 {
				t.Fatalf("final status %+v", st)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restore still running: %+v", st)
		}
		time.Sleep(time.Millisecond)
	}
}

// restoreOnlyProvider is a ChunkProvider whose cut and detach are unused.
type restoreOnlyProvider struct{ *fakeRestoreProvider }

func (restoreOnlyProvider) CutVolume(context.Context, string, uint64) (storage.ChunkCutResult, error) {
	return storage.ChunkCutResult{}, errors.New("unused")
}
func (restoreOnlyProvider) DetachVolume(context.Context, string) (storage.ChunkCutResult, uint64, error) {
	return storage.ChunkCutResult{}, 0, errors.New("unused")
}

var _ RestoreProvider = (*durable.DurableProvider)(nil)
