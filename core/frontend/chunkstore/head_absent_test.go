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

// T3(b): a live volume whose HEAD entry is removed (a stock tool pruning an
// entry whose needle is missing) stops every pass: nothing is reclaimed
// anywhere, the refusal names the volume and a remedy that works.
func TestGC_HeadAbsentFailsClosed(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err := r.Put(ctx, "chunks/e1/c0/k", []byte("published data")); err != nil {
		t.Fatal(err)
	}
	if err := f.DeleteEntry(ctx, path.Join(catBase, "v1"), headName, nil); err != nil {
		t.Fatal(err)
	}
	otherVolumeOrphan(t, f, b, c)
	for i := 0; i < 3; i++ {
		res, err := g.Run(ctx)
		if err == nil || len(res.RefusedVolumes) != 1 || res.RefusedVolumes[0].Volume != "v1" || !strings.Contains(err.Error(), "volume-delete") || res.Reclaimed != 0 {
			t.Fatalf("pass %d with HEAD pruned: %+v %v; want the whole pass refused, naming v1 and its remedy", i, res, err)
		}
		*now = now.Add(2*time.Hour + g.cfg.ClockSkew)
	}
	if len(f.dropped) != 0 {
		t.Fatalf("dropped %v; want nothing reclaimed anywhere", f.dropped)
	}
}

// otherVolumeOrphan makes volume v2 with an unrooted key and returns its fid.
func otherVolumeOrphan(t *testing.T, f *fakeFiler, b gcBlobs, c *Catalog) string {
	t.Helper()
	ctx := context.Background()
	if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v2", IsDirectory: true}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnsureVolume(ctx, "v2", catGeo); err != nil {
		t.Fatal(err)
	}
	r2, _ := NewChunkRemote(f, b, path.Join(catBase, "v2"))
	if err := r2.Put(ctx, "orphan", []byte("unrooted in v2")); err != nil {
		t.Fatal(err)
	}
	name, _ := r2.ownKeyName("orphan")
	e, err := f.LookupEntry(ctx, r2.keyDir(), name)
	if err != nil {
		t.Fatal(err)
	}
	live, err := decodeBinding(e)
	if err != nil {
		t.Fatal(err)
	}
	return live.fid
}

// The remedy the refusal names works: deleting the volume clears the
// refusal, and its keys are then reclaimed.
func TestGC_HeadAbsentRemedyDeleteVolume(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err := r.Put(ctx, "chunks/e1/c0/k", []byte("published data")); err != nil {
		t.Fatal(err)
	}
	if err := f.DeleteEntry(ctx, path.Join(catBase, "v1"), headName, nil); err != nil {
		t.Fatal(err)
	}
	if res, err := c.DeleteVolume(ctx, "v1", nil); err != nil || !res.Completed {
		t.Fatalf("volume-delete of a volume without HEAD: %+v %v", res, err)
	}
	reclaimed := 0
	for i := 0; i < 3; i++ {
		res, err := g.Run(ctx)
		if err != nil || len(res.RefusedVolumes) != 0 {
			t.Fatalf("pass %d after delete: %+v %v", i, res, err)
		}
		reclaimed += res.Reclaimed
		*now = now.Add(2*time.Hour + g.cfg.ClockSkew)
	}
	if reclaimed != 1 {
		t.Fatalf("reclaimed %d after delete; want the volume's key", reclaimed)
	}
}

// A created volume without HEAD is refused even when intents name its keys:
// the decision is from VOLUME and HEAD alone.
func TestGC_HeadAbsentIntentsDoNotExempt(t *testing.T) {
	f, b, _, g, _ := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if _, err := r.PutIntent(ctx, storage.ChunkIntent{Gen: 1, Epoch: 1, Holder: "h1", Keys: []string{"chunks/e1/c0/k"}}); err != nil {
		t.Fatal(err)
	}
	if err := r.Put(ctx, "chunks/e1/c0/k", []byte("first cut")); err != nil {
		t.Fatal(err)
	}
	if err := f.DeleteEntry(ctx, path.Join(catBase, "v1"), headName, nil); err != nil {
		t.Fatal(err)
	}
	if res, err := g.Run(ctx); err == nil || len(res.RefusedVolumes) != 1 {
		t.Fatalf("intent-named keys without HEAD: %+v %v; want a refusal", res, err)
	}
}

// Attach never recreates an empty HEAD over existing keys: the pruned
// HEAD's image is unknown, and an empty HEAD would read zeros and let GC
// reclaim every key.
func TestCatalog_AttachRefusesHeadMissingOverKeys(t *testing.T) {
	f, b, c, _, _ := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err := r.Put(ctx, "chunks/e1/c0/k", []byte("published data")); err != nil {
		t.Fatal(err)
	}
	if err := f.DeleteEntry(ctx, path.Join(catBase, "v1"), headName, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnsureVolume(ctx, "v1", catGeo); CodeOf(err) != CodeHeadMissing {
		t.Fatalf("attach over keys without HEAD: %v; want head_missing", err)
	}
	if _, err := f.LookupEntry(ctx, path.Join(catBase, "v1"), headName); !errors.Is(err, ErrNotFound) {
		t.Fatalf("attach wrote a HEAD: %v", err)
	}
}

// Every path that reads a volume's records refuses a format 1 VOLUME: the
// GC mark, retention, the snapshot list and delete, and the fork source.
func TestVolumeFormat_EveryPathRefusesFormat1(t *testing.T) {
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v1", IsDirectory: true}, true); err != nil {
		t.Fatal(err) // the directory the GC mark lists
	}
	p := catBase + "/v1/VOLUME"
	f.mu.Lock()
	f.entries[p].Content = bytes.Replace(f.entries[p].Content, []byte(`"format":2`), []byte(`"format":1`), 1)
	f.mu.Unlock()
	g, err := NewCollector(c, gcBlobs{b}, GCConfig{Grace: time.Hour, IntentTTL: time.Hour, Now: func() time.Time { return time.Now().Add(24 * time.Hour) }, IssuedEpoch: func(context.Context, string) (uint64, error) { return 1, nil }})
	if err != nil {
		t.Fatal(err)
	}
	written := len(f.allWritten())
	for name, run := range map[string]func() error{
		"gc mark":          func() error { _, err := g.Run(ctx); return err },
		"retention policy": func() error { return c.SetRetentionPolicy(ctx, "v1", RetentionPolicy{}) },
		"retain":           func() error { _, err := c.Retain(ctx, "v1", true); return err },
		"snapshot list":    func() error { _, _, err := c.ListSnapshots(ctx, "v1", ListOptions{}); return err },
		"snapshot delete":  func() error { _, err := c.DeleteSnapshot(ctx, "v1", "s1"); return err },
		"fork source": func() error {
			_, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"})
			return err
		},
	} {
		if err := run(); err == nil || !strings.Contains(err.Error(), "format 1 not supported") {
			t.Errorf("%s on a format 1 store: %v", name, err)
		}
	}
	if n := len(f.allWritten()) - written; n != 0 {
		t.Fatalf("%d entries written to a format 1 store", n)
	}
}

// A volume directory with live bindings but neither VOLUME nor HEAD (both
// pruned) is refused by GC the same way.
func TestGC_VolumeAndHeadAbsentFailsClosed(t *testing.T) {
	f, b, _, g, _ := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err := r.Put(ctx, "chunks/e1/c0/k", []byte("published data")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{headName, volumeName} {
		if err := f.DeleteEntry(ctx, path.Join(catBase, "v1"), name, nil); err != nil {
			t.Fatal(err)
		}
	}
	res, err := g.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "fs.rm") || res.Reclaimed != 0 {
		t.Fatalf("VOLUME and HEAD pruned: %+v %v; want the whole pass refused with its remedy", res, err)
	}
}

// A fork target holding stale keys but no VOLUME or HEAD is refused, never
// given a VOLUME.
func TestFork_TargetWithStaleKeysRefused(t *testing.T) {
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	stale, _ := NewChunkRemote(f, b, path.Join(catBase, "v2"))
	if err := stale.Put(ctx, "chunks/e1/c0/k", []byte("stale")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeNotChunkVolume {
		t.Fatalf("fork onto stale keys: %v", err)
	}
	if _, err := f.LookupEntry(ctx, path.Join(catBase, "v2"), volumeName); !errors.Is(err, ErrNotFound) {
		t.Fatalf("target VOLUME written: %v", err)
	}
}

// forkSharedFixture: v2 is a fork of v1's snapshot s1; v1's HEAD then moved
// on to an empty image and s1 was deleted, so v2's HEAD is the only root of
// the chunks they shared. It returns the shared keys' bindings' fids.
func forkSharedFixture(t *testing.T) (*fakeFiler, *Collector, *time.Time, []string) {
	t.Helper()
	f, b, c, h := catalogFixture(t)
	f.deleteChunk = gcBlobs{b}.deleteChunk
	ctx := context.Background()
	for _, v := range []string{"v1", "v2"} {
		if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: v, IsDirectory: true}, true); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); err != nil {
		t.Fatal(err)
	}
	r1, _ := NewChunkRemote(f, b, catBase+"/v1")
	if _, err := r1.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, storage.ChunkHead{Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.RemoteLSN, Chunks: map[uint64]storage.ChunkRef{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatal(err)
	}
	var shared []string
	f.mu.Lock()
	for p, e := range f.entries {
		if strings.HasPrefix(p, catBase+"/v1/keys/") {
			if bound, err := decodeBinding(e); err == nil && !bound.done {
				shared = append(shared, bound.fid)
			}
		}
	}
	f.mu.Unlock()
	now := time.Now().Add(24 * time.Hour)
	g, err := NewCollector(c, gcBlobs{b}, GCConfig{Grace: time.Hour, IntentTTL: time.Hour, Now: func() time.Time { return now }, IssuedEpoch: func(context.Context, string) (uint64, error) { return 1, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return f, g, &now, shared
}

// A fork whose HEAD is pruned stops the whole pass: the source's shared
// keys, which only that HEAD rooted, are never reclaimed.
func TestGC_ForkHeadAbsentStopsThePass(t *testing.T) {
	f, g, now, shared := forkSharedFixture(t)
	ctx := context.Background()
	if len(shared) == 0 {
		t.Fatal("fixture has no shared keys")
	}
	if err := f.DeleteEntry(ctx, catBase+"/v2", headName, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		res, err := g.Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "v2 is live but has no HEAD") || res.Reclaimed != 0 {
			t.Fatalf("pass %d with the fork's HEAD pruned: %+v %v; want the whole pass refused", i, res, err)
		}
		*now = now.Add(2*time.Hour + g.cfg.ClockSkew)
	}
	if len(f.dropped) != 0 {
		t.Fatalf("dropped %v; the source's shared keys %v must survive", f.dropped, shared)
	}
}

// The control: with the fork's HEAD present the same passes keep the shared
// keys (the fork roots them) and refuse nothing.
func TestGC_ForkHeadPresentRootsTheSourceKeys(t *testing.T) {
	f, g, now, _ := forkSharedFixture(t)
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if res, err := g.Run(ctx); err != nil || len(res.RefusedVolumes) != 0 {
			t.Fatalf("pass %d: %+v %v", i, res, err)
		}
		*now = now.Add(2*time.Hour + g.cfg.ClockSkew)
	}
	for _, fid := range f.dropped {
		t.Errorf("dropped %s while the fork's HEAD roots the shared keys", fid)
	}
}

// The hostile twin: a COMPLETED fork whose copy intent stayed (its drop
// failed after the publish), past the intent's deadline, then its HEAD
// pruned. Full apply passes (copy sweep, then mark) must never create a
// HEAD for it nor reclaim the fork's or the source's shared keys.
func TestGC_HostileTwinStaleForkIntentAndPrunedHead(t *testing.T) {
	f, g, now, shared := forkSharedFixture(t)
	ctx := context.Background()
	c := g.cat
	h2, ok, err := c.ReadHead(ctx, "v2")
	if err != nil || !ok || len(h2.Segments) == 0 {
		t.Fatalf("fork HEAD: %+v %t %v", h2, ok, err)
	}
	var keys []string
	for _, ref := range h2.Segments {
		keys = append(keys, ref.Key)
	}
	r2, _ := NewChunkRemote(f, g.blobs.(gcBlobs).fakeBlobs, catBase+"/v2")
	if _, err := r2.PutIntent(ctx, storage.ChunkIntent{Holder: "fork:v2/f", Keys: keys, Segments: h2.Segments, ChunkBytes: h2.ChunkBytes,
		DeadlineNs: time.Now().Add(time.Minute).UnixNano()}); err != nil {
		t.Fatal(err) // the stale intent, as the fork left it
	}
	if err := f.DeleteEntry(ctx, catBase+"/v2", headName, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		out, err := c.ExecuteGC(ctx, g.blobs, g.cfg, true)
		if err == nil || out.Reclaimed != 0 {
			t.Fatalf("pass %d: %+v %v; want the pass stopped", i, out.GCResult, err)
		}
		if _, ok, _ := c.ReadHead(ctx, "v2"); ok {
			t.Fatalf("pass %d created a HEAD for the fork", i)
		}
		*now = now.Add(2*time.Hour + g.cfg.ClockSkew)
	}
	if len(f.dropped) != 0 {
		t.Fatalf("dropped %v; the shared keys %v must survive", f.dropped, shared)
	}
}
