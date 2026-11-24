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
// entry whose needle is missing) must not have its keys reclaimed: the pass
// fails closed and nothing is marked or reclaimed.
func TestGC_HeadAbsentFailsClosed(t *testing.T) {
	f, b, _, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err := r.Put(ctx, "chunks/e1/c0/k", []byte("published data")); err != nil {
		t.Fatal(err)
	}
	if err := f.DeleteEntry(ctx, path.Join(catBase, "v1"), headName, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		res, err := g.Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "no HEAD") || res.Reclaimed != 0 || res.Candidates != 0 {
			t.Fatalf("pass %d with HEAD pruned: %+v %v; want a refusal, nothing marked", i, res, err)
		}
		*now = now.Add(2*time.Hour + g.cfg.ClockSkew)
	}
	if len(f.dropped) != 0 {
		t.Fatalf("dropped %v", f.dropped)
	}
}

// A live volume without HEAD whose every live binding an intent names (its
// first cut in flight) is not refused.
func TestGC_HeadAbsentWithIntentNamedKeysPasses(t *testing.T) {
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
	if res, err := g.Run(ctx); err != nil {
		t.Fatalf("intent-named keys without HEAD: %+v %v", res, err)
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
	if res, err := g.Run(ctx); err == nil || !strings.Contains(err.Error(), "no HEAD") || res.Candidates != 0 {
		t.Fatalf("VOLUME and HEAD pruned: %+v %v; want a refusal", res, err)
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
