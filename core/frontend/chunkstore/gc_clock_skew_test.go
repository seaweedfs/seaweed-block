package chunkstore

import (
	"context"
	"encoding/json"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"path"
	"testing"
	"time"
)

// The default must cover the supported deployment's pairwise clock difference
// (< one minute), rather than assuming every master uses its default20s.
const testedGCSkew = time.Minute

func TestGCSkewSnapshotDeleteGraceBothDirections(t *testing.T) {
	for _, offset := range []time.Duration{-testedGCSkew, testedGCSkew} {
		t.Run(offset.String(), func(t *testing.T) {
			f, b, c, g, now := gcFixture(t)
			ctx := context.Background()
			realDelete := *now
			c.SetClock(func() time.Time { return realDelete.Add(offset) })
			h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x41)})
			snapshotDir(t, f, "v1", "s1")
			if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "req"}, h); err != nil {
				t.Fatal(err)
			}
			if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
				t.Fatal(err)
			}
			if offset < 0 {
				*now = realDelete.Add(g.cfg.Grace - testedGCSkew/2)
			} else {
				*now = realDelete
			}
			roots, err := g.snapshotAttemptHeads(ctx, "v1", *now)
			if err != nil || len(roots) != 1 {
				t.Fatalf("skew shortened delete grace or blocked mark: roots=%d err=%v", len(roots), err)
			}
			*now = realDelete.Add(g.cfg.Grace + 2*testedGCSkew + time.Nanosecond)
			roots, err = g.snapshotAttemptHeads(ctx, "v1", *now)
			if err != nil || len(roots) != 0 {
				t.Fatalf("old deleting attempt did not release: roots=%d err=%v", len(roots), err)
			}
		})
	}
}
func TestGCSkewIntentExpiryUsesConservativeAge(t *testing.T) {
	f, b, _, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	id, err := r.PutIntent(ctx, storage.ChunkIntent{Epoch: 1, Holder: "old"})
	if err != nil {
		t.Fatal(err)
	}
	ins, _ := r.ListIntents(ctx)
	created := time.Unix(0, ins[0].CreatedNs)
	head, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.CASHead(ctx, storage.ChunkHeadVersion{Gen: head.Gen, Epoch: head.Epoch}, storage.ChunkHead{Epoch: 2, Holder: "new", Chunks: map[uint64]storage.ChunkRef{}}); err != nil {
		t.Fatal(err)
	}
	g.cfg.IssuedEpoch = func(context.Context, string) (uint64, error) { return 2, nil }
	*now = created.Add(g.cfg.IntentTTL + testedGCSkew/2)
	result, err := g.Run(ctx)
	if err != nil || result.ExpiredIntents != 0 {
		t.Fatalf("intent expired inside skew allowance: %+v %v", result, err)
	}
	*now = created.Add(g.cfg.IntentTTL + testedGCSkew + time.Nanosecond)
	result, err = g.Run(ctx)
	if err != nil || result.ExpiredIntents != 1 {
		t.Fatalf("eligible intent %s retained: %+v %v", id, result, err)
	}
}
func TestGCSkewFutureIntentKeepsRootsWithoutBlockingProgress(t *testing.T) {
	f, b, _, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	for _, key := range []string{"rooted", "orphan"} {
		if err := r.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	id, err := r.PutIntent(ctx, storage.ChunkIntent{Epoch: 1, Holder: "live", Keys: []string{"rooted"}})
	if err != nil {
		t.Fatal(err)
	}
	dir := path.Join(catBase, "v1", intentsDir)
	entry, err := f.LookupEntry(ctx, dir, id)
	if err != nil {
		t.Fatal(err)
	}
	var doc intentDoc
	if err = json.Unmarshal(entry.Content, &doc); err != nil {
		t.Fatal(err)
	}
	doc.CreatedNs = now.Add(testedGCSkew / 2).UnixNano()
	entry.Content, _ = json.Marshal(doc)
	if err = f.UpdateEntry(ctx, dir, entry, nil); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		out, err := g.Run(ctx)
		if err != nil {
			t.Fatalf("future intent blocked unrelated progress: %v", err)
		}
		if i == 1 && out.Reclaimed != 1 {
			t.Fatalf("unrelated orphan did not progress: %+v", out)
		}
		*now = now.Add(2 * time.Hour)
	}
	if _, err = r.Get(ctx, "rooted"); err != nil {
		t.Fatal("future root lost", err)
	}
}
func TestGCSkewPersistedCoverageAcrossHosts(t *testing.T) {
	for _, future := range []bool{false, true} {
		t.Run(map[bool]string{false: "fast-reader", true: "future-writer"}[future], func(t *testing.T) {
			f, b, c, g, now := gcFixture(t)
			ctx := context.Background()
			r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
			if err := r.Put(ctx, "orphan", []byte("orphan")); err != nil {
				t.Fatal(err)
			}
			first := *now
			if future {
				*now = first.Add(testedGCSkew)
			}
			if _, err := c.ExecuteVolumeGC(ctx, "v1", b, g.cfg, true); err != nil {
				t.Fatal(err)
			}
			if future {
				*now = first
			} else {
				*now = first.Add(g.cfg.Grace + testedGCSkew/2)
			}
			out, err := c.ExecuteVolumeGC(ctx, "v1", b, g.cfg, true)
			if err != nil || out.Reclaimed != 0 {
				t.Fatalf("cross-host coverage shortened grace or blocked pass: %+v %v", out, err)
			}
			*now = now.Add(g.cfg.Grace + testedGCSkew + time.Nanosecond)
			out, err = c.ExecuteVolumeGC(ctx, "v1", b, g.cfg, true)
			if err != nil || out.Reclaimed != 1 {
				t.Fatalf("fresh safe coverage did not reclaim: %+v %v", out, err)
			}
		})
	}
}
func TestGCSkewBindingAgeIncludesSecondResolution(t *testing.T) {
	f, b, _, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err := r.Put(ctx, "orphan", []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	born := time.Unix(now.Unix(), 900000000)
	*now = born.Add(g.cfg.Grace)
	dir := path.Join(catBase, "v1", keysDir)
	entry, err := f.LookupEntry(ctx, dir, "orphan")
	if err != nil {
		t.Fatal(err)
	}
	entry.Attributes.Crtime = born.Add(-testedGCSkew).Unix()
	if err = f.UpdateEntry(ctx, dir, entry, nil); err != nil {
		t.Fatal(err)
	}
	out, err := g.Run(ctx)
	if err != nil || out.Candidates != 0 {
		t.Fatalf("binding age lost skew/second resolution: %+v %v", out, err)
	}
}
