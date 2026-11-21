package chunkstore

import (
	"context"
	"encoding/json"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"path"
	"strings"
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

func TestGCSkewConfigAndMarkCoverage(t *testing.T) {
	for _, skew := range []time.Duration{0, 2 * time.Minute} {
		t.Run(skew.String(), func(t *testing.T) {
			f, b, c, g, now := gcFixture(t)
			ctx := context.Background()
			r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
			if err := r.Put(ctx, "orphan", []byte("orphan")); err != nil {
				t.Fatal(err)
			}
			cfg := g.cfg
			cfg.ClockSkew = skew
			if _, err := c.ExecuteVolumeGC(ctx, "v1", b, cfg, true); err != nil {
				t.Fatal(err)
			}
			entry, err := f.LookupEntry(ctx, path.Join(catBase, "v1"), gcMarkName)
			if err != nil {
				t.Fatal(err)
			}
			var mark savedGCMark
			if err = json.Unmarshal(entry.Content, &mark); err != nil {
				t.Fatal(err)
			}
			if mark.ClockSkewNs <= 0 {
				t.Fatal("normalized clock policy absent from mark")
			}
			// A legacy mark, or one from another skew policy, cannot authorize deletion.
			mark.ClockSkewNs = 0
			entry.Content, _ = json.Marshal(mark)
			if err = f.UpdateEntry(ctx, path.Join(catBase, "v1"), entry, nil); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(2 * time.Hour)
			out, err := c.ExecuteVolumeGC(ctx, "v1", b, cfg, true)
			if err != nil || out.Reclaimed != 0 {
				t.Fatalf("legacy coverage borrowed: %+v %v", out, err)
			}
			*now = now.Add(2 * time.Hour)
			out, err = c.ExecuteVolumeGC(ctx, "v1", b, cfg, true)
			if err != nil || out.Reclaimed != 1 {
				t.Fatalf("matching normalized policy lost coverage: %+v %v", out, err)
			}
		})
	}
	_, b, c, g, _ := gcFixture(t)
	for _, bad := range []time.Duration{-1, time.Duration(1<<63 - 1)} {
		cfg := g.cfg
		cfg.ClockSkew = bad
		if _, err := NewCollector(c, b, cfg); err == nil {
			t.Fatalf("bad skew %s accepted", bad)
		}
	}
}

func TestGCSkewBackwardStepDiscardsCoverageAndPreviewDoesNotMutate(t *testing.T) {
	f, b, _, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err := r.Put(ctx, "orphan", []byte("orphan")); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Run(ctx); err != nil {
		t.Fatal(err)
	}
	first := g.last
	*now = now.Add(-time.Second)
	if out, err := g.Preview(ctx); err != nil || out.Reclaimed != 0 {
		t.Fatalf("backward preview: %+v %v", out, err)
	}
	if !g.last.Equal(first) || len(g.candidates) != 1 {
		t.Fatal("preview changed coverage")
	}
	if out, err := g.Run(ctx); err != nil || out.Reclaimed != 0 {
		t.Fatalf("backward mark: %+v %v", out, err)
	}
	*now = now.Add(g.cfg.Grace + g.cfg.ClockSkew + time.Nanosecond)
	if out, err := g.Run(ctx); err != nil || out.Reclaimed != 1 {
		t.Fatalf("fresh coverage made no progress: %+v %v", out, err)
	}
}

func TestGCSkewFutureCopyDoesNotBlockOtherSettlement(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	c.SetClock(func() time.Time { return *now })
	h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x55)})
	for _, name := range []string{"future", "expired"} {
		snapshotDir(t, f, "v1", name)
		holdFirst(f, path.Join(catBase, "v1", snapsDir, name)+"/")
		if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: name, RequestID: name}, h); CodeOf(err) != CodeOutcomeUnknown {
			t.Fatalf("held copy: %v", err)
		}
	}
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	ins, err := r.ListIntents(ctx)
	if err != nil || len(ins) != 2 {
		t.Fatalf("intents: %+v %v", ins, err)
	}
	*now = now.Add(DefaultOpDeadline + time.Second)
	futureID := ""
	for _, in := range ins {
		if !strings.Contains(in.Holder, "/future/") {
			continue
		}
		futureID = in.ID
		dir := path.Join(catBase, "v1", intentsDir)
		e, err := f.LookupEntry(ctx, dir, in.ID)
		if err != nil {
			t.Fatal(err)
		}
		var d intentDoc
		if err = json.Unmarshal(e.Content, &d); err != nil {
			t.Fatal(err)
		}
		d.CreatedNs = now.Add(testedGCSkew / 2).UnixNano()
		e.Content, _ = json.Marshal(d)
		if err = f.UpdateEntry(ctx, dir, e, nil); err != nil {
			t.Fatal(err)
		}
	}
	if futureID == "" {
		t.Fatal("future copy fixture not found")
	}
	if count, err := g.SweepCopies(ctx); err != nil || count != 1 {
		t.Fatalf("unrelated settlement blocked: %d %v", count, err)
	}
	left, err := r.ListIntents(ctx)
	if err != nil || len(left) != 1 || left[0].ID != futureID {
		t.Fatalf("future protection lost: %+v %v", left, err)
	}
	if out, err := g.Run(ctx); err != nil || len(out.Roots) == 0 {
		t.Fatalf("future copy not rooted: %+v %v", out, err)
	}
}
