package chunkstore

import (
	"context"
	"errors"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestVolumeGC_ForeignIntentRootsAndLocalCandidates(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v2", IsDirectory: true}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnsureVolume(ctx, "v2", catGeo); err != nil {
		t.Fatal(err)
	}
	v1, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	v2, _ := NewChunkRemote(f, b, path.Join(catBase, "v2"))
	for _, key := range []string{"rooted-only-from-v2", "local-orphan"} {
		if err := v1.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := v2.Put(ctx, "foreign-orphan", []byte("foreign")); err != nil {
		t.Fatal(err)
	}
	if _, err := v2.PutIntent(ctx, storage.ChunkIntent{Epoch: 1, Holder: "snapshot:v2/s1/req", Keys: []string{"@v1/rooted-only-from-v2"}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.ExecuteVolumeGC(ctx, "v1", b, g.cfg, true); err != nil {
			t.Fatal(err)
		}
		*now = now.Add(2 * time.Hour)
	}
	if _, err := v1.Get(ctx, "local-orphan"); err == nil {
		t.Error("local orphan not reclaimed")
	}
	if _, err := v1.Get(ctx, "rooted-only-from-v2"); err != nil {
		t.Errorf("foreign root lost: %v", err)
	}
	if _, err := v2.Get(ctx, "foreign-orphan"); err != nil {
		t.Errorf("scoped pass reclaimed foreign candidate: %v", err)
	}
}

// Grace must cover the operation deadline, the clock skew and the CAS apply
// bound (default: the deadline), so a snapshot deleted while a fork of it is
// in flight stays rooted until that fork can no longer publish.
func TestGC_GraceConstructorRequiresDeadlineSkewAndApplyBound(t *testing.T) {
	_, b, c, g, _ := gcFixture(t)
	need := c.deadline() + DefaultGCClockSkew + c.deadline()
	for _, grace := range []time.Duration{0, c.deadline(), c.deadline() + DefaultGCClockSkew, need - time.Nanosecond} {
		cfg := g.cfg
		cfg.Grace, cfg.ClockSkew, cfg.CASApplyBound = grace, 0, 0
		if _, err := NewCollector(c, b, cfg); err == nil {
			t.Errorf("accepted grace %s < deadline + skew + apply bound %s", grace, need)
		}
	}
	cfg := g.cfg
	cfg.Grace, cfg.ClockSkew, cfg.CASApplyBound = need, 0, 0
	if _, err := NewCollector(c, b, cfg); err != nil {
		t.Fatalf("grace = deadline + skew + apply bound: %v", err)
	}
	cfg.Grace, cfg.CASApplyBound = need, time.Second // an explicit, smaller bound
	if _, err := NewCollector(c, b, cfg); err != nil {
		t.Fatal(err)
	}
	cfg.CASApplyBound = -1
	if _, err := NewCollector(c, b, cfg); err == nil {
		t.Fatal("accepted a negative CAS apply bound")
	}
}

func TestVolumeGC_DoesNotImportGlobalMark(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err := r.Put(ctx, "orphan", []byte("bytes")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ExecuteGC(ctx, b, g.cfg, true); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2 * time.Hour)
	// A separate scope must take its own first mark, even with mature global coverage.
	result, err := c.ExecuteVolumeGC(ctx, "v1", b, g.cfg, true)
	if err != nil || result.Reclaimed != 0 {
		t.Fatalf("scope imported namespace coverage: %+v %v", result, err)
	}
	if _, err = f.LookupEntry(ctx, path.Join(catBase, "v1"), gcMarkName); errors.Is(err, ErrNotFound) {
		t.Fatal("scoped watermark absent")
	}
}

func TestVolumeGC_IncompletePassAndConfigChangeDiscardCoverage(t *testing.T) {
	for _, mode := range []string{"incomplete", "config"} {
		t.Run(mode, func(t *testing.T) {
			f, b, c, g, now := gcFixture(t)
			ctx := context.Background()
			r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
			if err := r.Put(ctx, "orphan", []byte("bytes")); err != nil {
				t.Fatal(err)
			}
			if _, err := c.ExecuteVolumeGC(ctx, "v1", b, g.cfg, true); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(2 * time.Hour)
			if mode == "incomplete" {
				g.scope = "v1"
				_, token, err := g.readMark(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = g.writeMark(ctx, g.mark(false), token); err != nil {
					t.Fatal(err)
				}
			} else {
				g.cfg.IntentTTL += time.Second
			}
			result, err := c.ExecuteVolumeGC(ctx, "v1", b, g.cfg, true)
			if err != nil || result.Reclaimed != 0 {
				t.Fatalf("stale coverage used: %+v %v", result, err)
			}
			*now = now.Add(2 * time.Hour)
			result, err = c.ExecuteVolumeGC(ctx, "v1", b, g.cfg, true)
			if err != nil || result.Reclaimed != 1 {
				t.Fatalf("fresh coverage did not progress: %+v %v", result, err)
			}
		})
	}
}

func TestVolumeGC_ForeignForkHeadIsSoleRoot(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x22)})
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "req"}, h); err != nil {
		t.Fatal(err)
	}
	if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v2", IsDirectory: true}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "fork"}); err != nil {
		t.Fatal(err)
	}
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: h.RemoteLSN, Chunks: map[uint64]storage.ChunkRef{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if out, err := c.ExecuteVolumeGC(ctx, "v1", b, g.cfg, true); err != nil || out.Reclaimed != 0 {
			t.Fatalf("foreign-only fork root reclaimed: %+v %v", out, err)
		}
		*now = now.Add(2 * time.Hour)
	}
	for _, ref := range h.Chunks {
		if _, err := r.Get(ctx, ref.Key); err != nil {
			t.Fatal("fork bytes reclaimed", err)
		}
	}
}

func TestVolumeGC_ForeignEligibleCaptureIntentStaysProtected(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v2", IsDirectory: true}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnsureVolume(ctx, "v2", catGeo); err != nil {
		t.Fatal(err)
	}
	remotes := map[string]*ChunkRemote{}
	for _, vol := range []string{"v1", "v2"} {
		remote, _ := NewChunkRemote(f, b, path.Join(catBase, vol))
		remotes[vol] = remote
		if err := remote.Put(ctx, "capture", []byte("protected")); err != nil {
			t.Fatal(err)
		}
		if _, err := remote.PutIntent(ctx, storage.ChunkIntent{Epoch: 1, Holder: "old", Keys: []string{"capture"}}); err != nil {
			t.Fatal(err)
		}
		cur, err := remote.GetHead(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := remote.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, storage.ChunkHead{Epoch: 2, Holder: "new", Chunks: map[uint64]storage.ChunkRef{}}); err != nil {
			t.Fatal(err)
		}
	}
	g.cfg.IssuedEpoch = func(context.Context, string) (uint64, error) { return 2, nil }
	*now = time.Now().Add(3 * time.Hour)
	foreign, _ := remotes["v2"].ListIntents(ctx)
	result, err := c.ExecuteVolumeGC(ctx, "v1", b, g.cfg, true)
	local, _ := remotes["v1"].ListIntents(ctx)
	after, _ := remotes["v2"].ListIntents(ctx)
	if err != nil || result.ExpiredIntents != 1 || len(local) != 0 || !reflect.DeepEqual(foreign, after) {
		t.Fatalf("capture scope mutated foreign intent: %+v %v before=%+v after=%+v", result, err, foreign, after)
	}
	if _, err := remotes["v2"].Get(ctx, "capture"); err != nil {
		t.Fatal("foreign protection lost", err)
	}
	result, err = c.ExecuteVolumeGC(ctx, "v2", b, g.cfg, true)
	if err != nil || result.ExpiredIntents != 1 {
		t.Fatalf("foreign positive eligibility not witnessed: %+v %v", result, err)
	}
}
func TestVolumeGC_ForeignEligibleCopyIsNotSettled(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	c.SetClock(func() time.Time { return *now })
	if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: "v2", IsDirectory: true}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.EnsureVolume(ctx, "v2", catGeo); err != nil {
		t.Fatal(err)
	}
	for _, vol := range []string{"v1", "v2"} {
		h := publishSource(t, f, b.fakeBlobs, vol, map[uint64][]byte{0: chunkBytes(0x55)})
		snapshotDir(t, f, vol, "held")
		holdFirst(f, path.Join(catBase, vol, snapsDir, "held")+"/")
		if _, err := c.CreateSnapshot(ctx, vol, SnapshotRequest{Name: "held", RequestID: "dead"}, h); CodeOf(err) != CodeOutcomeUnknown {
			t.Fatalf("copy not held: %v", err)
		}
	}
	before := intentsOf(t, f, "v2")
	foreignPath := snapPath(t, f, "v2", "held")
	*now = now.Add(DefaultOpDeadline + time.Second)
	result, err := c.ExecuteVolumeGC(ctx, "v1", b, g.cfg, true)
	after := intentsOf(t, f, "v2")
	if err != nil || result.SettledCopies != 1 || len(intentsOf(t, f, "v1")) != 0 || !reflect.DeepEqual(before, after) {
		t.Fatalf("copy scope changed foreign attempt: %+v %v", result, err)
	}
	if _, err := f.LookupEntry(ctx, path.Dir(foreignPath), path.Base(foreignPath)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign pending copy claimed: %v", err)
	}
	result, err = c.ExecuteVolumeGC(ctx, "v2", b, g.cfg, true)
	if err != nil || result.SettledCopies != 1 || len(intentsOf(t, f, "v2")) != 0 {
		t.Fatalf("foreign positive settlement not witnessed: %+v %v", result, err)
	}
}
