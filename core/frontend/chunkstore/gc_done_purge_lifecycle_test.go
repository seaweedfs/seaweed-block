package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func purgeTwoMarks(t *testing.T, g *Collector, now *time.Time) {
	t.Helper()
	if _, err := g.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(g.cfg.Grace + g.cfg.ClockSkew + time.Second)
	if _, err := g.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type purgeStampFiler struct {
	Filer
	sawDone bool
	badDone bool
}

func (f *purgeStampFiler) UpdateEntry(ctx context.Context, dir string, e *filerpb.Entry, expected map[string][]byte) error {
	if string(e.Extended[extGCDeleted]) == "done" {
		f.sawDone = true
		if len(e.Extended[purgeTestDoneStamp]) == 0 || string(expected[extGCDeleted]) != "1" || len(expected[extFid]) == 0 {
			f.badDone = true
		}
	}
	return f.Filer.UpdateEntry(ctx, dir, e, expected)
}

// PurgeDone section 9.3: DONE and its collector-clock stamp are one
// conditional transition, not two separately visible updates.
func TestGCDonePurgeReclamationStampsDoneAtomically(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	r, entry := putPurgeTestBinding(t, f, b, "orphan", "", time.Time{})
	trace := &purgeStampFiler{Filer: f}
	c.f = trace
	purgeTwoMarks(t, g, now)
	e, err := f.LookupEntry(context.Background(), r.keyDir(), entry.Name)
	if err != nil || string(e.Extended[extGCDeleted]) != "done" {
		t.Fatalf("reclamation did not reach DONE: %v %v", e, err)
	}
	if !trace.sawDone || trace.badDone || string(e.Extended[purgeTestDoneStamp]) != strconv.FormatInt(now.UnixNano(), 10) {
		t.Fatalf("DONE was not atomically collector-stamped: saw=%t bad=%t entry=%v", trace.sawDone, trace.badDone, e)
	}
}

// Sections 9.4 and 9.6: the horizon changes only the tombstone lifetime;
// the surviving/rebound binding remains write-once while it exists.
func TestGCDonePurgeStalePlainAndRoundBindingsAreCollectedAgain(t *testing.T) {
	for _, mode := range []string{"plain-new-fid", "round-same-fid"} {
		t.Run(mode, func(t *testing.T) {
			f, b, _, g, now := gcFixture(t)
			ctx := context.Background()
			r, old := putPurgeTestBinding(t, f, b, "orphan", "done", now.Add(-2*time.Hour))
			if _, err := g.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := f.LookupEntry(ctx, r.keyDir(), old.Name); !errors.Is(err, ErrNotFound) {
				t.Fatalf("prerequisite old DONE purge failed: %v", err)
			}
			if mode == "plain-new-fid" {
				fresh, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
				if err != nil {
					t.Fatal(err)
				}
				if err = fresh.Put(ctx, "orphan", []byte("late create payload")); err != nil {
					t.Fatalf("key is reusable only after its DONE binding is purged: %v", err)
				}
			} else {
				delete(old.Extended, extGCDeleted)
				delete(old.Extended, purgeTestDoneStamp)
				if err := f.CreateEntry(ctx, r.keyDir(), old, true); err != nil {
					t.Fatalf("late round bind must be accepted after purge: %v", err)
				}
			}
			bound, err := f.LookupEntry(ctx, r.keyDir(), old.Name)
			if err != nil {
				t.Fatal(err)
			}
			if same := bytes.Equal(bound.Extended[extFid], old.Extended[extFid]); same != (mode == "round-same-fid") {
				t.Fatalf("wrong stale-create incarnation: old=%q new=%q", old.Extended[extFid], bound.Extended[extFid])
			}
			fresh, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
			if err != nil {
				t.Fatal(err)
			}
			if err = fresh.Put(ctx, "orphan", []byte("replacement")); !errors.Is(err, storage.ErrChunkKeyExists) {
				t.Fatalf("existing rebound key was overwritten: %v", err)
			}
			purgeTwoMarks(t, g, now)
			reclaimed, err := f.LookupEntry(ctx, r.keyDir(), old.Name)
			if err != nil || string(reclaimed.Extended[extGCDeleted]) != "done" {
				t.Fatalf("unrooted late binding not recollected: %v %v", reclaimed, err)
			}
			*now = now.Add(g.cfg.Grace + g.cfg.ClockSkew + time.Nanosecond)
			if _, err = g.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = f.LookupEntry(ctx, r.keyDir(), old.Name); !errors.Is(err, ErrNotFound) {
				t.Fatalf("recollected late binding not purged again: %v", err)
			}
		})
	}
}

func TestGCDonePurgeUsageDropsOnlyPurgedStoredBindings(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	c.f = manifestFilerBlobs{f, b}
	ctx := context.Background()
	_ = publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x31)})
	_, _ = putPurgeTestBinding(t, f, b, "old-data", "done", now.Add(-2*time.Hour))
	r, segment := putPurgeTestBinding(t, f, b, "old-segment", "done", now.Add(-2*time.Hour))
	segment.Extended["swb.object"] = []byte("segment")
	segment.Attributes.FileSize = 19
	if err := f.UpdateEntry(ctx, r.keyDir(), segment, nil); err != nil {
		t.Fatal(err)
	}
	before, err := c.Usage(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if before.StoredBindings != 4 || before.ManifestBindings != 2 {
		t.Fatalf("fixture usage: %+v", before)
	}
	if _, err = g.Run(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := c.Usage(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if after.StoredBindings != 2 || after.ManifestBindings != 1 || after.LiveBytes != before.LiveBytes {
		t.Fatalf("purged data/segment still counted or live bytes changed: before=%+v after=%+v", before, after)
	}
}

// Sections 9.7/9.9: restore an old root only AFTER purge to simulate a
// stale reader/invalid retained root. GC itself must never manufacture it.
func TestGCDonePurgeStaleRootIsMissingAndWrongRebindFailsHash(t *testing.T) {
	for _, rebind := range []bool{false, true} {
		name := "missing"
		if rebind {
			name = "wrong-rebind"
		}
		t.Run(name, func(t *testing.T) {
			f, b, c, g, now := gcFixture(t)
			c.f = manifestFilerBlobs{f, b}
			ctx := context.Background()
			h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x51)})
			r, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: h.RemoteLSN}); err != nil {
				t.Fatal(err)
			}
			dir, key, err := r.keyLocation(h.Chunks[0].Key)
			if err != nil {
				t.Fatal(err)
			}
			binding, err := f.LookupEntry(ctx, dir, key)
			if err != nil {
				t.Fatal(err)
			}
			binding.Extended[extGCDeleted] = []byte("done")
			binding.Extended[purgeTestDoneStamp] = []byte(strconv.FormatInt(now.Add(-2*time.Hour).UnixNano(), 10))
			if err = f.UpdateEntry(ctx, dir, binding, nil); err != nil {
				t.Fatal(err)
			}
			if err = b.DeleteBlob(ctx, string(binding.Extended[extFid])); err != nil {
				t.Fatal(err)
			}
			if _, err = g.Run(ctx); err != nil {
				t.Fatal(err)
			}
			fresh, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = fresh.Get(ctx, h.Chunks[0].Key); !errors.Is(err, ErrNotFound) {
				t.Fatalf("stale root must resolve to binding missing after purge: %v", err)
			}
			if rebind {
				if err = fresh.Put(ctx, h.Chunks[0].Key, chunkBytes(0x62)); err != nil {
					t.Fatal(err)
				}
			}
			cur, err := fresh.GetHead(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = fresh.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, h); err != nil {
				t.Fatal(err)
			}
			verified, err := c.Verify(ctx, "v1", VerifyOptions{Depth: VerifyData})
			if err != nil {
				t.Fatal(err)
			}
			want := FindingBindingMissing
			if rebind {
				want = FindingContentMismatch
			}
			found := false
			for _, finding := range verified.Findings {
				found = found || finding.Class == want
			}
			if !found {
				t.Fatalf("verify lacks %s: %+v", want, verified.Findings)
			}
			store, err := storage.CreateWALStoreWithOptions(filepath.Join(t.TempDir(), "volume"), 64, 4096, storage.WALStoreOptions{ChunkBlocks: 8})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if _, err = store.Recover(); err != nil {
				t.Fatal(err)
			}
			if _, err = storage.AttachChunkLayer(store, fresh, storage.ChunkLayerConfig{ChunkBlocks: 8, Epoch: 1, Holder: "h1", FreshFromHead: true}); err != nil {
				t.Fatal(err)
			}
			got, err := store.Read(0)
			if err == nil || len(got) != 0 {
				t.Fatalf("stale read returned bytes: len=%d err=%v", len(got), err)
			}
			if rebind && !strings.Contains(err.Error(), "hash") {
				t.Fatalf("wrong bytes failed for a reason other than hash validation: %v", err)
			}
		})
	}
}
