package chunkstore

import (
	"context"
	"errors"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// CHANGE M6: PurgeDone report section 9. Production is gated on the
// exhaustive model verdict and its final invariant row. No purge runs here
// against a real filer or volume server.
const purgeTestDoneStamp = "swb.gc.done_ns"

func purgeTestFixture(t *testing.T) (*fakeFiler, gcBlobs, *Catalog, *Collector, *time.Time) {
	t.Helper()
	f, b, _, g, now := gcFixture(t)
	c, err := NewCatalog(manifestFilerBlobs{f, b}, catBase)
	if err != nil {
		t.Fatal(err)
	}
	g.cat = c
	return f, b, c, g, now
}

func putPurgeTestBinding(t *testing.T, f *fakeFiler, b gcBlobs, key, state string, stamp time.Time) (*ChunkRemote, *filerpb.Entry) {
	t.Helper()
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Put(ctx, key, []byte("unrooted data")); err != nil {
		t.Fatal(err)
	}
	dir, name, err := r.keyLocation(key)
	if err != nil {
		t.Fatal(err)
	}
	e, err := f.LookupEntry(ctx, dir, name)
	if err != nil {
		t.Fatal(err)
	}
	if state != "" {
		e.Extended[extGCDeleted] = []byte(state)
	}
	if !stamp.IsZero() {
		e.Extended[purgeTestDoneStamp] = []byte(strconv.FormatInt(stamp.UnixNano(), 10))
	}
	if err = f.UpdateEntry(ctx, dir, e, nil); err != nil {
		t.Fatal(err)
	}
	if state == "done" {
		if err = b.DeleteBlob(ctx, string(e.Extended[extFid])); err != nil {
			t.Fatal(err)
		}
	}
	return r, e
}

func TestGCDonePurgeMissingOwnRootFailsButUnuploadedIntentDoesNot(t *testing.T) {
	for _, root := range []string{"HEAD", "snapshot"} {
		t.Run(root, func(t *testing.T) {
			f, b, c, g, _ := purgeTestFixture(t)
			ctx := context.Background()
			h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x21)})
			r, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
			if err != nil {
				t.Fatal(err)
			}
			if root == "snapshot" {
				if _, err = c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "purge-root"}, h); err != nil {
					t.Fatal(err)
				}
				if _, err = r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h.Gen, Epoch: h.Epoch}, storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: h.RemoteLSN}); err != nil {
					t.Fatal(err)
				}
			}
			dir, name, err := r.keyLocation(h.Chunks[0].Key)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.DeleteEntry(ctx, dir, name, nil); err != nil {
				t.Fatal(err)
			}
			deletes := f.deletes
			if _, err = g.Run(ctx); err == nil {
				t.Fatalf("%s names a missing own binding but GC accepted the mark", root)
			}
			if f.deletes != deletes {
				t.Fatal("failed root mark deleted metadata")
			}
		})
	}
	t.Run("unuploaded-intent", func(t *testing.T) {
		f, b, _, g, _ := purgeTestFixture(t)
		r, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = r.PutIntent(context.Background(), storage.ChunkIntent{Epoch: 1, Holder: "h1", Keys: []string{"not-yet-uploaded"}}); err != nil {
			t.Fatal(err)
		}
		if _, err = g.Run(context.Background()); err != nil {
			t.Fatalf("intent must protect a key before its first upload: %v", err)
		}
	})
}

type purgeReplaceFiler struct {
	Filer
	beforeDelete func(context.Context, string, string, map[string][]byte) error
}

func (f *purgeReplaceFiler) DeleteEntry(ctx context.Context, dir, name string, expected map[string][]byte) error {
	if err := f.beforeDelete(ctx, dir, name, expected); err != nil {
		return err
	}
	return f.Filer.DeleteEntry(ctx, dir, name, expected)
}

func TestGCDonePurgeConditionalDeleteProtectsNewIncarnation(t *testing.T) {
	for _, replacement := range []string{"new-fid", "same-fid-new-done-stamp"} {
		t.Run(replacement, func(t *testing.T) {
			f, b, c, g, now := purgeTestFixture(t)
			r, original := putPurgeTestBinding(t, f, b, "orphan", "done", now.Add(-2*time.Hour))
			attempted := false
			c.f = &purgeReplaceFiler{Filer: f, beforeDelete: func(ctx context.Context, dir, name string, expected map[string][]byte) error {
				if dir != r.keyDir() || name != original.Name {
					return nil
				}
				attempted = true
				if string(expected[extGCDeleted]) != "done" || string(expected[extFid]) != string(original.Extended[extFid]) || string(expected[purgeTestDoneStamp]) != string(original.Extended[purgeTestDoneStamp]) {
					t.Errorf("delete does not bind the observed DONE incarnation: %v", expected)
				}
				current, err := f.LookupEntry(ctx, dir, name)
				if err != nil {
					return err
				}
				if replacement == "new-fid" {
					current.Extended[extFid] = []byte("fresh-fid")
					delete(current.Extended, extGCDeleted)
				}
				current.Extended[purgeTestDoneStamp] = []byte(strconv.FormatInt(now.UnixNano(), 10))
				return f.UpdateEntry(ctx, dir, current, nil)
			}}
			_, err := g.Run(context.Background())
			if !attempted {
				t.Fatal("eligible DONE binding never reached conditional delete")
			}
			if !errors.Is(err, ErrPrecondition) {
				t.Fatalf("changed incarnation must fail the pass conservatively: %v", err)
			}
			entry, err := f.LookupEntry(context.Background(), r.keyDir(), original.Name)
			if err != nil || string(entry.Extended[purgeTestDoneStamp]) != strconv.FormatInt(now.UnixNano(), 10) {
				t.Fatalf("replacement was deleted: %v %v", entry, err)
			}
		})
	}
}

func TestGCDonePurgeLegacyDoneStartsItsOwnGrace(t *testing.T) {
	f, b, _, g, now := purgeTestFixture(t)
	r, entry := putPurgeTestBinding(t, f, b, "legacy", "done", time.Time{})
	ctx := context.Background()
	if _, err := g.Run(ctx); err != nil {
		t.Fatal(err)
	}
	stamped, err := f.LookupEntry(ctx, r.keyDir(), entry.Name)
	if err != nil || string(stamped.Extended[purgeTestDoneStamp]) != strconv.FormatInt(now.UnixNano(), 10) {
		t.Fatalf("legacy DONE must be stamped, not purged, on observation: %v %v", stamped, err)
	}
	*now = now.Add(time.Hour + time.Minute)
	if _, err = g.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = f.LookupEntry(ctx, r.keyDir(), entry.Name); err != nil {
		t.Fatal("legacy DONE purged at the exact grace+skew boundary", err)
	}
	*now = now.Add(time.Nanosecond)
	if _, err = g.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = f.LookupEntry(ctx, r.keyDir(), entry.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("legacy DONE not purged after its own grace: %v", err)
	}
}

func TestGCDonePurgePreviewNeverStampsOrDeletes(t *testing.T) {
	f, b, _, g, now := purgeTestFixture(t)
	_, _ = putPurgeTestBinding(t, f, b, "legacy", "done", time.Time{})
	_, _ = putPurgeTestBinding(t, f, b, "old", "done", now.Add(-2*time.Hour))
	updates, deletes := f.updates, f.deletes
	if _, err := g.Preview(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.updates != updates || f.deletes != deletes {
		t.Fatalf("preview changed metadata: updates=%d deletes=%d", f.updates-updates, f.deletes-deletes)
	}
}

func TestGCDonePurgeRootedDoneFailsWithoutDeletingIt(t *testing.T) {
	f, b, c, g, now := purgeTestFixture(t)
	ctx := context.Background()
	h := publishSource(t, f, b.fakeBlobs, "v1", map[uint64][]byte{0: chunkBytes(0x41)})
	r, err := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if err != nil {
		t.Fatal(err)
	}
	dir, name, err := r.keyLocation(h.Chunks[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := f.LookupEntry(ctx, dir, name)
	if err != nil {
		t.Fatal(err)
	}
	entry.Extended[extGCDeleted] = []byte("done")
	entry.Extended[purgeTestDoneStamp] = []byte(strconv.FormatInt(now.Add(-2*time.Hour).UnixNano(), 10))
	if err = f.UpdateEntry(ctx, dir, entry, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = g.Run(ctx); err == nil {
		t.Fatal("rooted DONE must fail the pass rather than be purged")
	}
	if _, err = f.LookupEntry(ctx, dir, name); err != nil {
		t.Fatal("rooted DONE binding was purged", err)
	}
	result, err := c.Verify(ctx, "v1", VerifyOptions{Depth: VerifyBindings})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Findings) != 1 || result.Findings[0].Class != FindingBindingTombstoned {
		t.Fatalf("before purge verify must report TOMBSTONED: %+v", result.Findings)
	}
}

func TestGCDonePurgeAdmissionUsesDoneAgeAndSkew(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		age         time.Duration
		purged      bool
	}{
		{"old-done", "done", time.Hour + time.Minute + time.Nanosecond, true},
		{"at-boundary", "done", time.Hour + time.Minute, false},
		{"young-done", "done", time.Hour, false},
		{"future-done", "done", -time.Hour, false},
		{"tombstoned", "1", 24 * time.Hour, false},
		{"unflagged", "", 24 * time.Hour, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, b, _, g, now := purgeTestFixture(t)
			r, e := putPurgeTestBinding(t, f, b, "orphan", tc.state, now.Add(-tc.age))
			if _, err := g.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err := f.LookupEntry(context.Background(), r.keyDir(), e.Name)
			if tc.purged {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("eligible DONE binding remains: %v", err)
				}
			} else if err != nil {
				t.Fatalf("ineligible binding disappeared: %v", err)
			}
		})
	}
}
