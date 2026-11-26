package chunkstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// rerunFixture is a source with a snapshot and a restore target that
// depends on it (so the target's delete releases a source), on fresh fakes.
func rerunFixture(t *testing.T) (*BCatalog, *fakeFiler, *fakeBlobs, VolumeRef, VolumeRef) {
	t.Helper()
	ctx := context.Background()
	f, b := newFakeFiler(), newFakeBlobs()
	c, err := NewBCatalog(f, b, "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := newRef(t, "src"), newRef(t, "t")
	if _, err := c.CreateVolume(ctx, src, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	commitSnapshot(t, c, src, []byte("source chunk"), "daily")
	if _, err := c.Restore(ctx, src, "daily", tgt); err != nil {
		t.Fatalf("restore: %v", err)
	}
	return c, f, b, src, tgt
}

// requireDeletedAndReleased: the target's HEAD is deleted, none of its
// objects is left, and the source's snapshot no longer has it as a dependent.
func requireDeletedAndReleased(t *testing.T, c *BCatalog, src, tgt VolumeRef) {
	t.Helper()
	ctx := context.Background()
	ts, _, err := c.open(tgt)
	if err != nil {
		t.Fatal(err)
	}
	if h, _, err := ts.ReadHead(ctx); err != nil || h.State != layout.StateDeleted {
		t.Fatalf("target HEAD %+v %v; want deleted", h, err)
	}
	if objs, err := c.listObjects(ctx, tgt); err != nil || len(objs) != 0 {
		t.Fatalf("target objects %v %v; want none", objs, err)
	}
	if _, err := c.DeleteSnapshot(ctx, src, "daily"); err != nil {
		t.Fatalf("the source's snapshot is still held: %v", err)
	}
}

// A re-run of a completed delete succeeds: HEAD stays deleted, and the roots
// HEAD still names (whose objects the first run deleted) are reported.
func TestBCatalog_DeleteRerunOnADeletedVolumeSucceeds(t *testing.T) {
	ctx := context.Background()
	c, _, _, src, tgt := rerunFixture(t)
	if skipped, err := c.DeleteVolume(ctx, tgt, okDeleteAuthority{}); err != nil || len(skipped) != 0 {
		t.Fatalf("first delete: skipped=%v err=%v", skipped, err)
	}
	skipped, err := c.DeleteVolume(ctx, tgt, okDeleteAuthority{})
	if err != nil {
		t.Fatalf("re-run on a deleted volume: %v", err)
	}
	if len(skipped) == 0 {
		t.Fatal("the re-run read every root of the deleted HEAD; want the deleted ones reported")
	}
	requireDeletedAndReleased(t, c, src, tgt)
}

// The delete interrupted at each of its mutations (HEAD deleting, the
// source's release, each object, HEAD deleted), before that mutation
// applies, completes when re-run.
func TestBCatalog_DeleteInterruptedAtEachStepCompletesOnRerun(t *testing.T) {
	ctx := context.Background()
	interrupted := 0
	for k := 1; ; k++ {
		c, f, _, src, tgt := rerunFixture(t)
		n := 0
		fail := func(p string) error {
			n++
			if n == k {
				return errors.New("injected: interrupted")
			}
			return nil
		}
		f.mu.Lock()
		f.failUpdate, f.failDelete = fail, fail
		f.mu.Unlock()
		_, err := c.DeleteVolume(ctx, tgt, okDeleteAuthority{})
		f.mu.Lock()
		f.failUpdate, f.failDelete = nil, nil
		f.mu.Unlock()
		if err == nil {
			if interrupted < 3 {
				t.Fatalf("only %d mutations were interrupted; the fixture does not reach every step", interrupted)
			}
			requireDeletedAndReleased(t, c, src, tgt)
			return
		}
		interrupted++
		if _, err := c.DeleteVolume(ctx, tgt, okDeleteAuthority{}); err != nil {
			t.Fatalf("interrupted at mutation %d, the re-run failed: %v", k, err)
		}
		requireDeletedAndReleased(t, c, src, tgt)
	}
}

// Only a missing object of a deleting or deleted HEAD is skipped: a corrupt
// root of a deleting HEAD, and a missing root of a live HEAD, are errors.
func TestBCatalog_DeleteRerunSkipsOnlyMissingRootsOfADeletingVolume(t *testing.T) {
	ctx := context.Background()
	rootOf := func(t *testing.T, f *fakeFiler, tgt VolumeRef) string {
		t.Helper()
		f.mu.Lock()
		defer f.mu.Unlock()
		for p := range f.entries {
			if strings.Contains(p, "/"+tgt.Dir()+"/"+layout.RootDir+"/") {
				return p
			}
		}
		t.Fatal("fixture: the target has no root object")
		return ""
	}

	t.Run("corrupt root of a deleting HEAD", func(t *testing.T) {
		c, f, b, _, tgt := rerunFixture(t)
		// Interrupt right after HEAD deleting (at the source's release): HEAD
		// is deleting and every root is still there.
		n := 0
		f.mu.Lock()
		f.failUpdate = func(string) error {
			n++
			if n == 2 {
				return errors.New("injected: interrupted")
			}
			return nil
		}
		f.mu.Unlock()
		if _, err := c.DeleteVolume(ctx, tgt, okDeleteAuthority{}); err == nil {
			t.Fatal("fixture: the delete was not interrupted")
		}
		f.mu.Lock()
		f.failUpdate = nil
		f.mu.Unlock()
		root := rootOf(t, f, tgt)
		f.mu.Lock()
		e := f.entries[root]
		if len(e.Content) > 0 {
			e.Content = append([]byte("corrupt"), e.Content...)
		}
		for _, ch := range e.Chunks {
			b.mu.Lock()
			b.data[ch.FileId] = append([]byte("corrupt"), b.data[ch.FileId]...)
			b.mu.Unlock()
		}
		f.mu.Unlock()
		if _, err := c.DeleteVolume(ctx, tgt, okDeleteAuthority{}); !errors.Is(err, layout.ErrCorrupt) {
			t.Fatalf("re-run with a corrupt root: %v; want ErrCorrupt", err)
		}
	})

	for _, deleted := range []bool{false, true} {
		name := map[bool]string{false: "read failure of a root of a deleting HEAD", true: "read failure of a root of a deleted HEAD"}[deleted]
		t.Run(name, func(t *testing.T) {
			c, f, _, _, tgt := rerunFixture(t)
			root := rootOf(t, f, tgt)
			if deleted {
				// A completed delete: HEAD deleted, its roots' objects gone.
				if _, err := c.DeleteVolume(ctx, tgt, okDeleteAuthority{}); err != nil {
					t.Fatal(err)
				}
			} else {
				// Interrupted at the source's release: HEAD deleting, roots intact.
				n := 0
				f.mu.Lock()
				f.failUpdate = func(string) error {
					n++
					if n == 2 {
						return errors.New("injected: interrupted")
					}
					return nil
				}
				f.mu.Unlock()
				if _, err := c.DeleteVolume(ctx, tgt, okDeleteAuthority{}); err == nil {
					t.Fatal("fixture: the delete was not interrupted")
				}
			}
			ts, _, err := c.open(tgt)
			if err != nil {
				t.Fatal(err)
			}
			want := map[bool]string{false: layout.StateDeleting, true: layout.StateDeleted}[deleted]
			before, _, err := ts.ReadHead(ctx)
			if err != nil || before.State != want {
				t.Fatalf("fixture: HEAD %+v %v; want %s", before, err, want)
			}
			sentinel := errors.New("injected: filer read failed")
			f.mu.Lock()
			f.failUpdate = nil
			f.failLookup = func(p string) error {
				if p == root {
					return sentinel
				}
				return nil
			}
			updates, deletes := f.updates, f.deletes
			f.mu.Unlock()
			skipped, err := c.DeleteVolume(ctx, tgt, okDeleteAuthority{})
			f.mu.Lock()
			f.failLookup = nil
			updatesAfter, deletesAfter := f.updates, f.deletes
			f.mu.Unlock()
			if !errors.Is(err, sentinel) || len(skipped) != 0 {
				t.Fatalf("re-run with a root read failure: skipped=%v err=%v; want the read error and nothing skipped", skipped, err)
			}
			if updatesAfter != updates || deletesAfter != deletes {
				t.Fatalf("the failed re-run wrote: updates %d -> %d, deletes %d -> %d; want no source release and no object delete", updates, updatesAfter, deletes, deletesAfter)
			}
			if after, _, err := ts.ReadHead(ctx); err != nil || after.Gen != before.Gen || after.State != want {
				t.Fatalf("HEAD after the failed re-run %+v %v; want it unchanged (gen %d, %s)", after, err, before.Gen, want)
			}
		})
	}

	t.Run("missing root of a live HEAD", func(t *testing.T) {
		c, f, _, _, tgt := rerunFixture(t)
		root := rootOf(t, f, tgt)
		f.mu.Lock()
		delete(f.entries, root)
		f.mu.Unlock()
		if _, err := c.DeleteVolume(ctx, tgt, okDeleteAuthority{}); !errors.Is(err, layout.ErrMissing) {
			t.Fatalf("delete of a live volume with a missing root: %v; want ErrMissing", err)
		}
		ts, _, err := c.open(tgt)
		if err != nil {
			t.Fatal(err)
		}
		if h, _, err := ts.ReadHead(ctx); err != nil || h.State == layout.StateDeleting || h.State == layout.StateDeleted {
			t.Fatalf("target HEAD %+v %v; want it left as it was", h, err)
		}
	})
}
