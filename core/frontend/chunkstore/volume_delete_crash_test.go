package chunkstore

import (
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// nthUpdate matches the n-th UpdateEntry (1-based) of the entry at p.
func nthUpdate(p string, n int) func(op, q string) bool {
	seen := 0
	return func(op, q string) bool {
		if op != "update" || q != p {
			return false
		}
		seen++
		return seen == n
	}
}

// requireVolumeDeleted checks the end state of a completed volume delete:
// HEAD deleted, the volume refused for ever, the reservation committed.
func requireVolumeDeleted(t *testing.T, c *Catalog, rsv *fakeReservation, vol string) {
	t.Helper()
	ctx := context.Background()
	h, ok, err := c.ReadHead(ctx, vol)
	if err != nil || !ok || h.State != storage.ChunkHeadDeleted {
		t.Fatalf("HEAD after the delete: %+v ok %v %v; want deleted", h, ok, err)
	}
	if _, err := c.EnsureVolume(ctx, vol, catGeo); CodeOf(err) != CodeVolumeDeleted {
		t.Fatalf("attach after the delete: %v; want volume_deleted", err)
	}
	rsv.mu.Lock()
	defer rsv.mu.Unlock()
	if rsv.state != reservationCommitted {
		t.Fatalf("the reservation is %q after the delete; want committed", rsv.state)
	}
}

// TestCatalogVolumeDeleteCrashAtEachStepThenRerunCompletes: a volume delete
// whose process dies at each of its filer writes (the HEAD mark, the commit
// to purging, the HEAD deleted, the VOLUME deleted), before the write reaches
// the filer or after it applied with its reply lost. A rerun with the same
// request, through a restarted catalog, completes: HEAD deleted, the volume
// refused for ever, the reservation committed. Nothing stays deleting or
// purging.
func TestCatalogVolumeDeleteCrashAtEachStepThenRerunCompletes(t *testing.T) {
	headPath := path.Join(catBase, "v1", headName)
	volumePath := path.Join(catBase, "v1", volumeName)
	// left is the HEAD state each crash leaves for the rerun: before the
	// write applied, and after.
	steps := []struct {
		name string
		p    string
		n    int
		left [2]string
	}{
		{"the HEAD mark (deleting)", headPath, 1, [2]string{storage.ChunkHeadDetached, storage.ChunkHeadDeleting}},
		{"the commit (purging)", headPath, 2, [2]string{storage.ChunkHeadDeleting, storage.ChunkHeadPurging}},
		{"HEAD deleted", headPath, 3, [2]string{storage.ChunkHeadPurging, storage.ChunkHeadDeleted}},
		{"VOLUME deleted", volumePath, 1, [2]string{storage.ChunkHeadDeleted, storage.ChunkHeadDeleted}},
	}
	for _, step := range steps {
		for _, after := range []bool{false, true} {
			name := fmt.Sprintf("%s, %s", step.name, map[bool]string{false: "before it applied", true: "applied, reply lost"}[after])
			t.Run(name, func(t *testing.T) {
				f, b, _, _ := deleteFixture(t)
				ctx := context.Background()
				detachHead(t, f, b, "v1")
				rsv := &fakeReservation{}
				crashed := &crashFiler{manifestFilerBlobs: manifestFilerBlobs{f, gcBlobs{b}}, after: after, match: nthUpdate(step.p, step.n)}
				dying, err := NewCatalog(crashed, catBase)
				if err != nil {
					t.Fatal(err)
				}
				if res, err := dying.DeleteVolume(ctx, "v1", rsv.admission("d1")); err == nil {
					t.Fatalf("the delete returned success although its process died: %+v", res)
				}
				crashed.mu.Lock()
				died := crashed.dead
				crashed.mu.Unlock()
				if !died {
					t.Fatalf("the delete never reached the step %q", name)
				}

				c, err := NewCatalog(manifestFilerBlobs{f, gcBlobs{b}}, catBase) // the restarted process
				if err != nil {
					t.Fatal(err)
				}
				left := step.left[0]
				if after {
					left = step.left[1]
				}
				if got := headState(t, c, "v1"); got.State != left {
					t.Fatalf("after the crash HEAD is %q; want %q", got.State, left)
				}
				res, err := c.DeleteVolume(ctx, "v1", rsv.admission("d1"))
				if err != nil || !res.Completed {
					t.Fatalf("the rerun after a crash at %s: %+v %v; want completed", name, res, err)
				}
				requireVolumeDeleted(t, c, rsv, "v1")
				if res, err := c.DeleteVolume(ctx, "v1", rsv.admission("d1")); err != nil || !res.Completed {
					t.Fatalf("a further rerun: %+v %v; want completed (replayed)", res, err)
				}
			})
		}
	}
}

// TestCatalogVolumeDeleteCrashAtTheRevertThenRerunCompletes: a volume delete
// finds a snapshot name and reverts its mark, and its process dies at the
// revert (before it applied, or applied with its reply lost). Through a
// restarted catalog a rerun is refused while the snapshot exists
// (volume_has_snapshots) and leaves HEAD as it was before the delete; once
// the snapshot is deleted a rerun completes.
func TestCatalogVolumeDeleteCrashAtTheRevertThenRerunCompletes(t *testing.T) {
	headPath := path.Join(catBase, "v1", headName)
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before it applied", true: "applied, reply lost"}[after], func(t *testing.T) {
			f, b, c0, h := deleteFixture(t)
			ctx := context.Background()
			detachHead(t, f, b, "v1")
			if _, err := c0.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r-s1"}, h); err != nil {
				t.Fatal(err)
			}
			before := headState(t, c0, "v1")
			rsv := &fakeReservation{}
			crashed := &crashFiler{manifestFilerBlobs: manifestFilerBlobs{f, gcBlobs{b}}, after: after, match: nthUpdate(headPath, 2)}
			dying, err := NewCatalog(crashed, catBase)
			if err != nil {
				t.Fatal(err)
			}
			if res, err := dying.DeleteVolume(ctx, "v1", rsv.admission("d1")); err == nil {
				t.Fatalf("the delete returned success although its process died: %+v", res)
			}
			crashed.mu.Lock()
			died := crashed.dead
			crashed.mu.Unlock()
			if !died {
				t.Fatal("the delete never reached its revert")
			}

			c, err := NewCatalog(manifestFilerBlobs{f, gcBlobs{b}}, catBase)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.DeleteVolume(ctx, "v1", rsv.admission("d1")); CodeOf(err) != CodeVolumeHasSnapshots {
				t.Fatalf("a rerun while the snapshot exists: %v; want volume_has_snapshots", err)
			}
			if got := headState(t, c, "v1"); got.State != before.State {
				t.Fatalf("HEAD after the refused rerun is %q; want %q as before the delete", got.State, before.State)
			}
			if _, err := c.DeleteSnapshot(ctx, "v1", "s1"); err != nil {
				t.Fatal(err)
			}
			if res, err := c.DeleteVolume(ctx, "v1", rsv.admission("d1")); err != nil || !res.Completed {
				t.Fatalf("the rerun once the snapshot is deleted: %+v %v; want completed", res, err)
			}
			requireVolumeDeleted(t, c, rsv, "v1")
		})
	}
}
