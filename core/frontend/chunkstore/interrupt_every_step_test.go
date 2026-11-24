package chunkstore

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// writeTrace records every filer write (create, update, delete) a command
// makes, in order: the command's step boundaries.
type writeTrace struct {
	manifestFilerBlobs
	mu     sync.Mutex
	writes []string
}

func (w *writeTrace) note(op, p string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, op+" "+p)
}

func (w *writeTrace) CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, o bool) error {
	w.note("create", path.Join(dir, e.GetName()))
	return w.manifestFilerBlobs.CreateEntry(ctx, dir, e, o)
}

func (w *writeTrace) UpdateEntry(ctx context.Context, dir string, e *filerpb.Entry, x map[string][]byte) error {
	w.note("update", path.Join(dir, e.GetName()))
	return w.manifestFilerBlobs.UpdateEntry(ctx, dir, e, x)
}

func (w *writeTrace) DeleteEntry(ctx context.Context, dir, name string, x map[string][]byte) error {
	w.note("delete", path.Join(dir, name))
	return w.manifestFilerBlobs.DeleteEntry(ctx, dir, name, x)
}

// nthWrite matches the n-th filer write (1-based) of any kind.
func nthWrite(n int) func(op, p string) bool {
	seen := 0
	return func(op, _ string) bool {
		if op != "create" && op != "update" && op != "delete" {
			return false
		}
		seen++
		return seen == n
	}
}

// interruptCase is one multi-step operator command: a fresh store, the
// command (with a fixed request id), and the end state it must reach.
type interruptCase struct {
	name  string
	setup func(t *testing.T) (*fakeFiler, *fakeBlobs)
	run   func(ctx context.Context, c *Catalog) error
	check func(t *testing.T, c *Catalog, f *fakeFiler, b *fakeBlobs)
}

// interruptAtEveryStep runs tc once to enumerate its filer writes, then for
// every write kills the command at it (before it reaches the filer, and
// after it applied with its reply lost), restarts the catalog, re-runs the
// command with the same request id, and checks the end state. A step added
// to the command is covered without editing the test.
func interruptAtEveryStep(t *testing.T, tc interruptCase) {
	t.Helper()
	ctx := context.Background()
	f, b := tc.setup(t)
	trace := &writeTrace{manifestFilerBlobs: manifestFilerBlobs{f, gcBlobs{b}}}
	c, err := NewCatalog(trace, catBase)
	if err != nil {
		t.Fatal(err)
	}
	if err := tc.run(ctx, c); err != nil {
		t.Fatalf("%s, uninterrupted: %v", tc.name, err)
	}
	tc.check(t, c, f, b)
	steps := append([]string(nil), trace.writes...)
	if len(steps) == 0 {
		t.Fatalf("%s made no filer write", tc.name)
	}
	t.Logf("%s: %d step boundaries: %v", tc.name, len(steps), steps)
	for k := range steps {
		for _, after := range []bool{false, true} {
			name := fmt.Sprintf("%s/step%d %s/%s", tc.name, k+1, steps[k], map[bool]string{false: "before", true: "applied-reply-lost"}[after])
			t.Run(name, func(t *testing.T) {
				f, b := tc.setup(t)
				crashed := &crashFiler{manifestFilerBlobs: manifestFilerBlobs{f, gcBlobs{b}}, after: after, match: nthWrite(k + 1)}
				dying, err := NewCatalog(crashed, catBase)
				if err != nil {
					t.Fatal(err)
				}
				_ = tc.run(ctx, dying)
				crashed.mu.Lock()
				died := crashed.dead
				crashed.mu.Unlock()
				if !died {
					t.Fatalf("the command never reached step %d", k+1)
				}
				c, err := NewCatalog(manifestFilerBlobs{f, gcBlobs{b}}, catBase) // the restarted process
				if err != nil {
					t.Fatal(err)
				}
				if err := tc.run(ctx, c); err != nil {
					t.Fatalf("re-run after a kill at step %d: %v", k+1, err)
				}
				tc.check(t, c, f, b)
				if err := tc.run(ctx, c); err != nil { // and a further re-run replays
					t.Fatalf("second re-run: %v", err)
				}
				tc.check(t, c, f, b)
			})
		}
	}
}

// forkSourceFixture: v1 holds chunks 0 and 3 and a snapshot s1 of them.
func forkSourceFixture(t *testing.T) (*fakeFiler, *fakeBlobs) {
	t.Helper()
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "s"}, h); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"v1", "v2", "v3"} { // the volume directories GC lists
		if err := f.CreateEntry(ctx, catBase, &filerpb.Entry{Name: v, IsDirectory: true}, true); err != nil {
			t.Fatal(err)
		}
	}
	return f, b
}

// requireImage checks vol's HEAD names exactly chunks 0 and 3 of the
// fixture and each reads back its bytes.
func requireImage(t *testing.T, f *fakeFiler, b *fakeBlobs, vol string) {
	t.Helper()
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, path.Join(catBase, vol))
	if err != nil {
		t.Fatal(err)
	}
	h, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint64][]byte{0: chunkBytes(0x10), 3: chunkBytes(0x13)}
	if len(h.Chunks) != len(want) {
		t.Fatalf("%s HEAD names %d chunks; want %d", vol, len(h.Chunks), len(want))
	}
	for idx, data := range want {
		ref, ok := h.Chunks[idx]
		if !ok {
			t.Fatalf("%s HEAD lacks chunk %d", vol, idx)
		}
		got, err := r.Get(ctx, ref.Key)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("%s chunk %d: %d bytes, %v", vol, idx, len(got), err)
		}
	}
}

// requireCopyIntentsSettle: a killed attempt's copy intent may outlive the
// replay (the replaying attempt cannot know the dead one's create will not
// still land); every intent left is this request's copy intent, and GC's
// copy sweep past the deadline settles all of them from the HEAD it reads,
// the image intact.
func requireCopyIntentsSettle(t *testing.T, c *Catalog, f *fakeFiler, b *fakeBlobs, vol, holder string) {
	t.Helper()
	ctx := context.Background()
	r, err := NewChunkRemote(f, b, path.Join(catBase, vol))
	if err != nil {
		t.Fatal(err)
	}
	left, err := r.ListIntents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range left {
		if in.Holder != holder {
			t.Fatalf("%s intent %s held by %q; want only %q", vol, in.ID, in.Holder, holder)
		}
	}
	later := time.Now().Add(DefaultOpDeadline + time.Hour)
	g, err := NewCollector(c, gcBlobs{b}, GCConfig{Grace: time.Hour, IntentTTL: time.Hour, Now: func() time.Time { return later }, IssuedEpoch: func(context.Context, string) (uint64, error) { return 1, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.SweepCopies(ctx); err != nil {
		t.Fatalf("copy sweep: %v", err)
	}
	if in, err := r.ListIntents(ctx); err != nil || len(in) != 0 {
		t.Fatalf("%s intents after the copy sweep: %d %v; want none", vol, len(in), err)
	}
	requireImage(t, f, b, vol)
}

// r2.149 (3): volume delete, fork and restore, each killed at every step
// boundary and re-run with the same request id, reach their end state with
// their data.
func TestInterruptAtEveryStep_VolumeDelete(t *testing.T) {
	var rsv *fakeReservation
	interruptAtEveryStep(t, interruptCase{
		name: "volume-delete",
		setup: func(t *testing.T) (*fakeFiler, *fakeBlobs) {
			f, b, _, _ := deleteFixture(t)
			detachHead(t, f, b, "v1")
			rsv = &fakeReservation{}
			return f, b
		},
		run: func(ctx context.Context, c *Catalog) error {
			res, err := c.DeleteVolume(ctx, "v1", rsv.admission("d1"))
			if err == nil && !res.Completed {
				return fmt.Errorf("not completed: %+v", res)
			}
			return err
		},
		check: func(t *testing.T, c *Catalog, _ *fakeFiler, _ *fakeBlobs) {
			requireVolumeDeleted(t, c, rsv, "v1")
		},
	})
}

func TestInterruptAtEveryStep_Fork(t *testing.T) {
	interruptAtEveryStep(t, interruptCase{
		name:  "fork",
		setup: forkSourceFixture,
		run: func(ctx context.Context, c *Catalog) error {
			_, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f1"})
			return err
		},
		check: func(t *testing.T, c *Catalog, f *fakeFiler, b *fakeBlobs) {
			v, ok, err := c.ReadVolume(context.Background(), "v2")
			if err != nil || !ok || v.Origin != OriginFork {
				t.Fatalf("fork VOLUME: %+v %t %v", v, ok, err)
			}
			if h, ok, err := c.ReadHead(context.Background(), "v2"); err != nil || !ok || h.State != storage.ChunkHeadActive {
				t.Fatalf("fork HEAD: %+v %t %v", h, ok, err)
			}
			requireImage(t, f, b, "v2")
			requireCopyIntentsSettle(t, c, f, b, "v2", "fork:v2/f1")
		},
	})
}

func TestInterruptAtEveryStep_Restore(t *testing.T) {
	interruptAtEveryStep(t, interruptCase{
		name:  "restore",
		setup: forkSourceFixture,
		run: func(ctx context.Context, c *Catalog) error {
			_, err := c.Restore(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v3", RequestID: "r1"})
			return err
		},
		check: func(t *testing.T, c *Catalog, f *fakeFiler, b *fakeBlobs) {
			v, ok, err := c.ReadVolume(context.Background(), "v3")
			if err != nil || !ok || v.Origin != OriginRestore {
				t.Fatalf("restore VOLUME: %+v %t %v", v, ok, err)
			}
			requireImage(t, f, b, "v3")
			requireCopyIntentsSettle(t, c, f, b, "v3", "fork:v3/r1")
		},
	})
}
