package chunkstore

import (
	"context"
	"errors"
	"path"
	"testing"
	"time"
)

func TestGCExecution_PreviewAndIndependentApplyMarks(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if e := r.Put(ctx, "orphan", []byte("orphan")); e != nil {
		t.Fatal(e)
	}
	preview, e := c.ExecuteGC(ctx, b, g.cfg, false)
	if e != nil || !preview.DryRun || preview.MarkSaved || len(preview.WouldDelete) != 0 {
		t.Fatalf("first preview: %+v %v", preview, e)
	}
	if _, e = f.LookupEntry(ctx, catBase, gcMarkName); !errors.Is(e, ErrNotFound) {
		t.Fatalf("preview created state: %v", e)
	}
	first, e := c.ExecuteGC(ctx, b, g.cfg, true)
	if e != nil || first.DryRun || !first.MarkSaved || first.Reclaimed != 0 {
		t.Fatalf("first applied mark: %+v %v", first, e)
	}
	*now = now.Add(time.Hour - time.Second)
	early, e := c.ExecuteGC(ctx, b, g.cfg, false)
	if e != nil || len(early.WouldDelete) != 0 {
		t.Fatalf("early independent preview: %+v %v", early, e)
	}
	*now = now.Add(2 * time.Second)
	before, _ := f.LookupEntry(ctx, catBase, gcMarkName)
	mature, e := c.ExecuteGC(ctx, b, g.cfg, false)
	if e != nil || len(mature.WouldDelete) != 1 || mature.Reclaimed != 0 {
		t.Fatalf("mature preview: %+v %v", mature, e)
	}
	after, _ := f.LookupEntry(ctx, catBase, gcMarkName)
	if string(before.Content) != string(after.Content) || string(before.Extended[gcToken]) != string(after.Extended[gcToken]) {
		t.Fatal("preview advanced persistent state")
	}
	if _, e = r.Get(ctx, "orphan"); e != nil {
		t.Fatal("preview deleted a chunk", e)
	}
	applied, e := c.ExecuteGC(ctx, b, g.cfg, true)
	if e != nil || applied.Reclaimed != 1 || !applied.MarkSaved {
		t.Fatalf("independent second apply: %+v %v", applied, e)
	}
	if _, e = r.Get(ctx, "orphan"); e == nil {
		t.Fatal("applied GC did not delete orphan")
	}
}

func TestGCExecution_FailedPassInvalidatesPersistedCoverage(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if e := r.Put(ctx, "orphan", []byte("orphan")); e != nil {
		t.Fatal(e)
	}
	if _, e := c.ExecuteGC(ctx, b, g.cfg, true); e != nil {
		t.Fatal(e)
	}
	*now = now.Add(2 * time.Hour)
	wrapped := &failingGCListing{Filer: f, fail: true}
	c.f = wrapped
	if _, e := c.ExecuteGC(ctx, b, g.cfg, true); e == nil {
		t.Fatal("failed scan reported success")
	}
	wrapped.fail = false
	pass, e := c.ExecuteGC(ctx, b, g.cfg, true)
	if e != nil || pass.Reclaimed != 0 {
		t.Fatalf("failed/crashed pass retained old coverage: %+v %v", pass, e)
	}
}

func TestGCExecution_WatermarkConflictDispatchesNoReclaim(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	if e := r.Put(ctx, "orphan", []byte("orphan")); e != nil {
		t.Fatal(e)
	}
	if _, e := c.ExecuteGC(ctx, b, g.cfg, true); e != nil {
		t.Fatal(e)
	}
	*now = now.Add(2 * time.Hour)
	f.failUpdate = func(p string) error {
		if p == path.Join(catBase, gcMarkName) {
			return ErrPrecondition
		}
		return nil
	}
	if _, e := c.ExecuteGC(ctx, b, g.cfg, true); e == nil {
		t.Fatal("watermark conflict reported success")
	}
	if _, e := r.Get(ctx, "orphan"); e != nil {
		t.Fatal("conflicting pass deleted bytes", e)
	}
}
