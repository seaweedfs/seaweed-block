package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"path"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// cleanFixture is v with chunk 1 live in the current root and in snapshot
// "s", plus orphans: chunk 2 in shard 0 and chunk ShardSize+3 in shard 1.
func cleanFixture(t *testing.T) (*BCatalog, VolumeRef, *LayoutStore, []layout.ChunkName) {
	t.Helper()
	ctx := context.Background()
	c, err := NewBCatalog(NewMemFiler(), newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	v := newRef(t, "v1")
	if _, err := c.CreateVolume(ctx, v); err != nil {
		t.Fatal(err)
	}
	commitSnapshot(t, c, v, bytes.Repeat([]byte{7}, 4096), "s")
	s, err := c.Store(v)
	if err != nil {
		t.Fatal(err)
	}
	var orphans []layout.ChunkName
	for _, idx := range []uint64{2, layout.ShardSize + 3} {
		data := bytes.Repeat([]byte{byte(idx)}, 4096)
		n := layout.NameChunk(idx, data)
		if err := layout.EnsureObject(ctx, s, n.Path(), data, "dead-session"); err != nil {
			t.Fatal(err)
		}
		orphans = append(orphans, n)
	}
	// A real filer lists the shard directories it created for these
	// entries; the in-memory filer keeps only the entries, so add them.
	pool := path.Join(c.base, v.Dir(), layout.PoolDir)
	for _, d := range []string{"0", "1"} {
		if err := c.f.CreateEntry(ctx, pool, &filerpb.Entry{Name: d, IsDirectory: true}, false); err != nil {
			t.Fatal(err)
		}
	}
	return c, v, s, orphans
}

func present(t *testing.T, s *LayoutStore, n layout.ChunkName) bool {
	t.Helper()
	_, found, err := s.Stat(context.Background(), n.Path())
	if err != nil {
		t.Fatal(err)
	}
	return found
}

var cleanHolder = layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}

// The holder's pass removes every orphan in every shard, keeps what the
// current root and the snapshot name, and leaves HEAD idle. With a page of
// one name the shard cursor is exercised too.
func TestBCatalog_CleanRemovesOnlyUnreachable(t *testing.T) {
	ctx := context.Background()
	prev := cleanPage
	cleanPage = 1
	t.Cleanup(func() { cleanPage = prev })
	c, v, s, orphans := cleanFixture(t)
	live := layout.NameChunk(1, bytes.Repeat([]byte{7}, 4096))

	res, err := c.Clean(ctx, v, cleanHolder, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Shards != 2 || res.Planned != 3 || res.Removed != 2 {
		t.Fatalf("clean: %+v; want 2 shards, 3 planned, 2 removed", res)
	}
	for _, n := range orphans {
		if present(t, s, n) {
			t.Fatalf("orphan %s survived the cleaning", n)
		}
	}
	if !present(t, s, live) {
		t.Fatal("the chunk the roots name was removed")
	}
	h, _, err := c.ReadHead(ctx, v)
	if err != nil || h.Activity.Kind != layout.ActIdle {
		t.Fatalf("HEAD after clean: %+v %v; want idle", h.Activity, err)
	}
	// A second pass finds nothing more.
	if res, err := c.Clean(ctx, v, cleanHolder, "c2"); err != nil || res.Removed != 0 {
		t.Fatalf("second clean: %+v %v", res, err)
	}
}

// Only the holder at HEAD's epoch cleans: another holder, an old epoch or
// another claim is refused at Begin, and nothing is removed.
func TestBCatalog_CleanIsHolderOnly(t *testing.T) {
	ctx := context.Background()
	c, v, s, orphans := cleanFixture(t)
	for name, me := range map[string]layout.HolderID{
		"other holder": {Epoch: 1, Holder: "x/y", ClaimID: "k"},
		"other claim":  {Epoch: 1, Holder: "s/r", ClaimID: "other"},
		"old epoch":    {Epoch: 0, Holder: "s/r", ClaimID: "k"},
	} {
		if _, err := c.Clean(ctx, v, me, "c1"); err == nil {
			t.Fatalf("%s: clean succeeded; want refused", name)
		}
	}
	for _, n := range orphans {
		if !present(t, s, n) {
			t.Fatalf("a refused clean removed %s", n)
		}
	}
	h, _, err := c.ReadHead(ctx, v)
	if err != nil || h.Activity.Kind != layout.ActIdle {
		t.Fatalf("HEAD after refusals: %+v %v; want idle", h.Activity, err)
	}
}

// While a snapshot session is open the cleaning cannot begin (ErrBusy) and
// removes nothing; the open session is left as it was.
func TestBCatalog_CleanWaitsForAnOpenSnapshot(t *testing.T) {
	ctx := context.Background()
	c, v, s, orphans := cleanFixture(t)
	if _, err := layout.RunBegin(ctx, s, cleanHolder, "snap2"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Clean(ctx, v, cleanHolder, "c1"); !errors.Is(err, layout.ErrBusy) {
		t.Fatalf("clean during a snapshot: %v; want ErrBusy", err)
	}
	for _, n := range orphans {
		if !present(t, s, n) {
			t.Fatalf("clean during a snapshot removed %s", n)
		}
	}
	h, _, err := c.ReadHead(ctx, v)
	if err != nil || h.Activity != (layout.Activity{Kind: layout.ActSnap, Session: "snap2"}) {
		t.Fatalf("HEAD after the refused clean: %+v %v; want the snapshot session open", h.Activity, err)
	}
}

// A pass that fails after Begin still closes the cleaning, so snapshots are
// not blocked by it.
func TestBCatalog_CleanClosesOnError(t *testing.T) {
	ctx := context.Background()
	c, v, _, _ := cleanFixture(t)
	s, err := c.Store(v)
	if err != nil {
		t.Fatal(err)
	}
	pool := path.Join(c.base, v.Dir(), layout.PoolDir)
	if err := s.f.CreateEntry(ctx, pool, layoutEntry("x", nil), true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Clean(ctx, v, cleanHolder, "c1"); !errors.Is(err, layout.ErrCorrupt) {
		t.Fatalf("clean over a malformed shard: %v; want ErrCorrupt", err)
	}
	h, _, err := c.ReadHead(ctx, v)
	if err != nil || h.Activity.Kind != layout.ActIdle {
		t.Fatalf("HEAD after a failed clean: %+v %v; want idle", h.Activity, err)
	}
}
