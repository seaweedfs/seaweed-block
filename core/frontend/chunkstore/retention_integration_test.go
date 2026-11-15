//go:build chunkstore_integration

package chunkstore

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Retention and accounting against a real SeaweedFS (CHUNKSTORE_WEED):
// N+5 semantic snapshots plus pinned ones; a dry run writes nothing; retain
// leaves N plus the pinned; accounting follows (GC not run: the deleted
// snapshots' chunks are still stored, now unreferenced).
func TestIntegration_RetainAndUsage(t *testing.T) {
	c := startWeed(t)
	ctx := context.Background()
	const base = "/swblock/chunks"
	geo := Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}
	full := uint64(geo.ChunkBlocks) * uint64(geo.BlockSize)
	cat, err := NewCatalog(c.client(t, base), base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.EnsureVolume(ctx, "v1", geo); err != nil {
		t.Fatal(err)
	}
	r := c.remote(t, path.Join(base, "v1"))
	clock := time.Now().Add(-72 * time.Hour)
	cat.SetClock(func() time.Time { return clock })

	// One snapshot per step; each step rewrites chunk 0 under a new key,
	// so every snapshot but the newest owns one chunk exclusively.
	cur, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	step := 0
	snapshot := func(name, class string) {
		t.Helper()
		data := bytes.Repeat([]byte{byte(step + 1)}, int(full))
		key := fmt.Sprintf("chunks/e1/c0/step-%02d", step)
		step++
		if err := r.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		next := storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: uint64(step), Chunks: map[uint64]storage.ChunkRef{0: ref(key, data)}}
		gen, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, next)
		if err != nil {
			t.Fatal(err)
		}
		next.Gen = gen
		cur = next
		clock = clock.Add(time.Minute)
		if _, err := cat.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: name, RequestID: "r-" + name, Class: class}, next); err != nil {
			t.Fatal(err)
		}
	}
	snapshot("golden-a", SnapshotClassPinned) // the oldest, but pinned
	snapshot("golden-b", SnapshotClassPinned)
	const n = DefaultKeepNewest
	for i := 0; i < n+5; i++ {
		snapshot(fmt.Sprintf("s%02d", i), SnapshotClassSemantic)
	}
	clock = time.Now()

	before, err := cat.Usage(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Snapshots) != n+7 || before.StoredBindings != n+7 || before.StoredUpperBound || before.LiveBytes != full {
		t.Fatalf("before: %d snapshots, %d bindings, upper=%t, live=%d", len(before.Snapshots), before.StoredBindings, before.StoredUpperBound, before.LiveBytes)
	}
	exclusive := func(u VolumeUsage) uint64 {
		var sum uint64
		for _, s := range u.Snapshots {
			sum += s.ExclusiveBytes
		}
		return sum
	}
	if got := exclusive(before); got != uint64(n+6)*full {
		t.Fatalf("before: exclusive %d, want %d (all but the newest snapshot)", got, uint64(n+6)*full)
	}

	// Dry run: exactly the 5 oldest semantic snapshots, nothing written.
	namesBefore, err := c.client(t, base).ListEntries(ctx, path.Join(base, "v1", snapNamesDir), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	dry, err := cat.Retain(ctx, "v1", true)
	if err != nil {
		t.Fatal(err)
	}
	var wouldGo []string
	for _, d := range dry.Decisions {
		if !d.Keep {
			wouldGo = append(wouldGo, d.Name)
		}
	}
	if fmt.Sprint(wouldGo) != "[s04 s03 s02 s01 s00]" || len(dry.Deleted) != 0 {
		t.Fatalf("dry run: would delete %v, deleted %v", wouldGo, dry.Deleted)
	}
	namesAfter, err := c.client(t, base).ListEntries(ctx, path.Join(base, "v1", snapNamesDir), "", "", 0)
	if err != nil || len(namesAfter) != len(namesBefore) {
		t.Fatalf("dry run changed the name records: %d -> %d (%v)", len(namesBefore), len(namesAfter), err)
	}

	res, err := cat.Retain(ctx, "v1", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Deleted) != 5 || res.Kept != n+2 {
		t.Fatalf("retain: deleted %v, kept %d", res.Deleted, res.Kept)
	}
	after, err := cat.Usage(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	left := map[string]bool{}
	for _, s := range after.Snapshots {
		left[s.Name] = true
	}
	if len(after.Snapshots) != n+2 || !left["golden-a"] || !left["golden-b"] || left["s04"] || !left["s05"] {
		t.Fatalf("after: snapshots %v", left)
	}
	if got := exclusive(after); got != uint64(n+1)*full {
		t.Fatalf("after: exclusive %d, want %d", got, uint64(n+1)*full)
	}
	if after.StoredBindings != n+7 || !after.StoredUpperBound {
		t.Fatalf("after: %d bindings, upper=%t; want all still stored (no GC yet), flagged as an upper bound", after.StoredBindings, after.StoredUpperBound)
	}
}
