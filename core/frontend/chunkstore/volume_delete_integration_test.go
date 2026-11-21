//go:build chunkstore_integration

package chunkstore

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Volume delete against a real SeaweedFS (CHUNKSTORE_WEED), with a real GC
// (the acceptance case): fork w2 from S1, delete w2,
// then DeleteSnapshot S1 succeeds and GC reclaims exactly the chunks only
// w2 and S1 reach, while w1 (HEAD, S2) stays intact; deleting an attached
// volume is refused; a deleted id cannot be recreated.
func TestIntegration_DeleteVolumeThenGC(t *testing.T) {
	c := startWeed(t)
	ctx := context.Background()
	const base = "/swblock/chunks"
	geo := Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}
	full := int(geo.ChunkBlocks) * int(geo.BlockSize)
	cl := c.client(t, base)
	cat, err := NewCatalog(cl, base)
	if err != nil {
		t.Fatal(err)
	}
	for _, vol := range []string{"w1"} {
		if _, err := cat.EnsureVolume(ctx, vol, geo); err != nil {
			t.Fatal(err)
		}
	}
	w1 := c.remote(t, path.Join(base, "w1"))
	put := func(r *ChunkRemote, key string, b byte) storage.ChunkRef {
		t.Helper()
		data := bytes.Repeat([]byte{b}, full)
		if err := r.Put(ctx, key, data); err != nil {
			t.Fatal(err)
		}
		return ref(key, data)
	}
	publish := func(r *ChunkRemote, holder string, epoch uint64, chunks map[uint64]storage.ChunkRef) storage.ChunkHead {
		t.Helper()
		cur, err := r.GetHead(ctx)
		if err != nil {
			t.Fatal(err)
		}
		next := storage.ChunkHead{Epoch: epoch, Holder: holder, RemoteLSN: cur.RemoteLSN + 1, Chunks: chunks, State: storage.ChunkHeadActive}
		return publishIntegrationHead(t, r, cur, next, uint64(full))
	}
	detach := func(r *ChunkRemote) {
		t.Helper()
		cur, err := r.GetHead(ctx)
		if err != nil {
			t.Fatal(err)
		}
		next := cur
		next.State, next.DetachedBy = storage.ChunkHeadDetached, &storage.ChunkDetachedBy{Holder: cur.Holder, Epoch: cur.Epoch, Gen: cur.Gen + 1}
		if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, next); err != nil {
			t.Fatal(err)
		}
	}

	// w1: S1 = {A, B}; then chunk 0 is rewritten: HEAD = S2 = {C, B}.
	a, bb := put(w1, "chunks/e1/c0/a", 1), put(w1, "chunks/e1/c1/b", 2)
	h1 := publish(w1, "h1", 1, map[uint64]storage.ChunkRef{0: a, 1: bb})
	if _, err := cat.CreateSnapshot(ctx, "w1", SnapshotRequest{Name: "S1", RequestID: "r1"}, h1); err != nil {
		t.Fatal(err)
	}
	cc := put(w1, "chunks/e1/c0/c", 3)
	h2 := publish(w1, "h1", 1, map[uint64]storage.ChunkRef{0: cc, 1: bb})
	if _, err := cat.CreateSnapshot(ctx, "w1", SnapshotRequest{Name: "S2", RequestID: "r2"}, h2); err != nil {
		t.Fatal(err)
	}

	// w2 = fork of S1, then it writes chunk 2 (D) of its own.
	if _, err := cat.Fork(ctx, ForkRequest{Source: "w1", Snapshot: "S1", Target: "w2", RequestID: "f2"}); err != nil {
		t.Fatalf("fork w2: %v", err)
	}
	w2 := c.remote(t, path.Join(base, "w2"))
	fh, err := w2.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d := put(w2, "chunks/e1/c2/d", 4)
	chunks := map[uint64]storage.ChunkRef{2: d}
	for k, v := range fh.Chunks {
		chunks[k] = v
	}
	h3 := publish(w2, "h2", 1, chunks)

	// An attached volume is refused (w1 is held by h1; w2 by h2).
	for _, vol := range []string{"w1", "w2"} {
		if _, err := cat.DeleteVolume(ctx, vol, nil); CodeOf(err) != CodeVolumeInUse {
			t.Fatalf("delete of attached %s: %v; want volume_in_use", vol, err)
		}
	}
	detach(w2)
	res, err := cat.DeleteVolume(ctx, "w2", nil)
	if err != nil || res.Replayed {
		t.Fatalf("delete w2: %+v %v", res, err)
	}
	if _, err := cat.EnsureVolume(ctx, "w2", geo); CodeOf(err) != CodeVolumeDeleted {
		t.Fatalf("recreate w2: %v; want volume_deleted", err)
	}
	if _, err := cat.Fork(ctx, ForkRequest{Source: "w1", Snapshot: "S2", Target: "w2", RequestID: "again"}); CodeOf(err) != CodeVolumeDeleted {
		t.Fatalf("fork to deleted w2: %v; want volume_deleted", err)
	}
	if _, err := cat.DeleteSnapshot(ctx, "w1", "S1"); err != nil {
		t.Fatalf("DeleteSnapshot S1 after w2 was deleted: %v", err)
	}

	// GC: two marks past grace reclaim exactly A (only S1 and w2 reached
	// it) and D (only w2); B and C (w1's HEAD and S2) stay.
	grace := 3 * DefaultOpDeadline // >= deadline + skew + CAS apply bound
	at := time.Now().Add(3 * grace)
	g, err := NewCollector(cat, cl, GCConfig{Grace: grace, IntentTTL: time.Hour, Now: func() time.Time { return at },
		IssuedEpoch: func(context.Context, string) (uint64, error) { return 1, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Run(ctx); err != nil {
		t.Fatalf("GC mark 1: %v", err)
	}
	at = at.Add(grace + time.Minute)
	gr, err := g.Run(ctx)
	if err != nil {
		t.Fatalf("GC mark 2: %v", err)
	}
	var got []string
	for _, o := range gr.WouldDelete {
		got = append(got, o.Volume+":"+o.Key)
	}
	want := []string{"w1:chunks/e1/c0/a", "w2:chunks/e1/c2/d"}
	for _, h := range []storage.ChunkHead{h1, h3} {
		for _, segment := range h.Segments {
			want = append(want, segment.Owner+":"+segment.Key)
		}
	}
	sort.Strings(want)
	sort.Strings(got)
	if fmt.Sprint(got) != fmt.Sprint(want) || gr.Reclaimed != len(want) {
		t.Fatalf("GC reclaimed %v (%d); want exactly w1's A, w2's D and their unreachable manifests; roots=%+v candidates=%d", got, gr.Reclaimed, gr.Roots, gr.Candidates)
	}
	for _, kept := range []string{bb.Key, cc.Key} {
		if _, err := w1.Get(ctx, kept); err != nil {
			t.Fatalf("w1 chunk %s after GC: %v; want intact", kept, err)
		}
	}
	if _, err := w1.Get(ctx, a.Key); err == nil {
		t.Fatalf("w1 chunk %s still readable after GC reclaimed it", a.Key)
	}
	if s, err := cat.readSnap(ctx, "w1", "S2"); err != nil || s.State != SnapActive {
		t.Fatalf("S2 after GC: %+v %v", s, err)
	}
}
