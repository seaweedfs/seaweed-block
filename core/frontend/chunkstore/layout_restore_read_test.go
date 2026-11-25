package chunkstore

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// The restore read path through the engine: the source writes and takes a
// named snapshot; a restore creates the target from it; a fresh target host
// reads every block, fetched lazily through its keys from the source's pool.
// The target then writes and cuts: its HEAD names its own chunk for what it
// changed and the source's chunks for the rest, it reads back its own
// image, and the source still reads back its snapshot image unchanged.
func TestLayoutRemote_RestoreTargetReadsItsSource(t *testing.T) {
	ctx := context.Background()
	src := newLayoutHost(t)
	s, l, r := src.attach("src", true, true)
	want := map[uint32][]byte{0: block(1), 9: block(2), 17: block(3), 63: block(4)}
	writeSync(t, s, want)
	r.SetSnapshot("daily", layout.SnapshotMeta{CreatedNs: 1, Class: layout.ClassSemantic})
	if _, err := l.Capture(ctx); err != nil {
		t.Fatalf("source capture: %v", err)
	}
	_ = s.Close()

	tv := newRef(t, "t")
	if _, err := src.cat.Restore(ctx, src.v, "daily", tv); err != nil {
		t.Fatalf("restore: %v", err)
	}
	ts, err := src.cat.Store(tv)
	if err != nil {
		t.Fatal(err)
	}
	tme := layout.HolderID{Epoch: 1, Holder: "b", ClaimID: "k-b"}
	if _, err := layout.Start(ctx, ts, okClaimLease{}, layout.HolderID{Epoch: 1, Holder: "b"}, layout.LocalRoot{ClaimID: "k-b", Fresh: true}); err != nil {
		t.Fatalf("start the target: %v", err)
	}
	tgt := &layoutHost{t: t, cat: src.cat, v: tv, me: tme, dir: t.TempDir()}

	// Every block of the target is the source's snapshot, read from the
	// source's pool through the target's keys.
	tgt.readBack("t-read", want)

	// The target writes one block of chunk 1 and cuts.
	s2, l2, _ := tgt.attach("t-write", true, true)
	changed := map[uint32][]byte{}
	for k, v := range want {
		changed[k] = v
	}
	changed[9] = block(9)
	writeSync(t, s2, map[uint32][]byte{9: changed[9]})
	if _, err := l2.Capture(ctx); err != nil {
		t.Fatalf("target capture: %v", err)
	}
	_ = s2.Close()

	_, tr, err := src.cat.open(tv)
	if err != nil {
		t.Fatal(err)
	}
	img, err := tr.Open(ctx, *tgt.head().Current)
	if err != nil {
		t.Fatal(err)
	}
	for idx, owner := range map[uint64]layout.VolumeID{0: src.v.ID, 1: tv.ID, 2: src.v.ID, 7: src.v.ID} {
		c, err := img.Chunk(ctx, idx)
		if err != nil || c.Ref.OwnerID != owner {
			t.Fatalf("target chunk %d: owner %s (%v); want %s", idx, c.Ref.OwnerID, err, owner)
		}
	}
	tgt.readBack("t-after", changed)
	src.readBack("src-after", want)
}
