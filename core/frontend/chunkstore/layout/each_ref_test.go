package layout

import (
	"context"
	"errors"
	"path"
	"testing"
)

// EachRef visits every reference of the selected shards and stops when fn
// says so; an unreadable segment is an error, never a partial walk.
// HeadRoots names each root HEAD holds once.
func TestEachRefAndHeadRoots(t *testing.T) {
	ctx := context.Background()
	m := &memStore{obj: map[string][]byte{}}
	rn := putImage(t, m, "v", map[uint64]string{0: "a", 1: "b", ShardSize + 2: "c"}, nil)
	r, err := NewReader(vbase, "v", m.fetch)
	if err != nil {
		t.Fatal(err)
	}
	visit := func(shard uint64, stop int) (map[uint64]bool, error) {
		got := map[uint64]bool{}
		err := r.EachRef(ctx, rn, shard, func(i uint64, _ Ref) bool { got[i] = true; return len(got) != stop })
		return got, err
	}
	if got, err := visit(AllShards, -1); err != nil || len(got) != 3 || !got[ShardSize+2] {
		t.Fatalf("all shards: %v %v; want 0, 1 and %d", got, err, ShardSize+2)
	}
	if got, err := visit(1, -1); err != nil || len(got) != 1 || !got[ShardSize+2] {
		t.Fatalf("shard 1: %v %v; want only %d", got, err, ShardSize+2)
	}
	if got, err := visit(7, -1); err != nil || len(got) != 0 {
		t.Fatalf("a shard the root does not name: %v %v; want none", got, err)
	}
	if got, err := visit(AllShards, 1); err != nil || len(got) != 1 {
		t.Fatalf("stop after one: %v %v", got, err)
	}
	img, err := r.Open(ctx, rn)
	if err != nil {
		t.Fatal(err)
	}
	seg := img.root.Segments[0]
	m.readErr = path.Join(vbase, "v", seg.Path())
	if _, err := visit(AllShards, -1); err == nil || errors.Is(err, ErrMissing) {
		t.Fatalf("unreadable segment: %v; want the read error", err)
	}
	m.readErr = ""
	delete(m.obj, path.Join(vbase, "v", seg.Path()))
	if _, err := visit(AllShards, -1); !errors.Is(err, ErrMissing) {
		t.Fatalf("missing segment: %v; want ErrMissing", err)
	}

	s := RootName{Hash: Sum([]byte("s"))}
	h := Head{Current: &rn, Snapshots: map[string]Snapshot{"a": {Root: rn}, "b": {Root: s}}}
	if roots := HeadRoots(h); len(roots) != 2 || roots[0] != rn {
		t.Fatalf("HEAD roots %v; want the current root once, then the snapshot's", roots)
	}
	if roots := HeadRoots(Head{}); len(roots) != 0 {
		t.Fatalf("an empty HEAD names %v", roots)
	}
}
