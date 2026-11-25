package layout

import (
	"context"
	"strings"
	"testing"
)

// PrepareRoot writes every segment the root names, in shard order, and the
// root last, all under the session; a segment whose bytes are not given is
// refused before anything is written.
func TestPrepareRootWritesSegmentsThenRoot(t *testing.T) {
	ctx := context.Background()
	m := newMemObjects("/v")
	manifest := map[uint64]Ref{}
	for _, idx := range []uint64{2*ShardSize + 1, 3, ShardSize + 4} {
		manifest[idx] = Ref{Owner: "v", OwnerID: VolumeID{1}, Chunk: NameChunk(idx, []byte{byte(idx)})}
	}
	root, segs, err := Build(manifest)
	if err != nil {
		t.Fatal(err)
	}
	rn, err := PrepareRoot(ctx, m, root, segs, "s1")
	if err != nil {
		t.Fatal(err)
	}
	want, _, _ := NameRoot(root)
	if rn != want {
		t.Fatalf("root name %v; want %v", rn, want)
	}
	if len(m.writes) != 4 {
		t.Fatalf("writes %v; want three segments and the root", m.writes)
	}
	for i, sh := range []uint64{0, 1, 2} {
		if !strings.HasSuffix(m.writes[i], root.Segments[sh].Path()) {
			t.Fatalf("write %d is %q; want shard %d's segment (shard order)", i, m.writes[i], sh)
		}
	}
	if !strings.HasSuffix(m.writes[3], rn.Path()) {
		t.Fatalf("last write %q; want the root", m.writes[3])
	}
	for _, p := range append([]string{rn.Path()}, root.Segments[0].Path(), root.Segments[1].Path(), root.Segments[2].Path()) {
		if o := m.objs[m.full(p)]; o == nil || o.meta.Session != "s1" {
			t.Fatalf("%s: %+v; want written by s1", p, o)
		}
	}

	m2 := newMemObjects("/v")
	delete(segs, root.Segments[1])
	if _, err := PrepareRoot(ctx, m2, root, segs, "s2"); err == nil || len(m2.writes) > 1 {
		t.Fatalf("a missing segment: %v after %v; want refused before the root", err, m2.writes)
	}
	for _, w := range m2.writes {
		if strings.HasSuffix(w, rn.Path()) {
			t.Fatalf("the root was written without all its segments: %v", m2.writes)
		}
	}
}
