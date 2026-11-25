package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const lrBlocks, lrChunkBlocks = 64, 8

// layoutHost is one host of a layout volume: its catalog, its claimed line,
// and attach() for a local store over a fresh LayoutRemote (as after a
// process start: no session in memory).
type layoutHost struct {
	t   *testing.T
	cat *BCatalog
	v   VolumeRef
	me  layout.HolderID
	dir string
}

func newLayoutHost(t *testing.T) *layoutHost {
	t.Helper()
	ctx := context.Background()
	cat, err := NewBCatalog(newFakeFiler(), newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	v := newRef(t, "v")
	if _, err := cat.CreateVolume(ctx, v); err != nil {
		t.Fatal(err)
	}
	s, _ := cat.Store(v)
	me := layout.HolderID{Epoch: 1, Holder: "a", ClaimID: "k-a"}
	if _, err := layout.Start(ctx, s, okClaimLease{}, layout.HolderID{Epoch: 1, Holder: "a"}, layout.LocalRoot{ClaimID: "k-a", Fresh: true}); err != nil {
		t.Fatal(err)
	}
	return &layoutHost{t: t, cat: cat, v: v, me: me, dir: t.TempDir()}
}

// attach opens (create: a new local store) the store named name over a new
// LayoutRemote.
func (lh *layoutHost) attach(name string, create, fresh bool) (*storage.WALStore, storage.ChunkLayer, *LayoutRemote) {
	t := lh.t
	t.Helper()
	p := filepath.Join(lh.dir, name)
	var s *storage.WALStore
	var err error
	if create {
		s, err = storage.CreateWALStoreWithOptions(p, lrBlocks, 4096, storage.WALStoreOptions{ChunkBlocks: lrChunkBlocks})
	} else {
		s, err = storage.OpenWALStore(p)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	l, err := storage.AttachChunkLayer(s, r, storage.ChunkLayerConfig{ChunkBlocks: lrChunkBlocks, Epoch: lh.me.Epoch, Holder: lh.me.Holder, FreshFromHead: fresh})
	if err != nil {
		t.Fatal(err)
	}
	return s, l, r
}

func (lh *layoutHost) head() layout.Head {
	h, _, err := lh.cat.ReadHead(context.Background(), lh.v)
	if err != nil {
		lh.t.Fatal(err)
	}
	return h
}

func block(b byte) []byte { return bytes.Repeat([]byte{b}, 4096) }

func writeSync(t *testing.T, s *storage.WALStore, w map[uint32][]byte) {
	t.Helper()
	for lba, d := range w {
		if _, err := s.Write(lba, d); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
}

// readBack attaches a fresh local store and checks every block reads as want
// (zero elsewhere), fetched lazily from HEAD through the layout.
func (lh *layoutHost) readBack(name string, want map[uint32][]byte) {
	t := lh.t
	t.Helper()
	s, _, _ := lh.attach(name, true, true)
	defer s.Close()
	zero := make([]byte, 4096)
	for lba := uint32(0); lba < lrBlocks; lba++ {
		got, err := s.Read(lba)
		if err != nil {
			t.Fatalf("read back %d: %v", lba, err)
		}
		exp, ok := want[lba]
		if !ok {
			exp = zero
		}
		if !bytes.Equal(got, exp) {
			t.Fatalf("block %d = %x...; want %x...", lba, got[:2], exp[:2])
		}
	}
}

// The engine's cut commits through the layout: HEAD is idle at the cut's
// position with the chunks as content-named objects, a named snapshot is
// recorded with its facts, and a fresh host reads everything back.
func TestLayoutRemote_CutCommitsThroughTheKernel(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	want := map[uint32][]byte{0: block(1), 9: block(2), 17: block(3), 63: block(4)}
	writeSync(t, s, want)
	h1, err := l.Capture(ctx)
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if h := lh.head(); h.Position != h1 || h.Activity.Kind != layout.ActIdle || h.Current == nil {
		t.Fatalf("HEAD after the cut: %+v; want idle at %d with a root", h, h1)
	}
	want[9] = block(9)
	writeSync(t, s, map[uint32][]byte{9: want[9]})
	r.SetSnapshot("daily", layout.SnapshotMeta{CreatedNs: 5, Class: layout.ClassSemantic})
	if _, err := l.Capture(ctx); err != nil {
		t.Fatalf("named capture: %v", err)
	}
	if sn, ok := lh.head().Snapshots["daily"]; !ok || sn.Meta.Class != layout.ClassSemantic || sn.Meta.CreatedNs != 5 {
		t.Fatalf("snapshot daily: %+v %t", sn, ok)
	}
	_ = s.Close()
	lh.readBack("b", want)
}

// A crash inside a session (Begin applied, no commit): the restarted
// process's first call rolls the dead session back; its base names the
// last commit's generation while HEAD is two later with the same root and
// position, and the cut after it uploads everything written since that
// position: the read-back has every acknowledged write.
func TestLayoutRemote_CrashInsideASessionRollsBack(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	want := map[uint32][]byte{0: block(1), 20: block(2)}
	writeSync(t, s, want)
	if _, err := l.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	committed := lh.head()
	// Written after the last commit, then a cut that dies after its Begin.
	want[20], want[40] = block(7), block(8)
	writeSync(t, s, map[uint32][]byte{20: want[20], 40: want[40]})
	if _, err := r.PutIntent(ctx, storage.ChunkIntent{Gen: committed.Gen}); err != nil {
		t.Fatal(err)
	}
	if h := lh.head(); h.Activity.Kind != layout.ActSnap {
		t.Fatalf("HEAD after the Begin: %+v", h.Activity)
	}
	_ = s.Close() // the process dies: its session id dies with it

	s2, l2, _ := lh.attach("a", false, false)
	defer s2.Close()
	if h := lh.head(); h.Activity.Kind != layout.ActIdle || h.Gen != committed.Gen+2 || *h.Current != *committed.Current || h.Position != committed.Position {
		t.Fatalf("HEAD after the restart: gen %d activity %+v; want idle at gen %d, same root and position", h.Gen, h.Activity, committed.Gen+2)
	}
	if _, err := l2.Capture(ctx); err != nil {
		t.Fatalf("cut after the restart: %v", err)
	}
	_ = s2.Close()
	lh.readBack("c", want)
}

// The engine's keys are layout references: a key that does not name its
// bytes, another volume's key for an upload, and a non-layout key are
// refused before anything is written.
func TestLayoutRemote_KeysNameTheirBytes(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	data := block(3)
	good := r.ChunkKey(2, layout.Sum(data))
	other := LayoutKey(layout.Ref{Owner: "x", OwnerID: layout.VolumeID{9}, Chunk: layout.NameChunk(2, data)})
	for name, key := range map[string]string{
		"wrong bytes":    r.ChunkKey(2, layout.Sum(block(4))),
		"other volume":   other,
		"not layout key": "chunks/e1/c2/n-1",
	} {
		if err := r.Put(ctx, key, data); !errors.Is(err, storage.ErrChunkKeyInvalid) {
			t.Errorf("put with %s: %v; want ErrChunkKeyInvalid", name, err)
		}
	}
	if h := lh.head(); h.Activity.Kind != layout.ActIdle {
		t.Fatalf("a refused put opened a session: %+v", h.Activity)
	}
	if err := r.Put(ctx, good, data); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Get(ctx, good); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("get %s: %v", good, err)
	}
	back, err := ParseLayoutKey(good)
	if err != nil || LayoutKey(back) != good {
		t.Fatalf("key round trip: %v", err)
	}
}
