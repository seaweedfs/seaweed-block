package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	ff  *fakeFiler
}

func newLayoutHost(t *testing.T) *layoutHost {
	t.Helper()
	ctx := context.Background()
	ff := newFakeFiler()
	cat, err := NewBCatalog(ff, newFakeBlobs(), "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	v := newRef(t, "v")
	if _, err := cat.CreateVolume(ctx, v, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 8 * 4096}); err != nil {
		t.Fatal(err)
	}
	s, _ := cat.Store(v)
	me := layout.HolderID{Epoch: 1, Holder: "a", ClaimID: "k-a"}
	if _, err := layout.Start(ctx, s, okClaimLease{}, layout.HolderID{Epoch: 1, Holder: "a"}, layout.LocalRoot{Geometry: layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 8 * 4096}, ClaimID: "k-a", Fresh: true, CanStartFrom: anyStart}); err != nil {
		t.Fatal(err)
	}
	return &layoutHost{t: t, cat: cat, v: v, me: me, dir: t.TempDir(), ff: ff}
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
	if _, err := r.Begin(ctx); err != nil {
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

// The seam's closure table: every kernel result maps to one engine action.
func TestLayoutRemote_ErrorMap(t *testing.T) {
	other := errors.New("connection refused")
	for _, c := range []struct {
		err  error
		want error // nil: returned as it is
	}{
		{layout.ErrNotHolder, storage.ErrChunkLayerFenced},
		{layout.ErrStaleEpoch, storage.ErrChunkLayerFenced},
		{layout.ErrDetached, storage.ErrChunkLayerFenced},
		{layout.ErrPrecondition, storage.ErrChunkHeadPrecondition},
		{layout.ErrNoSession, storage.ErrChunkHeadPrecondition},
		{layout.ErrVolumeDeleting, storage.ErrChunkHeadDeleted},
		{layout.ErrBusy, storage.ErrChunkRemoteUnavailable},
		{other, storage.ErrChunkRemoteUnavailable},
		{layout.ErrCorrupt, ErrRemoteDamaged},
		{layout.ErrIdentity, ErrRemoteDamaged},
		{layout.ErrMissing, ErrRemoteDamaged},
		{layout.ErrNotPrepared, nil},
		{layout.ErrTooManySnapshots, nil},
		{layout.ErrSnapshotExists, nil},
	} {
		got := layoutRemoteErr(c.err)
		if !errors.Is(got, c.err) {
			t.Errorf("%v: mapped to %v, which no longer wraps it", c.err, got)
		}
		if c.want != nil && !errors.Is(got, c.want) {
			t.Errorf("%v: mapped to %v; want %v", c.err, got, c.want)
		}
		for _, cls := range []error{storage.ErrChunkLayerFenced, storage.ErrChunkHeadPrecondition, storage.ErrChunkHeadDeleted, storage.ErrChunkRemoteUnavailable, ErrRemoteDamaged} {
			if cls != c.want && errors.Is(got, cls) {
				t.Errorf("%v: also maps to %v", c.err, cls)
			}
		}
	}
	if layoutRemoteErr(nil) != nil {
		t.Fatal("nil mapped to an error")
	}
}

// lostReplyStore applies the next HEAD update and then reports an error,
// as a store whose reply was lost.
type lostReplyStore struct {
	sessionStore
	armed bool
}

func (s *lostReplyStore) UpdateHead(ctx context.Context, gen uint64, next layout.Head) error {
	err := s.sessionStore.UpdateHead(ctx, gen, next)
	if err == nil && s.armed && next.Activity.Kind == layout.ActIdle {
		s.armed = false
		return errors.New("reply lost")
	}
	return err
}

// A commit that applied and lost its reply leaves no stale session: the
// engine's next HEAD read settles it, and the next cut commits.
func TestLayoutRemote_ACommitWhoseReplyIsLostDoesNotLivelock(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	defer s.Close()
	lost := &lostReplyStore{sessionStore: r.st, armed: true}
	r.st = lost
	want := map[uint32][]byte{3: block(3)}
	writeSync(t, s, want)
	_, _ = l.Capture(ctx) // the commit applied, its reply lost
	if lost.armed {
		t.Fatal("the fault never fired")
	}
	want[30] = block(30)
	writeSync(t, s, map[uint32][]byte{30: want[30]})
	if _, err := l.Capture(ctx); err != nil {
		t.Fatalf("the cut after a lost commit reply: %v", err)
	}
	if h := lh.head(); h.Activity.Kind != layout.ActIdle {
		t.Fatalf("HEAD after the second cut: %+v", h.Activity)
	}
	_ = s.Close()
	lh.readBack("b", want)
}

// Only the full encoding reaches the layout: any other is refused before a
// commit is written.
func TestLayoutRemote_RefusesOtherEncodings(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	h0, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := block(5)
	key := r.ChunkKey(0, layout.Sum(data))
	if err := r.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	next := storage.ChunkHead{Epoch: 1, RemoteLSN: 1, Chunks: map[uint64]storage.ChunkRef{0: {Key: key, SHA256: layout.Sum(data), Encoding: "zstd"}}}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h0.Gen, Epoch: 1}, next); !errors.Is(err, ErrUnsupportedEncoding) {
		t.Fatalf("commit of a zstd chunk: %v; want ErrUnsupportedEncoding", err)
	}
	if h := lh.head(); h.Current != nil {
		t.Fatal("a refused encoding committed a root")
	}
}

// A snapshot name the kernel refuses (taken, here) does not stop the cut:
// it commits unnamed and the request gets the typed refusal.
func TestLayoutRemote_ARefusedSnapshotNameStillCommitsTheCut(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	defer s.Close()
	writeSync(t, s, map[uint32][]byte{1: block(1)})
	r.SetSnapshot("daily", layout.SnapshotMeta{CreatedNs: 1})
	if _, err := l.Capture(ctx); err != nil || r.TakeSnapshotErr() != nil {
		t.Fatal(err)
	}
	writeSync(t, s, map[uint32][]byte{2: block(2)})
	r.SetSnapshot("daily", layout.SnapshotMeta{CreatedNs: 2})
	h2, err := l.Capture(ctx)
	if err != nil {
		t.Fatalf("cut with a taken snapshot name: %v", err)
	}
	if err := r.TakeSnapshotErr(); !errors.Is(err, layout.ErrSnapshotExists) {
		t.Fatalf("snapshot refusal: %v; want ErrSnapshotExists", err)
	}
	if h := lh.head(); h.Position != h2 || h.Snapshots["daily"].Meta.CreatedNs != 1 {
		t.Fatalf("HEAD after the refused name: position %d, daily %+v", h.Position, h.Snapshots["daily"])
	}
}

// A cut while this process's own cleaning is open ends the cleaning and
// commits (never busy); a cleaning left open by a dead process is ended by
// the next start's first call.
func TestLayoutRemote_ACutEndsMyOwnCleaning(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	defer s.Close()
	started := make(chan struct{})
	cleaned := make(chan error, 1)
	go func() {
		cleaned <- r.Clean(ctx, func(ctx context.Context, _ layout.HolderID, _ string) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	<-started
	writeSync(t, s, map[uint32][]byte{4: block(4)})
	if _, err := l.Capture(ctx); err != nil {
		t.Fatalf("cut during my own cleaning: %v", err)
	}
	if err := <-cleaned; err != nil {
		t.Fatalf("the cancelled cleaning: %v", err)
	}
	// A dead process's cleaning: HEAD clean(c) of mine, nothing in memory.
	st, _ := lh.cat.Store(lh.v)
	if _, err := layout.RunBeginClean(ctx, st, lh.me, "c-dead"); err != nil {
		t.Fatal(err)
	}
	r2, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r2.GetHead(ctx); err != nil {
		t.Fatal(err)
	}
	if h := lh.head(); h.Activity.Kind != layout.ActIdle {
		t.Fatalf("a dead cleaning after the restart's first call: %+v", h.Activity)
	}
}

// failingCreateStore fails object creates while armed: an upload error.
type failingCreateStore struct {
	sessionStore
	armed bool
}

func (s *failingCreateStore) Create(ctx context.Context, rel string, data []byte, session string) error {
	if s.armed {
		return errors.New("volume server unreachable")
	}
	return s.sessionStore.Create(ctx, rel, data, session)
}

// A named cut the engine gives up (an upload fails) leaves no name behind:
// the next, unnamed cut commits without it, from a new session.
func TestLayoutRemote_AnAbandonedNamedCutDropsItsName(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	defer s.Close()
	fail := &failingCreateStore{sessionStore: r.st, armed: true}
	r.st = fail
	writeSync(t, s, map[uint32][]byte{5: block(5)})
	r.SetSnapshot("weekly", layout.SnapshotMeta{CreatedNs: 1})
	if _, err := l.Capture(ctx); err == nil {
		t.Fatal("a cut whose upload failed committed")
	}
	// The failed cut keeps its session for the retry; the snapshot request
	// that failed withdraws its name (SnapshotVolume does), so the retry
	// names nothing.
	r.DropSnapshot()
	fail.armed = false
	if _, err := l.Capture(ctx); err != nil {
		t.Fatalf("the cut after the abandoned one: %v", err)
	}
	h := lh.head()
	if _, named := h.Snapshots["weekly"]; named || h.Activity.Kind != layout.ActIdle {
		t.Fatalf("after the abandoned named cut: snapshots %v, activity %+v; want no weekly, idle", h.Snapshots, h.Activity)
	}
}

// My session ended behind this remote's back: the remote drops it, the
// engine re-reads HEAD after one refused cut, and the next cut begins a
// new session and commits (no cut is refused for ever).
func TestLayoutRemote_ASessionLostWhileAliveIsReplaced(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	defer s.Close()
	want := map[uint32][]byte{6: block(6)}
	writeSync(t, s, want)
	if _, err := r.Begin(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ := lh.cat.Store(lh.v)
	if _, err := layout.RunAbort(ctx, st, lh.me, lh.head().Activity.Session); err != nil {
		t.Fatal(err)
	}
	// The abort is a write the engine did not see: its first cut after it
	// is a precondition (the engine re-reads HEAD); the next commits.
	if _, err := l.Capture(ctx); err != nil && !errors.Is(err, storage.ErrChunkHeadPrecondition) {
		t.Fatalf("first cut after my session was aborted behind my back: %v", err)
	}
	if _, err := l.Capture(ctx); err != nil {
		t.Fatalf("cut after the re-read: %v", err)
	}
	if h := lh.head(); h.Activity.Kind != layout.ActIdle || h.Current == nil {
		t.Fatalf("HEAD after the cut: %+v", h)
	}
	_ = s.Close()
	lh.readBack("b", want)
}

// Uploads staged in a session that has since ended (rolled back by this
// remote) are not prepared for the next session's commit: the commit is a
// precondition, the engine's re-read drops what it staged, and the retry
// uploads again and commits.
func TestLayoutRemote_StagedUploadsOfAnEndedSessionAreRedone(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	h0, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := block(8)
	key := r.ChunkKey(0, layout.Sum(data))
	if err := r.Put(ctx, key, data); err != nil { // staged in session s1
		t.Fatal(err)
	}
	// s1 ends (the cut was abandoned); a new session opens on the next call.
	r.Abort()
	next := storage.ChunkHead{Epoch: 1, RemoteLSN: 1, Chunks: map[uint64]storage.ChunkRef{0: {Key: key, SHA256: layout.Sum(data), Encoding: "full"}}}
	_, err = r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h0.Gen, Epoch: 1}, next)
	if !errors.Is(err, storage.ErrChunkHeadPrecondition) {
		t.Fatalf("commit naming an upload of an ended session: %v; want a precondition", err)
	}
	h1, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h1.Gen == h0.Gen {
		t.Fatal("the engine's re-read sees the same generation: it would keep what it staged")
	}
	if err := r.Put(ctx, key, data); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CASHead(ctx, storage.ChunkHeadVersion{Gen: h1.Gen, Epoch: 1}, next); err != nil {
		t.Fatalf("the retry after uploading again: %v", err)
	}
}

// A snapshot name the commit would refuse for its form (too long, here) is
// refused before the commit, and the cut commits unnamed; a malformed
// manifest is never reported as a name refusal.
func TestLayoutRemote_ABadSnapshotNameIsRefusedBeforeTheCommit(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	defer s.Close()
	writeSync(t, s, map[uint32][]byte{1: block(1)})
	r.SetSnapshot(strings.Repeat("n", layout.MaxSnapshotNameBytes+1), layout.SnapshotMeta{})
	h, err := l.Capture(ctx)
	if err != nil {
		t.Fatalf("cut with a too-long snapshot name: %v", err)
	}
	if err := r.TakeSnapshotErr(); !errors.Is(err, layout.ErrMalformed) {
		t.Fatalf("name refusal: %v; want ErrMalformed", err)
	}
	if hd := lh.head(); hd.Position != h || len(hd.Snapshots) != 0 {
		t.Fatalf("HEAD after the refused name: %+v", hd)
	}
}

// Writes of others that leave the image alone (a snapshot delete, a
// retention pass) do not move the engine's view: the next cut commits
// without a precondition and uploads nothing.
func TestLayoutRemote_AForeignSnapshotDeleteDoesNotMoveTheView(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	defer s.Close()
	writeSync(t, s, map[uint32][]byte{2: block(2)})
	r.SetSnapshot("old", layout.SnapshotMeta{CreatedNs: 1})
	if _, err := l.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lh.cat.DeleteSnapshot(ctx, lh.v, "old"); err != nil {
		t.Fatal(err)
	}
	after, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Gen != before.Gen {
		t.Fatalf("a snapshot delete moved the engine's view: gen %d -> %d", before.Gen, after.Gen)
	}
	writeSync(t, s, map[uint32][]byte{3: block(3)})
	if _, err := l.Capture(ctx); err != nil {
		t.Fatalf("cut after a foreign snapshot delete: %v", err)
	}
}

// After an abandoned cut, a cleaning is not refused busy: it takes the
// abandon notice first.
func TestLayoutRemote_CleaningAfterAnAbandonedCut(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Begin(ctx); err != nil {
		t.Fatal(err)
	}
	r.Abort()
	if err := r.Clean(ctx, func(context.Context, layout.HolderID, string) error { return nil }); err != nil {
		t.Fatalf("cleaning after an abandoned cut: %v", err)
	}
	if h := lh.head(); h.Activity.Kind != layout.ActIdle {
		t.Fatalf("HEAD after the cleaning: %+v", h.Activity)
	}
}

// AbortOpenSession settles a stop: an open session (held here, or left by
// a dead process) is rolled back and its pending name dropped; a detached
// HEAD of this holder reports detached; nothing open reports idle.
func TestLayoutRemote_AbortOpenSession(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := r.AbortOpenSession(ctx); err != nil || got != SessionIdle {
		t.Fatalf("nothing open: %q %v", got, err)
	}
	r.SetSnapshot("x", layout.SnapshotMeta{})
	if _, err := r.Begin(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := r.AbortOpenSession(ctx); err != nil || got != SessionAborted || lh.head().Activity.Kind != layout.ActIdle {
		t.Fatalf("own open session: %q %v, HEAD %+v", got, err, lh.head().Activity)
	}
	if _, err := r.GetHead(ctx); err != nil {
		t.Fatal(err)
	}
	if r.pending != nil || r.session != "" {
		t.Fatal("the aborted session or its pending name is still held after the next call")
	}
	// A dead process's session.
	st, _ := lh.cat.Store(lh.v)
	if _, err := layout.RunBegin(ctx, st, lh.me, "s-dead"); err != nil {
		t.Fatal(err)
	}
	if got, err := r.AbortOpenSession(ctx); err != nil || got != SessionAborted {
		t.Fatalf("a dead process's session: %q %v", got, err)
	}
	// A detached HEAD of this holder.
	s, l, _ := lh.attach("a", true, true)
	defer s.Close()
	writeSync(t, s, map[uint32][]byte{1: block(1)})
	if _, err := l.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	h := lh.head()
	if _, err := layout.RunBegin(ctx, st, lh.me, "s-stop"); err != nil {
		t.Fatal(err)
	}
	{
		// The stop's final commit: the current image, detached.
		m := map[uint64]layout.Ref{}
		img, _ := layout.NewReader("/blocks", lh.v.Dir(), st.Fetch)
		im, _ := img.Open(ctx, *h.Current)
		for _, sh := range im.Shards() {
			seg, _, _ := im.Segment(ctx, sh)
			for i, ref := range seg.Chunks {
				m[i] = ref
			}
		}
		root, segs, _ := layout.Build(m)
		rn, rb, _ := layout.NameRoot(root)
		for n, b := range segs {
			_ = layout.EnsureObject(ctx, st, n.Path(), b, "s-stop")
		}
		_ = layout.EnsureObject(ctx, st, rn.Path(), rb, "s-stop")
		if _, err := layout.RunCommit(ctx, st, st, lh.v.Name, lh.me, layout.CommitRequest{Session: "s-stop", Prev: m, Next: m, Position: h.Position, State: layout.StateDetached}); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := r.AbortOpenSession(ctx); err != nil || got != SessionDetached {
		t.Fatalf("a detached HEAD: %q %v", got, err)
	}
}

// blockingCommitStore holds the next commit (an idle HEAD update) inside
// its update until released.
type blockingCommitStore struct {
	sessionStore
	entered, release chan struct{}
}

func (s *blockingCommitStore) UpdateHead(ctx context.Context, gen uint64, next layout.Head) error {
	if next.Activity.Kind == layout.ActIdle && s.entered != nil {
		close(s.entered)
		s.entered = nil
		<-s.release
	}
	return s.sessionStore.UpdateHead(ctx, gen, next)
}

// The abort never waits behind the commit it aborts: while a commit is held
// inside its HEAD update, AbortOpenSession completes (aborted), and the
// commit then fails its condition.
func TestLayoutRemote_AbortOpenSessionDoesNotWaitForAHeldCommit(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, l, r := lh.attach("a", true, true)
	defer s.Close()
	hold := &blockingCommitStore{sessionStore: r.st, entered: make(chan struct{}), release: make(chan struct{})}
	entered := hold.entered
	r.st = hold
	writeSync(t, s, map[uint32][]byte{7: block(7)})
	cut := make(chan error, 1)
	go func() { _, err := l.Capture(ctx); cut <- err }()
	<-entered
	done := make(chan struct{})
	var got string
	var aerr error
	go func() { got, aerr = r.AbortOpenSession(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(hold.release)
		t.Fatal("AbortOpenSession waited behind the held commit")
	}
	if aerr != nil || got != SessionAborted {
		t.Fatalf("abort during a held commit: %q %v; want aborted", got, aerr)
	}
	close(hold.release)
	if err := <-cut; err == nil {
		t.Fatal("the held commit applied after its session was aborted")
	}
	if h := lh.head(); h.Activity.Kind != layout.ActIdle {
		t.Fatalf("HEAD after the abort: %+v", h.Activity)
	}
}

// A HEAD of another holder, epoch or claim id is a fence for the stop's
// settle, never idle (which would let it resume).
func TestLayoutRemote_AbortOpenSessionFencesAForeignHead(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	for name, me := range map[string]layout.HolderID{
		"another holder":   {Epoch: 1, Holder: "b", ClaimID: "k-a"},
		"another epoch":    {Epoch: 2, Holder: "a", ClaimID: "k-a"},
		"another claim id": {Epoch: 1, Holder: "a", ClaimID: "k-other"},
	} {
		r, err := NewLayoutRemote(lh.cat, lh.v, me)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := r.AbortOpenSession(ctx); !errors.Is(err, storage.ErrChunkLayerFenced) {
			t.Errorf("%s: %q %v; want ErrChunkLayerFenced", name, got, err)
		}
	}
}

// A layout key has exactly one spelling per reference: a key that does
// not round-trip, or names no volume, is refused.
func TestLayoutRemote_KeysAreCanonical(t *testing.T) {
	data := block(1)
	id := layout.VolumeID{0xab, 0xcd}
	good := LayoutKey(layout.Ref{Owner: "v", OwnerID: id, Chunk: layout.NameChunk(7, data)})
	ref, err := ParseLayoutKey(good)
	if err != nil || LayoutKey(ref) != good {
		t.Fatalf("canonical key %q: %+v %v", good, ref, err)
	}
	upper := "layout:v:" + strings.ToUpper(id.String()) + ":" + layout.NameChunk(7, data).String()
	padded := strings.Replace(good, ":7-", ":07-", 1)
	zero := LayoutKey(layout.Ref{Owner: "v", Chunk: layout.NameChunk(7, data)})
	noOwner := "layout::" + id.String() + ":" + layout.NameChunk(7, data).String()
	for name, k := range map[string]string{"uppercase id": upper, "padded index": padded, "zero owner id": zero, "empty owner": noOwner} {
		if k == good {
			t.Fatalf("%s: the fixture did not change the key", name)
		}
		if _, err := ParseLayoutKey(k); !errors.Is(err, storage.ErrChunkKeyInvalid) {
			t.Errorf("%s %q: %v; want ErrChunkKeyInvalid", name, k, err)
		}
	}
}
