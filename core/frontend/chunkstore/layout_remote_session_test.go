package chunkstore

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func headActivity(t *testing.T, lh *layoutHost) layout.Activity {
	t.Helper()
	h, _, err := lh.cat.ReadHead(context.Background(), lh.v)
	if err != nil {
		t.Fatal(err)
	}
	return h.Activity
}

// The engine's Abort never blocks and writes nothing; the session it gave
// up is ended at the remote's next HEAD read (the next cut's, or a
// scheduler tick's), so HEAD is never left in snap(s) for long.
func TestLayoutRemote_AbortEndsTheSessionAtTheNextHeadRead(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if act := headActivity(t, lh); act.Kind != layout.ActSnap || act.Session != s {
		t.Fatalf("after Begin: HEAD %+v, want snap(%s)", act, s)
	}
	r.Abort()
	if act := headActivity(t, lh); act.Kind != layout.ActSnap || act.Session != s {
		t.Fatalf("Abort wrote HEAD (%+v): it must only take the notice", act)
	}
	if _, err := r.GetHead(ctx); err != nil {
		t.Fatal(err)
	}
	if act := headActivity(t, lh); act.Kind != layout.ActIdle {
		t.Fatalf("after the next HEAD read: HEAD %+v, want the given-up session ended", act)
	}
}

// Abort, and the HEAD read that follows it, never end a session that is
// not this remote's own (epoch, holder and claim id): a newer holder's
// open session survives a deposed remote's Abort.
func TestLayoutRemote_AbortNeverEndsAnotherHoldersSession(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	old, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Begin(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ := lh.cat.Store(lh.v)
	newer := layout.HolderID{Epoch: 2, Holder: "b", ClaimID: "k-b"}
	if _, err := layout.Start(ctx, st, okClaimLease{}, layout.HolderID{Epoch: 2, Holder: "b"}, layout.LocalRoot{Geometry: layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 8 * 4096}, ClaimID: "k-b", Fresh: true, CanStartFrom: anyStart}); err != nil {
		t.Fatal(err)
	}
	if _, err := layout.RunBegin(ctx, st, newer, "s-b"); err != nil {
		t.Fatal(err)
	}
	old.Abort()
	_, _ = old.GetHead(ctx) // the deposed remote's next read
	if act := headActivity(t, lh); act.Kind != layout.ActSnap || act.Session != "s-b" {
		t.Fatalf("HEAD after the deposed remote's Abort and read: %+v, want the newer holder's snap(s-b)", act)
	}
}

// The bound on a given-up session with no cut after it: the next cleaning
// tick ends it before it cleans (no Begin, no cut in between), so HEAD
// leaves snap(s) at the next cut, cleaning tick or stop, whichever comes
// first.
func TestLayoutRemote_AbortedSessionEndsAtTheNextCleaningTick(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	r, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	s, err := r.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r.Abort()
	if act := headActivity(t, lh); act.Kind != layout.ActSnap || act.Session != s {
		t.Fatalf("after Abort: HEAD %+v, want the given-up snap(%s) still there", act, s)
	}
	ran := false
	if err := r.Clean(ctx, func(context.Context, layout.HolderID, string) error {
		ran = true
		if act := headActivity(t, lh); act.Kind != layout.ActClean {
			t.Fatalf("while cleaning: HEAD %+v, want the session ended and clean open", act)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if act := headActivity(t, lh); !ran || act.Kind != layout.ActIdle {
		t.Fatalf("after the cleaning tick: ran=%t HEAD %+v, want idle", ran, act)
	}
}

// countingRemote counts the engine's uploads and the commits the kernel
// refuses as not prepared (uploads of an ended session).
type countingRemote struct {
	*LayoutRemote
	// mu guards the counts: the chunk layer calls Put from parallel upload
	// workers.
	mu                sync.Mutex
	puts, notPrepared int
	again             []string // keys uploaded more than once
	seen              map[string]bool
}

func (c *countingRemote) Put(ctx context.Context, key string, data []byte) error {
	c.mu.Lock()
	c.puts++
	if c.seen == nil {
		c.seen = map[string]bool{}
	}
	if c.seen[key] {
		c.again = append(c.again, key)
	}
	c.seen[key] = true
	c.mu.Unlock()
	return c.LayoutRemote.Put(ctx, key, data)
}

// uploads is the number of Puts so far.
func (c *countingRemote) uploads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.puts
}

func (c *countingRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	gen, err := c.LayoutRemote.CASHead(ctx, expected, next)
	if errors.Is(err, layout.ErrNotPrepared) {
		c.mu.Lock()
		c.notPrepared++
		c.mu.Unlock()
	}
	return gen, err
}

// A cut refused by admission after its uploads keeps them and their
// session; the retry commits them in that session: no commit is refused as
// not prepared and nothing is uploaded again (pre-copy converges under
// sustained writes).
func TestLayoutRemote_AdmissionRefusalKeepsTheSessionSoTheRetryConverges(t *testing.T) {
	ctx := context.Background()
	lh := newLayoutHost(t)
	s, err := storage.CreateWALStoreWithOptions(filepath.Join(lh.dir, "adm"), lrBlocks, 4096, storage.WALStoreOptions{ChunkBlocks: lrChunkBlocks})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	lr, err := NewLayoutRemote(lh.cat, lh.v, lh.me)
	if err != nil {
		t.Fatal(err)
	}
	r := &countingRemote{LayoutRemote: lr}
	refuse := true
	l, err := storage.AttachChunkLayer(s, r, storage.ChunkLayerConfig{ChunkBlocks: lrChunkBlocks, Epoch: lh.me.Epoch, Holder: lh.me.Holder, FreshFromHead: true,
		CaptureAllowed: func() error {
			if refuse && r.uploads() > 0 { // refused after the uploads, before the commit
				return errors.New("demoted")
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	writeSync(t, s, map[uint32][]byte{1: block(1), 9: block(9), 17: block(17)})
	if _, err := l.Cut(ctx); err == nil {
		t.Fatal("cut refused by admission returned success")
	}
	uploaded := r.puts
	if uploaded == 0 {
		t.Fatal("control: the refusal came before any upload")
	}
	if act := headActivity(t, lh); act.Kind != layout.ActSnap {
		t.Fatalf("after the refusal: HEAD %+v, want the cut's session still open", act)
	}
	refuse = false
	if _, err := l.Cut(ctx); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if r.notPrepared != 0 || len(r.again) != 0 {
		t.Fatalf("retry: %d commits refused as not prepared, uploaded again %q; want 0 and none", r.notPrepared, r.again)
	}
	if act := headActivity(t, lh); act.Kind != layout.ActIdle {
		t.Fatalf("after the retry: HEAD %+v, want the commit to end the session", act)
	}
}
